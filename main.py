import asyncio
import asyncpg
import gc
from camoufox.async_api import AsyncCamoufox
from dotenv import load_dotenv
import os

from src.logging_config import logger
from src.database import claim_one_domain, insert_domain, delete_domain_ingestion, record_failed_domain
from src.analysis import extract_page_data, try_about_page, analyze_domain_with_llm

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")
CRAWL_LIMIT = int(os.getenv("CRAWL_LIMIT", "10"))  # -1 for infinite, default 10


async def crawl_one(conn, browser):
    row = await claim_one_domain(conn)
    if not row:
        return False  # No more domains

    domain = row["domain_name_text"]
    logger.info(f"Crawling {domain}")

    page = None
    try:
        page = await browser.new_page()

        # Always start with homepage analysis
        main_url = f"https://{domain}"
        logger.info(f"Analyzing homepage: {main_url}")
        response = await page.goto(main_url, wait_until="domcontentloaded", timeout=15000)
        
        # Check if homepage returned a successful status code
        if not response or response.status >= 400:
            logger.warning(f"❌ Skipping {domain}: HTTP {response.status if response else 'No response'}")
            raise Exception(f"Homepage returned HTTP {response.status if response else 'No response'}")
        
        homepage_content = await extract_page_data(page)

        # Try to get about page content as supplementary
        about_content = None
        has_about_page = await try_about_page(page, domain)
        if has_about_page:
            logger.info("Found about page, adding supplementary content")
            about_content = await extract_page_data(page, max_content_tokens=100)

        # Perform LLM analysis with homepage as primary
        analysis_summary = await analyze_domain_with_llm(
            domain, homepage_content, about_content
        )

        await insert_domain(conn, domain, analysis_summary, has_about_page)
        await delete_domain_ingestion(conn, domain)
        logger.info(f"✅ Inserted: {domain} (about_page: {has_about_page})")

    except Exception as e:
        logger.info(f"❌ Failed: {domain} — {e}")
        # Record failed domain and remove from ingestion queue to prevent retries
        await record_failed_domain(conn, domain, str(e))
    finally:
        # Always close the page to prevent memory leaks
        if page:
            try:
                await page.close()
                logger.debug(f"Page closed for {domain}")
            except Exception as close_error:
                logger.error(f"Failed to close page for {domain}: {close_error}")
    return True


async def crawl_batch(conn, batch_size, current_count, limit):
    """Crawl a batch of domains with a fresh browser context."""
    async with AsyncCamoufox(headless=True) as browser:
        for i in range(batch_size):
            more = await crawl_one(conn, browser)
            if not more:
                return False, i  # No more domains, return count
            
            current_count += 1
            
            # Show progress for each crawl
            if limit != -1:
                logger.info(f"Completed {current_count}/{limit} crawls")
            else:
                logger.info(f"Completed {current_count} crawls")
                
            # Stop if we've reached the limit
            if limit != -1 and current_count >= limit:
                return False, i + 1  # Stop crawling, return actual count
                
        return True, batch_size  # More domains available


async def main():
    conn = await asyncpg.connect(DB_URL)
    
    # Log the crawl limit configuration
    if CRAWL_LIMIT == -1:
        logger.info("Starting crawler with infinite loop (CRAWL_LIMIT=-1)")
    else:
        logger.info(f"Starting crawler with limit of {CRAWL_LIMIT} domains")
    
    crawl_count = 0
    batch_size = 20  # Recreate browser every 20 crawls to prevent memory leaks
    
    try:
        while True:
            # Crawl a batch with fresh browser context
            more_domains, batch_crawled = await crawl_batch(conn, batch_size, crawl_count, CRAWL_LIMIT)
            crawl_count += batch_crawled
            
            if batch_crawled > 0:
                logger.info(f"Batch complete: {batch_crawled} domains crawled (total: {crawl_count})")
                
                # Force garbage collection after each batch
                gc.collect()
                logger.debug(f"Browser context recycled and garbage collected after {crawl_count} crawls")
            
            if not more_domains:
                logger.info("🎉 All domains processed.")
                break
            
            # Check if we've reached the limit (unless it's infinite)
            if CRAWL_LIMIT != -1 and crawl_count >= CRAWL_LIMIT:
                logger.info(f"Reached crawl limit of {CRAWL_LIMIT}")
                break
                
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
