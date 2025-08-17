import asyncio
import asyncpg
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
        await page.close()

    except Exception as e:
        logger.warning(f"❌ Failed: {domain} — {e}")
        # Record failed domain and remove from ingestion queue to prevent retries
        await record_failed_domain(conn, domain, str(e))
    return True


async def main():
    conn = await asyncpg.connect(DB_URL)
    async with AsyncCamoufox(headless=True) as browser:
        # Log the crawl limit configuration
        if CRAWL_LIMIT == -1:
            logger.info("Starting crawler with infinite loop (CRAWL_LIMIT=-1)")
        else:
            logger.info(f"Starting crawler with limit of {CRAWL_LIMIT} domains")
        
        crawl_count = 0
        while True:
            more = await crawl_one(conn, browser)
            if not more:
                logger.info("🎉 All domains processed.")
                break
            
            crawl_count += 1
            
            # Check if we've reached the limit (unless it's infinite)
            if CRAWL_LIMIT != -1:
                logger.info(f"Completed {crawl_count}/{CRAWL_LIMIT} crawls")
                if crawl_count >= CRAWL_LIMIT:
                    logger.info(f"Reached crawl limit of {CRAWL_LIMIT}")
                    break
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
