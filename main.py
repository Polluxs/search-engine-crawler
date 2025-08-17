import asyncio
import asyncpg
from camoufox.async_api import AsyncCamoufox
from dotenv import load_dotenv
import os

from src.logging_config import logger
from src.database import claim_one_domain, insert_domain, delete_domain_ingestion
from src.analysis import extract_page_data, try_about_page, analyze_domain_with_llm

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")


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
        await page.goto(main_url, wait_until="domcontentloaded", timeout=15000)
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
    return True


async def main():
    conn = await asyncpg.connect(DB_URL)
    async with AsyncCamoufox(headless=True) as browser:
        if ENVIRONMENT == "production":
            # Production: infinite loop
            while True:
                more = await crawl_one(conn, browser)
                if not more:
                    logger.info("🎉 All domains processed.")
                    break
        else:
            # Development: loop only 10 times
            for i in range(10):
                more = await crawl_one(conn, browser)
                if not more:
                    logger.info("🎉 All domains processed.")
                    break
                logger.info(f"Non production mode: completed {i+1}/10 crawls")
            logger.info("Development mode: finished 10 crawls")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
