import asyncio
import asyncpg
from camoufox.async_api import AsyncCamoufox
from dotenv import load_dotenv
import os

from src.logging_config import logger
from src.database import claim_one_domain, insert_domain, delete_domain_ingestion
from src.analysis import analyze_page, try_about_page

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
    
    has_about_page = False
    analysis_summary = None

    try:
        page = await browser.new_page()
        
        # Try /about page first
        has_about_page = await try_about_page(page, domain)
        
        if has_about_page:
            logger.info(f"Analyzing about page for {domain}")
            analysis_summary = await analyze_page(page)
        else:
            # Fallback to main page
            main_url = f"https://{domain}"
            logger.info(f"No about page found, analyzing main page: {main_url}")
            await page.goto(main_url, wait_until="domcontentloaded", timeout=15000)
            analysis_summary = await analyze_page(page)
        
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