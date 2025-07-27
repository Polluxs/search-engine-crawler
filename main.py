import asyncio
import logging
import uuid
import asyncpg
from camoufox.async_api import AsyncCamoufox
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")

logging.basicConfig(level=logging.INFO)

async def claim_one_domain(conn):
    """Claim and lock the next available domain for crawling."""
    row = await conn.fetchrow("""
        WITH next AS (
            SELECT domain_name_text, public_suffix_text
            FROM domain_ingestion
            WHERE locked_at_ts IS NULL
            ORDER BY discovered_at_ts
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE domain_ingestion
        SET locked_at_ts = now()
        FROM next
        WHERE domain_ingestion.domain_name_text = next.domain_name_text
        RETURNING domain_ingestion.domain_name_text, domain_ingestion.public_suffix_text;
    """)
    return row

async def analyze_page(page):
    """Basic placeholder summary logic."""
    await page.wait_for_selector("body", timeout=10000)
    title = await page.title()
    return f"Title: {title}"

async def insert_domain_record(conn, domain_name, summary):
    """Insert analyzed data into the domain table."""
    domain_id = uuid.uuid5(uuid.NAMESPACE_URL, domain_name)
    await conn.execute("""
        INSERT INTO domain (
            domain_id_uuid,
            domain_name_text,
            semantic_summary_text,
            crawl_first_seen_at_ts,
            crawl_last_attempt_at_ts,
            crawl_status_text,
            audit_created_at_ts,
            audit_updated_at_ts
        )
        VALUES ($1, $2, $3, now(), now(), 'success', now(), now())
        ON CONFLICT (domain_name_text) DO NOTHING;
    """, domain_id, domain_name, summary)

async def crawl_one(conn, browser):
    row = await claim_one_domain(conn)
    if not row:
        return False  # No more domains

    domain = row["domain_name_text"]
    url = f"https://{domain}"
    logging.info(f"Crawling {url}")

    try:
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        summary = await analyze_page(page)
        await insert_domain_record(conn, domain, summary)
        logging.info(f"✅ Inserted: {domain}")
        await page.close()
    except Exception as e:
        logging.warning(f"❌ Failed: {domain} — {e}")
    return True

async def main():
    conn = await asyncpg.connect(DB_URL)
    async with AsyncCamoufox(headless=True) as browser:
        while True:
            more = await crawl_one(conn, browser)
            if not more:
                logging.info("🎉 All domains processed.")
                break
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
