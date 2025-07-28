import asyncio
import logging
import uuid
import asyncpg
import re
import json
from camoufox.async_api import AsyncCamoufox
from dotenv import load_dotenv
import os
import spacy
from openai import AsyncOpenAI

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

class CustomRailwayLogFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage()
        }
        return json.dumps(log_record)

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    handler = logging.StreamHandler()
    formatter = CustomRailwayLogFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# Initialize Railway-compatible logger
logger = get_logger()

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

# Initialize OpenAI client
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

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

def extract_important_tokens(text, max_tokens=500):
    """Extract and prioritize important tokens using spaCy with junk filtering."""
    doc = nlp(text)

    # Custom junk words to ignore (UI scaffolding, marketing, filler)
    JUNK_WORDS = {
        "account", "login", "signup", "subscribe", "sign", "register", "create", "click",
        "platform", "solution", "experience", "support", "discount", "offers", "order",
        "shop", "app", "center", "categories", "policy", "privacy", "help", "b2b", "search",
        "value", "promotion", "delivery", "products", "production", "contact", "username",
        "password", "terms", "conditions", "newsletter", "settings", "mobile", "website",
        "visit", "start", "email"
    }

    # Priority 1: Named entities (excluding numeric junk)
    entities = [
        ent.text.strip()
        for ent in doc.ents
        if len(ent.text.strip()) > 2 and not any(char.isdigit() for char in ent.text)
    ]

    # Priority 2a: Noun phrases (filtered and meaningful)
    noun_phrases = [
        chunk.text.strip().lower()
        for chunk in doc.noun_chunks
        if (
            len(chunk.text.split()) > 1 and
            len(chunk.text) > 3 and
            not any(tok.lemma_.lower() in JUNK_WORDS for tok in chunk)
        )
    ]

    # Priority 2b: Important nouns and proper nouns
    important_nouns = [
        token.lemma_.lower()
        for token in doc
        if (
            token.pos_ in ["NOUN", "PROPN"] and
            not token.is_stop and
            not token.is_punct and
            len(token.text) > 2 and
            token.lemma_.lower() not in JUNK_WORDS
        )
    ]

    # Priority 3: Adjectives and verbs (meaningful ones only)
    descriptors = [
        token.lemma_.lower()
        for token in doc
        if (
            token.pos_ in ["ADJ", "VERB"] and
            not token.is_stop and
            not token.is_punct and
            len(token.text) > 3 and
            token.lemma_.lower() not in JUNK_WORDS
        )
    ]

    # Combine and deduplicate while preserving order
    all_tokens = (
        entities[:20] +
        noun_phrases[:30] +
        list(dict.fromkeys(important_nouns))[:40] +
        list(dict.fromkeys(descriptors))[:20]
    )

    seen = set()
    unique_tokens = []
    for token in all_tokens:
        norm = token.lower().strip()
        if norm not in seen and len(norm) > 2:
            seen.add(norm)
            unique_tokens.append(token.strip())

    return unique_tokens[:max_tokens]

def detect_has_comments(page_html):
    """Detect if the page has a comment system."""
    comment_indicators = [
        "comment", "reply", "discuss", "disqus", "livefyre", 
        "facebook comment", "commento", "utterances"
    ]
    html_lower = page_html.lower()
    return any(indicator in html_lower for indicator in comment_indicators)

async def analyze_with_llm(title, cleaned_text, url, has_comments):
    """Use OpenAI LLM to analyze page content and return structured semantic data."""
    try:
        # Create prompt for LLM analysis
        prompt = f"""Analyze this website content and provide semantic classification in valid JSON format.

WEBSITE DATA:
Title: {title}
URL: {url}
Content: {cleaned_text[:2000]}  # Limit content to stay within token limits
Has Comments: {has_comments}

Please analyze this content and return a JSON object with the following fields:

{{
  "semantic_content_type_text": "blog|forum|docs|ecommerce|news|portfolio|corporate|personal|marketplace|landing|other",
  "semantic_primary_topic_text": "main topic in 1-2 words (e.g., 'technology', 'art', 'fitness')",
  "semantic_keywords_text_array": ["5-10 most relevant keywords/phrases"],
  "semantic_language_primary_text": "language code (e.g., 'en', 'es', 'fr')",
  "semantic_communication_goal_text": "sell|teach|inform|share|entertain|rant|advertise",
  "semantic_author_type_text": "individual|company|organization|government|unknown",
  "semantic_audience_type_text": "general|beginner|expert|professional|consumer|developer",
  "semantic_content_vibe_text": "professional|casual|academic|commercial|personal|technical|creative",
  "semantic_is_commercial_bool": true/false,
  "semantic_is_spammy_bool": true/false,
  "semantic_is_politically_loaded_bool": true/false,
  "semantic_quality_score_float": 0.0-1.0,
  "semantic_has_comments_bool": {has_comments}
}}

Rules:
- Be accurate and specific
- Use the exact field names provided
- Return only valid JSON
- Quality score: 0.8+ for high quality, 0.5-0.8 for decent, 0.5- for poor
- Consider the URL structure and domain name in your analysis"""

        response = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert web content analyst. Return only valid JSON as requested."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Parse the LLM response
        try:
            llm_analysis = json.loads(response_text)
            return llm_analysis
        except json.JSONDecodeError:
            # Try to extract JSON from response if LLM added extra text
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                llm_analysis = json.loads(json_match.group())
                return llm_analysis
            else:
                raise ValueError("LLM response is not valid JSON")
                
    except Exception as e:
        logger.warning(f"LLM analysis failed: {e}")
        raise Exception(f"LLM semantic analysis failed: {e}") from e

async def analyze_page(page):
    """Extract and analyze page content for structured semantic analysis."""
    await page.wait_for_selector("body", timeout=10000)

    # Extract basic metadata
    title = await page.title()
    url = page.url

    # Get full HTML for comment detection
    page_html = await page.content()

    # Extract text content from body_text, excluding script/style tags
    body_text = await page.evaluate("""
        () => {
            // Remove script and style elements
            const scripts = document.querySelectorAll('script, style, nav, footer, header');
            scripts.forEach(el => el.remove());
            
            // Get main content areas first
            return document.querySelector('main, [role="main"], .content, .main-content, article');
        }
    """)

    if not body_text or len(body_text.strip()) < 50:
        body_text = await page.evaluate("""
            () => {
                // Remove script and style elements
                const scripts = document.querySelectorAll('script, style, nav, footer, header');
                scripts.forEach(el => el.remove());

                // Fallback to body
                return document.body.innerText;
            }
        """)


    if not body_text or len(body_text.strip()) < 50:
        raise Exception("Body is too short")

    # Clean and normalize text
    cleaned_text = re.sub(r'\s+', ' ', body_text.strip())
    cleaned_text = re.sub(r'[^\w\s\-\.\,\!\?]', ' ', cleaned_text)

    # Extract important tokens for content preparation
    key_tokens = extract_important_tokens(cleaned_text, max_tokens=500)

    # Create clean content for LLM analysis (limit to ~500 tokens)
    summary_tokens = key_tokens
    raw_content_for_llm = " ".join(summary_tokens)

    # Detect comments first (simple rule-based check)
    has_comments = detect_has_comments(page_html)

    # Use LLM for semantic analysis - let exceptions bubble up with details
    return await analyze_with_llm(title, raw_content_for_llm, url, has_comments)


async def insert_domain_record(conn, domain_name, llm_analysis, has_about_page=False):
    """Insert analyzed data into the domain table."""
    domain_id = uuid.uuid5(uuid.NAMESPACE_URL, domain_name)

    # Prepare values with defaults from LLM analysis
    content_type = llm_analysis.get("semantic_content_type_text", "unknown")
    primary_topic = llm_analysis.get("semantic_primary_topic_text", "unknown")
    keywords = llm_analysis.get("semantic_keywords_text_array", [])
    language = llm_analysis.get("semantic_language_primary_text", "en")
    communication_goal = llm_analysis.get("semantic_communication_goal_text", "unknown")
    author_type = llm_analysis.get("semantic_author_type_text", "unknown")
    audience_type = llm_analysis.get("semantic_audience_type_text", "unknown")
    content_vibe = llm_analysis.get("semantic_content_vibe_text", "unknown")
    is_commercial = llm_analysis.get("semantic_is_commercial_bool", False)
    is_spammy = llm_analysis.get("semantic_is_spammy_bool", False)
    is_politically_loaded = llm_analysis.get("semantic_is_politically_loaded_bool", False)
    quality_score = llm_analysis.get("semantic_quality_score_float")
    has_comments = llm_analysis.get("semantic_has_comments_bool", False)
    semantic_summary = f"{content_type} content about {primary_topic}"[:2000]

    await conn.execute("""
        INSERT INTO domain (
            domain_id_uuid,
            domain_name_text,
            semantic_content_type_text,
            semantic_primary_topic_text,
            semantic_keywords_text_array,
            semantic_language_primary_text,
            semantic_communication_goal_text,
            semantic_author_type_text,
            semantic_audience_type_text,
            semantic_content_vibe_text,
            semantic_is_commercial_bool,
            semantic_is_spammy_bool,
            semantic_is_politically_loaded_bool,
            semantic_quality_score_float,
            semantic_has_comments_bool,
            semantic_summary_text,
            crawl_first_seen_at_ts,
            crawl_last_attempt_at_ts,
            crawl_status_text,
            crawl_processed_at_ts,
            crawl_has_about_bool,
            audit_created_at_ts,
            audit_updated_at_ts
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now(), now(), 'success', now(), $17, now(), now())
        ON CONFLICT (domain_name_text) DO UPDATE SET
            semantic_content_type_text = EXCLUDED.semantic_content_type_text,
            semantic_primary_topic_text = EXCLUDED.semantic_primary_topic_text,
            semantic_keywords_text_array = EXCLUDED.semantic_keywords_text_array,
            semantic_language_primary_text = EXCLUDED.semantic_language_primary_text,
            semantic_communication_goal_text = EXCLUDED.semantic_communication_goal_text,
            semantic_author_type_text = EXCLUDED.semantic_author_type_text,
            semantic_audience_type_text = EXCLUDED.semantic_audience_type_text,
            semantic_content_vibe_text = EXCLUDED.semantic_content_vibe_text,
            semantic_is_commercial_bool = EXCLUDED.semantic_is_commercial_bool,
            semantic_is_spammy_bool = EXCLUDED.semantic_is_spammy_bool,
            semantic_is_politically_loaded_bool = EXCLUDED.semantic_is_politically_loaded_bool,
            semantic_quality_score_float = EXCLUDED.semantic_quality_score_float,
            semantic_has_comments_bool = EXCLUDED.semantic_has_comments_bool,
            semantic_summary_text = EXCLUDED.semantic_summary_text,
            crawl_last_attempt_at_ts = now(),
            crawl_processed_at_ts = now(),
            crawl_has_about_bool = EXCLUDED.crawl_has_about_bool,
            audit_updated_at_ts = now();
    """,
    domain_id, domain_name, content_type, primary_topic, keywords, language,
    communication_goal, author_type, audience_type, content_vibe, is_commercial,
    is_spammy, is_politically_loaded, quality_score, has_comments, semantic_summary, has_about_page)
        
async def try_about_page(page, domain):
    """Try to navigate to /about page and check if it exists and has content."""
    about_urls = [
        f"https://{domain}/about",
        f"https://{domain}/about-us",
        f"https://{domain}/about.html"
    ]
    
    for about_url in about_urls:
        try:
            response = await page.goto(about_url, wait_until="domcontentloaded", timeout=10000)
            
            # Check if page loaded successfully (not 404, 403, etc.)
            if response and response.status < 400:
                # Check if page has meaningful content (not redirect to homepage)
                current_url = page.url.lower()
                if "about" in current_url:
                    # Verify there's actual content
                    content_check = await page.evaluate("""
                        () => {
                            const body = document.body;
                            if (!body) return false;
                            const text = body.innerText || '';
                            return text.trim().length > 100; // Has substantial content
                        }
                    """)
                    if content_check:
                        logger.info(f"✅ Found about page: {about_url}")
                        return True
            
        except Exception as e:
            logger.debug(f"About page {about_url} failed: {e}")
            continue
    
    return False

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
        
        await insert_domain_record(conn, domain, analysis_summary, has_about_page)
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
