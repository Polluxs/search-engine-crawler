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
from langdetect import detect, LangDetectException
from openai import AsyncOpenAI

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO)

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
    """Extract and prioritize important tokens using spaCy."""
    # Process text with spaCy
    doc = nlp(text)
    
    # Collect important tokens with priorities
    important_tokens = []
    
    # Priority 1: Named entities (all types)
    entities = []
    for ent in doc.ents:
        if len(ent.text.strip()) > 2:
            entities.append(ent.text.strip())
    
    # Priority 2: Noun phrases and important nouns
    noun_phrases = []
    important_nouns = []
    for chunk in doc.noun_chunks:
        if len(chunk.text.split()) > 1 and len(chunk.text) > 3:
            noun_phrases.append(chunk.text.strip())
    
    for token in doc:
        if (token.pos_ in ["NOUN", "PROPN"] and 
            not token.is_stop and 
            not token.is_punct and 
            len(token.text) > 2):
            important_nouns.append(token.text.lower())
    
    # Priority 3: Important adjectives and verbs
    descriptors = []
    for token in doc:
        if (token.pos_ in ["ADJ", "VERB"] and 
            not token.is_stop and 
            not token.is_punct and 
            len(token.text) > 3):
            descriptors.append(token.text.lower())
    
    # Combine and deduplicate
    all_tokens = (
        entities[:20] +  # Top entities
        noun_phrases[:30] +  # Top noun phrases
        list(set(important_nouns))[:40] +  # Unique nouns
        list(set(descriptors))[:20]  # Unique descriptors
    )
    
    # Remove duplicates while preserving order
    seen = set()
    unique_tokens = []
    for token in all_tokens:
        if token.lower() not in seen and len(token.strip()) > 0:
            seen.add(token.lower())
            unique_tokens.append(token.strip())
    
    # Truncate to max_tokens
    return unique_tokens[:max_tokens]

def detect_language(text):
    """Detect primary language of text content."""
    try:
        if len(text.strip()) < 20:
            return "unknown"  # Default to English for short text
        detected = detect(text)
        return detected if detected else "en"
    except (LangDetectException, Exception):
        return "unknown"

def analyze_content_type(title, text, url_path=""):
    """Analyze content type based on title, text, and URL patterns."""
    title_lower = title.lower()
    text_lower = text.lower()
    url_lower = url_path.lower()
    
    # Check for specific patterns
    if any(word in title_lower or word in text_lower for word in ["blog", "post", "article", "diary"]):
        return "blog"
    elif any(word in title_lower or word in text_lower for word in ["forum", "discussion", "thread", "reply"]):
        return "forum"
    elif any(word in title_lower or word in text_lower for word in ["documentation", "docs", "guide", "tutorial", "manual"]):
        return "docs"
    elif any(word in title_lower or word in text_lower for word in ["shop", "store", "buy", "price", "cart", "checkout"]):
        return "ecommerce"
    elif any(word in title_lower or word in text_lower for word in ["news", "breaking", "report", "journalist"]):
        return "news"
    elif any(word in title_lower or word in text_lower for word in ["portfolio", "gallery", "showcase", "work"]):
        return "portfolio"
    elif any(word in title_lower or word in text_lower for word in ["company", "business", "service", "about us"]):
        return "corporate"
    elif any(word in title_lower or word in text_lower for word in ["personal", "about me", "my story"]):
        return "personal"
    else:
        return "general"

def detect_commercial_intent(title, text):
    """Detect if the site has commercial intent."""
    combined_text = (title + " " + text).lower()
    commercial_indicators = [
        "buy", "purchase", "price", "sale", "discount", "shop", "store",
        "cart", "checkout", "payment", "order", "shipping", "product",
        "subscribe", "premium", "upgrade", "contact us", "hire", "service"
    ]
    return any(word in combined_text for word in commercial_indicators)

def detect_communication_goal(title, text, content_type):
    """Determine the primary communication goal."""
    combined_text = (title + " " + text).lower()
    
    if content_type in ["ecommerce", "corporate"]:
        return "sell"
    elif any(word in combined_text for word in ["tutorial", "guide", "how to", "learn", "course"]):
        return "teach"
    elif any(word in combined_text for word in ["news", "report", "update", "announce"]):
        return "inform"
    elif content_type == "blog" and any(word in combined_text for word in ["opinion", "think", "believe", "rant"]):
        return "opinion"
    elif content_type == "personal":
        return "share"
    else:
        return "inform"

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
        logging.warning(f"LLM analysis failed: {e}")
        return None

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
    key_tokens = extract_important_tokens(cleaned_text, max_tokens=400)

    # Create clean content for LLM analysis (limit to ~500 tokens)
    summary_tokens = key_tokens[:100]
    raw_content_for_llm = " ".join(summary_tokens)

    # Detect comments first (simple rule-based check)
    has_comments = detect_has_comments(page_html)

    # Use LLM for semantic analysis
    llm_analysis = await analyze_with_llm(title, cleaned_text, url, has_comments)

    if llm_analysis:
        # LLM analysis successful
        analysis_data = {
            "raw_content": raw_content_for_llm,
            "suggested_fields": llm_analysis,
            "analysis_tokens": key_tokens[:50],
            "title": title[:100],
            "url": url
        }
    else:
        # LLM analysis failed - raise exception to fail the crawl
        raise Exception("LLM semantic analysis failed")

    return json.dumps(analysis_data, ensure_ascii=False)
        


async def insert_domain_record(conn, domain_name, analysis_json):
    """Insert analyzed data into the domain table."""
    domain_id = uuid.uuid5(uuid.NAMESPACE_URL, domain_name)
    
    try:
        # Parse the analysis JSON to extract semantic fields
        analysis_data = json.loads(analysis_json)
        suggested_fields = analysis_data.get("suggested_fields", {})
        
        # Prepare values with defaults
        semantic_summary = analysis_data.get("raw_content", "")[:2000]  # Truncate if too long
        content_type = suggested_fields.get("semantic_content_type_text", "unknown")
        primary_topic = suggested_fields.get("semantic_primary_topic_text", "unknown")
        keywords = suggested_fields.get("semantic_keywords_text_array", [])
        language = suggested_fields.get("semantic_language_primary_text", "en")
        communication_goal = suggested_fields.get("semantic_communication_goal_text", "unknown")
        author_type = suggested_fields.get("semantic_author_type_text", "unknown")
        audience_type = suggested_fields.get("semantic_audience_type_text", "unknown")
        content_vibe = suggested_fields.get("semantic_content_vibe_text", "unknown")
        is_commercial = suggested_fields.get("semantic_is_commercial_bool", False)
        is_spammy = suggested_fields.get("semantic_is_spammy_bool", False)
        is_politically_loaded = suggested_fields.get("semantic_is_politically_loaded_bool", False)
        quality_score = suggested_fields.get("semantic_quality_score_float")
        has_comments = suggested_fields.get("semantic_has_comments_bool", False)
        has_about_page = analysis_data.get("has_about_page", False)
        
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
        
    except (json.JSONDecodeError, KeyError, Exception) as e:
        logging.warning(f"Error parsing analysis data for {domain_name}: {e}")
        # Fallback insert with minimal data
        await conn.execute("""
            INSERT INTO domain (
                domain_id_uuid,
                domain_name_text,
                semantic_content_type_text,
                semantic_summary_text,
                crawl_first_seen_at_ts,
                crawl_last_attempt_at_ts,
                crawl_status_text,
                audit_created_at_ts,
                audit_updated_at_ts
            )
            VALUES ($1, $2, 'error', $3, now(), now(), 'analysis_error', now(), now())
            ON CONFLICT (domain_name_text) DO NOTHING;
        """, domain_id, domain_name, str(analysis_json)[:2000])

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
                        logging.info(f"✅ Found about page: {about_url}")
                        return True
            
        except Exception as e:
            logging.debug(f"About page {about_url} failed: {e}")
            continue
    
    return False

async def crawl_one(conn, browser):
    row = await claim_one_domain(conn)
    if not row:
        return False  # No more domains

    domain = row["domain_name_text"]
    logging.info(f"Crawling {domain}")
    
    has_about_page = False
    analysis_summary = None

    try:
        page = await browser.new_page()
        
        # Try /about page first
        has_about_page = await try_about_page(page, domain)
        
        if has_about_page:
            logging.info(f"Analyzing about page for {domain}")
            analysis_summary = await analyze_page(page)
        else:
            # Fallback to main page
            main_url = f"https://{domain}"
            logging.info(f"No about page found, analyzing main page: {main_url}")
            await page.goto(main_url, wait_until="domcontentloaded", timeout=15000)
            analysis_summary = await analyze_page(page)
        
        # Update the analysis to include about page detection
        if analysis_summary:
            analysis_data = json.loads(analysis_summary)
            analysis_data["has_about_page"] = has_about_page
            analysis_summary = json.dumps(analysis_data)
        
        await insert_domain_record(conn, domain, analysis_summary)
        logging.info(f"✅ Inserted: {domain} (about_page: {has_about_page})")
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
