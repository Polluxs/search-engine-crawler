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

# Load environment variables
load_dotenv()
DB_URL = os.getenv("POSTGRES_URL")

logging.basicConfig(level=logging.INFO)

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

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
            return "en"  # Default to English for short text
        detected = detect(text)
        return detected if detected else "en"
    except (LangDetectException, Exception):
        return "en"

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

async def analyze_page(page):
    """Extract and analyze page content for structured semantic analysis."""
    try:
        await page.wait_for_selector("body", timeout=10000)
        
        # Extract basic metadata
        title = await page.title()
        url = page.url
        
        # Get full HTML for comment detection
        page_html = await page.content()
        
        # Extract text content from body, excluding script/style tags
        body_text = await page.evaluate("""
            () => {
                // Remove script and style elements
                const scripts = document.querySelectorAll('script, style, nav, footer, header');
                scripts.forEach(el => el.remove());
                
                // Get main content areas first
                const main = document.querySelector('main, [role="main"], .content, .main-content, article');
                if (main) {
                    return main.innerText;
                }
                
                // Fallback to body
                return document.body.innerText;
            }
        """)
        
        if not body_text or len(body_text.strip()) < 50:
            return json.dumps({
                "raw_content": "Insufficient content extracted",
                "suggested_fields": {
                    "semantic_content_type_text": "minimal",
                    "semantic_primary_topic_text": "unknown",
                    "semantic_keywords_text_array": [],
                    "semantic_language_primary_text": "en",
                    "semantic_communication_goal_text": "unknown",
                    "semantic_is_commercial_bool": False,
                    "semantic_has_comments_bool": False
                },
                "analysis_tokens": []
            })
        
        # Clean and normalize text
        cleaned_text = re.sub(r'\s+', ' ', body_text.strip())
        cleaned_text = re.sub(r'[^\w\s\-\.\,\!\?]', ' ', cleaned_text)
        
        # Extract important tokens for LLM analysis
        key_tokens = extract_important_tokens(cleaned_text, max_tokens=400)
        
        # Create summary for semantic_summary_text (500 tokens max)
        summary_tokens = key_tokens[:100]  
        raw_content_for_llm = " ".join(summary_tokens)
        
        # Detect language
        language = detect_language(cleaned_text)
        
        # Analyze content characteristics
        content_type = analyze_content_type(title, cleaned_text)
        is_commercial = detect_commercial_intent(title, cleaned_text)
        communication_goal = detect_communication_goal(title, cleaned_text, content_type)
        has_comments = detect_has_comments(page_html)
        
        # Extract primary topic from most important tokens
        primary_topic = "unknown"
        if key_tokens:
            # Use the first significant noun or entity as primary topic
            for token in key_tokens[:10]:
                if len(token) > 3 and token.lower() not in ["page", "site", "website", "home", "blog"]:
                    primary_topic = token.lower()
                    break
        
        # Create keywords array from key tokens
        keywords = list(set([token.lower() for token in key_tokens[:20] if len(token) > 2]))
        
        # Structure the output for LLM analysis
        analysis_data = {
            "raw_content": raw_content_for_llm,  # For LLM to analyze and enhance
            "suggested_fields": {
                "semantic_content_type_text": content_type,
                "semantic_primary_topic_text": primary_topic,
                "semantic_keywords_text_array": keywords,
                "semantic_language_primary_text": language,
                "semantic_communication_goal_text": communication_goal,
                "semantic_author_type_text": "unknown",  # LLM can determine this
                "semantic_audience_type_text": "unknown",  # LLM can determine this
                "semantic_content_vibe_text": "unknown",  # LLM can determine this
                "semantic_is_commercial_bool": is_commercial,
                "semantic_is_spammy_bool": False,  # LLM can determine this
                "semantic_is_politically_loaded_bool": False,  # LLM can determine this
                "semantic_quality_score_float": None,  # LLM can score this
                "semantic_has_comments_bool": has_comments
            },
            "analysis_tokens": key_tokens[:50],  # Additional tokens for LLM reference
            "title": title[:100],
            "url": url
        }
        
        return json.dumps(analysis_data, ensure_ascii=False)
        
    except Exception as e:
        logging.warning(f"Error in analyze_page: {e}")
        return json.dumps({
            "raw_content": f"Analysis failed: {str(e)}",
            "suggested_fields": {
                "semantic_content_type_text": "error",
                "semantic_primary_topic_text": "error",
                "semantic_keywords_text_array": [],
                "semantic_language_primary_text": "en",
                "semantic_communication_goal_text": "unknown",
                "semantic_is_commercial_bool": False,
                "semantic_has_comments_bool": False
            },
            "analysis_tokens": []
        })

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
                audit_created_at_ts,
                audit_updated_at_ts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now(), now(), 'success', now(), now(), now())
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
                audit_updated_at_ts = now();
        """, 
        domain_id, domain_name, content_type, primary_topic, keywords, language,
        communication_goal, author_type, audience_type, content_vibe, is_commercial,
        is_spammy, is_politically_loaded, quality_score, has_comments, semantic_summary)
        
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
