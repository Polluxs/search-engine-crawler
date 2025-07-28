import json
import re
from openai import AsyncOpenAI
from src.logging_config import logger
from src.text_processing import extract_important_tokens, detect_has_comments
import os


# Initialize OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


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
  "semantic_has_comments_bool": {has_comments},
  "natural_language_summary_text": "A comprehensive 200-500 word summary explaining what this website is about, its purpose, target audience, and key features. Write in natural language that would help users understand the site's content and relevance."
}}

Rules:
- Be accurate and specific
- Use the exact field names provided
- Return only valid JSON
- Quality score: 0.8+ for high quality, 0.5-0.8 for decent, 0.5- for poor
- Consider the URL structure and domain name in your analysis
- For the summary, synthesize all the information into a natural language description that explains the website comprehensively"""

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