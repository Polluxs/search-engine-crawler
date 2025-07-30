import json
import re
from openai import AsyncOpenAI
from src.logging_config import logger
from src.text_processing import extract_important_tokens, detect_has_comments, process_metadata_for_llm
import os


# Initialize OpenAI client
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


async def analyze_with_llm(title, cleaned_text, url, has_comments, description):
    """Use OpenAI LLM to analyze page content and return structured semantic data."""
    try:
        # Create prompt for LLM analysis
        prompt = f"""Analyze this website content and provide semantic classification in valid JSON format.

WEBSITE DATA:
Title: {title}
URL: {url}
Content: {cleaned_text}
Has Comments: {has_comments}
Description: {description}

Please analyze this content and return a JSON object with the following fields:

{{
  "semantic_content_type_text": "blog|forum|docs|ecommerce|news|portfolio|corporate|personal|marketplace|landing|other",
  "semantic_primary_topic_text": "main topic in 1-2 words (e.g., 'technology', 'art', 'fitness')",
  "semantic_keywords_text_array": ["5-10 most relevant keywords/phrases"],
  "semantic_language_primary_text": "language code (e.g., 'en', 'es', 'fr')",
  "semantic_communication_goal_text": "sell|teach|inform|share|entertain|rant|advertise",
  "semantic_author_type_text": "individual|company|organization|government|unknown",
  "semantic_audience_type_text": "general|beginner|expert|professional|consumer|developer",
  "semantic_tone_text": "dry|humorous|sarcastic|serious|playful|authoritative|conversational|inspiring",
  "semantic_formality_text": "casual|formal|semi-formal|academic|colloquial",
  "semantic_vibe_text": "corporate|edgy|minimalist|trendy|traditional|quirky|professional",
  "semantic_site_type_text": "company blog|wiki|comparison site|tutorial site|news site|portfolio|landing page|documentation",
  "semantic_is_commercial_bool": true/false,
  "semantic_is_spammy_bool": true/false,
  "semantic_is_politically_loaded_bool": true/false,
  "semantic_quality_score_float": 0.0-1.0,
  "natural_language_summary_text": "A comprehensive 200-500 word summary explaining what this website is about, its purpose, target audience, and key features. Write in natural language that would help users understand the site's content and relevance."
}}

Rules:
- Be accurate and specific
- Use the exact field names provided
- Return only valid JSON
- Quality score: 0.8+ for high quality, 0.5-0.8 for decent, 0.5- for poor
- Consider the URL structure and domain name in your analysis
- Use the provided description to enhance your analysis
- For the summary, synthesize all the information including the description into a natural language explanation of the website"""

        response = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert web content analyst. Return only valid JSON as requested."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1000
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


async def extract_page_data(page, max_content_tokens=250):
    """Extract all page data: content, metadata, and features in one comprehensive method."""
    await page.wait_for_selector("body", timeout=10000)

    # Single JavaScript evaluation to get everything at once
    page_data = await page.evaluate("""
        () => {
            // Extract metadata
            const getMetaContent = (selector) => {
                const el = document.querySelector(selector);
                return el ? el.getAttribute('content') : '';
            };
            
            const metadata = {
                description: getMetaContent('meta[name="description"]') || getMetaContent('meta[property="og:description"]'),
                keywords: getMetaContent('meta[name="keywords"]'),
                author: getMetaContent('meta[name="author"]'),
                language: getMetaContent('meta[http-equiv="content-language"]') || 
                         document.documentElement.getAttribute('lang') || 'en',
                og_title: getMetaContent('meta[property="og:title"]'),
                og_description: getMetaContent('meta[property="og:description"]'),
                og_type: getMetaContent('meta[property="og:type"]')
            };

            // Extract body content with cleanup
            const removeElements = document.querySelectorAll('script, style, nav, footer, header');
            removeElements.forEach(el => el.remove());
            
            // Try main content areas first
            let body_text = null;
            const mainSelectors = ['main', '[role="main"]', '.content', '.main-content', 'article'];
            for (const selector of mainSelectors) {
                const main = document.querySelector(selector);
                if (main && main.innerText.trim().length > 50) {
                    body_text = main.innerText;
                    break;
                }
            }
            
            // Fallback to full body
            if (!body_text) {
                body_text = document.body.innerText || '';
            }

            return {
                metadata: metadata,
                body_text: body_text,
                full_html: document.documentElement.outerHTML
            };
        }
    """)

    # Validate content length
    if not page_data['body_text'] or len(page_data['body_text'].strip()) < 50:
        raise Exception("Body content too short")

    # Clean and process content
    cleaned_text = re.sub(r'\s+', ' ', page_data['body_text'].strip())
    cleaned_text = re.sub(r'[^\w\s\-\.\,\!\?]', ' ', cleaned_text)
    
    # Extract important tokens and metadata description
    content_tokens = extract_important_tokens(cleaned_text, max_tokens=max_content_tokens)
    metadata_description = process_metadata_for_llm(page_data['metadata'], max_tokens=50)
    has_comments = detect_has_comments(page_data['full_html'])

    return {
        'content_tokens': content_tokens,
        'has_comments': has_comments,
        'metadata_description': metadata_description
    }


async def try_about_page(page, domain):
    """Check if domain has a valuable about page (simple existence check)."""
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
                # Check if page has meaningful content and isn't just a redirect
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


async def analyze_domain_with_llm(domain, homepage_data, about_data=None):
    """Perform comprehensive LLM analysis combining homepage and optional about page data."""
    # Prepare content for analysis
    homepage_content = " ".join(homepage_data['content_tokens'])
    about_content = " ".join(about_data['content_tokens'][:100]) if about_data else ""
    metadata_description = homepage_data['metadata_description']
    has_comments = homepage_data['has_comments']
    
    # Create analysis prompt
    prompt = f"""Analyze this website content and provide semantic classification in valid JSON format.

IMPORTANT: The HOMEPAGE content is the PRIMARY and MOST TRUSTED source. About page content may be redirected or inaccurate.

WEBSITE DATA:
Domain: {domain}
Homepage Content (PRIMARY): {homepage_content}
About Page Content (SUPPLEMENTARY): {about_content if about_content else "Not available"}
Metadata Description: {metadata_description}
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
  "semantic_tone_text": "dry|humorous|sarcastic|serious|playful|authoritative|conversational|inspiring",
  "semantic_formality_text": "casual|formal|semi-formal|academic|colloquial",
  "semantic_vibe_text": "corporate|edgy|minimalist|trendy|traditional|quirky|professional",
  "semantic_site_type_text": "company blog|wiki|comparison site|tutorial site|news site|portfolio|landing page|documentation",
  "semantic_is_commercial_bool": true/false,
  "semantic_is_spammy_bool": true/false,
  "semantic_is_politically_loaded_bool": true/false,
  "semantic_quality_score_float": 0.0-1.0,
  "natural_language_summary_text": "A comprehensive 200-500 word summary explaining what this website is about, its purpose, target audience, and key features. Write in natural language that would help users understand the site's content and relevance.",
  "llm_prior_knowledge_text": "If you recognize this domain or have knowledge about it from your training data, provide additional context about what this website/service/company is known for. If you don't recognize it, state 'No prior knowledge available.'"
}}

Rules:
- PRIORITIZE homepage content over about page content when they conflict
- Be accurate and specific
- Use the exact field names provided
- Return only valid JSON
- Quality score: 0.8+ for high quality, 0.5-0.8 for decent, 0.5- for poor
- Consider the URL structure and domain name in your analysis
- Use the provided metadata description to enhance your analysis, but trust homepage content more
- For the summary, synthesize all information with homepage content as the primary source
- Include your prior knowledge about the domain if available"""

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert web content analyst. Return only valid JSON as requested. Prioritize homepage content as the primary truth source."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1200
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Parse JSON response
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            # Try to extract JSON from response if LLM added extra text
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                raise ValueError("LLM response is not valid JSON")
                
    except Exception as e:
        logger.warning(f"LLM analysis failed: {e}")
        raise Exception(f"LLM semantic analysis failed: {e}") from e


