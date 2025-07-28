import uuid
from src.logging_config import logger


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


async def insert_domain(conn, domain_name, llm_analysis, has_about_page=False):
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
    natural_language_summary = llm_analysis.get("natural_language_summary_text", "")[:2000]

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
    is_spammy, is_politically_loaded, quality_score, has_comments, natural_language_summary, has_about_page)


async def delete_domain_ingestion(conn, domain_name):
    """Clean up domain from domain_ingestion after successful processing."""
    # Log that we should delete from domain_ingestion (actual deletion commented out for now)
    logger.info(f"Successfully processed {domain_name} - should delete from domain_ingestion")
    
    # TODO: Uncomment this when ready to clean up processed domains
    # await conn.execute("""
    #     DELETE FROM domain_ingestion 
    #     WHERE domain_name_text = $1
    # """, domain_name)