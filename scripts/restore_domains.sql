-- Restore Domains Script
-- 
-- This script moves domains from the domain table back to the domain_ingestion queue
-- for re-crawling. Choose one of the options below based on your needs.
--
-- Usage: Execute one of the sections below in your PostgreSQL client

-- =============================================================================
-- OPTION 1: RESTORE ALL FAILED DOMAINS
-- =============================================================================
-- Use this to restore only domains that failed during crawling

-- First, preview what will be restored (RECOMMENDED)
SELECT 
    domain_name_text,
    crawl_status_text,
    crawl_processed_at_ts,
    semantic_summary_text
FROM domain 
WHERE crawl_status_text = 'failed'
ORDER BY domain_name_text;

-- If the preview looks correct, execute this to restore failed domains:
/*
WITH restored_domains AS (
    INSERT INTO domain_ingestion (domain_name_text, discovered_at_ts, locked_at_ts)
    SELECT domain_name_text, now(), NULL
    FROM domain 
    WHERE crawl_status_text = 'failed'
    ON CONFLICT (domain_name_text) DO UPDATE SET
        discovered_at_ts = now(),
        locked_at_ts = NULL
    RETURNING domain_name_text
),
deleted_domains AS (
    DELETE FROM domain 
    WHERE domain_name_text IN (SELECT domain_name_text FROM restored_domains)
    RETURNING domain_name_text
)
SELECT 
    (SELECT count(*) FROM restored_domains) as restored_count,
    (SELECT count(*) FROM deleted_domains) as deleted_count;
*/

-- =============================================================================
-- OPTION 2: RESTORE ALL DOMAINS (SUCCESSFUL AND FAILED)
-- =============================================================================
-- Use this to restore ALL processed domains

-- First, preview what will be restored (RECOMMENDED)
SELECT 
    domain_name_text,
    crawl_status_text,
    crawl_processed_at_ts,
    semantic_content_type_text,
    semantic_primary_topic_text
FROM domain 
ORDER BY crawl_status_text, domain_name_text;

-- If the preview looks correct, execute this to restore all domains:
/*
WITH restored_domains AS (
    INSERT INTO domain_ingestion (domain_name_text, discovered_at_ts, locked_at_ts)
    SELECT domain_name_text, now(), NULL
    FROM domain 
    ON CONFLICT (domain_name_text) DO UPDATE SET
        discovered_at_ts = now(),
        locked_at_ts = NULL
    RETURNING domain_name_text
),
deleted_domains AS (
    DELETE FROM domain 
    WHERE domain_name_text IN (SELECT domain_name_text FROM restored_domains)
    RETURNING domain_name_text
)
SELECT 
    (SELECT count(*) FROM restored_domains) as restored_count,
    (SELECT count(*) FROM deleted_domains) as deleted_count;
*/

-- =============================================================================
-- OPTION 3: RESTORE SUCCESSFUL DOMAINS ONLY
-- =============================================================================
-- Use this to restore only successfully processed domains

-- First, preview what will be restored (RECOMMENDED)
SELECT 
    domain_name_text,
    crawl_status_text,
    crawl_processed_at_ts,
    semantic_content_type_text,
    semantic_primary_topic_text
FROM domain 
WHERE crawl_status_text = 'success'
ORDER BY domain_name_text;

-- If the preview looks correct, execute this to restore successful domains:
/*
WITH restored_domains AS (
    INSERT INTO domain_ingestion (domain_name_text, discovered_at_ts, locked_at_ts)
    SELECT domain_name_text, now(), NULL
    FROM domain 
    WHERE crawl_status_text = 'success'
    ON CONFLICT (domain_name_text) DO UPDATE SET
        discovered_at_ts = now(),
        locked_at_ts = NULL
    RETURNING domain_name_text
),
deleted_domains AS (
    DELETE FROM domain 
    WHERE domain_name_text IN (SELECT domain_name_text FROM restored_domains)
    RETURNING domain_name_text
)
SELECT 
    (SELECT count(*) FROM restored_domains) as restored_count,
    (SELECT count(*) FROM deleted_domains) as deleted_count;
*/

-- =============================================================================
-- OPTION 4: RESTORE SPECIFIC DOMAIN
-- =============================================================================
-- Use this to restore a single specific domain
-- Replace 'example.com' with the domain you want to restore

-- First, preview the specific domain (RECOMMENDED)
SELECT 
    domain_name_text,
    crawl_status_text,
    crawl_processed_at_ts,
    semantic_content_type_text,
    semantic_primary_topic_text,
    semantic_summary_text
FROM domain 
WHERE domain_name_text = 'victoriafalls24.com';  -- Change this domain name

-- If the preview looks correct, execute this to restore the specific domain:
/*
WITH restored_domains AS (
    INSERT INTO domain_ingestion (domain_name_text, discovered_at_ts, locked_at_ts)
    SELECT domain_name_text, now(), NULL
    FROM domain 
    WHERE domain_name_text = 'victoriafalls24.com'  -- Change this domain name
    ON CONFLICT (domain_name_text) DO UPDATE SET
        discovered_at_ts = now(),
        locked_at_ts = NULL
    RETURNING domain_name_text
),
deleted_domains AS (
    DELETE FROM domain 
    WHERE domain_name_text IN (SELECT domain_name_text FROM restored_domains)
    RETURNING domain_name_text
)
SELECT 
    (SELECT count(*) FROM restored_domains) as restored_count,
    (SELECT count(*) FROM deleted_domains) as deleted_count,
    (SELECT string_agg(domain_name_text, ', ') FROM restored_domains) as restored_domains;
*/

-- =============================================================================
-- UTILITY QUERIES
-- =============================================================================

-- Check current status of all domains
SELECT 
    crawl_status_text,
    count(*) as count
FROM domain 
GROUP BY crawl_status_text
ORDER BY crawl_status_text;

-- Check ingestion queue status
SELECT 
    count(*) as total_in_queue,
    count(*) FILTER (WHERE locked_at_ts IS NULL) as available_for_crawling,
    count(*) FILTER (WHERE locked_at_ts IS NOT NULL) as currently_locked
FROM domain_ingestion;

-- View recently failed domains with error details
SELECT 
    domain_name_text,
    crawl_last_attempt_at_ts,
    semantic_summary_text
FROM domain 
WHERE crawl_status_text = 'failed'
ORDER BY crawl_last_attempt_at_ts DESC
LIMIT 10;