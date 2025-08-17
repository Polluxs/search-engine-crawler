-- Drop existing tables and types (MVP - safe to recreate)
DROP TABLE IF EXISTS domain CASCADE;
DROP TABLE IF EXISTS domain_ingestion CASCADE;
DROP TYPE IF EXISTS crawl_status_enum CASCADE;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create enum type for crawl status
CREATE TYPE crawl_status_enum AS ENUM (
    'pending',      -- Domain claimed but not yet processed
    'processing',   -- Currently being crawled
    'success',      -- Successfully crawled and analyzed
    'failed',       -- Failed to crawl (HTTP errors, timeouts, etc.)
    'skipped'       -- Deliberately skipped (e.g., blacklisted)
);

CREATE TABLE domain
(
    -- Identity
    domain_id_uuid                      UUID PRIMARY KEY,
    domain_name_text                    VARCHAR(253) NOT NULL UNIQUE,

    -- Semantic classification (what the site is about)
    semantic_content_type_text          VARCHAR(30)  NOT NULL DEFAULT 'unknown',
    semantic_primary_topic_text         VARCHAR(64)  NOT NULL DEFAULT 'unknown',
    semantic_keywords_text_array        TEXT[]       DEFAULT '{}',
    semantic_language_primary_text      VARCHAR(8)   NOT NULL DEFAULT 'unknown',

    -- Purpose & intent (why the content exists)
    semantic_communication_goal_text    VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    semantic_author_type_text           VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    semantic_audience_type_text         VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    semantic_content_vibe_text          VARCHAR(32)  NOT NULL DEFAULT 'unknown',

    -- Emotional & stylistic tone
    semantic_tone_text                  VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    semantic_formality_text             VARCHAR(32)  NOT NULL DEFAULT 'unknown',
    semantic_vibe_text                  VARCHAR(32)  NOT NULL DEFAULT 'unknown',

    -- Functional type of site
    semantic_site_type_text             VARCHAR(32)  NOT NULL DEFAULT 'unknown',

    -- Quality & trust signals
    semantic_is_commercial_bool         BOOLEAN      NOT NULL DEFAULT false,
    semantic_is_spammy_bool             BOOLEAN      NOT NULL DEFAULT false,
    semantic_is_politically_loaded_bool BOOLEAN      NOT NULL DEFAULT false,
    semantic_quality_score_float        FLOAT,

    -- Crawl metadata (technical)
    crawl_first_seen_at_ts              TIMESTAMP    DEFAULT now(),
    crawl_last_attempt_at_ts            TIMESTAMP,
    crawl_status_text                   crawl_status_enum NOT NULL DEFAULT 'pending',
    crawl_processed_at_ts               TIMESTAMP,
    crawl_has_about_bool                BOOLEAN      NOT NULL DEFAULT false,

    -- Export status
    semantic_exported_to_weaviate_bool  BOOLEAN      NOT NULL DEFAULT false,

    -- Audit info (for debugging and traceability)
    audit_created_at_ts                 TIMESTAMP    DEFAULT now(),
    audit_updated_at_ts                 TIMESTAMP    DEFAULT now(),

    -- Summary (large field moved to end)
    semantic_summary_text               TEXT         NOT NULL DEFAULT 'unknown'
);

CREATE TABLE domain_ingestion
(
    domain_name_text   VARCHAR(253) PRIMARY KEY,
    public_suffix_text VARCHAR(30),
    discovered_at_ts   TIMESTAMP,
    locked_at_ts       TIMESTAMP
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ingestion_unlocked_discovery
    ON domain_ingestion (locked_at_ts, discovered_at_ts);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_domain_crawl_status 
    ON domain (crawl_status_text);
