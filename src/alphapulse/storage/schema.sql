CREATE DATABASE IF NOT EXISTS {database};

CREATE TABLE IF NOT EXISTS {database}.authors (
    source LowCardinality(String),
    source_entity_id String,
    username Nullable(String),
    display_name Nullable(String),
    profile_url Nullable(String),
    bio Nullable(String),
    followers Nullable(UInt64),
    following Nullable(UInt64),
    fetched_at DateTime64(3, 'UTC'),
    version UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source, source_entity_id);

CREATE TABLE IF NOT EXISTS {database}.posts (
    source LowCardinality(String),
    source_entity_id String,
    canonical_url String,
    author_entity_id Nullable(String),
    title Nullable(String),
    content_text String,
    language Nullable(String),
    published_at Nullable(DateTime64(3, 'UTC')),
    fetched_at DateTime64(3, 'UTC'),
    like_count Nullable(UInt64),
    comment_count Nullable(UInt64),
    repost_count Nullable(UInt64),
    raw_topic_ids Array(String),
    version UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source, source_entity_id);

CREATE TABLE IF NOT EXISTS {database}.comments (
    source LowCardinality(String),
    source_entity_id String,
    post_entity_id String,
    canonical_url Nullable(String),
    author_entity_id Nullable(String),
    parent_comment_entity_id Nullable(String),
    content_text String,
    published_at Nullable(DateTime64(3, 'UTC')),
    fetched_at DateTime64(3, 'UTC'),
    like_count Nullable(UInt64),
    version UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source, post_entity_id, source_entity_id);

CREATE TABLE IF NOT EXISTS {database}.crawl_runs (
    run_id UUID,
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    status LowCardinality(String),
    seeds_processed UInt64,
    tasks_enqueued UInt64,
    pages_fetched UInt64,
    posts_written UInt64,
    comments_written UInt64,
    authors_written UInt64,
    blocked_responses UInt64,
    errors UInt64,
    skipped_tasks UInt64
)
ENGINE = MergeTree
ORDER BY (started_at, run_id);

CREATE TABLE IF NOT EXISTS {database}.crawl_errors (
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    source LowCardinality(String),
    url String,
    error_message String
)
ENGINE = MergeTree
ORDER BY (created_at, source, url);

