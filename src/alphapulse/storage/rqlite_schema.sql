CREATE TABLE IF NOT EXISTS authors (
    source TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    username TEXT,
    display_name TEXT,
    profile_url TEXT,
    bio TEXT,
    followers INTEGER,
    following INTEGER,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (source, source_entity_id)
);

CREATE TABLE IF NOT EXISTS posts (
    source TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    author_entity_id TEXT,
    title TEXT,
    content_text TEXT NOT NULL,
    language TEXT,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    like_count INTEGER,
    comment_count INTEGER,
    repost_count INTEGER,
    raw_topic_ids_json TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (source, source_entity_id)
);

CREATE TABLE IF NOT EXISTS comments (
    source TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    post_entity_id TEXT NOT NULL,
    canonical_url TEXT,
    author_entity_id TEXT,
    parent_comment_entity_id TEXT,
    content_text TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    like_count INTEGER,
    PRIMARY KEY (source, source_entity_id)
);

CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    seeds_processed INTEGER NOT NULL,
    tasks_enqueued INTEGER NOT NULL,
    pages_fetched INTEGER NOT NULL,
    posts_written INTEGER NOT NULL,
    comments_written INTEGER NOT NULL,
    authors_written INTEGER NOT NULL,
    blocked_responses INTEGER NOT NULL,
    errors INTEGER NOT NULL,
    skipped_tasks INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crawl_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    source TEXT NOT NULL,
    url TEXT NOT NULL,
    error_message TEXT NOT NULL
);
