CREATE TABLE IF NOT EXISTS url_state (
    url TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    seed_name TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_fetched_at TEXT,
    last_status INTEGER
);

CREATE INDEX IF NOT EXISTS idx_url_state_source_fetched
    ON url_state (source, last_fetched_at);

CREATE TABLE IF NOT EXISTS pending_tasks (
    dedupe_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    seed_name TEXT NOT NULL,
    priority INTEGER NOT NULL,
    pubdate_ts INTEGER NOT NULL DEFAULT 0,
    discovered_at TEXT NOT NULL,
    task_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_tasks_seed_priority
    ON pending_tasks (
        seed_name,
        priority DESC,
        pubdate_ts DESC,
        discovered_at ASC
    );

CREATE TABLE IF NOT EXISTS item_state (
    source TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    last_fetched_at TEXT,
    last_comment_refresh_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (source, source_entity_id)
);

CREATE TABLE IF NOT EXISTS generated_seed_runs (
    run_id TEXT PRIMARY KEY,
    logical_set_name TEXT NOT NULL,
    generator_name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS generated_seed_items (
    logical_set_name TEXT NOT NULL,
    generator_name TEXT NOT NULL,
    item_kind TEXT NOT NULL,
    item_value TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (logical_set_name, generator_name, item_kind, item_value)
);

CREATE TABLE IF NOT EXISTS compiled_seed_sets (
    seed_set_name TEXT PRIMARY KEY,
    seed_json TEXT NOT NULL,
    refreshed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS guba_daily_ranking (
    day TEXT NOT NULL,
    section TEXT NOT NULL,
    rank INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    url TEXT,
    members TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (day, section, rank)
);

CREATE TABLE IF NOT EXISTS tgb_daily_ranking (
    day TEXT NOT NULL,
    section TEXT NOT NULL,
    rank INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    url TEXT,
    members TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (day, section, rank)
);

CREATE TABLE IF NOT EXISTS jiuyan_daily_ranking (
    day TEXT NOT NULL,
    section TEXT NOT NULL,
    rank INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    url TEXT,
    members TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (day, section, rank)
);
