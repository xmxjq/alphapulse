# Raw Response Retention

`[crawl.raw_store]` archives raw HTTP responses on disk: gzip blobs
content-addressed by sha256 under `{root_path}/blobs/<first two hex digits>/`,
plus `{root_path}/fetch_log.db`, a SQLite index with one row per fetch (status,
timing, content hash, block classification, parser errors).

Every fetch appends to both, so the archive is bounded by two independent age
windows:

| Key | Default | Range | Applies to |
| --- | --- | --- | --- |
| `blob_retention_days` | `7` | 1-365 | gzip blobs under `blobs/`, compared against file mtime |
| `fetch_log_retention_days` | `90` | 1-3650 | `fetch_log` rows, compared against `fetched_at` |

The windows are independent on purpose: a row can outlive its body, so the log
still explains a fetch after its blob is gone and `load_body` misses for that
row. Each key falls back to its default when omitted, and both reject `0` or
out-of-range values - at config load, and again inside `prune()` before
anything is read or deleted.

## When pruning runs

The crawler prunes inside its per-source cycle, at most once every 6 hours; the
timestamp lives in `raw_store_meta.last_prune`. Only one source thread prunes at
a time and the others skip the turn, so concurrent sources never race on the
same blob directory.

Preview first, then prune explicitly:

```bash
alphapulse raw-prune --dry-run                      # counts only, nothing deleted
alphapulse raw-prune                                # both windows, configured values
alphapulse raw-prune --days 3 --fetch-log-days 30   # one-off override
```

`--days` and `--fetch-log-days` override the settings for that run only.
`--force` ignores the 6-hour interval, which otherwise makes an interactive run
report `"skipped": true`.

## What pruning does and does not do

- Blob files are deleted individually. The two-character shard directories under
  `blobs/` are never removed, so a save that already created its directory
  cannot lose it mid-write; at most 256 directories remain, which is negligible.
- `fetch_log` rows are removed with `DELETE`. SQLite keeps the freed pages
  inside the file for reuse, so the `.db` file does not shrink. That is
  expected, not a failed prune; `VACUUM` is a separate maintenance decision that
  needs a write lock and spare disk.
- Retention bounds growth. It does not promise a specific archive size, and
  reclaimed space shows up as deleted blobs rather than as a smaller database
  file.
