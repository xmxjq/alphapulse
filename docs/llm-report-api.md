# LLM Daily Report API

AlphaPulse exposes compact daily reports in
[TOON](https://github.com/toon-format/toon) format:

```text
GET /api/llm/guba/report/YYYY-MM-DD
GET /api/llm/tgb/report/YYYY-MM-DD
```

The schemas are:

```text
alphapulse.guba.daily-report.v1
alphapulse.tgb.daily-report.v1
```

Both responses contain flat `sections`, `boards`, `posts`, and `comments`
tables. Posts include the full stored body when available. Empty boards are
omitted.

Optional query parameters:

- `limit=500`: maximum posts to include, up to 5000.
- `include_comments=true`: include stored comments.
- `max_comments_per_post=100`: cap comments included for each post, up to 500.

Use Cloudflare Access service-token credentials for remote access:

```bash
curl --fail-with-body -sS \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET" \
  -H "Accept: text/toon" \
  "https://alphapulse-api.sanae.edu.kg/api/llm/tgb/report/2026-07-28?limit=500"
```

For a smaller prompt without comments:

```bash
curl --fail-with-body -sS \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET" \
  -H "Accept: text/toon" \
  "https://alphapulse-api.sanae.edu.kg/api/llm/tgb/report/2026-07-28?limit=500&include_comments=false"
```
