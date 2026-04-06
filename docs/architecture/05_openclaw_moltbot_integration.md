# OpenClaw / Moltbot / Swarm integration — JSON payloads

This document defines stable JSON shapes for cross-component messaging. Related code: `nexus.services.recent_news_digest`, `nexus.worker.tasks.news_digest_refresh`, `nexus.shared.swarm_signals`.

## Central news digest cache (Redis)

**Key:** `nexus:news:digest:bundle`  
**Value:** JSON object (UTF-8 string)

```json
{
  "digest_text": "string",
  "anchor_title": "string",
  "anchor_link": "string",
  "image_url": "https://... | null"
}
```

**Key:** `nexus:news:digest:updated_at` — ISO-8601 UTC timestamp of last successful refresh.

**Key:** `nexus:news:digest:content_hash` — SHA-256 hex prefix of the cached bundle JSON (change detection).

## Pub/Sub: swarm wake channel

**Channel:** `nexus:swarm:news_digest`

Emitted when the digest **changes** after a central refresh (worker task `swarm.news_digest.refresh`).

```json
{
  "schema": "nexus.swarm.news_digest.v1",
  "event": "news_digest_updated",
  "ts": "2026-04-06T12:00:00+00:00",
  "digest_preview": "first ~500 chars of digest_text",
  "anchor_title": "string",
  "anchor_link": "https://..."
}
```

The same event (with `"engine": "news_digest"`) may also appear on `nexus:swarm:events` for dashboard compatibility.

## Environment

| Variable | Purpose |
|----------|---------|
| `NEWS_DIGEST_REFRESH_SEC` | Master scheduler interval for `swarm.news_digest.refresh` (default `300`; set `0` to disable). |
| `TELEGRAM_NEWS_RSS_URL` | Optional extra RSS URL (e.g. public mirror of a Telegram news channel). |
| `TELEGRAM_NEWS_RSS_LABEL` | Short source label for digest lines (default `telegram-news`). |
| `COMMUNITY_FACTORY_BOT_CHAIN_MAX` | Max consecutive factory-pool messages at the top of chat before converse slots skip (default `4`). |

## Related: OpenClaw sentiment blob (Redis)

**Key:** `openclaw:news:sentiment` (see `nexus.worker.tasks.openclaw`)

```json
{
  "score": 0.0,
  "channel_title": "string",
  "excerpt": "string",
  "source": "string",
  "updated_at": "ISO-8601"
}
```
