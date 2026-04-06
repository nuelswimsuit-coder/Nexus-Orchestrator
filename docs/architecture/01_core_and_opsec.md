# Rule Book 01 — Core Runtime & OPSEC (Telegram Swarm / TeleFix)

**Audience:** Cursor agents implementing orchestration, workers, networking, and media safety.  
**Scope:** Asyncio concurrency caps, Redis messaging, proxy strategy, device identity, and media hygiene.  
**Non-scope:** Do not rewrite `swarm.py` or unrelated modules unless explicitly tasked; follow this document when you touch these concerns.

> **Note:** This rule book encodes **MEGA-PROMPT V10 — Core & OPSEC** as provided by the project architect. If V10 is updated, revise this file in the same PR as the behavioral change.

---

## System prompt (binding)

When working on **core orchestration, Redis, proxies, or media pipeline** code in this repository, you MUST:

1. Treat **concurrency** as a first-class safety property: cap parallel Telegram I/O, parallel session sync, and parallel deploy/scrape operations with `asyncio.Semaphore` (or equivalent) whose limits are **configurable** (env or settings), not hard-coded magic numbers scattered across files.
2. Treat **Redis Pub/Sub** as a **dedicated connection** per subscriber loop: never interleave pub/sub mode with normal Redis commands on the same async client; reconnect with backoff on failure; log subscription channel names at startup.
3. Implement **proxy rotation** deterministically per session index (or per stable session id hash) so the same logical identity does not hop jurisdictions every tick; rotate only on policy triggers (ban, health failure, explicit admin command), not on random jitter alone.
4. Keep **device / client spoofing** consistent with **one Telethon session**: API layer parameters (device model, system version, app version) must be stored or derived from a **stable seed** tied to that session, not regenerated each connect.
5. Ensure **EXIF wiping** on any image bytes that leave the generation path toward Telegram: strip GPS, camera, software, and thumbnail IFDs before upload; verify with a second pass that no GPS IFD remains.
6. Apply **pixel salting** (imperceptible luminance/chroma noise or subpixel shifts) to non-unique raster assets so file hashes and perceptual hashes differ across personas when reusing base images, without visible artifacts.

You MUST NOT:

- Bypass semaphore limits “just for this task” or nest unbounded `asyncio.gather` on Telegram sends.
- Share one Redis connection between a pub/sub consumer and task producers without documented isolation.
- Upload raw camera JPEGs/PNGs with intact metadata.
- Randomize device fingerprints on every reconnect for the same `.session` file.

---

## Asyncio semaphore patterns

### Required pattern

- **One semaphore per resource class**, e.g. `TELEGRAM_SEND_SEM`, `SESSION_SYNC_SEM`, `DEPLOY_SEM`.
- Acquire with `async with sem:` around the smallest block that performs external I/O.
- When combining semaphores, define **lock ordering** in code comments to avoid deadlock (always acquire in the same order).

### TeleFix alignment

Existing code uses semaphores for deploy parallelism and session vault sync; new swarm work MUST follow the same style—**centralize** limits in `settings` or env, import once.

### Acceptance criteria

- [ ] Limits are documented in `.env.example` or settings schema.
- [ ] Logs include `sem_limit` and `task_name` when rejecting or queueing is implemented.

---

## Redis Pub/Sub

### Channels and responsibilities (canonical)

Define and use **namespaced** channels under `nexus:*`. Examples already present in the codebase include control and swarm surfaces; new V10 work MUST extend this namespace, not introduce ad-hoc unprefixed channels.

| Pattern | Purpose |
|--------|---------|
| `nexus:system:control` | Global panic / terminate / resume (existing listener pattern). |
| `nexus:swarm:events` | High-level swarm/UI broadcast (existing Israeli swarm path). |
| `nexus:swarm:factory:*` | Community factory state (existing). |
| **V10 extension:** `nexus:swarm:cognitive:*` | LLM job fan-out / results (documented in `03_llm_cognitive_engine.md`). |
| **V10 extension:** `nexus:grounding:*` | Moltbot → worker structured payloads (documented in `05_openclaw_moltbot_integration.md`). |

### Subscriber rules

- Use `decode_responses=True` unless binary payloads are explicitly specified.
- On reconnect, **re-subscribe** explicitly; do not assume auto-resubscribe across clients.
- Parse JSON in `try/except`; invalid payloads MUST be logged once with truncated body, not crash the loop.

---

## Proxy rotation

### Policy

- **Sticky default:** `proxy_id = H(session_material) % pool_size` unless health says otherwise.
- **Rotate on:** explicit ban, repeated `FLOOD_WAIT`, proxy timeout streak, or operator command.
- **Cooldown:** after rotation, enforce a minimum dwell time (e.g. 30–120 minutes) unless emergency flag set.

### Implementation hints

- Reuse existing proxy pool parsing helpers where present (`_parse_proxy_pool`, `_proxy_for_index` patterns in worker tasks).
- Never log full proxy credentials; log `proxy_index` or redacted host.

---

## Device spoofing (Telethon)

### Rules

- Persist generated **device model**, **system version**, and **app version** in session-side metadata (JSON next to vault or encrypted blob) keyed by session base name.
- On first run, sample from **allowed device profile tables** per archetype (see `02_personas_and_rhythm.md`), then freeze.
- Align language pack / system locale with persona locale when the stack exposes it.

---

## EXIF wiping

### Mandatory steps

1. Decode image to RGB/RGBA workspace when using PIL/CV; re-encode to Telegram-acceptable format.
2. Strip EXIF/IPTC/XMP; for PNG, clear textual chunks that embed metadata.
3. Optional but recommended: normalize orientation and discard EXIF orientation tag.

### Verification

- Unit-level check: after wipe, `exif` or equivalent read returns empty for GPS and device fields.

---

## Pixel salting

### Rules

- Salt MUST be **deterministic per (image_id, persona_id, upload_index)** so retries do not produce visibly different images, but different personas do not share identical bytes.
- Amplitude bounded (e.g. ≤ 2–4 LSb per channel) to avoid banding.
- Do not salt vectors (SVG) with raster noise; use structural variation instead.

---

## Dependencies (typical)

- `redis.asyncio` for async Redis and pub/sub.
- `asyncio` primitives (`Semaphore`, `Event`, bounded queues).
- `Pillow` or `piexif` (or project-standard image lib) for EXIF.
- `python-socks` / Telethon proxy tuple compatibility for SOCKS5/HTTP.

---

## Checklist before merge

- [ ] Semaphore limits configurable and logged at startup.
- [ ] Pub/sub uses isolated connection; reconnects are handled.
- [ ] Proxies sticky-by-default; rotation audited in logs.
- [ ] Device profile frozen per session.
- [ ] Images: EXIF wiped + salt applied where raster reuse occurs.
