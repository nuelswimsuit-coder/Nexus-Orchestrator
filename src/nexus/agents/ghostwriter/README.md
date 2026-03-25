# 🇮🇱 Israeli AI Swarm Ghostwriter

A stealth Telethon-based ghostwriter that monitors Israeli AI Telegram groups and fires context-aware Hebrew replies when trigger keywords appear. Powered by OpenAI or Anthropic.

---

## ⚡ 3-Step Quick Start

### Step 1 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 2 — Configure
Edit `config.yaml`:
```yaml
telegram:
  api_id: 12345678          # from https://my.telegram.org
  api_hash: "abc123..."

ai:
  provider: "openai"
  openai_api_key: "sk-..."  # or set OPENAI_API_KEY env var

groups:
  - "israeliAIcommunity"    # group username (without @)

personality: "Expert"       # Expert | Skeptic | Hype-Man | Beginner
```

Or use environment variables (takes priority over config.yaml):
```
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abc123...
OPENAI_API_KEY=sk-...
```

### Step 3 — Run
```bash
# From the ghostwriter folder:
python main.py

# With stealth mode (only replies to direct questions):
python main.py --stealth

# Override personality at runtime:
python main.py --personality Hype-Man

# Enable verbose debug logging:
python main.py --debug
```

---

## 📁 Project Structure

```
ghostwriter/
├── main.py           # CLI entrypoint with live coloured log
├── ghostwriter.py    # Telethon engine — listens, detects triggers, fires replies
├── ai_reply.py       # OpenAI / Anthropic prompt builder & caller
├── config.yaml       # All settings (groups, triggers, personality, delays)
├── requirements.txt
└── sessions/         # Place your .session files here (one per account)
```

---

## 🔐 First-Time Session Setup

Telethon needs to authenticate each account once. Run this snippet once per phone number:

```python
from telethon.sync import TelegramClient

api_id   = 12345678
api_hash = "your_api_hash"
phone    = "+972501234567"

with TelegramClient(f"sessions/{phone}", api_id, api_hash) as client:
    client.start(phone=phone)
    print("Session saved!")
```

This creates `sessions/+972501234567.session`. After that, `main.py` picks it up automatically.

---

## ⚙️ Key Config Options

| Setting | Description |
|---|---|
| `personality` | `Expert` / `Skeptic` / `Hype-Man` / `Beginner` |
| `triggers` | Hebrew/English keywords that activate a reply |
| `behavior.stealth_mode` | Only reply to messages ending with `?` |
| `behavior.min_delay_seconds` | Minimum wait before sending (anti-flood) |
| `behavior.max_delay_seconds` | Maximum wait before sending |
| `behavior.max_replies_per_hour` | Rate limit per account per group |
| `behavior.context_messages` | How many past messages to read for context |

---

## 🛡️ Safety Notes

- Randomized delays (5–15s default) prevent flood-ban detection.
- Rate limiter caps replies at 8/hour per account per group by default.
- Stealth mode reduces activity to direct questions only.
- Never run more than ~3 accounts from the same IP simultaneously.
