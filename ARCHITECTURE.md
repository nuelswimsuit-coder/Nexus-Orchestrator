# Nexus Orchestrator — Architecture Map

> Version 1.0.0 · Production · 2026

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NEXUS ORCHESTRATOR                               │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                      MASTER NODE (Windows Desktop)               │  │
│  │                                                                  │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │  │
│  │  │ Dispatcher  │  │  HITL Gate   │  │  Autonomous          │   │  │
│  │  │             │  │              │  │  Orchestrator        │   │  │
│  │  │ dispatch()  │  │ approve/     │  │  (5-min brain loop)  │   │  │
│  │  │ get_result()│  │ reject tasks │  │  Decision Engine     │   │  │
│  │  └──────┬──────┘  └──────┬───────┘  └──────────────────────┘   │  │
│  │         │                │                                       │  │
│  │  ┌──────┴──────┐  ┌──────┴───────┐  ┌──────────────────────┐   │  │
│  │  │   Vault     │  │  HITL Store  │  │  Reporting Service   │   │  │
│  │  │ (Fernet     │  │  (Redis      │  │  (Daily 20:00        │   │  │
│  │  │  encrypted) │  │   pub/sub)   │  │   WhatsApp/Telegram) │   │  │
│  │  └─────────────┘  └──────────────┘  └──────────────────────┘   │  │
│  │                                                                  │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │  │
│  │  │ Resource    │  │  Watchdog    │  │  CronScheduler       │   │  │
│  │  │ Guard       │  │  (process    │  │  (nightly scrape     │   │  │
│  │  │ (CPU/RAM    │  │   monitor)   │  │   02:00 local)       │   │  │
│  │  │  cap)       │  │              │  │                      │   │  │
│  │  └─────────────┘  └──────────────┘  └──────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                │                                        │
│                    Redis (shared broker)                                │
│                                │                                        │
│  ┌─────────────────────────────┼──────────────────────────────────┐    │
│  │         WORKER NODES        │                                  │    │
│  │                             ▼                                  │    │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐  │    │
│  │  │  Worker 1    │   │  Worker 2    │   │  Worker N        │  │    │
│  │  │  (Windows)   │   │  (Linux)     │   │  (Docker)        │  │    │
│  │  │              │   │              │   │                  │  │    │
│  │  │  ARQ + Tasks │   │  ARQ + Tasks │   │  ARQ + Tasks     │  │    │
│  │  │  Telethon    │   │  Telethon    │   │  Telethon        │  │    │
│  │  └──────────────┘   └──────────────┘   └──────────────────┘  │    │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    CONTROL CENTER (FastAPI)                      │  │
│  │                                                                  │  │
│  │  GET /api/cluster/status        GET /api/business/stats         │  │
│  │  GET /api/hitl/pending          POST /api/hitl/resolve          │  │
│  │  GET /api/business/decisions    GET /api/business/report        │  │
│  │  GET /api/content/previews      POST /api/content/resolve       │  │
│  │  GET /api/notifications/status  GET /api/super-scraper/status   │  │
│  │  Rate: 100 req/min · Global exception handler · Request IDs     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │              REACT DASHBOARD (Next.js 16 + Tailwind v4)          │  │
│  │                                                                  │  │
│  │  IntelDashboard  ClusterStatus  ContentPreview  ProfitHeatmap   │  │
│  │  AgentThinkingLog  AnalyticsCharts  MobileHitl  Header          │  │
│  │                                                                  │  │
│  │  Framer Motion animations · Recharts · SWR polling              │  │
│  │  Stealth Mode · Stealth Override · Mobile-first HITL            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Task Dispatch Pipeline

```
Producer (script / cron / orchestrator)
    │
    ▼
Dispatcher.dispatch(TaskPayload)
    │
    ├─ 1. Vault.inject()          ← secrets merged in-memory
    ├─ 2. capability check        ← scan Redis heartbeat keys
    ├─ 3. HitlGate.request_approval()  ← suspend if requires_approval=True
    │         │
    │         ├─ publish HitlRequest → Redis HITL_REQUEST_CHANNEL
    │         ├─ NotificationService → WhatsApp + Telegram (parallel)
    │         └─ await asyncio.Event (blocks only this coroutine)
    │
    └─ 4. arq.enqueue_job()       ← push to nexus:tasks queue
              │
              ▼
         Worker picks up job
              │
         runner.run_task()
              ├─ validate Pydantic
              ├─ capability check
              ├─ execute handler (with exponential backoff, 3 retries)
              └─ return {output, error, worker_id, duration_s, attempts}
```

### HITL Flow

```
Task with requires_approval=True
    │
    ▼
HitlGate.request_approval()
    │
    ├─ Publish HitlRequest → nexus:hitl:requests
    │       │
    │       ├─ API HitlStore subscribes → GET /api/hitl/pending
    │       ├─ WhatsAppProvider.send_approval_request()
    │       └─ TelegramProvider.send_hitl_alert() (with Approve/Reject buttons)
    │
    └─ await asyncio.Event (up to HITL_APPROVAL_TIMEOUT = 1 hour)
              │
              ▼
    Operator clicks Approve/Reject
    (Dashboard POST /api/hitl/resolve  OR  Telegram bot callback)
              │
              ▼
    HitlStore.resolve() → publish HitlResponse → nexus:hitl:responses
              │
              ▼
    HitlGate._listen() receives response → event.set()
              │
              ▼
    request_approval() unblocks → dispatch continues
```

### Autonomous Orchestrator Loop (every 5 min)

```
AutonomousOrchestrator.run_loop()
    │
    ├─ Set nexus:engine:state = "calculating"  (Dashboard: Deep Indigo)
    │
    ├─ _collect_context()  ← telefix.db + cluster heartbeats
    │
    ├─ Score all rules:
    │   ├─ _score_pause()                  (no sessions → block)
    │   ├─ _score_emergency_warmup()       (safety ratio < 30%)
    │   ├─ _score_scrape()                 (stale > 6h)
    │   ├─ _score_adder()                  (users ready)
    │   ├─ _score_idle_worker_opportunity() (workers idle → pivot)
    │   ├─ _score_scale_workers()          (load > 75%)
    │   └─ _score_forecast()               (forecast > 7 days old)
    │
    ├─ Sort by composite score (P×0.50 + S×0.35 - C×0.15)
    │
    ├─ If confidence ≥ 70 → dispatch top action
    │   └─ Set nexus:engine:state = "dispatching"  (Dashboard: Gold)
    │
    └─ Write reasoning to nexus:agent:log  (Dashboard terminal)
```

---

## Directory Structure

```
nexus/
├── api/                          FastAPI Control Center
│   ├── main.py                   App factory + rate limiting + global handler
│   ├── hitl_store.py             Redis pub/sub HITL bridge
│   ├── dependencies.py           DI providers
│   ├── schemas.py                API response models
│   ├── routers/
│   │   ├── cluster.py            GET /api/cluster/status
│   │   ├── hitl.py               GET/POST /api/hitl/*
│   │   ├── business.py           Business intelligence + report + windowed stats
│   │   ├── content.py            Content factory previews
│   │   └── notifications.py      ChatOps status + super-scraper status
│   └── services/
│       └── telefix_bridge.py     Read-only telefix.db queries
│
├── master/
│   ├── dispatcher.py             Core orchestrator + CronScheduler
│   ├── hitl_gate.py              HITL suspend/resume + notifications
│   ├── resource_guard.py         CPU/RAM cap (shim → worker/)
│   └── services/
│       ├── vault.py              Secrets store (Env + Fernet encrypted)
│       ├── decision_engine.py    Weighted scoring + AutonomousOrchestrator
│       ├── reporting.py          Daily profit report + WhatsApp delivery
│       └── watchdog.py           Process health monitor + auto-restart
│
├── shared/
│   ├── schemas.py                Pydantic contracts (TaskPayload, etc.)
│   ├── constants.py              Queue names, channel names, timeouts
│   ├── config.py                 pydantic-settings .env loader
│   ├── logging_config.py         structlog JSON setup
│   └── notifications/
│       ├── base.py               Alert, AlertLevel, NotificationProvider ABC
│       ├── service.py            Fan-out dispatcher + HITL special-casing
│       └── providers/
│           ├── whatsapp.py       Mock / Twilio / Evolution API
│           └── telegram.py       aiogram 3.x + inline buttons
│
└── worker/
    ├── listener.py               ARQ WorkerSettings + startup heartbeat
    ├── hardware.py               CPU/GPU/RAM detection (cached)
    ├── resource_guard.py         ResourceGuard (canonical location)
    ├── task_registry.py          @registry.register() decorator
    ├── executor/
    │   └── runner.py             Exponential backoff + global handler
    └── tasks/
        ├── auto_scrape.py        telegram.auto_scrape
        ├── telegram_adder.py     telegram.auto_add
        ├── super_scraper.py      telegram.super_scrape (intelligence hunter)
        └── content_factory.py   telegram.content_factory (AI + dedup + style)
```

---

## Redis Key Map

| Key | TTL | Written by | Read by |
|-----|-----|-----------|---------|
| `nexus:tasks` | — | Dispatcher | ARQ Workers |
| `arq:job:<id>` | 24h | ARQ Worker | Dispatcher.get_result() |
| `nexus:heartbeat:<node_id>` | 60s | Master/Worker | API cluster endpoint |
| `nexus:hitl:requests` | pub/sub | HitlGate | HitlStore, WhatsApp, Telegram |
| `nexus:hitl:responses` | pub/sub | HitlStore, Telegram bot | HitlGate |
| `nexus:engine:state` | 10min | AutonomousOrchestrator | Dashboard (RGB sync) |
| `nexus:agent:log` | list | Orchestrator, tasks | Dashboard terminal |
| `nexus:scrape:status` | 1h | auto_scrape task | Dashboard |
| `nexus:super_scraper:status` | 2h | super_scrape task | Dashboard |
| `nexus:content:status` | 1h | content_factory | Dashboard |
| `nexus:content:previews` | 24h | content_factory | Dashboard |
| `nexus:content:hashes:<group>` | 30d | content_factory | Duplicate check |
| `nexus:report:sending` | 10s | ReportingService | Dashboard (Neon Blue) |
| `nexus:report:last` | 7d | ReportingService | API /report endpoint |
| `nexus:watchdog:status` | 2min | Watchdog | Monitoring |

---

## RGB Colour State Machine

```
Master PC RGB colour is driven by nexus:engine:state (polled every 3s):

  ONLINE (default)  →  Neon Green    #00ff88
  OFFLINE           →  Neon Red      #ff2244
  calculating       →  Deep Indigo   #6366f1  (orchestrator scoring)
  dispatching       →  Gold          #f59e0b  (task being dispatched)
  warning           →  Red           #ef4444  (HITL required)
  report:sending    →  Neon Blue     #00b4ff  (daily report delivery, 10s)

  Stealth Mode: all glows suppressed, UI functional
  Stealth Override: stealth ON but scraper runs at full CPU priority
```

---

## Security Model

```
Secrets lifecycle:
  .env file  →  EnvVaultBackend (dev)
               EncryptedFileVaultBackend (prod, Fernet AES-128-CBC)
                    │
                    ▼
              Vault.inject(task)  ←  called at dispatch time only
                    │
                    ▼
              task.injected_secrets  ←  excluded from model_dump() / logs
                    │
                    ▼
              Worker handler  ←  reads via parameters["__secrets__"]
                    │
                    ▼
              GC'd with TaskPayload after job completes

Never persisted to disk on workers.
Never appears in logs (excluded field).
Transmitted over Redis (use TLS + AUTH in production).
```

---

## Scaling

```
Horizontal scaling:
  1. Build Docker image:   python scripts/package_worker.py --build
  2. Scale workers:        python scripts/package_worker.py --scale 5
  3. Or via compose:       docker compose -f docker-compose.workers.yml up --scale worker=5

Capability routing:
  Set WORKER_CAPABILITIES=linux-only,high-ram on each worker.
  Tasks declare required_capabilities; dispatcher routes accordingly.

One-click from dashboard:
  🐳 ONE-CLICK SCALE WORKER button → POST /api/business/scale-worker
```
