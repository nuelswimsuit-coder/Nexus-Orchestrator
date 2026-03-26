# Nexus Orchestrator — תיעוד מלא ומפורט

> **גרסה:** 1.0.0 | **API Base URL:** `http://localhost:8001` | **Docs UI:** `http://localhost:8001/docs`

---

## תוכן עניינים

1. [מה זה Nexus Orchestrator?](#1-מה-זה-nexus-orchestrator)
2. [ארכיטקטורה — איך הכל עובד יחד](#2-ארכיטקטורה)
3. [רכיבי הליבה](#3-רכיבי-הליבה)
4. [משתני סביבה (.env)](#4-משתני-סביבה)
5. [API — כל ה-Endpoints לפי קטגוריה](#5-api-endpoints)
   - [Meta — בריאות המערכת](#51-meta--בריאות-המערכת)
   - [Cluster — מצב הרשת](#52-cluster--מצב-הרשת)
   - [Business — אינטליגנציה עסקית](#53-business--אינטליגנציה-עסקית)
   - [HITL — אישור אנושי](#54-hitl--אישור-אנושי)
   - [System — חירום ובקרה](#55-system--חירום-ובקרה)
   - [Sentinel — ניטור יציבות](#56-sentinel--ניטור-יציבות)
   - [Incubator — פרויקטים חדשים](#57-incubator--פרויקטים-חדשים)
   - [Evolution — אבולוציה אוטונומית](#58-evolution--אבולוציה-אוטונומית)
   - [Projects — ניהול פרויקטים](#59-projects--ניהול-פרויקטים)
   - [Content — ייצור תוכן](#510-content--ייצור-תוכן)
   - [Sessions — ניהול סשנים](#511-sessions--ניהול-סשנים)
   - [Swarm — ניהול נחיל](#512-swarm--ניהול-נחיל)
   - [Modules — מודולים חיצוניים](#513-modules--מודולים-חיצוניים)
   - [Polymarket — מסחר בשוק ניבויים](#514-polymarket--מסחר-בשוק-ניבויים)
   - [Prediction — מנוע ניבוי](#515-prediction--מנוע-ניבוי)
   - [Scalper — סקאלפר מהיר](#516-scalper--סקאלפר-מהיר)
   - [Deploy — פריסה לשרתים](#517-deploy--פריסה-לשרתים)
   - [Config — הגדרות חיות](#518-config--הגדרות-חיות)
   - [Flight Mode — מצב טיסה](#519-flight-mode--מצב-טיסה)
   - [Scan — סריקת פלוטה](#520-scan--סריקת-פלוטה)
   - [Proxy — ניהול פרוקסי](#521-proxy--ניהול-פרוקסי)
   - [Notifications — התראות](#522-notifications--התראות)
6. [Worker Tasks — משימות הפועל](#6-worker-tasks)
7. [Redis — מפת המפתחות](#7-redis--מפת-המפתחות)
8. [תהליך הפעלה מלא](#8-תהליך-הפעלה-מלא)
9. [Frontend — ממשק המשתמש](#9-frontend)
10. [אבטחה](#10-אבטחה)

---

## 1. מה זה Nexus Orchestrator?

**Nexus Orchestrator** הוא מערכת אוטומציה מבוזרת ואוטונומית שנועדה לנהל פעולות עסקיות ב-Telegram, מסחר ב-Polymarket, ייצור תוכן, וניהול נחיל חשבונות — הכל מבלי התערבות אנושית רציפה.

### מה המערכת עושה בפועל?

| תחום | מה קורה |
|------|---------|
| **Telegram Automation** | גרידת משתמשים מקבוצות, הוספת משתמשים לקבוצות מנוהלות, ניטור גדילת קהילה |
| **Polymarket Trading** | מסחר אוטומטי בשוק ניבויים, סקאלפינג מהיר, ניתוח ארביטראז' |
| **Content Factory** | ייצור תוכן AI אוטומטי לקבוצות Telegram |
| **Incubator** | זיהוי נישות רווחיות חדשות, הפעלת פרויקטים חדשים אוטומטית |
| **Fleet Management** | ניהול עשרות חשבונות Telegram (סשנים) במקביל |
| **Cluster Orchestration** | ניהול מחשב Master + מחשבי Worker מרוחקים דרך Redis |

---

## 2. ארכיטקטורה

### תרשים כללי

```
┌─────────────────────────────────────────────────────────────┐
│                    MASTER NODE (המחשב שלך)                   │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  Dispatcher │  │  HITL Gate   │  │  Resource Guard  │   │
│  │  (שולח      │  │  (אישור      │  │  (מגביל CPU/RAM) │   │
│  │   משימות)   │  │   אנושי)     │  │                  │   │
│  └──────┬──────┘  └──────────────┘  └──────────────────┘   │
│         │                                                    │
│  ┌──────▼──────────────────────────────────────────────┐   │
│  │  FastAPI Control Center (port 8001)                  │   │
│  │  REST API + SSE streams + OpenAPI Docs               │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────┘
                          │
                    ┌─────▼──────┐
                    │   Redis    │  ← הברוקר המרכזי
                    │  (::1:6379)│     מחבר הכל
                    └─────┬──────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
    ┌─────────────────┐     ┌─────────────────┐
    │  WORKER (Linux) │     │ WORKER (Windows)│
    │  ARQ Listener   │     │  ARQ Listener   │
    │  Tasks: scrape, │     │  Tasks: browser,│
    │  add, content   │     │  openclaw, etc. │
    └─────────────────┘     └─────────────────┘
```

### זרימת משימה טיפוסית

```
1. Master מחליט לגרד משתמשים
2. Dispatcher יוצר TaskPayload (JSON)
3. אם requires_approval=True → HITL Gate מחכה לאישור אנושי
4. המשימה נדחפת ל-Redis queue: arq:queue:nexus:tasks
5. Worker פנוי מושך את המשימה
6. Worker מריץ את הפונקציה המתאימה מ-TaskRegistry
7. תוצאה נכתבת ל-Redis: arq:job:<task_id>
8. Master קורא את התוצאה ומחליט מה הלאה
```

---

## 3. רכיבי הליבה

### nexus/master/ — המוח של המערכת

| קובץ | תפקיד |
|------|--------|
| `dispatcher.py` | שולח משימות ל-Workers, מחכה לתוצאות, מנהל heartbeats |
| `hitl_gate.py` | עוצר משימות רגישות ומחכה לאישור אנושי (כרגע auto-approve) |
| `resource_guard.py` | מוודא שה-Master לא אוכל יותר מ-X% CPU/RAM |
| `sentinel.py` | מנטר יציבות המערכת ברקע |
| `supervisor.py` | Watchdog — מפעיל מחדש Workers שקרסו |
| `flight_mode.py` | מצב "טיסה" — עצירה מבוקרת של כל הפעולות |

### nexus/master/services/ — שירותי ה-Master

| שירות | תפקיד |
|--------|--------|
| `decision_engine.py` | מנוע החלטות AI — מחליט מה לעשות הבא לפי נתונים |
| `reporting.py` | מייצר דוח רווח יומי ושולח ב-Telegram בשעה 20:00 |
| `evolution.py` | מזהה נישות חדשות ומפעיל פרויקטים אוטונומית |
| `architect.py` | מנתח ומייעל את ארכיטקטורת הפרויקטים |
| `strategy_brain.py` | "War Room" — אינטליגנציה אסטרטגית ומסחרית |
| `poly_5m_scalper.py` | סקאלפר Polymarket — מחזורי 5 דקות |
| `deployer.py` | פורס קוד ל-Workers מרוחקים דרך SSH |

### nexus/worker/ — הפועל

| קובץ | תפקיד |
|------|--------|
| `listener.py` | ARQ WorkerSettings — מאזין לתור ומריץ משימות |
| `task_registry.py` | מפה: `"telegram.auto_scrape"` → פונקציה async |
| `tasks/` | כל הפונקציות שה-Worker יכול להריץ |

### nexus/shared/ — קוד משותף

| קובץ | תפקיד |
|------|--------|
| `config.py` | Settings מ-.env (pydantic-settings) |
| `schemas.py` | מודלים: TaskPayload, TaskResult, NodeHeartbeat |
| `kill_switch.py` | מנגנון PANIC — עצירה מיידית של הכל |
| `notifications/` | שליחת הודעות Telegram/WhatsApp |
| `redis_util.py` | כלי עזר ל-Redis, כולל Degraded Mode |

### nexus/api/ — ה-REST API

| קובץ | תפקיד |
|------|--------|
| `main.py` | FastAPI factory, CORS, Rate Limiting, Lifespan |
| `routers/` | 22 קבצי Router (אחד לכל קטגוריה) |
| `schemas.py` | מודלי HTTP response |
| `hitl_store.py` | ניהול תור HITL ב-Redis |
| `services/telefix_bridge.py` | גשר ל-SQLite של Telefix |

---

## 4. משתני סביבה

### הגדרות בסיסיות

| משתנה | ברירת מחדל | הסבר |
|--------|------------|-------|
| `NODE_ROLE` | `master` | תפקיד הצומת: `master` או `worker` |
| `REDIS_URL` | `redis://[::1]:6379/0` | כתובת Redis. `[::1]` = IPv6 loopback (Windows) |
| `NODE_ID` | `master` | שם ייחודי לצומת הזה ב-cluster |
| `LOG_LEVEL` | `INFO` | רמת לוגים: DEBUG/INFO/WARNING/ERROR |

### Telegram

| משתנה | הסבר |
|--------|-------|
| `TELEGRAM_BOT_TOKEN` | טוקן הבוט (מ-@BotFather) |
| `TELEGRAM_ADMIN_CHAT_ID` | Chat ID שלך — לשם נשלחות ההתראות |
| `TELEGRAM_API_ID` | מ-my.telegram.org — לסשנים של Telethon |
| `TELEGRAM_API_HASH` | מ-my.telegram.org |

### AI

| משתנה | הסבר |
|--------|-------|
| `GEMINI_API_KEY` | Google Gemini API — לייצור תוכן ו-AI |
| `OPENAI_API_KEY` | OpenAI (אופציונלי) |

### ניהול משאבים

| משתנה | ברירת מחדל | הסבר |
|--------|------------|-------|
| `MASTER_CPU_CAP_PERCENT` | `25` | מקסימום CPU% שה-Master ישתמש |
| `MASTER_RAM_CAP_MB` | `512` | מקסימום RAM ב-MB |
| `WORKER_MAX_JOBS` | `4` | כמה משימות במקביל לכל Worker |
| `TASK_DEFAULT_TIMEOUT` | — | Timeout ברירת מחדל למשימה |

### SSH / Deploy

| משתנה | הסבר |
|--------|-------|
| `WORKER_IP` | IP של Worker מרוחק (למשל: `10.100.102.20`) |
| `WORKER_SSH_USER` | שם משתמש SSH (למשל: `yadmin`) |
| `WORKER_SSH_PASSWORD` | סיסמת SSH |
| `WORKER_SSH_KEY_FILE` | נתיב לקובץ מפתח SSH (אופציונלי) |

### Power Management

| משתנה | הסבר |
|--------|-------|
| `NEXUS_DYNAMIC_POWER` | `1` = הפעל ניהול כוח דינמי |
| `NEXUS_POWER_NIGHT_CPU_PCT` | CPU cap בלילה (00:00–08:00): ברירת מחדל 90% |
| `NEXUS_POWER_ACTIVE_CPU_PCT` | CPU cap ביום: ברירת מחדל 50% |
| `NEXUS_POLY_SCALPER_ENABLED` | `1` = הפעל סקאלפר Polymarket ברקע |

### Retention Guardian

| משתנה | הסבר |
|--------|-------|
| `RETENTION_MONITOR_ENABLED` | `1` = הפעל ניטור גדילת קבוצות |
| `RETENTION_MONITOR_INTERVAL_S` | כל כמה שניות לבדוק (ברירת מחדל: 14400 = 4 שעות) |
| `RETENTION_MEMBER_BASELINE` | מספר חברים בסיסי לניטור |
| `RETENTION_DROP_ALERT_PCT` | אחוז ירידה שמפעיל התראה |

---

## 5. API Endpoints

> **Base URL:** `http://localhost:8001`
> **Rate Limit:** 100 בקשות לדקה לכל IP
> **Docs UI:** `http://localhost:8001/docs` (Swagger)
> **ReDoc:** `http://localhost:8001/redoc`

---

### 5.1 Meta — בריאות המערכת

> בדיקות חיות ומוכנות של ה-API עצמו.

#### `GET /health`
**תפקיד:** Liveness probe — האם ה-API בכלל רץ?

**תגובה:**
```json
{ "status": "ok", "version": "1.0.0" }
```

---

#### `GET /ready`
**תפקיד:** Readiness probe — האם ה-API מחובר ל-Redis ומוכן לקבל בקשות?

**תגובה תקינה:**
```json
{ "status": "ready", "redis": "ok" }
```

**תגובה ב-Degraded Mode** (Redis לא זמין, רץ עם fakeredis):
```json
{ "status": "ready", "redis": "degraded" }
```

**תגובה כשלא מוכן (503):**
```json
{ "status": "not_ready", "redis": "unreachable" }
```

---

### 5.2 Cluster — מצב הרשת

> מידע על כל הצמתים (Master + Workers) ב-cluster.

#### `GET /api/cluster/status`
**תפקיד:** מצב מלא של ה-cluster — כל הצמתים, תורי המשימות, ומגבלות משאבים.

**מה מוחזר:**
- רשימת כל הצמתים עם: CPU%, RAM, מספר משימות פעילות, תפקיד (master/worker), זמן heartbeat אחרון
- מגבלות CPU/RAM של ה-Master
- כמה משימות ממתינות בתור `nexus:tasks`

**תגובה לדוגמה:**
```json
{
  "nodes": [
    {
      "node_id": "master",
      "role": "master",
      "cpu_percent": 12.5,
      "ram_used_mb": 340,
      "active_jobs": 0,
      "online": true,
      "last_seen": "2026-03-26T10:00:00Z"
    }
  ],
  "master_resource_caps": { "cpu_cap_percent": 25, "ram_cap_mb": 512 },
  "queues": [{ "queue_name": "nexus:tasks", "pending_jobs": 3 }],
  "timestamp": "2026-03-26T10:00:00Z"
}
```

---

#### `GET /api/cluster/health`
**תפקיד:** בדיקת בריאות מעמיקה — מודד latency ל-Redis ולכל צומת, מחזיר פעילות Swarm ו-heatmap.

**מה מיוחד כאן:**
- מודד זמן תגובה (ms) לכל heartbeat key ב-Redis
- מחזיר `swarm_activity` — שורות אחרונות של פעילות ה-Swarm
- מחזיר `targets` — heatmap של נושאים חמים (BTC Regulation, Whale Alerts)

---

#### `GET /api/cluster/fleet/assets`
**תפקיד:** טבלת נכסי הפלוטה — כל הקבוצות המנוהלות עם מספרי חברים, סשן בעלים, ומונים מ-Redis.

**מה מוחזר:**
- רשימת קבוצות Telegram מנוהלות (מ-telefix.db)
- `total_managed_members` — סה"כ חברים מנוהלים
- `total_premium_members` — חברי Telegram Premium
- `latest_audit` — תוצאות הסריקה האחרונה

---

#### `GET /api/cluster/fleet/scan/status`
**תפקיד:** מצב הסריקה האחרונה של הפלוטה (polling, ללא SSE).

---

#### `GET /api/cluster/fleet/scan/stream`
**תפקיד:** SSE stream — עדכונים בזמן אמת על התקדמות סריקת הפלוטה.

**איך עובד:** מתחבר ל-Redis Pub/Sub על channel `nexus:fleet:scan` ומשדר JSON events:
- `phase: "started"` — הסריקה התחילה
- `phase: "progress"` — עדכון התקדמות
- `phase: "ended"` — הסריקה הסתיימה

---

#### `POST /api/cluster/test-sentinel`
**תפקיד:** שולח pulse בדיקה לוודא שהתקשורת Master↔Worker עובדת.

**גוף הבקשה:**
```json
{ "target_id": "worker-1" }
```

---

### 5.3 Business — אינטליגנציה עסקית

> הלב העסקי של המערכת — נתונים מ-Telefix, החלטות AI, ודוחות.

#### `GET /api/business/stats`
**תפקיד:** סנאפשוט אופרציונלי מלא מ-telefix.db.

**מה מוחזר:**
```json
{
  "total_managed_groups": 15,
  "total_targets": 45,
  "source_groups": 30,
  "target_groups": 15,
  "total_scraped_users": 125000,
  "total_users_pipeline": 8500,
  "active_sessions": 12,
  "frozen_sessions": 3,
  "manager_sessions": 2,
  "last_scraper_run": "2026-03-26T08:00:00Z",
  "last_adder_run": "2026-03-26T09:00:00Z",
  "last_forecast_run": "2026-03-25T20:00:00Z",
  "forecast_history": ["2026-03-25", "2026-03-24"],
  "db_available": true,
  "queried_at": "2026-03-26T10:00:00Z"
}
```

---

#### `GET /api/business/stats/windowed?window=1440`
**תפקיד:** סטטיסטיקות לחלון זמן ספציפי.

**פרמטרים:**
- `window=60` — 60 דקות אחרונות
- `window=1440` — 24 שעות אחרונות (ברירת מחדל)

---

#### `GET /api/business/fleet-assets`
**תפקיד:** טבלת נכסי הפלוטה מנקודת מבט עסקית, כולל נתוני account-mapper.

---

#### `GET /api/business/scrape-status`
**תפקיד:** מצב משימת הגרידה הנוכחית.

**ערכי status אפשריים:**
- `idle` — אין גרידה פעילה
- `running` — גרידה בתהליך
- `completed` — הסתיימה בהצלחה
- `failed` — נכשלה
- `low_resources` — נעצרה בגלל מחסור במשאבים

---

#### `POST /api/business/force-scrape`
**תפקיד:** מפעיל גרידה מיידית, עוקף את לוח הזמנים הרגיל.

**גוף הבקשה:**
```json
{
  "sources": [],
  "force": true
}
```
- `sources` — רשימת קישורי קבוצות לגרוד (ריק = שימוש ב-DB)
- `force` — עוקף את הגנת MIN_RESCRAPE_HOURS

---

#### `GET /api/business/decisions`
**תפקיד:** מריץ את מנוע ההחלטות AI ומחזיר המלצות מדורגות.

**מה מוחזר:** רשימת החלטות עם:
- `decision_type` — סוג ההחלטה (scale_up, scrape, add_users, וכו')
- `confidence` — ביטחון ב-% (0–100)
- `roi_impact` — השפעה צפויה על הכנסות
- `requires_approval` — האם צריך אישור אנושי (confidence < 70)
- `reasoning` — הסבר AI מדוע

---

#### `GET /api/business/agent-log?limit=50`
**תפקיד:** לוג "מחשבות" ה-AI — מה המנוע חשב ועשה.

**שימוש:** poll כל 5 שניות לעדכון ה-"AI Terminal" בדשבורד.

---

#### `GET /api/business/engine-state`
**תפקיד:** מצב נוכחי של ה-Orchestrator האוטונומי (לסנכרון RGB).

**ערכים:**
- `idle` — ירוק רגיל
- `calculating` — פולס אינדיגו כהה
- `dispatching` — הבזק זהב/צהוב
- `warning` — פולס אדום

---

#### `GET /api/business/report`
**תפקיד:** דוח הרווח האחרון (נוצר יומי בשעה 20:00).

---

#### `GET /api/business/report-status`
**תפקיד:** האם הדוח היומי נשלח כרגע? (מפעיל הבזק Neon Blue ב-RGB).

---

#### `GET /api/business/stuck-state`
**תפקיד:** האם ה-Orchestrator תקוע? (אותה פעולה ≥ 3 מחזורים ≈ 15 דקות).

---

#### `POST /api/business/force-run`
**תפקיד:** מריץ משימה בכוח, עוקף בדיקת confidence.

**גוף הבקשה:**
```json
{
  "task_type": "telegram.auto_scrape",
  "task_params": {},
  "reviewer_id": "dashboard"
}
```

---

#### `GET /api/business/threshold-info/{action_type}`
**תפקיד:** מחזיר threshold נוכחי ורצף אישורים לסוג פעולה.

---

#### `GET /api/business/supervisor-status`
**תפקיד:** מצב ה-Supervisor Watchdog — כל ה-Workers ומצבם.

**ערכי status:**
- `healthy` — פועל תקין
- `recovering` — בתהליך שחזור (צהוב)
- `critical` — 3 כישלונות, דורש התערבות ידנית (אדום)

---

#### `POST /api/business/supervisor-reset/{worker_name}`
**תפקיד:** איפוס ידני של Worker ב-CRITICAL state.

---

#### `GET /api/business/war-room`
**תפקיד:** "חדר מלחמה" — אינטליגנציה אסטרטגית: confidence מאסטר, sentiment heatmap, whale alerts.

---

### 5.4 HITL — אישור אנושי

> Human-in-the-Loop — מנגנון לעצירת משימות רגישות ובקשת אישור אנושי.

#### `GET /api/hitl/pending`
**תפקיד:** רשימת כל המשימות שממתינות לאישור אנושי.

**מתי משימה מגיעה לכאן:** כאשר `requires_approval=True` ב-TaskPayload, ה-Dispatcher עוצר ומחכה.

---

#### `POST /api/hitl/resolve`
**תפקיד:** אישור או דחיית משימה ממתינה.

**גוף הבקשה:**
```json
{
  "task_id": "uuid-של-המשימה",
  "approved": true,
  "reviewer_id": "yarin"
}
```

---

### 5.5 System — חירום ובקרה

> פקודות חירום — **זהירות! פעולות אלה בלתי הפיכות.**

#### `POST /api/system/panic`
**תפקיד:** כפתור PANIC — עוצר הכל מיידית (< 100ms).

**מה קורה:**
1. מגדיר `SYSTEM_STATE:PANIC = "true"` ב-Redis
2. שולח `TERMINATE` לכל Workers דרך Pub/Sub
3. שולח הודעת Telegram חירום עם: סיבה, מחיר עסקה אחרון, CPU/RAM, Workers פעילים

**תגובה:**
```json
{
  "status": "PANIC_ENGAGED",
  "activated_at": "2026-03-26T10:00:00Z",
  "workers_terminated": ["worker-1", "worker-2"],
  "elapsed_ms": 15,
  "cpu_percent": 23.5,
  "ram_used_mb": 340,
  "last_trade_price": "$0.73"
}
```

---

#### `POST /api/system/kill-switch`
**תפקיד:** Kill-switch מלא — עצירת מסחר, Workers, חשיפה, ומחיקת env.

**אבטחה כפולה:**
- גוף הבקשה חייב להכיל: `"confirm": "TERMINATE_NEXUS_NOW"`
- Header חייב להכיל: `X-Nexus-Kill-Auth: <secret>`

---

#### `POST /api/system/panic/reset`
**תפקיד:** מנקה מצב PANIC ומחזיר את המערכת לפעולה.

**מה קורה:** מוחק את `PANIC_KEY`, שולח `RESUME` לכל Workers.

---

#### `GET /api/system/panic/state`
**תפקיד:** האם המערכת ב-PANIC? ומתי הופעל?

---

#### `GET /api/system/retention-health`
**תפקיד:** סנאפשוט בריאות קבוצות Telegram (Retention Guardian).

---

#### `GET /api/system/power-profile`
**תפקיד:** פרופיל כוח נוכחי — מצב לילה/יום, CPU cap, מחזור Poly5M.

---

#### `GET /api/system/blackbox/status`
**תפקיד:** האם קיים קובץ crash dump? (Black Box).

---

#### `GET /api/system/blackbox/download`
**תפקיד:** הורדת קובץ crash dump האחרון (JSON).

---

### 5.6 Sentinel — ניטור יציבות

> מנטר יציבות המערכת ומזהה בעיות לפני שהן הופכות לקריסות.

#### `GET /api/sentinel/status`
**תפקיד:** מצב נוכחי של ה-Sentinel.

#### `GET /api/sentinel/events`
**תפקיד:** אירועי יציבות אחרונים.

#### `GET /api/sentinel/metrics`
**תפקיד:** מדדי יציבות (latency, error rates, וכו').

#### `POST /api/sentinel/report`
**תפקיד:** דיווח ידני על בעיה ל-Sentinel.

#### `POST /api/sentinel/recover-worker`
**תפקיד:** הפעלת שחזור Worker ספציפי.

---

### 5.7 Incubator — פרויקטים חדשים

> מנוע זיהוי נישות ויצירת פרויקטים אוטונומיים.

#### `GET /api/incubator/niches`
**תפקיד:** רשימת נישות רווחיות שזוהו.

#### `POST /api/incubator/niches/refresh`
**תפקיד:** סריקה מחדש לנישות חדשות.

#### `GET /api/incubator/projects`
**תפקיד:** פרויקטים שנוצרו ע"י ה-Incubator.

#### `POST /api/incubator/generate`
**תפקיד:** יצירת פרויקט חדש אוטומטית.

#### `POST /api/incubator/approve/{project_id}`
**תפקיד:** אישור פרויקט להפעלה.

#### `POST /api/incubator/kill/{project_id}`
**תפקיד:** הריגת פרויקט פעיל.

#### `GET /api/incubator/god-mode` / `POST /api/incubator/god-mode`
**תפקיד:** מצב "אל" — אוטומציה מלאה ללא אישורים.

#### `POST /api/incubator/kill-switch`
**תפקיד:** עצירת כל ה-Incubator.

#### `GET /api/incubator/state`
**תפקיד:** מצב כללי של ה-Incubator.

---

### 5.8 Evolution — אבולוציה אוטונומית

> מנוע ה-Evolution מנהל את "לידת" פרויקטים חדשים ואת הסיור (Scout) אחר הזדמנויות.

#### `GET /api/evolution/incubator`
**תפקיד:** מצב ה-Incubator מנקודת מבט ה-Evolution.

#### `GET /api/evolution/state`
**תפקיד:** מצב מנוע האבולוציה.

#### `POST /api/evolution/birth-resolve`
**תפקיד:** פתרון "לידת" פרויקט חדש.

#### `POST /api/evolution/scout`
**תפקיד:** הפעלת סיור לחיפוש הזדמנויות חדשות.

---

### 5.9 Projects — ניהול פרויקטים

> ניהול פרויקטים פעילים (Telefix, OpenClaw, וכו').

#### `GET /api/projects/`
**תפקיד:** רשימת כל הפרויקטים.

#### `GET /api/projects/{project_name}`
**תפקיד:** פרטי פרויקט ספציפי.

#### `POST /api/projects/{project_name}/action`
**תפקיד:** ביצוע פעולה על פרויקט (start, stop, restart).

#### `GET /api/projects/budget/widget`
**תפקיד:** ווידג'ט תקציב לדשבורד.

#### `GET /api/projects/architect/audit`
**תפקיד:** ביקורת ארכיטקט — ניתוח בריאות הפרויקטים.

#### `POST /api/projects/architect/run`
**תפקיד:** הרצת ניתוח ארכיטקט.

#### `GET /api/projects/architect/otp-optimizations`
**תפקיד:** אופטימיזציות OTP שהארכיטקט מציע.

#### `GET /api/projects/architect/prompts`
**תפקיד:** Prompts שהארכיטקט משתמש בהם.

#### `POST /api/projects/scan`
**תפקיד:** סריקת כל הפרויקטים.

---

### 5.10 Content — ייצור תוכן

> מפעל תוכן AI — יצירה ואישור תוכן לקבוצות Telegram.

#### `GET /api/content/previews`
**תפקיד:** תצוגה מקדימה של תוכן שנוצר.

#### `POST /api/content/resolve`
**תפקיד:** אישור/דחיית תוכן.

#### `POST /api/content/generate`
**תפקיד:** יצירת תוכן חדש ע"י AI.

#### `GET /api/content/factory-active`
**תפקיד:** האם מפעל התוכן פעיל?

---

### 5.11 Sessions — ניהול סשנים

> ניהול סשנים של Telegram (Telethon) — חשבונות משתמש.

#### `POST /api/sessions/send-code`
**תפקיד:** שליחת קוד אימות לטלפון (שלב 1 ביצירת סשן).

#### `POST /api/sessions/verify-code`
**תפקיד:** אימות הקוד ויצירת הסשן (שלב 2).

#### `GET /api/sessions/list`
**תפקיד:** רשימת כל הסשנים הפעילים.

#### `GET /api/sessions/vault/commander`
**תפקיד:** מידע על סשן ה-Commander (המנהל הראשי).

---

### 5.12 Swarm — ניהול נחיל

> ניהול "נחיל" חשבונות Telegram — קבוצות, מלאי סשנים.

#### `GET /api/swarm/dashboard`
**תפקיד:** דשבורד נחיל — סקירה כללית.

#### `POST /api/swarm/groups/{group_key}`
**תפקיד:** הוספת קבוצה לנחיל.

#### `DELETE /api/swarm/groups/{group_key}`
**תפקיד:** הסרת קבוצה מהנחיל.

#### `POST /api/swarm/groups/by-id/{group_id}`
**תפקיד:** הוספת קבוצה לפי ID.

#### `GET/POST /api/swarm/sessions/inventory`
**תפקיד:** מלאי סשנים — קריאה ועדכון.

#### `GET/POST /api/swarm/sessions/all_scanned`
**תפקיד:** כל הסשנים שנסרקו.

---

### 5.13 Modules — מודולים חיצוניים

> מודולים נוספים: OpenClaw (גרידת דפדפן) ו-Moltbot.

#### `GET /api/modules/`
**תפקיד:** רשימת כל המודולים הזמינים.

#### `GET /api/modules/{module_id}`
**תפקיד:** פרטי מודול ספציפי.

#### `POST /api/modules/openclaw/launch`
**תפקיד:** הפעלת OpenClaw — גרידה מבוססת דפדפן (Playwright).

#### `POST /api/modules/moltbot/launch`
**תפקיד:** הפעלת Moltbot.

#### `GET /api/modules/widgets/module-health`
**תפקיד:** ווידג'ט בריאות מודולים.

#### `GET /api/modules/widgets/fuel-gauge`
**תפקיד:** מד דלק — כמה "כוח" נשאר לפעולות.

#### `GET /api/modules/widgets/financial-pulse`
**תפקיד:** דופק פיננסי — מדד פעילות כלכלית.

---

### 5.14 Polymarket — מסחר בשוק ניבויים

> ממשק ל-Polymarket — שוק ניבויים מבוסס בלוקצ'יין.

#### `GET /api/polymarket/dashboard.json`
**תפקיד:** נתוני דשבורד Polymarket — פוזיציות פתוחות, P&L.

#### `GET /api/polymarket/orderbook`
**תפקיד:** ספר הזמנות נוכחי.

#### `POST /api/polymarket/manual-order`
**תפקיד:** ביצוע הזמנה ידנית.

---

### 5.15 Prediction — מנוע ניבוי

> ניתוח ניבויים, ארביטראז', ומסחר אוטומטי.

#### `GET /api/prediction/poly5m-scalper`
**תפקיד:** מצב סקאלפר 5 דקות.

#### `GET /api/prediction/cross-exchange`
**תפקיד:** הזדמנויות ארביטראז' בין בורסות.

#### `GET /api/prediction/polymarket-bot`
**תפקיד:** מצב הבוט של Polymarket.

#### `GET /api/prediction/chart-data`
**תפקיד:** נתוני גרף לתצוגה.

#### `GET /api/prediction/paper-trades`
**תפקיד:** עסקאות Paper Trading (סימולציה).

#### `GET /api/prediction/trading-mode`
**תפקיד:** מצב מסחר נוכחי: paper/live.

#### `GET /api/prediction/performance`
**תפקיד:** ביצועי המסחר.

#### `GET /api/prediction/trade-log`
**תפקיד:** לוג עסקאות.

#### `POST /api/prediction/manual-override`
**תפקיד:** עקיפה ידנית של החלטת מסחר.

#### `POST /api/prediction/manual-override/clear`
**תפקיד:** ניקוי עקיפה ידנית.

#### `GET /api/prediction/manual-override/status`
**תפקיד:** מצב עקיפה ידנית.

---

### 5.16 Scalper — סקאלפר מהיר

> מנוע סקאלפינג מהיר ל-Polymarket.

#### `GET /api/scalper/status`
**תפקיד:** מצב הסקאלפר.

#### `POST /api/scalper/simulation-mode`
**תפקיד:** הפעלה/כיבוי מצב סימולציה.

#### `GET /api/scalper/ledger`
**תפקיד:** ספר חשבונות — כל העסקאות.

#### `POST /api/scalper/ingest-news-sentiment`
**תפקיד:** הזנת נתוני sentiment מחדשות.

---

### 5.17 Deploy — פריסה לשרתים

> פריסת קוד ל-Workers מרוחקים דרך SSH.

#### `POST /api/deploy/cluster`
**תפקיד:** פריסת גרסה חדשה לכל ה-Workers.

#### `GET /api/deploy/progress/{node_id}`
**תפקיד:** SSE stream — התקדמות הפריסה ל-Worker ספציפי.

#### `GET /api/deploy/status`
**תפקיד:** מצב פריסה נוכחי.

#### `DELETE /api/deploy/progress/{node_id}`
**תפקיד:** ניקוי לוג התקדמות.

#### `POST /api/deploy/sync`
**תפקיד:** סנכרון קוד עם Workers.

---

### 5.18 Config — הגדרות חיות

> שינוי הגדרות בזמן ריצה ללא הפעלה מחדש.

#### `GET /api/config/`
**תפקיד:** קריאת כל ההגדרות הנוכחיות.

#### `PATCH /api/config/`
**תפקיד:** עדכון הגדרות — כותב ל-.env ומבצע hot-reload.

**גוף הבקשה לדוגמה:**
```json
{
  "master_cpu_cap_percent": 30,
  "worker_max_jobs": 6
}
```

---

### 5.19 Flight Mode — מצב טיסה

> עצירה מבוקרת של כל הפעולות (כמו "מצב טיסה" בטלפון).

#### `GET /api/flight-mode/status`
**תפקיד:** האם מצב טיסה פעיל?

#### `POST /api/flight-mode/recover`
**תפקיד:** יציאה ממצב טיסה וחזרה לפעולה.

---

### 5.20 Scan — סריקת פלוטה

> סריקה מלאה של כל הקבוצות והחשבונות המנוהלים.

#### `POST /api/scan/run`
**תפקיד:** הפעלת סריקה מלאה.

#### `GET /api/scan/status`
**תפקיד:** מצב הסריקה הנוכחית.

#### `GET /api/scan/stream`
**תפקיד:** SSE stream — עדכוני סריקה בזמן אמת.

#### `GET /api/scan/history`
**תפקיד:** היסטוריית סריקות.

---

### 5.21 Proxy — ניהול פרוקסי

> ניהול פרוקסי לפעולות Telegram (מקובץ `proxies.txt`).

#### `GET /api/proxy/status`
**תפקיד:** מצב כל הפרוקסי.

#### `GET /api/proxy/rotations`
**תפקיד:** היסטוריית רוטציות פרוקסי.

#### `POST /api/proxy/rotate`
**תפקיד:** רוטציה ידנית לפרוקסי הבא.

---

### 5.22 Notifications — התראות

> סטטוס שירותי ההתראות.

#### `GET /api/notifications/status`
**תפקיד:** האם שירות ההתראות (Telegram/WhatsApp) פעיל?

#### `GET /api/super-scraper/status`
**תפקיד:** מצב ה-Super Scraper.

---

## 6. Worker Tasks

> כל המשימות שה-Worker יכול להריץ — רשומות ב-TaskRegistry.

### קטגוריה: Telegram

| task_type | תפקיד |
|-----------|--------|
| `telegram.auto_scrape` | גרידת משתמשים מקבוצות מקור |
| `telegram.auto_add` | הוספת משתמשים לקבוצות יעד |
| `telegram.group_warmer` | "חימום" קבוצה — פעילות אורגנית |
| `telegram.staged_session_warmup` | חימום סשן חדש בהדרגה |
| `telegram.account_mapper` | מיפוי כוח כל חשבון |
| `telegram.retention_monitor` | ניטור גדילת/ירידת קבוצות |
| `telegram.super_scraper` | גרידה מתקדמת עם יכולות נוספות |

### קטגוריה: Content

| task_type | תפקיד |
|-----------|--------|
| `content.factory` | ייצור תוכן AI לקבוצות |
| `content.incubator_spawn` | יצירת פרויקט תוכן חדש |

### קטגוריה: Trading

| task_type | תפקיד |
|-----------|--------|
| `polymarket.bot` | בוט מסחר Polymarket |
| `polymarket.prediction` | ניתוח ניבויים |
| `scalper.poly5m` | סקאלפר 5 דקות |

### קטגוריה: Modules

| task_type | תפקיד |
|-----------|--------|
| `scraper.openclaw` | גרידה מבוססת דפדפן (Playwright) |
| `moltbot.run` | הרצת Moltbot |

### קטגוריה: System

| task_type | תפקיד |
|-----------|--------|
| `system.echo` | משימת בדיקה — מחזיר את הפרמטרים |
| `system.sleep` | שינה X שניות — לבדיקות |
| `sentinel.report` | דיווח sentinel |

---

## 7. Redis — מפת המפתחות

> כל המפתחות שהמערכת כותבת/קוראת מ-Redis.

### Heartbeats (TTL קצר)

| מפתח | תוכן |
|------|------|
| `nexus:heartbeat:<node_id>` | JSON של NodeHeartbeat — CPU, RAM, jobs |

### Task Queue

| מפתח | תוכן |
|------|------|
| `arq:queue:nexus:tasks` | Sorted Set — משימות ממתינות |
| `arq:job:<task_id>` | תוצאת משימה |

### HITL

| מפתח | תוכן |
|------|------|
| `nexus:hitl:requests` | Channel — בקשות אישור |
| `nexus:hitl:responses` | Channel — תשובות אישור |

### Business Intelligence

| מפתח | תוכן |
|------|------|
| `nexus:scrape:status` | מצב גרידה נוכחית |
| `nexus:agent:log` | לוג החלטות AI (List) |
| `nexus:engine:state` | מצב ה-Orchestrator |
| `nexus:report:last` | דוח רווח אחרון |
| `nexus:war_room:intel` | אינטליגנציה אסטרטגית |

### System

| מפתח | תוכן |
|------|------|
| `SYSTEM_STATE:PANIC` | `"true"` אם PANIC פעיל |
| `nexus:system:control` | Channel — TERMINATE/RESUME |
| `nexus:power:snapshot` | פרופיל כוח נוכחי |

### Fleet

| מפתח | תוכן |
|------|------|
| `nexus:fleet:scan` | Channel — עדכוני סריקה |
| `nexus:fleet:scan:status` | מצב סריקה אחרון |
| `nexus:fleet:counters` | מונים: managed/premium members |

### Sentinel

| מפתח | תוכן |
|------|------|
| `nexus:sentinel:pulses` | List — pulses שנשלחו |

---

## 8. תהליך הפעלה מלא

### אפשרות 1: Launcher (מומלץ)

```powershell
# מפעיל הכל בבת אחת עם TUI יפה
python scripts/nexus_launcher.py
```

ה-Launcher מפעיל לפי הסדר:
1. Redis (דרך WSL)
2. ARQ Worker
3. Master Node
4. API Server (port 8001)
5. Deployer
6. ניטור ורסטארטים אוטומטיים

### אפשרות 2: ידנית

```powershell
# טרמינל 1 — Worker
python scripts/start_worker.py

# טרמינל 2 — Master
python scripts/start_master.py

# טרמינל 3 — API
python scripts/start_api.py

# טרמינל 4 — Frontend (אופציונלי)
cd frontend && npm run dev
```

### בדיקת תקינות

```powershell
# בדיקת API
curl http://localhost:8001/health

# בדיקת Redis
curl http://localhost:8001/ready

# מצב Cluster
curl http://localhost:8001/api/cluster/status
```

---

## 9. Frontend

> ממשק משתמש Next.js 16 עם React 19 ו-Tailwind CSS.

**הפעלה:**
```bash
cd frontend
npm install
npm run dev
# זמין ב: http://localhost:3000
```

**טכנולוגיות:**
- **Next.js 16** — Framework
- **React 19** — UI
- **Tailwind CSS 4** — עיצוב
- **TanStack Query** — ניהול state ו-fetching
- **SWR** — polling אוטומטי
- **Recharts** — גרפים
- **Three.js** — אנימציות 3D
- **Framer Motion** — אנימציות

**הדשבורד מציג:**
- Fleet Grid — מצב כל הצמתים
- AI Terminal — מחשבות ה-AI בזמן אמת
- Business Stats — נתונים עסקיים
- War Room — אינטליגנציה אסטרטגית
- Trading Dashboard — Polymarket
- RGB Sync — סנכרון תאורה עם מצב המערכת

---

## 10. אבטחה

### Rate Limiting
- 100 בקשות לדקה לכל IP
- מנוהל ע"י `slowapi`

### CORS
- מאפשר רק: `localhost:3000`, `127.0.0.1:3000`, ו-Tailscale VPN (`100.x.x.x`)

### Kill Switch Auth
- `POST /api/system/kill-switch` דורש:
  1. גוף הבקשה: `"confirm": "TERMINATE_NEXUS_NOW"`
  2. Header: `X-Nexus-Kill-Auth: <secret>`

### Request ID
- כל בקשה מקבלת `X-Request-ID` header לצורך tracing

### Redis Security
- Redis רץ על loopback בלבד (`[::1]:6379`)
- אין חשיפה לרשת חיצונית

### Degraded Mode
- אם Redis לא זמין, ה-API עולה עם `fakeredis` בזיכרון
- מוגדר ע"י `NEXUS_ALLOW_DEGRADED=1`

---

## נספח: מבנה הקבצים המלא

```
Nexus-Orchestrator/
├── nexus/                          # קוד ראשי
│   ├── api/                        # FastAPI
│   │   ├── main.py                 # App factory
│   │   ├── routers/                # 22 routers
│   │   ├── schemas.py              # HTTP models
│   │   ├── hitl_store.py           # HITL management
│   │   └── services/
│   │       └── telefix_bridge.py   # SQLite bridge
│   ├── master/                     # Master node
│   │   ├── dispatcher.py           # Task dispatch
│   │   ├── hitl_gate.py            # Human approval
│   │   ├── resource_guard.py       # CPU/RAM cap
│   │   ├── sentinel.py             # Stability monitor
│   │   ├── supervisor.py           # Process watchdog
│   │   ├── flight_mode.py          # Controlled stop
│   │   └── services/               # Business services
│   ├── worker/                     # Worker node
│   │   ├── listener.py             # ARQ settings
│   │   ├── task_registry.py        # Task map
│   │   └── tasks/                  # Task handlers
│   ├── shared/                     # Shared code
│   │   ├── config.py               # Settings (.env)
│   │   ├── schemas.py              # Wire models
│   │   ├── kill_switch.py          # PANIC mechanism
│   │   ├── notifications/          # Telegram/WhatsApp
│   │   └── redis_util.py           # Redis helpers
│   ├── trading/                    # Polymarket
│   └── modules/                    # OpenClaw, Moltbot
├── scripts/                        # CLI entrypoints
│   ├── nexus_launcher.py           # All-in-one launcher
│   ├── start_master.py
│   ├── start_worker.py
│   ├── start_api.py
│   └── start_deployer.py
├── frontend/                       # Next.js dashboard
├── .env                            # הגדרות (לא ב-git!)
├── proxies.txt                     # פרוקסי
├── requirements.txt                # Python deps
├── pyproject.toml                  # Package config
└── README.md                       # תיעוד בסיסי
```

---

*תיעוד זה נוצר אוטומטית מניתוח קוד המקור. עודכן: מרץ 2026.*
