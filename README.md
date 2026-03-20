# Nexus Orchestrator

A distributed Agentic Workflow System. One Master Node (powerful desktop) dispatches tasks to multiple Worker Nodes (Linux + Windows) over a shared Redis broker using the ARQ async job queue.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     MASTER NODE                         │
│  (this desktop)                                         │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐  ┌─────────────┐  │
│  │  Dispatcher  │──▶│  HITL Gate   │  │  Resource   │  │
│  │              │   │  (pause/     │  │  Guard      │  │
│  │  dispatch()  │   │   approve)   │  │  (CPU/RAM   │  │
│  │  get_result()│   └──────────────┘  │   cap)      │  │
│  └──────┬───────┘                     └─────────────┘  │
│         │ enqueue_job()                                 │
└─────────┼───────────────────────────────────────────────┘
          │
          ▼  Redis (shared broker)
     ┌─────────────────────────────┐
     │   Queue: nexus:tasks        │
     │   Results: arq:job:<id>     │
     │   HITL:   nexus:hitl:*      │
     │   Heartbeats: nexus:hb      │
     └──────────┬──────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
┌───────────────┐  ┌───────────────┐
│  WORKER NODE  │  │  WORKER NODE  │
│  (Linux)      │  │  (Windows)    │
│               │  │               │
│  listener.py  │  │  listener.py  │
│  execute_task │  │  execute_task │
│  TaskRegistry │  │  TaskRegistry │
└───────────────┘  └───────────────┘
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Redis running locally (or accessible via network)

```bash
# Start Redis with Docker
docker run -d -p 6379:6379 redis:7-alpine
```

### Install

```bash
# Clone / copy the project, then:
pip install -e ".[dev]"
cp .env.example .env
# Edit .env — set REDIS_URL, NODE_ID, resource caps
```

### Run the Master (this machine)

```bash
python scripts/start_master.py
# or: nexus-master
```

### Run a Worker (each remote machine)

Copy the project to the worker machine, install dependencies, set `.env`, then:

```bash
python scripts/start_worker.py
# or: nexus-worker
```

---

## Project Structure

```
nexus/
├── shared/
│   ├── schemas.py        # Pydantic task/result models — the wire contract
│   ├── constants.py      # Queue names, timeouts, HITL channels
│   ├── config.py         # Settings loaded from .env
│   └── logging_config.py # Structured JSON logging (structlog)
│
├── master/
│   ├── dispatcher.py     # Enqueues tasks, collects results, heartbeats
│   ├── resource_guard.py # CPU/RAM cap — keeps master quiet in background
│   └── hitl_gate.py      # Human-in-the-Loop pause/approve mechanism
│
└── worker/
    ├── listener.py       # ARQ WorkerSettings + execute_task entry point
    └── task_registry.py  # Maps task_type strings → async handler functions

scripts/
├── start_master.py       # Master entrypoint
└── start_worker.py       # Worker entrypoint
```

---

## Adding a New Task Type

1. Write an async handler anywhere in the codebase:

```python
from nexus.worker.task_registry import registry

@registry.register("llm.summarise")
async def summarise(parameters: dict) -> dict:
    text = parameters["text"]
    # ... call your LLM here ...
    return {"summary": "..."}
```

2. Import the module in `nexus/worker/listener.py` so the decorator runs at startup.

3. Dispatch it from the master:

```python
task = TaskPayload(task_type="llm.summarise", parameters={"text": "..."})
result = await dispatcher.dispatch_and_wait(task)
```

---

## Human-in-the-Loop (HITL)

Mark any task as requiring approval before it executes:

```python
task = TaskPayload(
    task_type="file.delete_all",
    parameters={"path": "/important/data"},
    requires_approval=True,
    approval_context="Worker will permanently delete /important/data. Approve?",
)
```

The Dispatcher suspends at `await hitl_gate.request_approval(task)` and
publishes a `HitlRequest` to `nexus:hitl:requests`.  An approval UI
(CLI, web dashboard, Slack bot) subscribes, shows the context to a human,
and publishes a `HitlResponse` back.  Only then does the task proceed to
the worker queue.

**Current state:** auto-approve stub is active in `hitl_gate.py`.
Wire a real UI to `HITL_REQUEST_CHANNEL` / `HITL_RESPONSE_CHANNEL` and
remove the stub to activate the gate.

---

## Resource Management

The Master runs at below-normal OS scheduling priority (Windows:
`BELOW_NORMAL_PRIORITY_CLASS`; Unix: `nice +10`).  The `ResourceGuard`
additionally monitors CPU and RAM every 5 seconds and inserts async sleeps
if CPU usage exceeds `MASTER_CPU_CAP_PERCENT`.  Both caps are configurable
in `.env` with no code changes required.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Shared broker URL |
| `NODE_ID` | `master` | Unique name for this node |
| `MASTER_CPU_CAP_PERCENT` | `25` | Max CPU % for master process |
| `MASTER_RAM_CAP_MB` | `512` | Max RAM MB for master process |
| `WORKER_MAX_JOBS` | `4` | Concurrent jobs per worker |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
