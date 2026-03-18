"""
Shared constants — queue names, routing keys, and timeout values.

Both master and workers import from here so queue names never drift out of sync.
"""

# ── ARQ queue names ────────────────────────────────────────────────────────────
# The master enqueues tasks onto TASK_QUEUE; workers consume from it.
TASK_QUEUE = "nexus:tasks"

# Workers publish completed results onto RESULT_QUEUE; master reads from it.
# (With ARQ the result is stored per-job-id in Redis; this constant is kept
#  for any custom pub/sub result streaming added later.)
RESULT_QUEUE = "nexus:results"

# ── HITL (Human-in-the-Loop) channel ──────────────────────────────────────────
# When a task requires human approval before proceeding, the master publishes
# a HitlRequest to this channel and suspends the job.  The approval UI (CLI,
# web dashboard, or Slack bot — TBD) subscribes here and sends back a decision.
HITL_REQUEST_CHANNEL = "nexus:hitl:requests"
HITL_RESPONSE_CHANNEL = "nexus:hitl:responses"

# ── Timeouts (seconds) ────────────────────────────────────────────────────────
TASK_DEFAULT_TIMEOUT = 300       # 5 min — worker hard-kills a job after this
HITL_APPROVAL_TIMEOUT = 3600     # 1 hour — HITL gate expires if no human responds
RESULT_TTL = 86400               # 24 h — how long ARQ keeps job results in Redis

# ── Task priority levels ──────────────────────────────────────────────────────
PRIORITY_HIGH = 1
PRIORITY_NORMAL = 5
PRIORITY_LOW = 10
