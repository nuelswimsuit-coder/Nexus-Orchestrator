"""
Alerting sub-system for the Nexus platform.

Built-in rules cover the six key failure modes; additional rules can be
registered at runtime.  Alerts are persisted in Redis, optionally forwarded
to Slack and/or Telegram, and always emitted as structured log events.

Usage
-----
    from nexus.shared.alerting import alert_manager
    import asyncio

    asyncio.create_task(alert_manager.start_monitoring(interval=30.0))
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable

import structlog
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

_ALERTS_ACTIVE_KEY = "nexus:alerts:active"
_COOLDOWN_KEY_PREFIX = "nexus:alerts:cooldown:"
_ALERTS_TTL = 86_400  # 24 h


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class Alert(BaseModel):
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    severity: AlertSeverity
    title: str
    message: str
    source: str
    timestamp: float = Field(default_factory=time.time)
    resolved: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


@dataclass
class AlertRule:
    name: str
    check: Callable[[], Awaitable[bool]]
    severity: AlertSeverity
    message_template: str
    cooldown_seconds: int = 300
    _extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class AlertManager:
    def __init__(self) -> None:
        self._rules: list[AlertRule] = []
        self._redis: Any | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------

    async def _get_redis(self) -> Any:
        if self._redis is None:
            import redis.asyncio as aioredis  # noqa: PLC0415
            from nexus.shared.config import settings  # noqa: PLC0415

            self._redis = aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return self._redis

    # ------------------------------------------------------------------
    # Rule registration
    # ------------------------------------------------------------------

    def add_rule(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    # ------------------------------------------------------------------
    # Built-in rules (registered lazily in check_all)
    # ------------------------------------------------------------------

    def _register_builtin_rules(self) -> None:
        """Register default rules.  Called once before first check."""
        from nexus.shared.metrics import metrics  # noqa: PLC0415

        # 1. Error rate > 5%
        async def _check_error_rate() -> bool:
            rate = await metrics.get_error_rate("*")
            self._builtin_state["error_rate"] = rate
            return rate > 0.05

        self.add_rule(
            AlertRule(
                name="high_error_rate",
                check=_check_error_rate,
                severity=AlertSeverity.CRITICAL,
                message_template="Task error rate at {error_rate:.1%}",
                cooldown_seconds=300,
            )
        )

        # 2. Worker heartbeat missing > 30s
        async def _check_worker_heartbeat() -> bool:
            try:
                r = await self._get_redis()
                now = time.time()
                cutoff = now - 30
                heartbeat_keys = await r.keys("nexus:workers:heartbeat:*")
                for k in heartbeat_keys:
                    val = await r.get(k)
                    if val and float(val) < cutoff:
                        wid = k.split(":")[-1]
                        self._builtin_state["unresponsive_worker"] = wid
                        return True
                return False
            except Exception:  # pylint: disable=broad-except
                return False

        self.add_rule(
            AlertRule(
                name="worker_heartbeat_missing",
                check=_check_worker_heartbeat,
                severity=AlertSeverity.CRITICAL,
                message_template="Worker {unresponsive_worker} is unresponsive",
                cooldown_seconds=60,
            )
        )

        # 3. Redis memory > 80%
        async def _check_redis_memory() -> bool:
            try:
                r = await self._get_redis()
                info = await r.info("memory")
                used = info.get("used_memory", 0)
                maxmem = info.get("maxmemory", 0)
                if maxmem == 0:
                    return False
                pct = used / maxmem
                self._builtin_state["redis_mem_pct"] = pct
                return pct > 0.80
            except Exception:  # pylint: disable=broad-except
                return False

        self.add_rule(
            AlertRule(
                name="redis_memory_high",
                check=_check_redis_memory,
                severity=AlertSeverity.WARNING,
                message_template="Redis memory usage critical: {redis_mem_pct:.0%}",
                cooldown_seconds=300,
            )
        )

        # 4. Circuit breaker OPEN
        async def _check_circuit_breaker() -> bool:
            try:
                r = await self._get_redis()
                cb_keys = await r.keys("nexus:metrics:gauge:circuit_state:*")
                for k in cb_keys:
                    state = await r.get(k)
                    if state and state.upper() == "OPEN":
                        wid = k.split(":")[-1]
                        self._builtin_state["circuit_worker"] = wid
                        return True
                return False
            except Exception:  # pylint: disable=broad-except
                return False

        self.add_rule(
            AlertRule(
                name="circuit_breaker_open",
                check=_check_circuit_breaker,
                severity=AlertSeverity.WARNING,
                message_template="Circuit breaker opened for {circuit_worker}",
                cooldown_seconds=120,
            )
        )

        # 5. Queue depth > 1000
        async def _check_queue_depth() -> bool:
            try:
                r = await self._get_redis()
                gauge_keys = await r.keys("nexus:metrics:gauge:queue_depth:*")
                for k in gauge_keys:
                    val = await r.get(k)
                    if val and float(val) > 1000:
                        self._builtin_state["queue_depth"] = int(float(val))
                        return True
                # Also check raw list lengths for known queues
                queue_keys = await r.keys("nexus:queue:*")
                for k in queue_keys:
                    depth = await r.llen(k)
                    if depth > 1000:
                        self._builtin_state["queue_depth"] = depth
                        return True
                return False
            except Exception:  # pylint: disable=broad-except
                return False

        self.add_rule(
            AlertRule(
                name="queue_backlog",
                check=_check_queue_depth,
                severity=AlertSeverity.WARNING,
                message_template="Task queue backlogged: {queue_depth} pending",
                cooldown_seconds=120,
            )
        )

        # 6. DLQ size > 50
        async def _check_dlq() -> bool:
            try:
                r = await self._get_redis()
                dlq_keys = await r.keys("nexus:dlq:*")
                total = 0
                for k in dlq_keys:
                    total += await r.llen(k)
                self._builtin_state["dlq_size"] = total
                return total > 50
            except Exception:  # pylint: disable=broad-except
                return False

        self.add_rule(
            AlertRule(
                name="dlq_backlog",
                check=_check_dlq,
                severity=AlertSeverity.WARNING,
                message_template="Dead letter queue has {dlq_size} failed tasks",
                cooldown_seconds=300,
            )
        )

    # Shared mutable state passed into message templates
    _builtin_state: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def check_all(self) -> None:
        for rule in self._rules:
            try:
                triggered = await rule.check()
                if not triggered:
                    continue

                # Cooldown check
                r = await self._get_redis()
                cooldown_key = f"{_COOLDOWN_KEY_PREFIX}{rule.name}"
                if await r.exists(cooldown_key):
                    continue  # still in cooldown

                # Build message
                try:
                    msg = rule.message_template.format(**self._builtin_state)
                except (KeyError, ValueError):
                    msg = rule.message_template

                alert = Alert(
                    severity=rule.severity,
                    title=rule.name.replace("_", " ").title(),
                    message=msg,
                    source="alert_manager",
                    metadata=dict(self._builtin_state),
                )
                await self.fire(alert)

                # Set cooldown
                await r.setex(cooldown_key, rule.cooldown_seconds, "1")

            except Exception as exc:  # pylint: disable=broad-except
                log.error("alert_rule_failed", rule=rule.name, error=str(exc))

    async def fire(self, alert: Alert) -> None:
        log.warning(
            "alert_fired",
            alert_id=alert.alert_id,
            severity=alert.severity,
            title=alert.title,
            message=alert.message,
            source=alert.source,
        )
        # Persist to Redis sorted set (score = timestamp)
        try:
            r = await self._get_redis()
            pipe = r.pipeline(transaction=False)
            serialized = alert.model_dump_json()
            pipe.zadd(_ALERTS_ACTIVE_KEY, {serialized: alert.timestamp})
            # Also store by ID for fast lookup
            pipe.setex(f"nexus:alerts:id:{alert.alert_id}", _ALERTS_TTL, serialized)
            await pipe.execute()
        except Exception as exc:  # pylint: disable=broad-except
            log.error("alert_persist_failed", error=str(exc))

        # Delivery channels (fire-and-forget, errors must not propagate)
        await asyncio.gather(
            self._send_slack(alert),
            self._send_telegram(alert),
            return_exceptions=True,
        )

    async def resolve(self, alert_id: str) -> None:
        try:
            r = await self._get_redis()
            raw = await r.get(f"nexus:alerts:id:{alert_id}")
            if not raw:
                return
            data = json.loads(raw)
            data["resolved"] = True
            serialized = json.dumps(data)
            pipe = r.pipeline(transaction=False)
            # Remove old score-keyed entry — we iterate and remove by member
            all_members = await r.zrange(_ALERTS_ACTIVE_KEY, 0, -1)
            for member in all_members:
                try:
                    m = json.loads(member)
                    if m.get("alert_id") == alert_id:
                        pipe.zrem(_ALERTS_ACTIVE_KEY, member)
                        break
                except (json.JSONDecodeError, AttributeError):
                    continue
            pipe.setex(f"nexus:alerts:id:{alert_id}", _ALERTS_TTL, serialized)
            await pipe.execute()
            log.info("alert_resolved", alert_id=alert_id)
        except Exception as exc:  # pylint: disable=broad-except
            log.error("alert_resolve_failed", alert_id=alert_id, error=str(exc))

    async def get_active(self) -> list[Alert]:
        try:
            r = await self._get_redis()
            members = await r.zrange(_ALERTS_ACTIVE_KEY, 0, -1)
            alerts: list[Alert] = []
            for raw in members:
                try:
                    data = json.loads(raw)
                    if not data.get("resolved"):
                        alerts.append(Alert.model_validate(data))
                except (json.JSONDecodeError, Exception):  # pylint: disable=broad-except
                    continue
            return alerts
        except Exception as exc:  # pylint: disable=broad-except
            log.error("get_active_alerts_failed", error=str(exc))
            return []

    # ------------------------------------------------------------------
    # Delivery channels
    # ------------------------------------------------------------------

    async def _send_slack(self, alert: Alert) -> None:
        webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
        if not webhook_url:
            return
        try:
            import aiohttp  # noqa: PLC0415

            colour = {"info": "#36a64f", "warning": "#ffa500", "critical": "#ff0000"}.get(
                alert.severity.value, "#cccccc"
            )
            payload = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"[{alert.severity.value.upper()}] {alert.title}",
                        },
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": alert.message},
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"source: `{alert.source}` | id: `{alert.alert_id}`",
                            }
                        ],
                    },
                ],
                "attachments": [{"color": colour}],
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status >= 400:
                        log.warning("slack_webhook_error", status=resp.status)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("slack_send_failed", error=str(exc))

    async def _send_telegram(self, alert: Alert) -> None:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")
        if not bot_token or not chat_id:
            return
        try:
            import aiohttp  # noqa: PLC0415

            severity_emoji = {
                "info": "ℹ️",
                "warning": "⚠️",
                "critical": "🔴",
            }.get(alert.severity.value, "❓")
            text = (
                f"{severity_emoji} *[{alert.severity.value.upper()}] {alert.title}*\n"
                f"{alert.message}\n"
                f"`source: {alert.source}`\n"
                f"`id: {alert.alert_id}`"
            )
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        log.warning("telegram_send_error", status=resp.status, body=body[:200])
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("telegram_send_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Background monitoring loop
    # ------------------------------------------------------------------

    async def start_monitoring(self, interval: float = 30.0) -> None:
        """Runs forever; call with asyncio.create_task()."""
        if not self._rules:
            self._register_builtin_rules()
        self._running = True
        log.info("alert_monitoring_started", interval=interval)
        while self._running:
            try:
                await self.check_all()
            except Exception as exc:  # pylint: disable=broad-except
                log.error("alert_check_loop_error", error=str(exc))
            await asyncio.sleep(interval)

    def stop_monitoring(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

alert_manager = AlertManager()
