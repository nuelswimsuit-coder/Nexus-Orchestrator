"""
Nexus Supreme — Unified SQLAlchemy Schema
Merges data from: Nexus-Orchestrator + Management AHU (TeleFix)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint, create_engine, event
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ── Telegram Sessions ──────────────────────────────────────────────────────────

class TgSession(Base):
    __tablename__ = "tg_sessions"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    stem        = Column(String, nullable=False, unique=True)      # filename without .session
    category    = Column(String, default="general")               # managers/bots/adders/spammers/general
    phone       = Column(String)
    username    = Column(String)
    is_premium  = Column(Boolean, default=False)
    is_active   = Column(Boolean, default=True)
    last_used   = Column(DateTime)
    last_checked= Column(DateTime)
    session_path= Column(String)
    notes       = Column(Text, default="")
    created_at  = Column(DateTime, default=_utcnow)

    enrollments = relationship("Enrollment", back_populates="session", cascade="all, delete-orphan")


# ── Scraped Users ──────────────────────────────────────────────────────────────

class ScrapedUser(Base):
    __tablename__ = "scraped_users"
    __table_args__ = (UniqueConstraint("user_id"),)

    user_id           = Column(Integer, primary_key=True)
    access_hash       = Column(Integer)
    username          = Column(String)
    first_name        = Column(String)
    last_name         = Column(String)
    source_group      = Column(String)
    is_premium        = Column(Integer, default=0)
    last_active       = Column(Integer)
    scraped_by_session= Column(String)
    added_at          = Column(DateTime, default=_utcnow)


# ── Target Groups ──────────────────────────────────────────────────────────────

class Target(Base):
    __tablename__ = "targets"

    id    = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String)
    link  = Column(String)
    role  = Column(String, default="target")   # source | target


# ── Managed Groups ─────────────────────────────────────────────────────────────

class ManagedGroup(Base):
    __tablename__ = "managed_groups"

    group_id        = Column(Integer, primary_key=True)
    title           = Column(String)
    username        = Column(String)
    owner_session   = Column(String)
    last_automation = Column(DateTime)


# ── Enrollments (add history) ──────────────────────────────────────────────────

class Enrollment(Base):
    __tablename__ = "enrollments"
    __table_args__ = (UniqueConstraint("user_id", "target_link"),)

    id          = Column(Integer, primary_key=True, autoincrement=True)
    user_id     = Column(Integer, ForeignKey("tg_sessions.id", ondelete="SET NULL"), nullable=True)
    target_link = Column(String, nullable=False)
    status      = Column(String, nullable=False)   # success | failed | flood | banned
    timestamp   = Column(DateTime, default=_utcnow)

    session     = relationship("TgSession", back_populates="enrollments")


# ── Bots (managed bot fleet) ──────────────────────────────────────────────────

class ManagedBot(Base):
    __tablename__ = "managed_bots"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String, nullable=False)
    username      = Column(String, unique=True)
    bot_token     = Column(String)
    owner_session = Column(String)
    niche         = Column(String)
    channel_id    = Column(String)
    keywords      = Column(Text, default="[]")     # JSON array
    auto_start    = Column(Boolean, default=False)
    is_active     = Column(Boolean, default=True)
    # SEO fields
    search_rank   = Column(Integer, default=-1)
    start_count   = Column(Integer, default=0)
    unique_users  = Column(Integer, default=0)
    last_rank_check = Column(DateTime)
    last_start    = Column(DateTime)
    last_scanned  = Column(DateTime, default=_utcnow)
    created_at    = Column(DateTime, default=_utcnow)
    stats_json    = Column(Text, default="{}")     # arbitrary JSON metrics

    @property
    def keyword_list(self) -> list[str]:
        try:
            return json.loads(self.keywords or "[]")
        except Exception:
            return []

    @property
    def stats(self) -> dict:
        try:
            return json.loads(self.stats_json or "{}")
        except Exception:
            return {}

    start_events = relationship("BotStartEvent", back_populates="bot", cascade="all, delete-orphan")


# ── Bot /start event log ───────────────────────────────────────────────────────

class BotStartEvent(Base):
    __tablename__ = "bot_start_events"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    bot_id      = Column(Integer, ForeignKey("managed_bots.id", ondelete="CASCADE"))
    user_id     = Column(Integer, nullable=False)
    is_new_user = Column(Boolean, default=False)
    referral    = Column(String)               # deep-link param
    timestamp   = Column(DateTime, default=_utcnow)

    bot         = relationship("ManagedBot", back_populates="start_events")


# ── Chat Archive ───────────────────────────────────────────────────────────────

class ArchivedChat(Base):
    __tablename__ = "archived_chats"
    __table_args__ = (UniqueConstraint("chat_id"),)

    id          = Column(Integer, primary_key=True, autoincrement=True)
    chat_id     = Column(Integer, nullable=False, unique=True)
    title       = Column(String)
    chat_type   = Column(String)             # user | group | channel
    session_stem= Column(String)
    last_synced = Column(DateTime)
    total_msgs  = Column(Integer, default=0)
    created_at  = Column(DateTime, default=_utcnow)

    messages    = relationship("ArchivedMessage", back_populates="chat", cascade="all, delete-orphan")


class ArchivedMessage(Base):
    __tablename__ = "archived_messages"
    __table_args__ = (UniqueConstraint("chat_id", "msg_id"),)

    id          = Column(Integer, primary_key=True, autoincrement=True)
    chat_id     = Column(Integer, ForeignKey("archived_chats.chat_id", ondelete="CASCADE"))
    msg_id      = Column(Integer, nullable=False)
    sender_id   = Column(Integer)
    sender_name = Column(String)
    text        = Column(Text, default="")
    media_type  = Column(String)             # photo | video | document | None
    media_path  = Column(String)             # local path after download
    media_size  = Column(Integer)            # bytes
    timestamp   = Column(DateTime)
    # AI analysis fields (populated by /analyze command)
    ai_tags     = Column(Text, default="[]") # JSON: ["lead","task","question"]
    ai_sentiment= Column(Float)              # -1.0 to 1.0
    ai_summary  = Column(Text, default="")

    chat        = relationship("ArchivedChat", back_populates="messages")


# ── Settings ───────────────────────────────────────────────────────────────────

class Setting(Base):
    __tablename__ = "settings"

    key        = Column(String, primary_key=True)
    value      = Column(Text)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


class Metric(Base):
    __tablename__ = "metrics"

    key        = Column(String, primary_key=True)
    value      = Column(Float)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


# ── Remote CLI Audit Log ───────────────────────────────────────────────────────

class CliAuditLog(Base):
    __tablename__ = "cli_audit_log"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, nullable=False)
    command     = Column(Text, nullable=False)
    exit_code   = Column(Integer)
    output_head = Column(Text, default="")   # first 500 chars of stdout
    timestamp   = Column(DateTime, default=_utcnow)


# ── DB factory ────────────────────────────────────────────────────────────────

_engine = None


def get_engine(db_path: str = "data/nexus_supreme.db"):
    global _engine
    if _engine is None:
        from pathlib import Path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{db_path}"
        _engine = create_engine(url, connect_args={"check_same_thread": False})

        @event.listens_for(_engine, "connect")
        def _set_pragmas(conn, _):
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(_engine)
    return _engine


def get_session(db_path: str = "data/nexus_supreme.db") -> Session:
    from sqlalchemy.orm import sessionmaker
    engine = get_engine(db_path)
    return sessionmaker(bind=engine)()
