import os
from sqlalchemy import create_engine, Column, String, Integer, Float, ForeignKey, UniqueConstraint, event
from sqlalchemy.orm import declarative_base, sessionmaker

# On Render, use the persistent disk at /data. Locally fall back to ./linkplease.db
_default_db = "sqlite:////data/linkplease.db" if os.getenv("RENDER") else "sqlite:///./linkplease.db"
DATABASE_URL = os.getenv("DATABASE_URL", _default_db)

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {
        "timeout": 30.0,
        "check_same_thread": False,
    }

engine = create_engine(DATABASE_URL, connect_args=connect_args)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Rule(Base):
    __tablename__ = "rules"
    id = Column(String, primary_key=True, index=True)
    keyword = Column(String, unique=True, index=True, nullable=False)
    dm_message = Column(String, nullable=False)

class ReceivedEvent(Base):
    __tablename__ = "received_events"
    event_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, nullable=False)
    comment_id = Column(String, index=True, nullable=True)
    text = Column(String, nullable=True)
    user_id = Column(String, index=True, nullable=True)
    username = Column(String, nullable=True)
    created_at = Column(String, nullable=True)
    sent_at = Column(String, nullable=True)
    received_at = Column(String, nullable=False)

class DeletedComment(Base):
    __tablename__ = "deleted_comments"
    comment_id = Column(String, primary_key=True, index=True)
    deleted_at = Column(String, nullable=False)

class DM(Base):
    __tablename__ = "dms"
    id = Column(String, primary_key=True)  # Locally generated UUID
    dm_id = Column(String, index=True, nullable=True)  # Mock API's dm_id
    recipient_user_id = Column(String, index=True, nullable=False)
    comment_id = Column(String, index=True, nullable=False)
    rule_id = Column(String, ForeignKey("rules.id"), nullable=False)
    status = Column(String, index=True, nullable=False)  # 'queued', 'sending', 'sent', 'failed', 'suppressed'
    message = Column(String, nullable=False)
    retry_count = Column(Integer, default=0, nullable=False)
    next_retry_at = Column(Float, nullable=True)  # Epoch timestamp
    idempotency_key = Column(String, unique=True, nullable=False)
    error_detail = Column(String, nullable=True)
    updated_at = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint('recipient_user_id', 'rule_id', name='uq_recipient_rule'),
    )


class BlockedDuplicate(Base):
    __tablename__ = "blocked_duplicates"
    comment_id = Column(String, primary_key=True)
    rule_id = Column(String, primary_key=True)
    user_id = Column(String, index=True, nullable=False)
    blocked_at = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint('comment_id', 'rule_id', name='uq_blocked_comment_rule'),
    )

def init_db():
    Base.metadata.create_all(bind=engine)

