import os
import hmac
import hashlib
import json
import time
import pytest
from fastapi.testclient import TestClient

# Set environment variables for testing before importing main
os.environ["DATABASE_URL"] = "sqlite:///./test.db"
os.environ["SIGNATURE_VERIFICATION_ENABLED"] = "true"
os.environ["PSEUDOGRAM_API_KEY"] = "test_key"
os.environ["MAX_RETRIES"] = "3"

from app.database import Base, engine, SessionLocal, Rule, ReceivedEvent, DeletedComment, DM, BlockedDuplicate
from app.main import app

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_and_teardown():
    # Setup: create schemas
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    # Clear tables
    db.query(Rule).delete()
    db.query(ReceivedEvent).delete()
    db.query(DeletedComment).delete()
    db.query(DM).delete()
    db.query(BlockedDuplicate).delete()
    db.commit()
    db.close()
    yield
    # Teardown: drop tables
    Base.metadata.drop_all(bind=engine)

def get_signature_headers(payload_dict: dict, key: str = "test_key"):
    body_str = json.dumps(payload_dict, separators=(',', ':'))
    sig = hmac.new(key.encode(), body_str.encode(), hashlib.sha256).hexdigest()
    return {"X-PseudoGram-Signature": f"sha256={sig}"}, body_str

def test_create_rule():
    payload = {"keyword": "PRICE", "dm_message": "Hello, here is the price!"}
    response = client.post("/rules", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert "rule_id" in data
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Hello, here is the price!"

def test_webhook_signature_verification():
    payload = {
        "event_id": "evt_001",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_001",
            "post_id": "post_1",
            "text": "PRICE please!",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_1",
                "username": "tester"
            }
        }
    }
    # No signature header -> 401
    response = client.post("/webhook", json=payload)
    assert response.status_code == 401

    # Invalid signature -> 401
    response = client.post("/webhook", json=payload, headers={"X-PseudoGram-Signature": "sha256=invalid"})
    assert response.status_code == 401

    # Valid signature -> 200
    headers, body_str = get_signature_headers(payload)
    response = client.post("/webhook", content=body_str, headers=headers)
    assert response.status_code == 200

def test_webhook_dedup_and_rule_matching():
    # 1. Create rule
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list!"})

    # 2. Trigger webhook
    payload = {
        "event_id": "evt_002",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_002",
            "post_id": "post_1",
            "text": "How much is the PRICE?",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_123",
                "username": "tester"
            }
        }
    }
    headers, body_str = get_signature_headers(payload)
    response = client.post("/webhook", content=body_str, headers=headers)
    assert response.status_code == 200

    # Verify event is received and queued in DB
    db = SessionLocal()
    event_in_db = db.query(ReceivedEvent).filter(ReceivedEvent.event_id == "evt_002").first()
    assert event_in_db is not None
    assert event_in_db.text == "How much is the PRICE?"

    dm_in_db = db.query(DM).filter(DM.comment_id == "cmt_002").first()
    assert dm_in_db is not None
    assert dm_in_db.recipient_user_id == "usr_123"
    assert dm_in_db.status == "queued"
    
    # 3. Duplicate event delivery -> should ignore
    response_dup = client.post("/webhook", content=body_str, headers=headers)
    assert response_dup.status_code == 200
    assert response_dup.json()["detail"] == "duplicate event ignored"

    # Verify only one DM was queued
    dms_count = db.query(DM).filter(DM.recipient_user_id == "usr_123").count()
    assert dms_count == 1
    db.close()

def test_user_rule_dedup():
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list!"})

    # First comment by usr_123 -> queues DM
    payload_1 = {
        "event_id": "evt_c1",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_c1",
            "post_id": "post_1",
            "text": "PRICE please!",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_123",
                "username": "tester"
            }
        }
    }
    headers_1, body_1 = get_signature_headers(payload_1)
    client.post("/webhook", content=body_1, headers=headers_1)

    # Second comment by usr_123 matching same rule -> should be blocked
    payload_2 = {
        "event_id": "evt_c2",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:23.481Z",
        "data": {
            "comment_id": "cmt_c2",
            "post_id": "post_1",
            "text": "Also check the PRICE here",
            "created_at": "2026-08-10T09:14:23.900Z",
            "from": {
                "user_id": "usr_123",
                "username": "tester"
            }
        }
    }
    headers_2, body_2 = get_signature_headers(payload_2)
    client.post("/webhook", content=body_2, headers=headers_2)

    db = SessionLocal()
    # Check that only one DM exists for usr_123
    dms = db.query(DM).filter(DM.recipient_user_id == "usr_123").all()
    assert len(dms) == 1

    # Check that duplicate block is recorded
    blocked = db.query(BlockedDuplicate).filter(BlockedDuplicate.user_id == "usr_123").all()
    assert len(blocked) == 1
    assert blocked[0].comment_id == "cmt_c2"
    db.close()

def test_comment_deleted_normal():
    # 1. Create rule
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price!"})

    # 2. Created comment -> queues DM
    payload_created = {
        "event_id": "evt_created",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_del1",
            "post_id": "post_1",
            "text": "PRICE please!",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_del1",
                "username": "tester"
            }
        }
    }
    headers_c, body_c = get_signature_headers(payload_created)
    client.post("/webhook", content=body_c, headers=headers_c)

    db = SessionLocal()
    dm = db.query(DM).filter(DM.comment_id == "cmt_del1").first()
    assert dm is not None
    assert dm.status == "queued"

    # 3. Deleted comment arrives -> cancels DM
    payload_deleted = {
        "event_id": "evt_deleted",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:23.481Z",
        "data": {
            "comment_id": "cmt_del1"
        }
    }
    headers_d, body_d = get_signature_headers(payload_deleted)
    client.post("/webhook", content=body_d, headers=headers_d)

    db.refresh(dm)
    assert dm.status == "suppressed"
    db.close()

def test_comment_deleted_out_of_order():
    # 1. Create rule
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price!"})

    # 2. Deleted comment event arrives FIRST
    payload_deleted = {
        "event_id": "evt_deleted_first",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_ooo"
        }
    }
    headers_d, body_d = get_signature_headers(payload_deleted)
    client.post("/webhook", content=body_d, headers=headers_d)

    # 3. Created comment event arrives LATER
    payload_created = {
        "event_id": "evt_created_later",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:23.481Z",
        "data": {
            "comment_id": "cmt_ooo",
            "post_id": "post_1",
            "text": "PRICE please!",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_ooo",
                "username": "tester"
            }
        }
    }
    headers_c, body_c = get_signature_headers(payload_created)
    client.post("/webhook", content=body_c, headers=headers_c)

    # Verify no DM was ever created
    db = SessionLocal()
    dm = db.query(DM).filter(DM.comment_id == "cmt_ooo").first()
    assert dm is None
    db.close()

def test_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["sent"] == 0
    assert data["failed"] == 0
    assert data["queued"] == 0
    assert data["duplicates_blocked"] == 0

    db = SessionLocal()
    rule = Rule(id="r1", keyword="PRICE", dm_message="Price!")
    db.add(rule)
    db.commit()

    dms = [
        DM(id="d1", recipient_user_id="u1", comment_id="c1", rule_id="r1", status="sent", message="...", idempotency_key="i1", updated_at="..."),
        DM(id="d2", recipient_user_id="u2", comment_id="c2", rule_id="r1", status="sent", message="...", idempotency_key="i2", updated_at="..."),
        DM(id="d3", recipient_user_id="u3", comment_id="c3", rule_id="r1", status="failed", message="...", idempotency_key="i3", updated_at="..."),
        DM(id="d4", recipient_user_id="u4", comment_id="c4", rule_id="r1", status="queued", message="...", idempotency_key="i4", updated_at="..."),
        DM(id="d5", recipient_user_id="u5", comment_id="c5", rule_id="r1", status="sending", message="...", idempotency_key="i5", updated_at="..."),
        DM(id="d6", recipient_user_id="u6", comment_id="c6", rule_id="r1", status="accepted", message="...", idempotency_key="i6", updated_at="..."),
    ]
    for d in dms:
        db.add(d)
    
    dups = [
        BlockedDuplicate(comment_id="c_dup1", rule_id="r1", user_id="u1", blocked_at=time.time()),
        BlockedDuplicate(comment_id="c_dup2", rule_id="r1", user_id="u2", blocked_at=time.time()),
    ]
    for dup in dups:
        db.add(dup)
        
    db.commit()
    db.close()

    response = client.get("/stats")
    data = response.json()
    assert data["sent"] == 2
    assert data["failed"] == 1
    assert data["queued"] == 3
    assert data["duplicates_blocked"] == 2
