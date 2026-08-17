import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import time
from typing import List, Optional
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.database import init_db, SessionLocal, Rule, ReceivedEvent, DeletedComment, DM, BlockedDuplicate

# Load environment variables
load_dotenv()

API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")
BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
SIGNATURE_VERIFICATION_ENABLED = os.getenv("SIGNATURE_VERIFICATION_ENABLED", "true").lower() == "true"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
# Max concurrent DM sends at once
DM_CONCURRENCY = int(os.getenv("DM_CONCURRENCY", "10"))

# Semaphore for limiting concurrent DM sends
_dm_semaphore: asyncio.Semaphore = None

# Database session dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Signature verification function
def verify_signature(raw_body: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected_sig = signature_header.split("sha256=")[1]
    computed_sig = hmac.new(
        secret.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(computed_sig, expected_sig)

# ----------------------------------------------------------
# DM Send: sends a single DM with retry logic
# ----------------------------------------------------------
async def send_single_dm(dm_id_local: str):
    """Attempt to send one DM (fetch from DB, send, update status)."""
    global _dm_semaphore

    async with _dm_semaphore:
        # Fetch current DM state
        with SessionLocal() as db:
            dm = db.query(DM).filter(DM.id == dm_id_local).first()
            if not dm:
                return
            if dm.status not in ("queued", "failed_retry"):
                return
            # Check if comment has been deleted since scheduling
            comment_deleted = db.query(DeletedComment).filter(
                DeletedComment.comment_id == dm.comment_id
            ).first()
            if comment_deleted:
                dm.status = "suppressed"
                dm.error_detail = "Comment deleted before sending DM"
                dm.updated_at = datetime.now(timezone.utc).isoformat()
                db.commit()
                print(f"DM {dm.id} suppressed because comment {dm.comment_id} was deleted.")
                return

            # Mark as sending to reserve it
            dm.status = "sending"
            dm.updated_at = datetime.now(timezone.utc).isoformat()
            db.commit()
            db.refresh(dm)

            recipient_user_id = dm.recipient_user_id
            message = dm.message
            comment_id = dm.comment_id
            idempotency_key = dm.idempotency_key
            retry_count = dm.retry_count

        # Make HTTP call
        try:
            headers = {
                "X-API-Key": API_KEY,
                "Idempotency-Key": idempotency_key,
                "Content-Type": "application/json"
            }
            payload = {
                "recipient_user_id": recipient_user_id,
                "message": message,
                "comment_id": comment_id
            }

            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{BASE_URL}/v1/dm/send",
                    json=payload,
                    headers=headers
                )

            if response.status_code == 202:
                res_data = response.json()
                api_dm_id = res_data.get("dm_id")
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == dm_id_local).first()
                    if db_dm:
                        db_dm.dm_id = api_dm_id
                        db_dm.status = "accepted"
                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                        db.commit()
                print(f"DM send accepted: local={dm_id_local}, api={api_dm_id}")

            elif response.status_code == 429:
                retry_after = 60.0
                try:
                    retry_after = float(response.headers.get("Retry-After", 60.0))
                except (ValueError, TypeError):
                    pass
                next_retry = time.time() + retry_after
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == dm_id_local).first()
                    if db_dm:
                        db_dm.status = "failed_retry"
                        db_dm.retry_count = retry_count + 1
                        db_dm.next_retry_at = next_retry
                        db_dm.error_detail = f"API 429: Retry-After {retry_after}s"
                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                        db.commit()
                print(f"API 429 rate limit. Retrying DM {dm_id_local} in {retry_after}s.")
                await asyncio.sleep(retry_after)

            elif response.status_code == 500:
                backoff = min(2 ** retry_count, 60.0)
                next_retry = time.time() + backoff
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == dm_id_local).first()
                    if db_dm:
                        if retry_count + 1 >= MAX_RETRIES:
                            db_dm.status = "failed"
                            db_dm.error_detail = "API 500: Max retries exceeded"
                        else:
                            db_dm.status = "failed_retry"
                            db_dm.retry_count = retry_count + 1
                            db_dm.next_retry_at = next_retry
                            db_dm.error_detail = "API 500: Internal Server Error"
                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                        db.commit()
                print(f"API 500 error. Retrying DM {dm_id_local} in {backoff}s.")

            else:
                error_msg = f"API {response.status_code}: {response.text[:200]}"
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == dm_id_local).first()
                    if db_dm:
                        db_dm.status = "failed"
                        db_dm.error_detail = error_msg
                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                        db.commit()
                print(f"Terminal failure for DM {dm_id_local}: {error_msg}")

        except httpx.RequestError as exc:
            backoff = min(2 ** retry_count, 60.0)
            next_retry = time.time() + backoff
            with SessionLocal() as db:
                db_dm = db.query(DM).filter(DM.id == dm_id_local).first()
                if db_dm:
                    if retry_count + 1 >= MAX_RETRIES:
                        db_dm.status = "failed"
                        db_dm.error_detail = f"Network error: {str(exc)}"
                    else:
                        db_dm.status = "failed_retry"
                        db_dm.retry_count = retry_count + 1
                        db_dm.next_retry_at = next_retry
                        db_dm.error_detail = f"Network error: {str(exc)}"
                    db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                    db.commit()
            print(f"Network error sending DM {dm_id_local}: {exc}. Retrying in {backoff}s.")


# ----------------------------------------------------------
# Background DM Sender Worker — concurrent batch dispatch
# ----------------------------------------------------------
def _claim_dm_batch(now: float) -> list:
    """
    Atomically fetch and claim a batch of DMs for sending.

    Runs in a thread-pool thread (called via asyncio.to_thread).
    Marks each selected row status='sending' in the same transaction
    before returning their IDs, so a concurrent worker process cannot
    claim the same rows (SQLite WAL serialises writers; only the first
    commit wins — the second updates 0 rows and gets an empty list).
    """
    with SessionLocal() as db:
        dms = db.query(DM).filter(
            (DM.status == "queued") |
            ((DM.status == "failed_retry") & (DM.next_retry_at <= now))
        ).order_by(DM.updated_at.asc()).limit(DM_CONCURRENCY).all()

        if not dms:
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        claimed_ids = []
        for dm in dms:
            # Re-check status inside the write transaction — another worker
            # process may have already claimed this row.
            if dm.status in ("queued", "failed_retry"):
                dm.status = "sending"
                dm.updated_at = now_iso
                claimed_ids.append(dm.id)

        if claimed_ids:
            db.commit()
        return claimed_ids


async def send_dms_worker():
    print("DM Sender Worker started.")
    while True:
        try:
            now = time.time()

            # Atomically claim a batch (runs in thread-pool to avoid blocking
            # the event loop with synchronous SQLAlchemy I/O).
            dm_ids = await asyncio.to_thread(_claim_dm_batch, now)

            if not dm_ids:
                await asyncio.sleep(0.1)
                continue

            # Fire all sends concurrently (each respects semaphore)
            await asyncio.gather(*[send_single_dm(dm_id) for dm_id in dm_ids])

        except asyncio.CancelledError:
            print("DM Sender Worker cancelled.")
            break
        except Exception as e:
            print(f"Error in send_dms_worker loop: {e}")
            await asyncio.sleep(1)


# ----------------------------------------------------------
# Background DM Reconciliation Worker
# ----------------------------------------------------------
async def reconcile_single_dm(local_id: str, api_dm_id: str, retry_count: int):
    """Poll status for one accepted DM and update DB accordingly."""
    try:
        headers = {"X-API-Key": API_KEY}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BASE_URL}/v1/dm/{api_dm_id}",
                headers=headers
            )

        if response.status_code == 200:
            res_data = response.json()
            api_status = res_data.get("status")

            if api_status == "delivered":
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == local_id).first()
                    if db_dm and db_dm.status == "accepted":
                        db_dm.status = "sent"
                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                        db.commit()
                print(f"DM {api_dm_id} delivered → marked sent.")

            elif api_status == "failed":
                with SessionLocal() as db:
                    db_dm = db.query(DM).filter(DM.id == local_id).first()
                    if db_dm and db_dm.status == "accepted":
                        if retry_count + 1 >= MAX_RETRIES:
                            db_dm.status = "failed"
                            db_dm.error_detail = "Mock API reported delivery failure, retries exhausted"
                            db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                            db.commit()
                            print(f"DM {api_dm_id} delivery failed. Max retries reached.")
                        else:
                            # Reset to queued with a fresh idempotency key
                            db_dm.status = "queued"
                            db_dm.dm_id = None
                            db_dm.idempotency_key = str(uuid.uuid4())
                            db_dm.retry_count = retry_count + 1
                            db_dm.next_retry_at = 0.0
                            db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                            db.commit()
                            print(f"DM {api_dm_id} delivery failed. Queued for fresh retry.")

            # elif api_status == "queued": still in flight, do nothing

        elif response.status_code == 404:
            with SessionLocal() as db:
                db_dm = db.query(DM).filter(DM.id == local_id).first()
                if db_dm and db_dm.status == "accepted":
                    db_dm.status = "failed"
                    db_dm.error_detail = "DM ID not found on mock API"
                    db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                    db.commit()
            print(f"DM {api_dm_id} not found on API (404). Marked failed.")

    except httpx.RequestError as exc:
        print(f"Network error reconciling DM {api_dm_id}: {exc}")


async def reconcile_dms_worker():
    print("DM Reconciler Worker started.")
    while True:
        try:
            with SessionLocal() as db:
                pending_dms = db.query(DM).filter(DM.status == "accepted").all()
                dms_to_check = [
                    {"id": d.id, "dm_id": d.dm_id, "retry_count": d.retry_count}
                    for d in pending_dms if d.dm_id
                ]

            if dms_to_check:
                await asyncio.gather(*[
                    reconcile_single_dm(d["id"], d["dm_id"], d["retry_count"])
                    for d in dms_to_check
                ])

            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            print("DM Reconciler Worker cancelled.")
            break
        except Exception as e:
            print(f"Error in reconcile_dms_worker loop: {e}")
            await asyncio.sleep(2)


# ----------------------------------------------------------
# Lifespan manager
# ----------------------------------------------------------
background_tasks_set = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _dm_semaphore
    _dm_semaphore = asyncio.Semaphore(DM_CONCURRENCY)

    # Ensure database tables exist
    init_db()

    # Start workers
    loop = asyncio.get_running_loop()
    send_task = loop.create_task(send_dms_worker())
    reconcile_task = loop.create_task(reconcile_dms_worker())

    background_tasks_set.add(send_task)
    background_tasks_set.add(reconcile_task)

    yield

    # Clean up tasks on shutdown
    send_task.cancel()
    reconcile_task.cancel()
    await asyncio.gather(send_task, reconcile_task, return_exceptions=True)


# Create FastAPI app
app = FastAPI(title="LinkPlease Instagram Automation", lifespan=lifespan)

# Pydantic models for request validation
class RuleCreate(BaseModel):
    keyword: str
    dm_message: str

class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


# =============================================================
# ENDPOINTS
# =============================================================

@app.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(rule_in: RuleCreate, db: Session = Depends(get_db)):
    """Create a keyword automation rule. Keyword matching is case-insensitive substring."""
    rule_id = str(uuid.uuid4())
    db_rule = Rule(id=rule_id, keyword=rule_in.keyword, dm_message=rule_in.dm_message)
    try:
        db.add(db_rule)
        db.commit()
        db.refresh(db_rule)
    except IntegrityError:
        db.rollback()
        existing = db.query(Rule).filter(Rule.keyword == rule_in.keyword).first()
        if existing:
            return RuleResponse(
                rule_id=existing.id,
                keyword=existing.keyword,
                dm_message=existing.dm_message
            )
        raise HTTPException(status_code=400, detail="Rule constraint violation")

    return RuleResponse(
        rule_id=db_rule.id,
        keyword=db_rule.keyword,
        dm_message=db_rule.dm_message
    )


# ----------------------------------------------------------
# Synchronous DB helpers — each manages its own session so they
# can safely run in asyncio.to_thread() without blocking the event loop.
# ----------------------------------------------------------

def _check_and_insert_event(
    event_id: str,
    event_type: str,
    comment_id,
    text,
    user_id,
    username,
    created_at,
    sent_at,
) -> str:
    """
    Deduplicate by event_id and insert a ReceivedEvent row.

    Returns:
        "duplicate"  — event already exists, caller should return 200 early.
        "inserted"   — new event was recorded.
    """
    with SessionLocal() as db:
        existing = db.query(ReceivedEvent).filter(
            ReceivedEvent.event_id == event_id
        ).first()
        if existing:
            return "duplicate"

        db_event = ReceivedEvent(
            event_id=event_id,
            event_type=event_type,
            comment_id=comment_id,
            text=text,
            user_id=user_id,
            username=username,
            created_at=created_at,
            sent_at=sent_at,
            received_at=datetime.now(timezone.utc).isoformat(),
        )
        db.add(db_event)
        try:
            db.commit()
            return "inserted"
        except IntegrityError:
            db.rollback()
            return "duplicate"


def _handle_comment_deleted(comment_id: str) -> None:
    """Record a deleted comment and suppress any in-flight DMs for it."""
    with SessionLocal() as db:
        db_del = DeletedComment(
            comment_id=comment_id,
            deleted_at=datetime.now(timezone.utc).isoformat(),
        )
        db.add(db_del)

        # Suppress any queued/retrying/sending DMs for this comment
        db.query(DM).filter(
            (DM.comment_id == comment_id)
            & (DM.status.in_(["queued", "failed_retry", "sending"]))
        ).update(
            {"status": "suppressed", "error_detail": "Comment deleted event received"},
            synchronize_session=False,
        )

        try:
            db.commit()
        except IntegrityError:
            db.rollback()


def _handle_comment_created(
    comment_id: str,
    text: str,
    user_id: str,
    username,
    created_at,
) -> str:
    """
    Match rules against the comment text and queue DMs.

    Returns:
        "suppressed" — comment was already deleted before this event arrived.
        "processed"  — rule matching ran (zero or more DMs queued).
    """
    with SessionLocal() as db:
        # Out-of-order deletion check
        deleted_record = db.query(DeletedComment).filter(
            DeletedComment.comment_id == comment_id
        ).first()
        if deleted_record:
            return "suppressed"

        # Case-insensitive substring keyword matching
        rules = db.query(Rule).all()
        comment_text_lower = text.lower()
        matched_rules = [r for r in rules if r.keyword.lower() in comment_text_lower]

        for rule in matched_rules:
            # Per-(user, rule) deduplication
            existing_dm = db.query(DM).filter(
                (DM.recipient_user_id == user_id) & (DM.rule_id == rule.id)
            ).first()

            if existing_dm:
                db_dup = BlockedDuplicate(
                    comment_id=comment_id,
                    rule_id=rule.id,
                    user_id=user_id,
                    blocked_at=time.time(),
                )
                db.add(db_dup)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                continue

            # Queue a new DM
            dm_id = str(uuid.uuid4())
            new_dm = DM(
                id=dm_id,
                recipient_user_id=user_id,
                comment_id=comment_id,
                rule_id=rule.id,
                status="queued",
                message=rule.dm_message,
                idempotency_key=str(uuid.uuid4()),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
            db.add(new_dm)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                # Concurrent duplicate — record as blocked
                db_dup = BlockedDuplicate(
                    comment_id=comment_id,
                    rule_id=rule.id,
                    user_id=user_id,
                    blocked_at=time.time(),
                )
                db.add(db_dup)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                print(f"Concurrent duplicate blocked for user {user_id} rule {rule.id}")

        return "processed"


@app.post("/webhook")
async def webhook(request: Request):
    """
    Receive Pseudogram webhook events.
    Must return HTTP 200 within 5 seconds — no heavy work happens here.
    All DB I/O is offloaded to the thread-pool via asyncio.to_thread() so
    synchronous SQLAlchemy calls never block the event loop.  All DM
    sending is handled by the background workers started in lifespan().
    """
    body_bytes = await request.body()
    signature_header = request.headers.get("X-PseudoGram-Signature")

    # Signature verification (uses raw body bytes — correct)
    if SIGNATURE_VERIFICATION_ENABLED and API_KEY:
        if not signature_header:
            return JSONResponse(status_code=401, content={"error": "Missing signature header"})
        if not verify_signature(body_bytes, signature_header, API_KEY):
            return JSONResponse(status_code=401, content={"error": "Invalid signature"})

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON payload"})

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    sent_at = payload.get("sent_at")
    data = payload.get("data", {})

    if not event_id or not event_type:
        return JSONResponse(status_code=400, content={"error": "Missing event_id or event_type"})

    # Parse comment fields
    comment_id = data.get("comment_id")
    text = data.get("text")
    from_user = data.get("from", {})
    user_id = from_user.get("user_id")
    username = from_user.get("username")
    created_at = data.get("created_at")

    # --- Deduplication + event recording (offloaded to thread-pool) ---
    insert_result = await asyncio.to_thread(
        _check_and_insert_event,
        event_id, event_type, comment_id, text,
        user_id, username, created_at, sent_at,
    )
    if insert_result == "duplicate":
        return {"ok": True, "detail": "duplicate event ignored"}

    # --- Event-type dispatch (all DB work offloaded to thread-pool) ---
    if event_type == "comment.deleted":
        if not comment_id:
            return JSONResponse(status_code=400, content={"error": "Missing comment_id"})
        await asyncio.to_thread(_handle_comment_deleted, comment_id)
        return {"ok": True, "detail": "deletion recorded"}

    if event_type == "comment.created":
        if not comment_id or not text or not user_id:
            return JSONResponse(status_code=400, content={"error": "Malformed comment.created payload"})
        result = await asyncio.to_thread(
            _handle_comment_created,
            comment_id, text, user_id, username, created_at,
        )
        if result == "suppressed":
            return {"ok": True, "detail": "comment already deleted, suppressed DM creation"}

    return {"ok": True}


@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Return real-time DM delivery statistics from the database."""
    sent_count = db.query(DM).filter(DM.status == "sent").count()
    failed_count = db.query(DM).filter(DM.status == "failed").count()
    queued_count = db.query(DM).filter(
        DM.status.in_(["queued", "sending", "accepted", "failed_retry"])
    ).count()
    duplicates_blocked_count = db.query(BlockedDuplicate).count()

    return {
        "sent": sent_count,
        "failed": failed_count,
        "queued": queued_count,
        "duplicates_blocked": duplicates_blocked_count
    }


@app.delete("/reset", status_code=200)
async def reset_db(db: Session = Depends(get_db)):
    """
    Reset all tracking data for a fresh simulator run.
    Clears DMs, events, deleted comments, and duplicates — but keeps rules.
    """
    db.query(DM).delete(synchronize_session=False)
    db.query(ReceivedEvent).delete(synchronize_session=False)
    db.query(DeletedComment).delete(synchronize_session=False)
    db.query(BlockedDuplicate).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "detail": "All tracking data cleared. Rules preserved."}


@app.get("/health")
async def health():
    return {"status": "ok"}
