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

from app.database import init_db, SessionLocal, Rule, ReceivedEvent, DeletedComment, DM, BlockedDuplicate, RateLimitLog

# Load environment variables
load_dotenv()

API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")
BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
SIGNATURE_VERIFICATION_ENABLED = os.getenv("SIGNATURE_VERIFICATION_ENABLED", "true").lower() == "true"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

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

# Background DM Sender Worker
async def send_dms_worker():
    print("DM Sender Worker started.")
    while True:
        try:
            now = time.time()
            
            # 1. Enforce Rate Limit: max 10 requests per rolling 60s
            sixty_seconds_ago = now - 60.0
            with SessionLocal() as db:
                count = db.query(RateLimitLog).filter(RateLimitLog.timestamp > sixty_seconds_ago).count()
                if count >= 10:
                    # Find oldest call in this window to determine wait time
                    oldest = db.query(RateLimitLog).filter(RateLimitLog.timestamp > sixty_seconds_ago).order_by(RateLimitLog.timestamp.asc()).first()
                    if oldest:
                        wait_time = 60.0 - (now - oldest.timestamp) + 0.1
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                            continue

            # 2. Fetch the next queued DM (including failed retries ready to run)
            with SessionLocal() as db:
                dm = db.query(DM).filter(
                    (DM.status == "queued") | 
                    ((DM.status == "failed_retry") & (DM.next_retry_at <= now))
                ).order_by(DM.updated_at.asc()).first()
                
                if not dm:
                    await asyncio.sleep(0.1)
                    continue
                
                # Check if comment has been deleted since scheduling
                comment_deleted = db.query(DeletedComment).filter(DeletedComment.comment_id == dm.comment_id).first()
                if comment_deleted:
                    dm.status = "suppressed"
                    dm.error_detail = "Comment deleted before sending DM"
                    dm.updated_at = datetime.now(timezone.utc).isoformat()
                    db.commit()
                    print(f"DM {dm.id} suppressed because comment {dm.comment_id} was deleted.")
                    continue
                
                # Mark as sending to reserve it
                dm.status = "sending"
                dm.updated_at = datetime.now(timezone.utc).isoformat()
                db.commit()
                db.refresh(dm)
                
                # Copy values for the HTTP call
                dm_id_local = dm.id
                recipient_user_id = dm.recipient_user_id
                message = dm.message
                comment_id = dm.comment_id
                idempotency_key = dm.idempotency_key
                retry_count = dm.retry_count

            # 3. Log call to Rate Limit Log
            with SessionLocal() as db:
                db.add(RateLimitLog(timestamp=time.time()))
                db.commit()
            
            # 4. Make HTTP call
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
                
                async with httpx.AsyncClient(timeout=10.0) as client:
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
                    print(f"DM send accepted by API: local_id={dm_id_local}, api_dm_id={api_dm_id}")
                    
                elif response.status_code == 429:
                    retry_after = 60.0
                    try:
                        retry_after = float(response.headers.get("Retry-After", 60.0))
                    except ValueError:
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
                    error_msg = f"API {response.status_code}: {response.text}"
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
                
        except asyncio.CancelledError:
            print("DM Sender Worker cancelled.")
            break
        except Exception as e:
            print(f"Error in send_dms_worker loop: {e}")
            await asyncio.sleep(1)

# Background DM Reconciliation Worker (Part C)
async def reconcile_dms_worker():
    print("DM Reconciler Worker started.")
    while True:
        try:
            # Fetch all DMs that are accepted but not terminal
            with SessionLocal() as db:
                pending_dms = db.query(DM).filter(DM.status == "accepted").all()
                dms_to_check = [
                    {"id": d.id, "dm_id": d.dm_id, "retry_count": d.retry_count} 
                    for d in pending_dms if d.dm_id
                ]
            
            if not dms_to_check:
                await asyncio.sleep(2)
                continue
                
            for d_info in dms_to_check:
                local_id = d_info["id"]
                api_dm_id = d_info["dm_id"]
                retry_count = d_info["retry_count"]
                
                try:
                    headers = {
                        "X-API-Key": API_KEY
                    }
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
                                if db_dm:
                                    db_dm.status = "sent"
                                    db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                                    db.commit()
                            print(f"DM {api_dm_id} delivered successfully.")
                            
                        elif api_status == "failed":
                            # API failed after acceptance -> retry with a NEW idempotency key
                            with SessionLocal() as db:
                                db_dm = db.query(DM).filter(DM.id == local_id).first()
                                if db_dm:
                                    if retry_count + 1 >= MAX_RETRIES:
                                        db_dm.status = "failed"
                                        db_dm.error_detail = "Mock API reported delivery failure, retries exhausted"
                                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                                        db.commit()
                                        print(f"DM {api_dm_id} failed delivery. Max retries reached.")
                                    else:
                                        # Reset status to queued, clear dm_id, generate new idempotency key
                                        db_dm.status = "queued"
                                        db_dm.dm_id = None
                                        db_dm.idempotency_key = str(uuid.uuid4())
                                        db_dm.retry_count = retry_count + 1
                                        db_dm.next_retry_at = 0.0
                                        db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                                        db.commit()
                                        print(f"DM {api_dm_id} failed delivery. Resetting for fresh retry with new key.")
                                        
                        elif api_status == "queued":
                            # Still pending in mock API queue
                            pass
                            
                    elif response.status_code == 404:
                        # Unknown ID
                        with SessionLocal() as db:
                            db_dm = db.query(DM).filter(DM.id == local_id).first()
                            if db_dm:
                                db_dm.status = "failed"
                                db_dm.error_detail = "DM ID not found on mock API"
                                db_dm.updated_at = datetime.now(timezone.utc).isoformat()
                                db.commit()
                        print(f"DM {api_dm_id} not found on API (404). Marked failed.")
                        
                except httpx.RequestError as exc:
                    print(f"Network error reconciling DM {api_dm_id}: {exc}")
                    
            await asyncio.sleep(1)
            
        except asyncio.CancelledError:
            print("DM Reconciler Worker cancelled.")
            break
        except Exception as e:
            print(f"Error in reconcile_dms_worker loop: {e}")
            await asyncio.sleep(5)

# Lifespan manager
background_tasks = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure database tables exist
    init_db()
    
    # Start workers
    loop = asyncio.get_running_loop()
    send_task = loop.create_task(send_dms_worker())
    reconcile_task = loop.create_task(reconcile_dms_worker())
    
    background_tasks.add(send_task)
    background_tasks.add(reconcile_task)
    
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

# ----------------- ENDPOINTS -----------------

@app.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(rule_in: RuleCreate, db: Session = Depends(get_db)):
    # Standardize keyword: uppercase or keep as is? Substring matching is case-insensitive.
    # We'll save it as is.
    rule_id = str(uuid.uuid4())
    db_rule = Rule(id=rule_id, keyword=rule_in.keyword, dm_message=rule_in.dm_message)
    try:
        db.add(db_rule)
        db.commit()
        db.refresh(db_rule)
    except IntegrityError:
        db.rollback()
        # Already exists
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

@app.post("/webhook")
async def webhook(request: Request, db: Session = Depends(get_db)):
    # Fast webhook response: process everything locally in the DB and return immediately
    body_bytes = await request.body()
    signature_header = request.headers.get("X-PseudoGram-Signature")
    
    # 1. Signature verification (Part B)
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

    # 2. Event deduplication (~8% redelivered events)
    # Enforce uniqueness of event_id
    existing_event = db.query(ReceivedEvent).filter(ReceivedEvent.event_id == event_id).first()
    if existing_event:
        return {"ok": True, "detail": "duplicate event ignored"}

    # Save event to DB
    comment_id = data.get("comment_id")
    text = data.get("text")
    from_user = data.get("from", {})
    user_id = from_user.get("user_id")
    username = from_user.get("username")
    created_at = data.get("created_at")

    db_event = ReceivedEvent(
        event_id=event_id,
        event_type=event_type,
        comment_id=comment_id,
        text=text,
        user_id=user_id,
        username=username,
        created_at=created_at,
        sent_at=sent_at,
        received_at=datetime.now(timezone.utc).isoformat()
    )
    db.add(db_event)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return {"ok": True, "detail": "duplicate event ignored concurrently"}

    # 3. Handle Deletions (Part C)
    if event_type == "comment.deleted":
        if not comment_id:
            return JSONResponse(status_code=400, content={"error": "Missing comment_id"})
        
        # Save deletion record
        db_del = DeletedComment(comment_id=comment_id, deleted_at=datetime.now(timezone.utc).isoformat())
        db.add(db_del)
        
        # Update matching DMs that are not sent to "suppressed"
        db.query(DM).filter(
            (DM.comment_id == comment_id) & 
            (DM.status.in_(["queued", "failed_retry", "sending"]))
        ).update({"status": "suppressed", "error_detail": "Comment deleted event received"}, synchronize_session=False)
        
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            
        return {"ok": True, "detail": "deletion recorded"}

    # 4. Handle comment.created
    if event_type == "comment.created":
        if not comment_id or not text or not user_id:
            return JSONResponse(status_code=400, content={"error": "Malformed comment.created payload"})

        # Check if comment was already deleted (out-of-order deletion)
        deleted_record = db.query(DeletedComment).filter(DeletedComment.comment_id == comment_id).first()
        if deleted_record:
            return {"ok": True, "detail": "comment already deleted, suppressed DM creation"}

        # Fetch all rules and perform substring matching in memory
        rules = db.query(Rule).all()
        matched_rules = []
        comment_text_lower = text.lower()
        for rule in rules:
            if rule.keyword.lower() in comment_text_lower:
                matched_rules.append(rule)

        # For each matched rule, attempt to queue a DM
        for rule in matched_rules:
            # Check user/rule level deduplication (stable identity check via user_id)
            existing_dm = db.query(DM).filter(
                (DM.recipient_user_id == user_id) & 
                (DM.rule_id == rule.id)
            ).first()
            
            if existing_dm:
                # Record blocked duplicate
                db_dup = BlockedDuplicate(
                    comment_id=comment_id,
                    rule_id=rule.id,
                    user_id=user_id,
                    blocked_at=time.time()
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
                updated_at=datetime.now(timezone.utc).isoformat()
            )
            db.add(new_dm)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                # Unique constraint (recipient_user_id, rule_id) violated concurrently
                # Record blocked duplicate instead
                db_dup = BlockedDuplicate(
                    comment_id=comment_id,
                    rule_id=rule.id,
                    user_id=user_id,
                    blocked_at=time.time()
                )
                db.add(db_dup)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                print(f"Concurrent duplicate blocked for user {user_id} and rule {rule.id}")

    return {"ok": True}

@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    # Concurrency-safe, real-time counters queried from the database state
    sent_count = db.query(DM).filter(DM.status == "sent").count()
    failed_count = db.query(DM).filter(DM.status == "failed").count()
    queued_count = db.query(DM).filter(DM.status.in_(["queued", "sending", "accepted", "failed_retry"])).count()
    duplicates_blocked_count = db.query(BlockedDuplicate).count()
    
    return {
        "sent": sent_count,
        "failed": failed_count,
        "queued": queued_count,
        "duplicates_blocked": duplicates_blocked_count
    }
