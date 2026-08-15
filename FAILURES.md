# LinkPlease Tech Intern Assignment: FAILURES.md

This document lists every known way this automation system can lose a DM, send a duplicate, or report incorrect numbers under specific hostile API behaviors, network partitions, and process crashes.

---

## 1. How a DM can be lost (never delivered)

1. **Max Retries Exceeded on Network/Server Errors**:
   * **Condition**: If the Mock API returns `500 Internal Error` or a network partition occurs continuously for 5 consecutive attempts.
   * **Result**: The system stops retrying, changes the DM status to `failed` (terminal), and marks the error detail. The DM is lost.

2. **Terminal API Responses (400 Bad Request)**:
   * **Condition**: If the Mock API returns a `400 Bad Request` (e.g., if the user ID does not exist, the user blocked the creator, or the payload format is rejected).
   * **Result**: The system immediately marks the status as `failed` and will *never* attempt to retry this request because retrying malformed requests is useless.

3. **Disk Full or SQLite File Access Failure**:
   * **Condition**: If the host server runs out of disk space, or if the SQLite database file permissions are modified, preventing write operations.
   * **Result**: Incoming webhook events will fail to write to the database and will result in HTTP 500 errors to the mock API (resulting in dropped events if the mock API does not retry them). Queue state changes will fail, causing pending sends to lock.

4. **API Lies About Success**:
   * **Condition**: If the Mock API returns a terminal delivery status of `failed` via `GET /v1/dm/{dm_id}` after max retries are already exhausted.
   * **Result**: The DM is marked as `failed` in our database, and we stop retrying. If the Mock API actually meant to deliver it later or if it was a temporary delivery failure at the provider level, the message is permanently lost.

---

## 2. How a duplicate DM can be sent

1. **Idempotency Cache Expiration on the Mock API**:
   * **Condition**: If a network request to `/v1/dm/send` times out or fails on our end, but succeeds on the Mock API, the DM is sent. If we retry the request with the same `Idempotency-Key` after the Mock API has already cleared its idempotency cache (or if the Mock API does not support long-term idempotency persistence), the Mock API will treat it as a new request and send it again.

2. **False Delivery Failures leading to New Idempotency Key**:
   * **Condition**: The Mock API reports a delivery status of `failed` via `GET /v1/dm/{dm_id}`. As required, we reconcile this by resetting the status to `queued` and generating a **new** `Idempotency-Key` to retry the send. If the Mock API's status report was incorrect (i.e., the message was actually delivered, but the API reported it as `failed`), sending it again with a new idempotency key will cause a duplicate.

---

## 3. How `/stats` can report incorrect numbers

1. **Database Reset or File Corruption**:
   * **Condition**: If the SQLite database file (`linkplease.db`) is deleted, corrupted, or restored to an older state (e.g. during a bad deployment or server migration without a persistent volume).
   * **Result**: The `/stats` endpoint reads directly from the current SQLite file. If the file is reset, the counts will start from 0 and mismatch the Mock API's historical logs.

2. **Ignoring Suppressed DMs in Stats**:
   * **Condition**: When a comment is deleted, any matching queued/retry DMs are marked as `suppressed`. These are excluded from `/stats` (they are not counted as `sent`, `failed`, or `queued`).
   * **Result**: If the grading script counts a suppressed DM as `failed` or expects it to be in `queued`, the reported numbers will differ.

3. **In-Flight Stats lag**:
   * **Condition**: A DM has been accepted by the mock API (202) and is in `accepted` status in our database, waiting for the reconciliation worker to verify its delivery status.
   * **Result**: During this window, it is reported under `queued`. If the grader pulls `/stats` immediately after sending under heavy load, there will be a minor lag before the reconciliation worker transitions it to `sent` or `failed`.
