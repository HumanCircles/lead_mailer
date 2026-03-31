"""
Shared two-phase concurrent pipeline.

Phase 1 — LLM generation (LLM_MAX_CONCURRENT threads):
  prospect → OpenAI → draft_queue

Phase 2 — SMTP send (CONCURRENT_SENDS semaphore):
  draft_queue → smtp_deliver → on_result callback

Both phases respect stop_event for graceful shutdown.
"""

from __future__ import annotations

import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable

from dotenv import load_dotenv

from core.deliverability import is_suppressed, strip_control_chars
from core.email_drafter import draft_email
from core.logger import get_logger
from core.smtp_sender import (
    DAILY_LIMIT,
    _hourly_safe_limit,
    _pool_lock,
    _sender_state,
    is_siteground_hourly_lockout,
    seconds_until_capacity_frees,
    send_email,
)

load_dotenv()

log = get_logger()

LLM_MAX_CONCURRENT = int(os.getenv("LLM_MAX_CONCURRENT", "20"))
CONCURRENT_SENDS   = int(os.getenv("CONCURRENT_SENDS", "5"))
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "5"))
SENT_LOG_FILE      = os.getenv("SENT_LOG_FILE", "sent_log.csv")

_SENTINEL = object()  # poison pill for draft_queue


def _make_log_row(
    prospect: dict,
    subject: str,
    status: str,
    error: str = "",
    from_email: str = "",
) -> dict:
    return {
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "prospect_email": prospect.get("email", ""),
        "prospect_name":  f"{prospect.get('first_name', '')} {prospect.get('last_name', '')}".strip(),
        "company":        prospect.get("company", ""),
        "subject":        subject,
        "status":         status,
        "error":          error,
        "from_email":     from_email,
    }


def _all_senders_at_cap() -> bool:
    now = time.time()
    with _pool_lock:
        for email in _sender_state:
            state = _sender_state[email]
            if now - state["hour_start"] >= 3600:
                state["hourly"] = 0
                state["hour_start"] = now
            if state["daily"] < DAILY_LIMIT and state["hourly"] < _hourly_safe_limit():
                return False
    return True


def _generate_one(
    prospect: dict,
    draft_queue: "queue.Queue",
    on_result: Callable[[dict], None],
    stop_event: threading.Event,
    dry_run: bool,
) -> None:
    """Phase 1 worker: generate one email and put draft on queue."""
    if stop_event.is_set():
        return
    email = prospect.get("email", "").strip().lower()
    if is_suppressed(email):
        on_result(_make_log_row(prospect, "", "skipped_suppressed"))
        return
    try:
        lead = {
            "name":         strip_control_chars(
                f"{prospect.get('first_name', '')} {prospect.get('last_name', '')}".strip()
            ),
            "company":      strip_control_chars(str(prospect.get("company", "") or "")),
            "title":        strip_control_chars(str(prospect.get("title", "") or "")),
            "hcm_platform": strip_control_chars(str(prospect.get("hcm_platform", "") or "")),
        }
        ec = draft_email(lead)
        if dry_run:
            on_result(_make_log_row(prospect, ec.get("subject", ""), "dry_run"))
        else:
            draft_queue.put((prospect, ec))
    except Exception as e:
        log.warning("LLM generation failed for %s: %s", email, e)
        on_result(_make_log_row(prospect, "", "failed_generation", str(e)))


def _send_phase(
    draft_queue: "queue.Queue",
    sem: threading.Semaphore,
    on_result: Callable[[dict], None],
    stop_event: threading.Event,
) -> None:
    """Phase 2 worker: consume drafts from queue and send."""
    while True:
        try:
            item = draft_queue.get(timeout=1)
        except queue.Empty:
            if stop_event.is_set() and draft_queue.empty():
                break
            continue

        if item is _SENTINEL:
            draft_queue.put(_SENTINEL)  # re-enqueue for other phase-2 workers
            break

        prospect, ec = item
        subject = ec.get("subject", "")
        body    = ec.get("body", "")
        email   = prospect.get("email", "").strip().lower()

        # Block until account has capacity (avoid 550 lockout)
        while _all_senders_at_cap() and not stop_event.is_set():
            wait = seconds_until_capacity_frees()
            log.info("All senders at hourly cap — waiting %.0fs", wait)
            time.sleep(min(wait, 60))

        if stop_event.is_set():
            draft_queue.task_done()
            break

        with sem:
            try:
                from_addr = send_email(email, subject, body, prospect.get("first_name", ""))
                row = _make_log_row(prospect, subject, "pushed", from_email=from_addr)
            except Exception as e:
                if is_siteground_hourly_lockout(e):
                    log.warning("SiteGround 550 lockout hit — putting draft back in queue")
                    draft_queue.put((prospect, ec))
                    draft_queue.task_done()
                    time.sleep(5)
                    continue
                log.warning("Send failed for %s: %s", email, e)
                row = _make_log_row(prospect, subject, "failed_api", str(e))

        on_result(row)
        draft_queue.task_done()


def run_pipeline(
    prospects: list[dict],
    already_sent: set[str],
    on_result: Callable[[dict], None],
    on_progress: Callable[[int, int], None],
    stop_event: threading.Event,
    dry_run: bool = False,
) -> None:
    """
    Run the full generate→send pipeline.

    Args:
        prospects:    List of canonical prospect dicts.
        already_sent: Set of lowercase emails already pushed (from sent_log.csv).
        on_result:    Called with a log-row dict for every outcome (thread-safe).
        on_progress:  Called with (completed, total) after each outcome.
        stop_event:   Set externally to trigger graceful shutdown.
        dry_run:      Generate emails but skip sending.
    """
    pending = [
        p for p in prospects
        if str(p.get("email", "")).strip().lower() not in already_sent
    ]
    total = len(prospects)
    completed = [0]
    lock = threading.Lock()

    def _wrapped_on_result(row: dict) -> None:
        on_result(row)
        with lock:
            completed[0] += 1
            on_progress(completed[0], total)

    # Emit skipped_duplicate for already-sent entries
    for p in prospects:
        if str(p.get("email", "")).strip().lower() in already_sent:
            _wrapped_on_result(_make_log_row(p, "", "skipped_duplicate"))

    if not pending:
        return

    draft_queue: queue.Queue = queue.Queue(maxsize=LLM_MAX_CONCURRENT * 2)
    send_sem = threading.Semaphore(CONCURRENT_SENDS)

    # Phase 2: send workers (started first so they're ready to consume)
    send_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="send")
    for _ in range(MAX_WORKERS):
        send_executor.submit(_send_phase, draft_queue, send_sem, _wrapped_on_result, stop_event)

    # Phase 1: LLM generation
    with ThreadPoolExecutor(max_workers=LLM_MAX_CONCURRENT, thread_name_prefix="llm") as llm_exec:
        gen_futures = [
            llm_exec.submit(_generate_one, p, draft_queue, _wrapped_on_result, stop_event, dry_run)
            for p in pending
        ]
        for f in as_completed(gen_futures):
            try:
                f.result()  # surface exceptions from generation workers
            except Exception as e:
                log.error("Unhandled exception in LLM worker: %s", e)

    # Signal phase 2 workers to stop
    draft_queue.put(_SENTINEL)
    send_executor.shutdown(wait=True)

    log.info("Pipeline complete — total=%d completed=%d", total, completed[0])
