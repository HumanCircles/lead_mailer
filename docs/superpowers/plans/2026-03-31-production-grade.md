# Production-Grade Lead Mailer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the lead mailer into a production-grade system with a shared concurrent pipeline, 3-tab Streamlit UI (Outreach + CSV mapping preview, Batch, Activity Log), IST-aware rotating logs, and SiteGround-aware SMTP throttling that maximises throughput without hitting the 550 hourly lockout.

**Architecture:** Extract all pipeline logic into `core/pipeline.py` (two-phase queue: parallel LLM → throttled SMTP); both `agent.py` (CLI) and the new Streamlit Batch tab drive the same engine via callbacks. The UI grows to three tabs; the CLI becomes a thin argparse wrapper with SIGTERM support.

**Tech Stack:** Python 3.11+, Streamlit, OpenAI SDK, smtplib, threading/concurrent.futures, python-dotenv, pandas

---

## Task 1: core/logger.py — IST-aware rotating logger

**Files:**
- Create: `core/logger.py`

- [ ] **Step 1: Create `core/logger.py`**

```python
"""Structured rotating logger — timestamps and file rotation in IST (UTC+5:30)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler

IST = timezone(timedelta(hours=5, minutes=30))
_LOG_DIR = os.getenv("LOG_DIR", "logs")
_KEEP_DAYS = int(os.getenv("LOG_KEEP_DAYS", "7"))

_logger: logging.Logger | None = None


class _ISTFormatter(logging.Formatter):
    """Format log records with IST timestamps."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=IST)
        return dt.strftime("%Y-%m-%d %H:%M:%S IST")


class _ISTRotatingHandler(TimedRotatingFileHandler):
    """Rotate at midnight IST, not UTC."""

    def computeRollover(self, currentTime: float) -> float:  # noqa: N802
        # Shift to IST for rollover calculation
        ist_now = datetime.fromtimestamp(currentTime, tz=IST)
        tomorrow = (ist_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return tomorrow.timestamp()

    def doRollover(self) -> None:  # noqa: N802
        super().doRollover()
        # Rename to include IST date
        ist_date = datetime.now(tz=IST).strftime("%Y-%m-%d")
        src = f"{self.baseFilename}.{datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')}"
        dst = self.baseFilename.replace(".log", f"_{ist_date}_IST.log")
        if os.path.exists(src) and not os.path.exists(dst):
            os.rename(src, dst)


def get_logger(name: str = "lead_mailer") -> logging.Logger:
    """Return a configured logger. Safe to call from any thread; initialises once."""
    global _logger
    if _logger is not None:
        return _logger

    os.makedirs(_LOG_DIR, exist_ok=True)
    log_path = os.path.join(_LOG_DIR, "agent.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # File handler — rotates at midnight IST, keeps _KEEP_DAYS files
    fh = _ISTRotatingHandler(
        log_path,
        when="midnight",
        interval=1,
        backupCount=_KEEP_DAYS,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_ISTFormatter("%(asctime)s %(levelname)-8s %(message)s"))

    # Stderr handler — WARNING+ only (surfaces in docker logs / EC2 console)
    sh = logging.StreamHandler()
    sh.setLevel(logging.WARNING)
    sh.setFormatter(_ISTFormatter("%(asctime)s %(levelname)-8s %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    _logger = logger
    return logger
```

- [ ] **Step 2: Smoke-test the logger**

```bash
cd /Volumes/part_one/Coding/Projects/HireQuotient/lead_mailer
source .venv/bin/activate
python -c "
from core.logger import get_logger, IST
from datetime import datetime
log = get_logger()
log.info('Logger smoke test')
log.warning('Warning test')
print('IST now:', datetime.now(tz=IST))
import os; print('log file created:', os.path.exists('logs/agent.log'))
"
```
Expected: no exception, IST time printed, `logs/agent.log` exists.

- [ ] **Step 3: Add `logs/` to `.gitignore`**

Open `.gitignore` and add:
```
logs/
.superpowers/
```

- [ ] **Step 4: Commit**

```bash
git add core/logger.py .gitignore
git commit -m "feat: add IST-aware rotating logger"
```

---

## Task 2: core/smtp_sender.py — hourly limits + SiteGround fix

**Files:**
- Modify: `core/smtp_sender.py`

- [ ] **Step 1: Read the current file top-to-bottom before editing**

Confirm the existing globals: `_pool`, `_cycler`, `_counters`, `_pool_lock`, `DAILY_LIMIT`, `SENT_LOG_FILE`.

- [ ] **Step 2: Replace `core/smtp_sender.py` with the hardened version**

```python
import csv
import itertools
import os
import random
import re
import smtplib
import threading
import time
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

from dotenv import load_dotenv

from core.deliverability import (
    append_signature_block,
    append_unsubscribe_footer,
    apply_list_unsubscribe_headers,
    is_suppressed,
    smtp_from_header,
)
from core.logger import get_logger

load_dotenv()

log = get_logger()

SMTP_HOST        = os.getenv("SMTP_HOST", "mail.recruitagents.net")
SMTP_PORT        = int(os.getenv("SMTP_PORT", "465"))
_fn_raw          = os.getenv("FROM_NAME")
FROM_DISPLAY     = ("HireQuotient" if _fn_raw is None else _fn_raw).strip()
DAILY_LIMIT      = int(os.getenv("DAILY_LIMIT", "150"))
HOURLY_LIMIT     = int(os.getenv("HOURLY_LIMIT", "100"))
HOURLY_BUFFER    = int(os.getenv("HOURLY_BUFFER", "5"))   # stop this many before the cap
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "0"))
SENT_LOG_FILE    = os.getenv("SENT_LOG_FILE", "sent_log.csv").strip() or "sent_log.csv"
SMTP_TIMEOUT     = int(os.getenv("SMTP_TIMEOUT", "20"))
SMTP_MAX_RETRIES = int(os.getenv("SMTP_MAX_RETRIES", "3"))

_pool_lock = threading.Lock()

# Per-sender state: {email: {"daily": int, "hourly": int, "hour_start": float}}
_sender_state: dict[str, dict] = {}


def _load_pool() -> list[tuple[str, str]]:
    raw = os.getenv("SENDER_POOL", "").strip()
    if not raw:
        raise ValueError("SENDER_POOL is not set in .env")
    pairs = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            email, pwd = entry.split(":", 1)
            pairs.append((email.strip(), pwd.strip()))
    if not pairs:
        raise ValueError("SENDER_POOL has no valid email:password entries")
    return pairs


_pool   = _load_pool()
_cycler = itertools.cycle(_pool)


def _init_sender_state() -> None:
    """Initialise per-sender counters, hydrating daily counts from sent_log.csv."""
    now = time.time()
    today_utc = datetime.now(timezone.utc).date().isoformat()
    hourly_start = now - 3600

    daily_from_log: dict[str, int] = {}
    hourly_from_log: dict[str, int] = {}

    if os.path.isfile(SENT_LOG_FILE):
        try:
            with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames and "from_email" in reader.fieldnames:
                    for row in reader:
                        if row.get("status") != "pushed":
                            continue
                        ts = row.get("timestamp") or ""
                        if not ts.startswith(today_utc):
                            continue
                        fe = (row.get("from_email") or "").strip().lower()
                        if not fe:
                            continue
                        daily_from_log[fe] = daily_from_log.get(fe, 0) + 1
                        try:
                            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            if dt.timestamp() >= hourly_start:
                                hourly_from_log[fe] = hourly_from_log.get(fe, 0) + 1
                        except ValueError:
                            pass
        except Exception as e:
            log.warning("Could not hydrate sender state from log: %s", e)

    with _pool_lock:
        for email, _ in _pool:
            key = email.lower()
            _sender_state[email] = {
                "daily": daily_from_log.get(key, 0),
                "hourly": hourly_from_log.get(key, 0),
                "hour_start": now,
            }


_init_sender_state()


def _hourly_safe_limit() -> int:
    return max(1, HOURLY_LIMIT - HOURLY_BUFFER)


def _next_sender() -> tuple[str, str]:
    """Round-robin sender with per-sender hourly + daily enforcement."""
    with _pool_lock:
        now = time.time()
        for _ in range(len(_pool)):
            email, pwd = next(_cycler)
            state = _sender_state[email]
            # Reset hourly window if expired
            if now - state["hour_start"] >= 3600:
                state["hourly"] = 0
                state["hour_start"] = now
            if state["daily"] < DAILY_LIMIT and state["hourly"] < _hourly_safe_limit():
                return email, pwd
    raise RuntimeError("All sender accounts have hit their daily or hourly limit.")


def _sender_at_capacity() -> bool:
    """True when every sender is at daily or hourly cap."""
    now = time.time()
    with _pool_lock:
        for email, _ in _pool:
            state = _sender_state[email]
            if now - state["hour_start"] >= 3600:
                state["hourly"] = 0
                state["hour_start"] = now
            if state["daily"] < DAILY_LIMIT and state["hourly"] < _hourly_safe_limit():
                return False
    return True


def seconds_until_capacity_frees() -> float:
    """Return seconds until the earliest hourly window resets."""
    now = time.time()
    with _pool_lock:
        waits = []
        for email, _ in _pool:
            state = _sender_state[email]
            elapsed = now - state["hour_start"]
            remaining = max(0.0, 3600 - elapsed)
            waits.append(remaining)
    return min(waits) + 1  # +1s safety margin


def is_siteground_hourly_lockout(exc: Exception) -> bool:
    """
    Detect SiteGround 550 hourly lockout from any exception format.
    Error arrives as smtplib.SMTPRecipientsRefused:
      {'recipient@x.com': (550, b'Message rejected. ... already sent N messages for 1h ...')}
    """
    text = str(exc).lower()
    return (
        "550" in text
        and "already sent" in text
        and "messages for 1h" in text
    )


def classify_smtp_error(exc: Exception) -> str:
    """Return 'lockout' | 'auth' | 'network' | 'unknown'."""
    text = str(exc).lower()
    if is_siteground_hourly_lockout(exc):
        return "lockout"
    if "535" in text or "authentication" in text or "username and password" in text:
        return "auth"
    if any(t in text for t in ("connection", "timeout", "network", "eof", "broken pipe")):
        return "network"
    return "unknown"


def smtp_deliver(from_email: str, password: str, to_email: str, msg_as_string: str) -> None:
    """Connect, login, send with retries on transient errors."""
    for attempt in range(SMTP_MAX_RETRIES):
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as server:
                server.login(from_email, password)
                server.sendmail(from_email, to_email, msg_as_string)
            return
        except Exception as e:
            kind = classify_smtp_error(e)
            if kind == "lockout":
                raise  # caller handles lockout specially
            if kind == "auth":
                log.error("Auth failure for %s — removing from pool this run: %s", from_email, e)
                raise
            if attempt >= SMTP_MAX_RETRIES - 1:
                raise
            backoff = 2 ** (attempt + 1) + random.uniform(0, 1)
            log.warning("SMTP transient error (attempt %d/%d): %s", attempt + 1, SMTP_MAX_RETRIES, e)
            time.sleep(backoff)


def _recipient_first_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z\s'-]", " ", (name or "")).strip()
    if not cleaned:
        return ""
    first = cleaned.split()[0]
    return first[:1].upper() + first[1:].lower()


def _ensure_recipient_greeting(body: str, recipient_first_name: str) -> str:
    text = (body or "").strip()
    if not text:
        return text
    if re.match(r"^(hi|hello)\b", text, flags=re.IGNORECASE):
        return text
    first = _recipient_first_name(recipient_first_name)
    return f"Hi {first},\n\n{text}" if first else text


def send_email(to_email: str, subject: str, body: str, recipient_first_name: str = "") -> str:
    """Send one email. Returns sender address. Raises on failure."""
    if is_suppressed(to_email):
        raise ValueError(f"Address on suppression list: {to_email.strip().lower()}")

    sender_email, app_password = _next_sender()
    body_out = _ensure_recipient_greeting(body, recipient_first_name)
    body_out = append_signature_block(body_out, sender_email=sender_email)
    body_out = append_unsubscribe_footer(body_out)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_from_header(FROM_DISPLAY, sender_email)
    msg["To"]      = to_email
    msg["Date"]    = formatdate(localtime=True)
    msg.attach(MIMEText(body_out, "plain"))
    apply_list_unsubscribe_headers(msg)

    smtp_deliver(sender_email, app_password, to_email, msg.as_string())

    with _pool_lock:
        _sender_state[sender_email]["daily"] += 1
        _sender_state[sender_email]["hourly"] += 1

    if SEND_DELAY_SECONDS > 0:
        time.sleep(SEND_DELAY_SECONDS)

    log.info("pushed email=%s from=%s subject=%r", to_email, sender_email, subject[:60])
    return sender_email
```

- [ ] **Step 3: Smoke-test**

```bash
python -c "
from core.smtp_sender import _next_sender, _sender_state, classify_smtp_error
print('sender state:', _sender_state)
# Test lockout detection
class FakeErr(Exception): pass
err = FakeErr(\"{'x@y.com': (550, b'already sent 433 messages for 1h')}\")
print('lockout detected:', classify_smtp_error(err))  # expect: lockout
"
```
Expected: state dict printed with all senders, `lockout detected: lockout`.

- [ ] **Step 4: Commit**

```bash
git add core/smtp_sender.py
git commit -m "feat: smtp_sender hourly limits, SiteGround lockout detection, SEND_DELAY_SECONDS"
```

---

## Task 3: core/deliverability.py — add reload_suppression()

**Files:**
- Modify: `core/deliverability.py`

- [ ] **Step 1: Add `reload_suppression()` after `get_suppression_set()`**

Open `core/deliverability.py`. After the `get_suppression_set` function, add:

```python
def reload_suppression() -> int:
    """Force reload of suppression list from disk. Returns count of suppressed addresses."""
    global _suppression_set
    _suppression_set = frozenset(load_suppression_set())
    return len(_suppression_set)
```

- [ ] **Step 2: Verify**

```bash
python -c "
from core.deliverability import reload_suppression, is_suppressed
n = reload_suppression()
print('suppression entries:', n)
"
```
Expected: integer printed, no exception.

- [ ] **Step 3: Commit**

```bash
git add core/deliverability.py
git commit -m "feat: add reload_suppression() for hot-reload without restart"
```

---

## Task 4: core/prospect_csv.py — detect_column_mapping()

**Files:**
- Modify: `core/prospect_csv.py`

- [ ] **Step 1: Add `detect_column_mapping()` at the bottom of the file**

```python
# ── Column mapping detection ───────────────────────────────────────────────────

_FIELD_ALIASES: dict[str, list[str]] = {
    "first_name":    ["first_name", "first name", "firstname"],
    "last_name":     ["last_name", "last name", "lastname"],
    "email":         ["email", "preferred email", "email professional", "professional email",
                      "work email", "business email", "email personal", "personal email"],
    "company":       ["company", "company name", "companyname", "current organization",
                      "organization", "current company"],
    "title":         ["title", "current position", "job title", "current title", "position"],
    "hcm_platform":  ["hcm_platform", "hcm platform", "platform"],
}

_HIGH_CONFIDENCE = {alias for aliases in _FIELD_ALIASES.values() for alias in aliases}


def detect_column_mapping(df: "pd.DataFrame") -> dict[str, dict]:
    """
    Return mapping confidence for each canonical field.

    Returns:
        {
          "first_name": {"mapped_from": "First Name", "confidence": "high"},
          "email":      {"mapped_from": "Email Professional", "confidence": "high"},
          "title":      {"mapped_from": "name",  "confidence": "low"},   # split fallback
          "hcm_platform": {"mapped_from": None, "confidence": "missing"},
        }
    """
    lower_cols = {str(c).lower().strip(): str(c) for c in df.columns}
    result: dict[str, dict] = {}

    for field, aliases in _FIELD_ALIASES.items():
        matched_alias = None
        for alias in aliases:
            if alias in lower_cols:
                matched_alias = lower_cols[alias]
                break

        if matched_alias is not None:
            result[field] = {"mapped_from": matched_alias, "confidence": "high"}
            continue

        # Low-confidence fallbacks
        if field in ("first_name", "last_name"):
            # Check for a full name column we can split
            for fallback in ("name", "full name", "candidate name"):
                if fallback in lower_cols:
                    result[field] = {"mapped_from": lower_cols[fallback], "confidence": "low"}
                    break
            else:
                result[field] = {"mapped_from": None, "confidence": "missing"}
        else:
            result[field] = {"mapped_from": None, "confidence": "missing"}

    return result
```

- [ ] **Step 2: Verify**

```bash
python -c "
import pandas as pd
from core.prospect_csv import detect_column_mapping

df = pd.DataFrame(columns=['First Name', 'Last Name', 'Email Professional', 'Company Name', 'Current Title'])
mapping = detect_column_mapping(df)
for field, info in mapping.items():
    print(f'{field:15} <- {str(info[\"mapped_from\"]):25} [{info[\"confidence\"]}]')
"
```
Expected: all 5 real fields show `high`, `hcm_platform` shows `missing`.

- [ ] **Step 3: Commit**

```bash
git add core/prospect_csv.py
git commit -m "feat: detect_column_mapping() with high/low/missing confidence"
```

---

## Task 5: core/pipeline.py — shared concurrent pipeline engine

**Files:**
- Create: `core/pipeline.py`

- [ ] **Step 1: Create `core/pipeline.py`**

```python
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
    _sender_state,
    _pool_lock,
    _hourly_safe_limit,
    DAILY_LIMIT,
    send_email,
    is_siteground_hourly_lockout,
    seconds_until_capacity_frees,
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
        "prospect_name":  f"{prospect.get('first_name','')} {prospect.get('last_name','')}".strip(),
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
        row = _make_log_row(prospect, "", "skipped_suppressed")
        on_result(row)
        return
    try:
        lead = {
            "name":         strip_control_chars(f"{prospect.get('first_name','')} {prospect.get('last_name','')}".strip()),
            "company":      strip_control_chars(str(prospect.get("company", "") or "")),
            "title":        strip_control_chars(str(prospect.get("title", "") or "")),
            "hcm_platform": strip_control_chars(str(prospect.get("hcm_platform", "") or "")),
        }
        ec = draft_email(lead)
        if dry_run:
            row = _make_log_row(prospect, ec.get("subject", ""), "dry_run")
            on_result(row)
        else:
            draft_queue.put((prospect, ec))
    except Exception as e:
        log.warning("LLM generation failed for %s: %s", email, e)
        row = _make_log_row(prospect, "", "failed_generation", str(e))
        on_result(row)


def _send_phase(
    draft_queue: "queue.Queue",
    sem: threading.Semaphore,
    on_result: Callable[[dict], None],
    stop_event: threading.Event,
    total_generated: list,  # mutable counter shared with phase 1
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
            break

        with sem:
            try:
                from_addr = send_email(email, subject, body, prospect.get("first_name", ""))
                row = _make_log_row(prospect, subject, "pushed", from_email=from_addr)
            except Exception as e:
                if is_siteground_hourly_lockout(e):
                    log.warning("SiteGround 550 lockout hit — putting draft back in queue")
                    draft_queue.put((prospect, ec))
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
    total = len(pending)
    completed = [0]
    lock = threading.Lock()

    def _wrapped_on_result(row: dict) -> None:
        on_result(row)
        with lock:
            completed[0] += 1
            on_progress(completed[0], total)

    # Skip duplicates
    for p in prospects:
        if str(p.get("email", "")).strip().lower() in already_sent:
            _wrapped_on_result(_make_log_row(p, "", "skipped_duplicate"))

    if not pending:
        return

    draft_queue: queue.Queue = queue.Queue(maxsize=LLM_MAX_CONCURRENT * 2)
    send_sem = threading.Semaphore(CONCURRENT_SENDS)

    # Phase 2: send workers
    send_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="send")
    send_futures = [
        send_executor.submit(_send_phase, draft_queue, send_sem, _wrapped_on_result, stop_event, completed)
        for _ in range(MAX_WORKERS)
    ]

    # Phase 1: LLM generation
    with ThreadPoolExecutor(max_workers=LLM_MAX_CONCURRENT, thread_name_prefix="llm") as llm_exec:
        gen_futures = [
            llm_exec.submit(_generate_one, p, draft_queue, _wrapped_on_result, stop_event, dry_run)
            for p in pending
        ]
        for f in as_completed(gen_futures):
            f.result()  # surface exceptions

    # Signal phase 2 workers to stop
    draft_queue.put(_SENTINEL)
    send_executor.shutdown(wait=True)

    log.info("Pipeline complete — total=%d completed=%d", total, completed[0])
```

- [ ] **Step 2: Smoke-test pipeline imports**

```bash
python -c "
from core.pipeline import run_pipeline, _make_log_row
import threading
print('pipeline imported ok')
stop = threading.Event()
results = []
run_pipeline([], set(), lambda r: results.append(r), lambda d, t: None, stop)
print('empty run ok, results:', results)
"
```
Expected: `pipeline imported ok`, `empty run ok, results: []`

- [ ] **Step 3: Commit**

```bash
git add core/pipeline.py
git commit -m "feat: shared concurrent pipeline (Phase1 LLM + Phase2 SMTP)"
```

---

## Task 6: agent.py — thin CLI wrapper

**Files:**
- Modify: `agent.py`

- [ ] **Step 1: Replace `agent.py` with the thin wrapper**

```python
"""
BD Outreach Agent — batch CLI
Usage:
  python agent.py                    # process all pending prospects
  python agent.py --dry-run          # generate but do not send
  python agent.py --limit 20         # cap at 20 prospects this run
  python agent.py --file custom.csv  # override PROSPECTS_FILE env var
"""

import argparse
import csv
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from core.deliverability import is_suppressed
from core.logger import get_logger, IST
from core.pipeline import run_pipeline
from core.prospect_csv import canonicalize_prospect_row

load_dotenv()

log = get_logger()

SENT_LOG_FILE   = os.getenv("SENT_LOG_FILE", "sent_log.csv")
PROSPECTS_FILE  = os.getenv("PROSPECTS_FILE", "prospects.csv")

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error", "from_email",
]

_log_lock   = threading.Lock()
_stop_event = threading.Event()


def _init_log() -> None:
    if not os.path.exists(SENT_LOG_FILE):
        with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()


def _migrate_log() -> None:
    """Add from_email column to old sent_log.csv files that lack it."""
    if not os.path.isfile(SENT_LOG_FILE):
        return
    with open(SENT_LOG_FILE, encoding="utf-8") as f:
        first = f.readline()
    if "from_email" in first:
        return
    with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        return
    header, *body = rows
    new_header = header + ["from_email"]
    padded = [list(r) + [""] * (len(new_header) - len(r)) for r in body]
    with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(new_header)
        w.writerows(padded)


def _load_sent() -> set[str]:
    if not os.path.exists(SENT_LOG_FILE):
        return set()
    with open(SENT_LOG_FILE, encoding="utf-8") as f:
        return {
            row["prospect_email"].strip().lower()
            for row in csv.DictReader(f)
            if row.get("status") == "pushed"
        }


def _append_log(row: dict) -> None:
    with _log_lock:
        with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writerow(row)


def _load_prospects(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return [canonicalize_prospect_row(r) for r in csv.DictReader(f)]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BD Outreach batch agent")
    p.add_argument("--dry-run", action="store_true", help="Generate emails but do not send")
    p.add_argument("--limit", type=int, default=0, metavar="N",
                   help="Process at most N prospects (0 = all)")
    p.add_argument("--file", default="", metavar="PATH",
                   help="Prospects CSV (overrides PROSPECTS_FILE env var)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    prospects_file = args.file or PROSPECTS_FILE

    log.info("BD Outreach agent starting — dry_run=%s limit=%s file=%s",
             args.dry_run, args.limit or "all", prospects_file)
    print(f"[{datetime.now(tz=IST).strftime('%H:%M:%S IST')}] BD Outreach agent starting...",
          flush=True)

    _init_log()
    _migrate_log()

    all_prospects = _load_prospects(prospects_file)
    already_sent  = _load_sent()

    if args.limit > 0:
        pending = [
            p for p in all_prospects
            if str(p.get("email", "")).strip().lower() not in already_sent
        ][:args.limit]
        # Keep all for duplicate marking but cap pending
        all_prospects = list({p["email"]: p for p in all_prospects}.values())

    total = len([p for p in all_prospects if str(p.get("email","")).strip()])
    print(f"Loaded {len(all_prospects)} prospects — {total} with email — "
          f"{len(already_sent)} already sent"
          + (" [DRY RUN]" if args.dry_run else ""), flush=True)

    # SIGTERM / SIGINT → graceful shutdown
    def _handle_signal(sig, _frame):
        print(f"\n[signal {sig}] Stopping after in-flight work completes...", flush=True)
        log.warning("Received signal %s — setting stop_event", sig)
        _stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    counts: dict[str, int] = {}
    start_time = time.time()

    def _on_result(row: dict) -> None:
        _append_log(row)
        st = row["status"]
        counts[st] = counts.get(st, 0) + 1

    def _on_progress(done: int, total_: int) -> None:
        if total_ == 0:
            return
        elapsed = time.time() - start_time
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total_ - done) / rate if rate > 0 else 0
        ts = datetime.now(tz=IST).strftime("%H:%M:%S IST")
        print(
            f"[{ts}] {done}/{total_}  {rate:.1f}/min  ETA ~{eta/60:.1f}min",
            flush=True,
        )

    run_pipeline(
        prospects=all_prospects,
        already_sent=already_sent,
        on_result=_on_result,
        on_progress=_on_progress,
        stop_event=_stop_event,
        dry_run=args.dry_run,
    )

    elapsed = time.time() - start_time
    summary = (
        f"Done — pushed: {counts.get('pushed', 0)}, "
        f"dry_run: {counts.get('dry_run', 0)}, "
        f"skipped_suppressed: {counts.get('skipped_suppressed', 0)}, "
        f"skipped_duplicate: {counts.get('skipped_duplicate', 0)}, "
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}  "
        f"({elapsed:.0f}s)"
    )
    print(f"\n{summary}", flush=True)
    log.info(summary)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify CLI help works**

```bash
python agent.py --help
```
Expected: argparse help output listing `--dry-run`, `--limit`, `--file`.

- [ ] **Step 3: Dry-run against main.csv**

```bash
python agent.py --dry-run --limit 2 --file main.csv
```
Expected: generates 2 emails, logs `dry_run` status, no SMTP connections.

- [ ] **Step 4: Commit**

```bash
git add agent.py
git commit -m "feat: agent.py refactored as thin CLI wrapper around core/pipeline"
```

---

## Task 7: ui.py — Tab structure + CSV mapping preview

**Files:**
- Modify: `ui.py`

- [ ] **Step 1: Add tab structure and mapping preview to ui.py**

Replace the top of `ui.py` (after imports and page config) with a 3-tab shell, and add the mapping preview inside Tab 1 after CSV upload. Full replacement of `ui.py`:

```python
import csv
import io
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.deliverability import is_suppressed, reload_suppression
from core.email_drafter import draft_email
from core.logger import IST
from core.pipeline import run_pipeline, SENT_LOG_FILE
from core.prospect_csv import normalise_prospects_dataframe, detect_column_mapping
from core.smtp_sender import send_email, _sender_state, _pool_lock, DAILY_LIMIT, HOURLY_LIMIT

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BD Outreach",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; max-width: 1200px; }
    footer { visibility: hidden; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_COLS = {"first_name", "last_name", "email", "company", "title"}
OPTIONAL_COLS = {"hcm_platform"}

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error", "from_email",
]

# ── Session defaults ──────────────────────────────────────────────────────────

_DEFAULTS = {
    "prospects": [],
    "results": {},
    "sel": 0,
    # batch tab
    "batch_prospects": [],
    "batch_log": [],
    "batch_running": False,
    "batch_done": 0,
    "batch_total": 0,
    "batch_stop_event": None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Helpers ───────────────────────────────────────────────────────────────────

def _key(p: dict) -> str:
    rid = p.get("_row_id", 0)
    e = p.get("email", "").strip().lower()
    return f"{rid}:{e}"


def _has_contact_email(p: dict) -> bool:
    return bool(str(p.get("email", "")).strip())


def _initial_outreach_status(p: dict) -> str:
    return "no_email" if not _has_contact_email(p) else "pending"


def _outreach_status_label(p: dict, results: dict) -> str:
    if not _has_contact_email(p):
        return "no_email"
    if is_suppressed(p.get("email", "")):
        return "suppressed"
    st_ = results.get(_key(p), {}).get("status", "pending")
    if st_ == "done":
        return "generated"
    if st_ in ("sent", "failed"):
        return st_
    return "pending"


def _validate(df: pd.DataFrame) -> list[str]:
    errs: list[str] = []
    if df.empty:
        return ["No rows found in uploaded CSV."]
    if not df["email"].astype(str).str.strip().any():
        errs.append("No usable email column found.")
    return errs


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = normalise_prospects_dataframe(df)
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = ""
    return df


def _to_csv(prospects: list[dict], results: dict) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["first_name", "last_name", "email", "company", "title",
                "hcm_platform", "outreach_status", "subject", "body", "status", "error"])
    for p in prospects:
        r = results.get(_key(p), {})
        w.writerow([p.get("first_name",""), p.get("last_name",""), p.get("email",""),
                    p.get("company",""), p.get("title",""), p.get("hcm_platform",""),
                    _outreach_status_label(p, results),
                    r.get("subject",""), r.get("body",""),
                    r.get("status","pending"), r.get("error","")])
    return buf.getvalue().encode()


def _icon(status: str) -> str:
    return {"done": "✅", "sent": "📤", "failed": "❌", "sending": "⏳"}.get(status, "⏸️")


def _row_icon(p: dict, results: dict) -> str:
    if not _has_contact_email(p):
        return "📭"
    if is_suppressed(p.get("email", "")):
        return "🚫"
    return _icon(results.get(_key(p), {}).get("status", "pending"))


def _confidence_badge(conf: str) -> str:
    return {"high": "✅ high", "low": "⚠️ low", "missing": "❌ missing"}.get(conf, conf)


def _load_sent_log() -> pd.DataFrame:
    if not os.path.isfile(SENT_LOG_FILE):
        return pd.DataFrame(columns=_LOG_HEADERS)
    try:
        df = pd.read_csv(SENT_LOG_FILE, dtype=str, keep_default_na=False)
        return df
    except Exception:
        return pd.DataFrame(columns=_LOG_HEADERS)


def _ist_display(ts_utc: str) -> str:
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return ts_utc


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## BD Outreach")
    st.caption("CSV → OpenAI → SMTP")
    st.divider()
    oai_ok     = "configured" if os.getenv("OPENAI_API_KEY","").startswith("sk-") else "missing"
    pool_count = len([e for e in os.getenv("SENDER_POOL","").split(",") if ":" in e])
    pool_ok    = f"{pool_count} senders configured" if pool_count else "missing"
    st.markdown(f"**OpenAI key:** {oai_ok}")
    st.markdown(f"**Sender pool:** {pool_ok}")
    st.markdown(f"**Model:** `{os.getenv('OPENAI_MODEL','gpt-4.1-mini')}`")
    st.markdown(f"**SMTP:** `{os.getenv('SMTP_HOST','mail.recruitagents.net')}`")
    st.caption("Edit `.env` to change settings")

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_outreach, tab_batch, tab_log = st.tabs(["📧 Outreach", "⚡ Batch", "📋 Activity Log"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OUTREACH (individual, existing flow + mapping preview)
# ══════════════════════════════════════════════════════════════════════════════

with tab_outreach:
    st.markdown("# BD Outreach")
    st.caption("Upload CSV · Generate personalised emails · Preview & send")
    st.divider()

    with st.expander("**Step 1 — Upload prospects CSV**",
                     expanded=not bool(st.session_state.prospects)):
        st.caption(
            "Required: `first_name` `last_name` `email` `company` `title`  ·  Optional: `hcm_platform`"
        )
        uploaded = st.file_uploader("CSV file", type=["csv"], label_visibility="collapsed",
                                    key="tab1_upload")
        if uploaded:
            try:
                raw_df = pd.read_csv(uploaded, dtype=str, keep_default_na=False,
                                     engine="python", on_bad_lines="skip")
                df = _normalise(raw_df)
                errs = _validate(df)
                if errs:
                    for e in errs:
                        st.error(e)
                else:
                    # ── Mapping preview ──────────────────────────────────────
                    mapping = detect_column_mapping(raw_df)
                    st.markdown("**Detected column mapping:**")
                    rows_preview = []
                    any_low = False
                    any_missing = False
                    for field, info in mapping.items():
                        conf = info["confidence"]
                        if conf == "low":
                            any_low = True
                        if conf == "missing":
                            any_missing = True
                        rows_preview.append({
                            "Field": field,
                            "Mapped from": info["mapped_from"] or "(not found)",
                            "Confidence": _confidence_badge(conf),
                        })
                    st.dataframe(pd.DataFrame(rows_preview), hide_index=True, width=600)
                    if any_missing and "email" in [
                        f for f, i in mapping.items() if i["confidence"] == "missing"
                    ]:
                        st.error("No email column detected — cannot proceed.")
                        st.stop()
                    if any_low:
                        st.warning("Some columns mapped with low confidence — review the table above.")
                    if any_missing:
                        st.caption("Missing optional fields will be blank.")
                    # ── Load prospects ───────────────────────────────────────
                    all_cols = list(REQUIRED_COLS | OPTIONAL_COLS)
                    raw_rows = df[[c for c in all_cols if c in df.columns]].fillna("").to_dict("records")
                    new_prospects = []
                    for i, row in enumerate(raw_rows):
                        p = dict(row)
                        p["_row_id"] = i
                        p["outreach_status"] = _initial_outreach_status(p)
                        new_prospects.append(p)
                    if new_prospects != st.session_state.prospects:
                        st.session_state.prospects = new_prospects
                        st.session_state.results = {}
                        st.session_state.sel = 0
                    st.success(f"Loaded **{len(new_prospects)}** prospects")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")

    prospects: list[dict] = st.session_state.prospects
    results:   dict       = st.session_state.results

    for i, p in enumerate(prospects):
        if "_row_id" not in p:
            p["_row_id"] = i
        if "outreach_status" not in p:
            p["outreach_status"] = _initial_outreach_status(p)

    if not prospects:
        st.info("Upload a CSV above to get started.")
        st.stop()

    # Prospects table
    st.markdown("### Prospects")

    def _err_cell(p: dict, results: dict) -> str:
        r = results.get(_key(p), {})
        if r.get("status") != "failed":
            return ""
        return str(r.get("error", "") or "")[:60]

    st.dataframe(
        pd.DataFrame([{
            "#":               i + 1,
            "Name":            f"{p['first_name']} {p['last_name']}",
            "Email":           p["email"],
            "Company":         p["company"],
            "Title":           p["title"],
            "outreach_status": _outreach_status_label(p, results),
            "Error":           _err_cell(p, results),
            "":                _row_icon(p, results),
        } for i, p in enumerate(prospects)]),
        width="stretch",
        hide_index=True,
        column_config={
            "": st.column_config.TextColumn(width="small"),
            "Error": st.column_config.TextColumn(width="large"),
        },
    )

    # Generate + send
    st.markdown("### Generate emails")

    pending_gen = [
        p for p in prospects
        if _has_contact_email(p)
        and results.get(_key(p), {}).get("status") not in ("done", "sent")
    ]
    pending_gs = [
        p for p in prospects
        if _has_contact_email(p)
        and not is_suppressed(p.get("email", ""))
        and results.get(_key(p), {}).get("status") != "sent"
    ]

    col_gen, col_gs, col_dl = st.columns([2, 2, 1])
    with col_gen:
        if st.button(
            f"Generate all ({len(pending_gen)} pending)" if pending_gen else "All generated",
            type="primary", disabled=not pending_gen, use_container_width=True,
        ):
            prog = st.progress(0)
            stat = st.empty()
            for i, p in enumerate(pending_gen):
                stat.caption(f"[{i+1}/{len(pending_gen)}] Drafting {p['first_name']} {p['last_name']}…")
                prog.progress(int((i + 1) / len(pending_gen) * 100))
                k = _key(p)
                try:
                    ec = draft_email({
                        "name":         f"{p['first_name']} {p['last_name']}",
                        "company":      p.get("company", ""),
                        "title":        p.get("title", ""),
                        "hcm_platform": p.get("hcm_platform", ""),
                    })
                    st.session_state.results[k] = {"subject": ec["subject"], "body": ec["body"],
                                                    "status": "done", "error": ""}
                except Exception as e:
                    st.session_state.results[k] = {"subject":"","body":"","status":"failed","error":str(e)}
            prog.empty(); stat.empty()
            st.rerun()

    with col_gs:
        if st.button(
            f"Generate & send all ({len(pending_gs)})" if pending_gs else "Nothing to send",
            type="primary", disabled=not pending_gs, use_container_width=True,
        ):
            prog = st.progress(0)
            stat = st.empty()
            n = len(pending_gs)
            for i, p in enumerate(pending_gs):
                k = _key(p)
                r0 = st.session_state.results.get(k, {})
                has_draft = bool(str(r0.get("subject","")).strip() and str(r0.get("body","")).strip())
                stat.caption(
                    f"[{i+1}/{n}] {p['first_name']} {p['last_name']}: "
                    + ("send…" if has_draft else "draft + send…")
                )
                prog.progress(int((i + 1) / n * 100))
                if not has_draft:
                    try:
                        ec = draft_email({
                            "name":         f"{p['first_name']} {p['last_name']}",
                            "company":      p.get("company", ""),
                            "title":        p.get("title", ""),
                            "hcm_platform": p.get("hcm_platform", ""),
                        })
                        st.session_state.results[k] = {"subject": ec["subject"], "body": ec["body"],
                                                        "status": "done", "error": ""}
                    except Exception as e:
                        st.session_state.results[k] = {"subject":"","body":"","status":"failed","error":str(e)}
                        continue
                r = st.session_state.results[k]
                try:
                    send_email(p["email"], r["subject"], r["body"], p.get("first_name",""))
                    st.session_state.results[k]["status"] = "sent"
                    st.session_state.results[k]["error"] = ""
                except Exception as e:
                    st.session_state.results[k]["status"] = "failed"
                    st.session_state.results[k]["error"] = str(e)
            prog.empty(); stat.empty()
            st.rerun()

    failed_rows = [p for p in prospects
                   if _has_contact_email(p) and results.get(_key(p), {}).get("status") == "failed"]
    if failed_rows:
        if st.button(f"↺ Retry failed ({len(failed_rows)})", type="secondary", key="retry_bulk"):
            for p in failed_rows:
                st.session_state.results[_key(p)]["status"] = "pending"
            st.rerun()

    with col_dl:
        n_done = sum(1 for r in results.values() if r.get("status") in ("done","sent"))
        if n_done:
            st.download_button("Download CSV", data=_to_csv(prospects, results),
                               file_name="outreach_emails.csv", mime="text/csv",
                               use_container_width=True)

    # Preview & Send
    generated = [p for p in prospects if results.get(_key(p), {}).get("status") in ("done","sent")]
    if not generated:
        st.stop()

    st.divider()
    hdr_col, send_all_col = st.columns([3, 1])
    hdr_col.markdown("### Preview & Send")

    unsent = [
        p for p in generated
        if results.get(_key(p), {}).get("status") != "sent"
        and not is_suppressed(p.get("email", ""))
    ]
    with send_all_col:
        if st.button(f"Send all ({len(unsent)})", type="primary",
                     disabled=not unsent, use_container_width=True):
            prog = st.progress(0)
            stat = st.empty()
            failed = 0
            for i, p in enumerate(unsent):
                k   = _key(p)
                res = results[k]
                stat.caption(f"[{i+1}/{len(unsent)}] Sending to {p['first_name']} {p['last_name']}…")
                prog.progress(int((i + 1) / len(unsent) * 100))
                try:
                    send_email(p["email"], res["subject"], res["body"], p.get("first_name",""))
                    st.session_state.results[k]["status"] = "sent"
                except Exception as e:
                    st.session_state.results[k]["status"] = "failed"
                    st.session_state.results[k]["error"]  = str(e)
                    failed += 1
            prog.empty(); stat.empty()
            st.success(f"Sent {len(unsent)-failed}" + (f", {failed} failed" if failed else ""))
            st.rerun()

    left, right = st.columns([1, 2], gap="large")
    with left:
        st.caption(f"{len(generated)} emails generated")
        for i, p in enumerate(generated):
            k      = _key(p)
            name   = f"{p['first_name']} {p['last_name']}"
            if st.button(f"{_row_icon(p, results)} **{name}**  \n{p['company']}",
                         key=f"sel_{i}", use_container_width=True,
                         type="primary" if st.session_state.sel == i else "secondary"):
                st.session_state.sel = i
                st.rerun()

    with right:
        idx = min(st.session_state.sel, len(generated) - 1)
        p   = generated[idx]
        k   = _key(p)
        res = results[k]
        name   = f"{p['first_name']} {p['last_name']}"
        status = res.get("status", "done")
        st.markdown(f"#### {name}  {_icon(status)}")
        st.caption(f"{p['title']} · {p['company']}"
                   + (f" · {p['hcm_platform']}" if p.get("hcm_platform") else ""))
        st.caption(f"Email: {p['email']}")
        st.divider()
        subj = st.text_input("Subject", value=res.get("subject",""), key=f"s_{k}")
        body = st.text_area("Body", value=res.get("body",""), height=300, key=f"b_{k}")
        if subj != res.get("subject") or body != res.get("body"):
            st.session_state.results[k]["subject"] = subj
            st.session_state.results[k]["body"]    = body

        btn1, btn2, btn3 = st.columns(3)
        with btn1:
            st.download_button("Download .txt", data=f"Subject: {subj}\n\n{body}".encode(),
                               file_name=f"{name.replace(' ','_')}.txt",
                               use_container_width=True)
        with btn2:
            if st.button("↺ Regenerate", use_container_width=True, key=f"regen_{k}"):
                with st.spinner("Regenerating…"):
                    try:
                        ec = draft_email({"name": name, "company": p.get("company",""),
                                          "title": p.get("title",""),
                                          "hcm_platform": p.get("hcm_platform","")})
                        st.session_state.results[k].update(subject=ec["subject"],
                                                            body=ec["body"], status="done", error="")
                    except Exception as e:
                        st.error(str(e))
                st.rerun()
        with btn3:
            if is_suppressed(p["email"]):
                st.caption("Send disabled (suppression list)")
            elif status != "sent":
                if st.button("Send", type="primary", use_container_width=True, key=f"send_{k}"):
                    with st.spinner("Sending…"):
                        try:
                            from_addr = send_email(p["email"], subj, body, p.get("first_name",""))
                            st.session_state.results[k]["status"] = "sent"
                            st.success(f"Sent via {from_addr}")
                        except Exception as e:
                            st.session_state.results[k]["status"] = "failed"
                            st.session_state.results[k]["error"]  = str(e)
                            st.error(str(e))
                    st.rerun()
            else:
                st.success("Sent")
        if res.get("error"):
            st.error(res["error"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — BATCH
# ══════════════════════════════════════════════════════════════════════════════

with tab_batch:
    st.markdown("# ⚡ Batch Run")
    st.caption("Upload a CSV, click Start — the full concurrent pipeline runs in-browser.")

    uploaded_batch = st.file_uploader("Prospects CSV", type=["csv"],
                                      label_visibility="collapsed", key="batch_upload")
    if uploaded_batch:
        try:
            raw_df_b = pd.read_csv(uploaded_batch, dtype=str, keep_default_na=False,
                                   engine="python", on_bad_lines="skip")
            df_b = _normalise(raw_df_b)
            if not df_b.empty:
                mapping_b = detect_column_mapping(raw_df_b)
                with st.expander("Column mapping", expanded=False):
                    st.dataframe(pd.DataFrame([{
                        "Field": f,
                        "Mapped from": i["mapped_from"] or "(not found)",
                        "Confidence": _confidence_badge(i["confidence"]),
                    } for f, i in mapping_b.items()]), hide_index=True)

                all_cols_b = list(REQUIRED_COLS | OPTIONAL_COLS)
                rows_b = df_b[[c for c in all_cols_b if c in df_b.columns]].fillna("").to_dict("records")
                new_batch = [dict(r, _row_id=i) for i, r in enumerate(rows_b)]
                if new_batch != st.session_state.batch_prospects:
                    st.session_state.batch_prospects = new_batch
                    st.session_state.batch_log = []
                    st.session_state.batch_done = 0
                    st.session_state.batch_total = len(new_batch)
                st.success(f"Loaded **{len(new_batch)}** prospects")
        except Exception as e:
            st.error(f"Could not read CSV: {e}")

    batch_prospects = st.session_state.batch_prospects
    if not batch_prospects:
        st.info("Upload a CSV above to start a batch run.")
    else:
        already_sent_b = set()
        if os.path.isfile(SENT_LOG_FILE):
            try:
                sl = pd.read_csv(SENT_LOG_FILE, dtype=str, keep_default_na=False)
                already_sent_b = set(sl[sl["status"]=="pushed"]["prospect_email"].str.strip().str.lower().tolist())
            except Exception:
                pass

        pending_b = [
            p for p in batch_prospects
            if str(p.get("email","")).strip().lower() not in already_sent_b
        ]

        col_start, col_stop, col_dryrun = st.columns([2, 1, 1])

        dry_run_b = col_dryrun.checkbox("Dry run (no send)", key="batch_dryrun")

        with col_start:
            start_disabled = st.session_state.batch_running or not pending_b
            if st.button(
                f"▶ Start batch ({len(pending_b)} pending)" if not st.session_state.batch_running
                else "⏳ Running…",
                type="primary",
                disabled=start_disabled,
                use_container_width=True,
            ):
                stop_ev = threading.Event()
                st.session_state.batch_stop_event = stop_ev
                st.session_state.batch_running = True
                st.session_state.batch_log = []
                st.session_state.batch_done = 0
                st.session_state.batch_total = len(pending_b)

                _log_lock_b = threading.Lock()

                def _on_result_b(row: dict) -> None:
                    with _log_lock_b:
                        st.session_state.batch_log.append(row)
                    # Append to sent_log.csv
                    try:
                        if not os.path.isfile(SENT_LOG_FILE):
                            with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                                csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()
                        with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
                            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writerow(
                                {h: row.get(h, "") for h in _LOG_HEADERS}
                            )
                    except Exception:
                        pass

                def _on_progress_b(done: int, total: int) -> None:
                    st.session_state.batch_done = done
                    st.session_state.batch_total = total

                def _run_batch():
                    run_pipeline(
                        prospects=batch_prospects,
                        already_sent=already_sent_b,
                        on_result=_on_result_b,
                        on_progress=_on_progress_b,
                        stop_event=stop_ev,
                        dry_run=dry_run_b,
                    )
                    st.session_state.batch_running = False

                t = threading.Thread(target=_run_batch, daemon=True)
                t.start()
                st.rerun()

        with col_stop:
            if st.button("⏹ Stop", disabled=not st.session_state.batch_running,
                         use_container_width=True):
                if st.session_state.batch_stop_event:
                    st.session_state.batch_stop_event.set()

        # Progress bar
        done  = st.session_state.batch_done
        total_b = st.session_state.batch_total
        if total_b > 0:
            st.progress(done / total_b, text=f"{done}/{total_b} processed")

        # Live log table
        batch_log = st.session_state.batch_log
        if batch_log:
            st.markdown(f"**Results** ({len(batch_log)} rows so far)")
            log_df = pd.DataFrame(batch_log[::-1])  # newest first
            if "timestamp" in log_df.columns:
                log_df["timestamp"] = log_df["timestamp"].apply(_ist_display)
            st.dataframe(log_df, hide_index=True, use_container_width=True,
                         column_config={
                             "error": st.column_config.TextColumn(width="large"),
                         })

        # Auto-rerun while running
        if st.session_state.batch_running:
            time.sleep(0.8)
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ACTIVITY LOG
# ══════════════════════════════════════════════════════════════════════════════

with tab_log:
    st.markdown("# 📋 Activity Log")

    col_ref, col_supp, col_auto = st.columns([1, 1, 1])

    with col_ref:
        if st.button("↺ Refresh log", use_container_width=True):
            reload_suppression()
            st.rerun()

    auto_refresh = col_auto.checkbox("Auto-refresh (30s)", key="log_autorefresh")

    log_df = _load_sent_log()

    if log_df.empty:
        st.info("No sends logged yet.")
    else:
        # IST timestamps
        if "timestamp" in log_df.columns:
            log_df["timestamp_ist"] = log_df["timestamp"].apply(_ist_display)

        # ── Summary metrics ───────────────────────────────────────────────────
        today_ist = datetime.now(tz=IST).strftime("%Y-%m-%d")
        today_rows = log_df[log_df.get("timestamp_ist", log_df["timestamp"]).str.startswith(today_ist)]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sent today",   int((today_rows.get("status", pd.Series()) == "pushed").sum()))
        c2.metric("Failed today", int((today_rows.get("status", pd.Series()) == "failed_api").sum()))
        c3.metric("Total rows",   len(log_df))
        pushed_total = int((log_df.get("status", pd.Series()) == "pushed").sum())
        c4.metric("Total sent",   pushed_total)

        st.divider()

        # ── Per-sender daily usage ────────────────────────────────────────────
        if "from_email" in log_df.columns:
            st.markdown("**Sender usage today**")
            sender_today = today_rows[today_rows["status"] == "pushed"]["from_email"].value_counts()
            if not sender_today.empty:
                for sender, count in sender_today.items():
                    pct = min(count / max(DAILY_LIMIT, 1), 1.0)
                    st.markdown(f"`{sender}` — {count}/{DAILY_LIMIT}")
                    st.progress(pct)
            else:
                st.caption("No sends today.")

        st.divider()

        # ── Filters ───────────────────────────────────────────────────────────
        f1, f2, f3 = st.columns(3)
        status_opts = ["all"] + sorted(log_df["status"].dropna().unique().tolist()) if "status" in log_df.columns else ["all"]
        sender_opts = ["all"] + sorted(log_df["from_email"].dropna().unique().tolist()) if "from_email" in log_df.columns else ["all"]

        sel_status = f1.selectbox("Status", status_opts, key="log_status_filter")
        sel_sender = f2.selectbox("Sender", sender_opts, key="log_sender_filter")
        sel_date   = f3.date_input("Date (IST)", value=None, key="log_date_filter")

        filtered = log_df.copy()
        if sel_status != "all" and "status" in filtered.columns:
            filtered = filtered[filtered["status"] == sel_status]
        if sel_sender != "all" and "from_email" in filtered.columns:
            filtered = filtered[filtered["from_email"] == sel_sender]
        if sel_date and "timestamp_ist" in filtered.columns:
            filtered = filtered[filtered["timestamp_ist"].str.startswith(str(sel_date))]

        # Show newest first, use IST timestamp
        show_cols = ["timestamp_ist", "prospect_email", "prospect_name",
                     "company", "status", "from_email", "subject", "error"]
        show_cols = [c for c in show_cols if c in filtered.columns or c == "timestamp_ist"]
        display_df = filtered[show_cols].iloc[::-1].reset_index(drop=True)

        st.markdown(f"**{len(display_df)} rows** (newest first)")
        st.dataframe(display_df, hide_index=True, use_container_width=True,
                     column_config={
                         "error":         st.column_config.TextColumn(width="large"),
                         "timestamp_ist": st.column_config.TextColumn("Timestamp (IST)", width="medium"),
                     })

        # Download filtered
        st.download_button("Download filtered CSV",
                           data=display_df.to_csv(index=False).encode(),
                           file_name="activity_log_filtered.csv",
                           mime="text/csv")

    # ── Suppression list ──────────────────────────────────────────────────────
    st.divider()
    st.markdown("**Suppression list**")
    supp_file = os.getenv("SUPPRESSION_FILE", "suppression.txt")
    if os.path.isfile(supp_file):
        with open(supp_file, encoding="utf-8") as f:
            supp_contents = f.read().strip()
        supp_count = len([l for l in supp_contents.splitlines() if l.strip() and not l.startswith("#")])
        st.caption(f"{supp_count} address(es) suppressed — `{supp_file}`")
        with st.expander("View suppression list"):
            st.code(supp_contents, language=None)
    else:
        st.caption(f"`{supp_file}` not found — no addresses suppressed.")

    if auto_refresh:
        time.sleep(30)
        st.rerun()
```

- [ ] **Step 2: Run Streamlit and verify all 3 tabs load**

```bash
streamlit run ui.py
```

Open http://localhost:8501. Verify:
- Tab "📧 Outreach" renders (upload + mapping preview on CSV load)
- Tab "⚡ Batch" renders with upload widget and Start button
- Tab "📋 Activity Log" renders with metrics and filter row

- [ ] **Step 3: Upload a small CSV in the Outreach tab and verify mapping preview shows**

Use `main.csv`. Confirm the mapping table appears after upload with confidence badges.

- [ ] **Step 4: Commit**

```bash
git add ui.py
git commit -m "feat: 3-tab UI (Outreach+mapping, Batch, Activity Log) with IST timestamps"
```

---

## Task 8: .env.example — updated defaults

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Update `.env.example` with new variables**

Add the following lines under the appropriate sections in `.env.example`:

Under `# ── SMTP sending`:
```
HOURLY_LIMIT=95
HOURLY_BUFFER=5
SEND_DELAY_SECONDS=0
```

Under `# ── Agent (batch runner)`:
```
LLM_MAX_CONCURRENT=20
CONCURRENT_SENDS=5
LOG_DIR=logs
LOG_KEEP_DAYS=7
```

Remove the old `HOURLY_LIMIT=100` line if present and replace with `95` (5-buffer below 100).

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "chore: update .env.example with new rate-limit and logging defaults"
```

---

## Task 9: End-to-end smoke test

- [ ] **Step 1: Dry-run CLI against main.csv**

```bash
python agent.py --dry-run --limit 3 --file main.csv
```
Expected output contains IST timestamp, progress lines, `dry_run: 3` in summary.

- [ ] **Step 2: Verify logs directory**

```bash
ls logs/
```
Expected: `agent.log` exists.

- [ ] **Step 3: Open Streamlit and run a batch dry-run**

```bash
streamlit run ui.py
```
1. Go to **⚡ Batch** tab
2. Upload `main.csv`
3. Check **Dry run**
4. Click **▶ Start batch (N pending)**
5. Watch progress bar move and results table populate

- [ ] **Step 4: Check Activity Log tab**

Switch to **📋 Activity Log** — metrics should show rows, IST timestamps visible, sender usage bars shown.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: production-grade lead mailer — pipeline, batch UI, IST logs, SiteGround throttle"
```
