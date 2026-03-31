import csv
import itertools
import os
import random
import re
import smtplib
import threading
import time
from datetime import datetime, timezone
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
# Missing key → brand default; explicit empty → derive display name from sender address in smtp_from_header.
_fn_raw          = os.getenv("FROM_NAME")
FROM_DISPLAY     = ("HireQuotient" if _fn_raw is None else _fn_raw).strip()
DAILY_LIMIT      = int(os.getenv("DAILY_LIMIT", "150"))
HOURLY_LIMIT     = int(os.getenv("HOURLY_LIMIT", "100"))
HOURLY_BUFFER    = int(os.getenv("HOURLY_BUFFER", "5"))
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "0"))
SENT_LOG_FILE    = os.getenv("SENT_LOG_FILE", "sent_log.csv").strip() or "sent_log.csv"
SMTP_TIMEOUT     = int(os.getenv("SMTP_TIMEOUT", "20"))
SMTP_MAX_RETRIES = int(os.getenv("SMTP_MAX_RETRIES", "3"))

_pool_lock = threading.Lock()


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

# Per-sender state: {email: {"daily": int, "hourly": int, "hour_start": float}}
_sender_state: dict[str, dict] = {
    e: {"daily": 0, "hourly": 0, "hour_start": time.time()}
    for e, _ in _pool
}


def _hydrate_counters_from_sent_log() -> None:
    """Restore today's per-sender pushed counts (daily + hourly) from sent_log.csv.

    Called once at module import so Streamlit restarts and process restarts respect
    both DAILY_LIMIT and HOURLY_LIMIT without waiting for the first lockout.
    """
    if not os.path.isfile(SENT_LOG_FILE):
        return
    now = time.time()
    today_utc = datetime.now(timezone.utc).date().isoformat()
    hour_start_utc = now - (now % 3600)  # top of the current UTC hour (epoch)

    daily:  dict[str, int] = {}
    hourly: dict[str, int] = {}

    try:
        with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "from_email" not in reader.fieldnames:
                return
            for row in reader:
                if row.get("status") != "pushed":
                    continue
                ts = row.get("timestamp") or ""
                if not ts.startswith(today_utc):
                    continue
                fe = (row.get("from_email") or "").strip().lower()
                if not fe:
                    continue
                daily[fe] = daily.get(fe, 0) + 1
                try:
                    sent_epoch = datetime.fromisoformat(
                        ts.replace("Z", "+00:00")
                    ).timestamp()
                    if sent_epoch >= hour_start_utc:
                        hourly[fe] = hourly.get(fe, 0) + 1
                except ValueError:
                    pass
    except Exception as e:
        log.warning("Could not hydrate sender state from log: %s", e)
        return

    with _pool_lock:
        for pool_email in _sender_state:
            key = pool_email.lower()
            _sender_state[pool_email]["daily"]      = daily.get(key, 0)
            _sender_state[pool_email]["hourly"]     = hourly.get(key, 0)
            _sender_state[pool_email]["hour_start"] = hour_start_utc


_hydrate_counters_from_sent_log()


def _next_sender() -> tuple[str, str]:
    """Return the next available (email, password) pair, respecting daily and hourly limits."""
    with _pool_lock:
        now = time.time()
        for _ in range(len(_pool)):
            email, pwd = next(_cycler)
            state = _sender_state[email]

            # Reset hourly counter when the 1-hour window has elapsed
            if now - state["hour_start"] >= 3600:
                state["hourly"] = 0
                state["hour_start"] = now

            daily_ok  = state["daily"]  < DAILY_LIMIT
            hourly_ok = state["hourly"] < (HOURLY_LIMIT - HOURLY_BUFFER)

            if daily_ok and hourly_ok:
                state["hourly"] += 1  # reserve slot now — prevents concurrent over-selection
                return email, pwd

        raise RuntimeError("All sender accounts have hit the daily or hourly limit.")


# ---------------------------------------------------------------------------
# SiteGround / SMTP error classification helpers
# ---------------------------------------------------------------------------

def is_siteground_hourly_lockout(exc: BaseException) -> bool:
    """Return True when str(exc) indicates a SiteGround per-hour 550 lockout.

    SiteGround wraps the SMTP error in a dict when raising
    SMTPRecipientsRefused, so the string representation looks like:
        {'email@x.com': (550, b'...already sent N messages for 1h...')}
    We match on all three substrings to avoid false positives.
    """
    s = str(exc)
    return "550" in s and "already sent" in s and "messages for 1h" in s


def classify_smtp_error(exc: BaseException) -> str:
    """Classify an SMTP exception into a broad category string.

    Returns one of: "lockout" | "auth" | "network" | "unknown"
    """
    if is_siteground_hourly_lockout(exc):
        return "lockout"
    s = str(exc).lower()
    if isinstance(exc, smtplib.SMTPAuthenticationError) or "authentication" in s or "535" in s:
        return "auth"
    if isinstance(exc, (OSError, TimeoutError)) or "connection" in s or "timeout" in s or "network" in s:
        return "network"
    return "unknown"


def seconds_until_capacity_frees() -> float:
    """Return the number of seconds until the earliest per-sender hourly window resets.

    Useful for callers that want to know how long to wait before retrying
    after all senders have hit their hourly limit.  Returns 0.0 if any
    sender already has capacity.
    """
    now = time.time()
    with _pool_lock:
        earliest_free: float | None = None
        for email, _ in _pool:
            state = _sender_state[email]
            elapsed = now - state["hour_start"]

            # Already reset or has capacity right now
            if elapsed >= 3600 or state["hourly"] < (HOURLY_LIMIT - HOURLY_BUFFER):
                return 0.0

            secs_left = 3600.0 - elapsed
            if earliest_free is None or secs_left < earliest_free:
                earliest_free = secs_left

    return earliest_free if earliest_free is not None else 0.0


def _hourly_safe_limit() -> int:
    """Return the effective per-sender hourly send cap (HOURLY_LIMIT - HOURLY_BUFFER)."""
    return HOURLY_LIMIT - HOURLY_BUFFER


# ---------------------------------------------------------------------------
# Core delivery
# ---------------------------------------------------------------------------

_SMTP_HOST_MAP = {
    "superchargedai.org": "gvam1039.siteground.biz",
    # recruitagents.net → mail.recruitagents.net (default derivation, no override needed)
}


def _smtp_host_for(email: str) -> str:
    """Derive SMTP host from sender email domain, using explicit overrides where needed."""
    try:
        domain = email.split("@", 1)[1]
        return _SMTP_HOST_MAP.get(domain, f"mail.{domain}")
    except IndexError:
        return SMTP_HOST


def smtp_deliver(from_email: str, password: str, to_email: str, msg_as_string: str) -> None:
    """Connect, login, send with socket timeout and retries (transient SMTP / network errors)."""
    host = _smtp_host_for(from_email)
    for attempt in range(SMTP_MAX_RETRIES):
        try:
            with smtplib.SMTP_SSL(host, SMTP_PORT, timeout=SMTP_TIMEOUT) as server:
                server.login(from_email, password)
                server.sendmail(from_email, to_email, msg_as_string)
            return
        except (smtplib.SMTPException, OSError, TimeoutError) as e:
            category = classify_smtp_error(e)
            if category == "lockout":
                log.warning("SiteGround hourly lockout detected for %s: %s", from_email, e)
                raise
            if attempt >= SMTP_MAX_RETRIES - 1:
                log.error("SMTP delivery failed after %d attempts (%s): %s", SMTP_MAX_RETRIES, category, e)
                raise
            backoff = 2 ** (attempt + 1) + random.uniform(0, 1)
            log.warning("SMTP attempt %d failed (%s), retrying in %.1fs: %s", attempt + 1, category, backoff, e)
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
    """Send one email. Returns the sender address used. Raises on unrecoverable failure.

    On a SiteGround hourly lockout, marks the sender at-limit and retries with the
    next available sender (up to len(_pool) attempts) before raising.
    """
    if is_suppressed(to_email):
        raise ValueError(f"Not sending: address is on suppression list ({to_email.strip().lower()})")

    body_out = _ensure_recipient_greeting(body, recipient_first_name)

    last_exc: Exception | None = None
    for _attempt in range(len(_pool)):
        sender_email, app_password = _next_sender()

        body_with_sig = append_signature_block(body_out, sender_email=sender_email)
        body_with_sig = append_unsubscribe_footer(body_with_sig)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = smtp_from_header(FROM_DISPLAY, sender_email)
        msg["To"]      = to_email
        msg["Date"]    = formatdate(localtime=True)
        msg.attach(MIMEText(body_with_sig, "plain"))
        apply_list_unsubscribe_headers(msg)

        try:
            smtp_deliver(sender_email, app_password, to_email, msg.as_string())
        except Exception as e:
            last_exc = e
            if is_siteground_hourly_lockout(e):
                # Mark this sender as at-limit so _next_sender() skips it
                with _pool_lock:
                    _sender_state[sender_email]["hourly"] = HOURLY_LIMIT
                log.warning("Lockout on %s — trying next sender", sender_email)
                continue
            raise

        with _pool_lock:
            _sender_state[sender_email]["daily"] += 1
            # hourly already incremented in _next_sender() at selection time

        if SEND_DELAY_SECONDS > 0:
            time.sleep(SEND_DELAY_SECONDS)

        log.info("pushed to=%s from=%s", to_email, sender_email)
        return sender_email

    raise last_exc or RuntimeError("All senders exhausted")
