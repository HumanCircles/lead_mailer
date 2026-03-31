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

load_dotenv()

SMTP_HOST       = os.getenv("SMTP_HOST", "mail.recruitagents.net")
SMTP_PORT       = int(os.getenv("SMTP_PORT", "465"))
# Missing key → brand default; explicit empty → derive display name from sender address in smtp_from_header.
_fn_raw         = os.getenv("FROM_NAME")
FROM_DISPLAY    = ("HireQuotient" if _fn_raw is None else _fn_raw).strip()
DAILY_LIMIT     = int(os.getenv("DAILY_LIMIT", "150"))
SENT_LOG_FILE   = os.getenv("SENT_LOG_FILE", "sent_log.csv").strip() or "sent_log.csv"
SMTP_TIMEOUT    = int(os.getenv("SMTP_TIMEOUT", "20"))
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


_pool     = _load_pool()
_cycler   = itertools.cycle(_pool)
_counters: dict[str, int] = {e: 0 for e, _ in _pool}


def _hydrate_counters_from_sent_log() -> None:
    """Restore today's per-sender pushed counts so Streamlit restarts respect DAILY_LIMIT."""
    if not os.path.isfile(SENT_LOG_FILE):
        return
    today_utc = datetime.now(timezone.utc).date().isoformat()
    loaded: dict[str, int] = {}
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
            loaded[fe] = loaded.get(fe, 0) + 1
    with _pool_lock:
        for pool_email in _counters:
            _counters[pool_email] = loaded.get(pool_email.lower(), 0)


_hydrate_counters_from_sent_log()


def _next_sender() -> tuple[str, str]:
    with _pool_lock:
        for _ in range(len(_pool)):
            email, pwd = next(_cycler)
            if _counters[email] < DAILY_LIMIT:
                return email, pwd
    raise RuntimeError("All sender accounts have hit the daily limit.")


def smtp_deliver(from_email: str, password: str, to_email: str, msg_as_string: str) -> None:
    """Connect, login, send with socket timeout and retries (transient SMTP / network errors)."""
    for attempt in range(SMTP_MAX_RETRIES):
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as server:
                server.login(from_email, password)
                server.sendmail(from_email, to_email, msg_as_string)
            return
        except (smtplib.SMTPException, OSError, TimeoutError) as e:
            if attempt >= SMTP_MAX_RETRIES - 1:
                raise
            backoff = 2 ** (attempt + 1) + random.uniform(0, 1)
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
    if is_suppressed(to_email):
        raise ValueError(f"Not sending: address is on suppression list ({to_email.strip().lower()})")

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
        _counters[sender_email] += 1
    time.sleep(random.uniform(1.5, 4.0))
    return sender_email
