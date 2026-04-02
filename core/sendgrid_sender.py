"""SendGrid API sender — replaces SMTP.

Domain-to-account routing:
  SENDGRID_ACCOUNTS=domain:api_key:pool_name,...   (comma-separated triples)
  SENDGRID_API_KEY / SENDGRID_IP_POOL              (fallback defaults)

SENDER_POOL format: email:anything or just email (password field ignored — auth is via API key).
"""

from __future__ import annotations

import csv
import os
import re
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Email, Header, IpPoolName, Mail

from core.deliverability import (
    append_signature_block,
    append_unsubscribe_footer,
    is_suppressed,
)
from core.logger import get_logger

load_dotenv()

log = get_logger()

# ── Limits (kept for rate-limiting & Activity Log UI) ─────────────────────
DAILY_LIMIT        = int(os.getenv("DAILY_LIMIT",    "1000"))
HOURLY_LIMIT       = int(os.getenv("HOURLY_LIMIT",   "300"))
HOURLY_BUFFER      = int(os.getenv("HOURLY_BUFFER",  "10"))
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "0"))
SENT_LOG_FILE      = os.getenv("SENT_LOG_FILE", "sent_log.csv").strip() or "sent_log.csv"

_fn_raw      = os.getenv("FROM_NAME")
FROM_DISPLAY = ("HireQuotient" if _fn_raw is None else _fn_raw).strip()

SEED_EMAIL_RECIPIENT = os.getenv("SEED_EMAIL", "ashutosh@HireQuotient.com")

_pool_lock = threading.Lock()


# ── SendGrid account registry ──────────────────────────────────────────────

def _load_sg_accounts() -> dict[str, dict]:
    """Parse SENDGRID_ACCOUNTS=domain:api_key:pool_name,... into a lookup dict.

    Each entry maps a sender domain to its SendGrid subaccount credentials.
    """
    raw = os.getenv("SENDGRID_ACCOUNTS", "").strip()
    accounts: dict[str, dict] = {}
    if not raw:
        return accounts
    for entry in raw.split(","):
        parts = entry.strip().split(":", 2)
        if len(parts) == 3:
            domain, api_key, pool_name = (p.strip() for p in parts)
            if domain and api_key:
                accounts[domain.lower()] = {
                    "api_key":   api_key,
                    "pool_name": pool_name,
                    "client":    SendGridAPIClient(api_key),
                }
    return accounts


_sg_accounts: dict[str, dict] = _load_sg_accounts()

_sg_default_key    = os.getenv("SENDGRID_API_KEY", "").strip()
_sg_default_pool   = os.getenv("SENDGRID_IP_POOL", "").strip()
_sg_default_client = SendGridAPIClient(_sg_default_key) if _sg_default_key else None


def _sg_for_domain(domain: str) -> tuple[SendGridAPIClient, str]:
    """Return (client, pool_name) for a sender email domain."""
    acc = _sg_accounts.get(domain.strip().lower())
    if acc:
        return acc["client"], acc["pool_name"]
    if _sg_default_client:
        return _sg_default_client, _sg_default_pool
    raise ValueError(
        f"No SendGrid API key for domain '{domain}' and SENDGRID_API_KEY is not set."
    )


# ── Sender pool ────────────────────────────────────────────────────────────

def _load_pool() -> list[str]:
    raw = os.getenv("SENDER_POOL", "").strip()
    if not raw:
        raise ValueError("SENDER_POOL is not set in .env")
    emails = [e.split(":", 1)[0].strip() for e in raw.split(",") if "@" in e.split(":", 1)[0]]
    if not emails:
        raise ValueError("SENDER_POOL has no valid email addresses")
    return emails


_pool:     list[str]         = _load_pool()
_pool_idx: list[int]         = [0]

# Per-sender state: {email: {"daily": int, "hourly": int, "hour_start": float}}
_sender_state: dict[str, dict] = {
    e: {"daily": 0, "hourly": 0, "hour_start": time.time()}
    for e in _pool
}


def _hydrate_counters_from_sent_log() -> None:
    """Restore today's per-sender send counts from sent_log.csv at startup."""
    if not os.path.isfile(SENT_LOG_FILE):
        return
    now = time.time()
    today_utc      = datetime.now(timezone.utc).date().isoformat()
    hour_start_utc = now - (now % 3600)

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
                    sent_epoch = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
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


def _next_sender() -> str:
    """Return next available sender email, respecting daily/hourly limits."""
    with _pool_lock:
        now = time.time()
        for _ in range(len(_pool)):
            email = _pool[_pool_idx[0] % len(_pool)]
            _pool_idx[0] += 1
            state = _sender_state[email]

            if now - state["hour_start"] >= 3600:
                state["hourly"]     = 0
                state["hour_start"] = now

            if state["daily"] < DAILY_LIMIT and state["hourly"] < (HOURLY_LIMIT - HOURLY_BUFFER):
                state["hourly"] += 1  # reserve slot
                return email

        raise RuntimeError("All sender accounts have hit the daily or hourly limit.")


def _hourly_safe_limit() -> int:
    return HOURLY_LIMIT - HOURLY_BUFFER


def seconds_until_capacity_frees() -> float:
    now = time.time()
    with _pool_lock:
        earliest: float | None = None
        for email in _sender_state:
            state   = _sender_state[email]
            elapsed = now - state["hour_start"]
            if elapsed >= 3600 or state["hourly"] < _hourly_safe_limit():
                return 0.0
            secs_left = 3600.0 - elapsed
            if earliest is None or secs_left < earliest:
                earliest = secs_left
    return earliest if earliest is not None else 0.0


# Kept as stub — SiteGround-specific lockout no longer applies with SendGrid
def is_siteground_hourly_lockout(_exc: BaseException) -> bool:  # noqa: ARG001
    return False


# ── Core delivery ──────────────────────────────────────────────────────────

def _full_name_from_email(email: str) -> str:
    """Derive display name from sender email local-part.

    ethan.parker@superchargedai.org  →  'Ethan Parker'
    brandon.kelly@recruitagents.net  →  'Brandon Kelly'
    """
    local = email.split("@", 1)[0]
    parts = [p for p in re.split(r"[._\-]+", local) if p]
    return " ".join(p[:1].upper() + p[1:].lower() for p in parts)


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


def _sendgrid_deliver(
    sender_email: str,
    to_email: str,
    subject: str,
    body: str,
) -> None:
    """Build and dispatch a SendGrid Mail object."""
    domain = sender_email.split("@", 1)[1] if "@" in sender_email else ""
    sg_client, pool_name = _sg_for_domain(domain)

    display = FROM_DISPLAY or _full_name_from_email(sender_email)

    message = Mail(
        from_email=Email(sender_email, display),
        to_emails=to_email,
        subject=subject,
        plain_text_content=body,
    )

    if pool_name:
        message.ip_pool_name = IpPoolName(pool_name)

    # List-Unsubscribe header
    unsubscribe_parts: list[str] = []
    mailto = (os.getenv("UNSUBSCRIBE_MAILTO") or os.getenv("UNSUBSCRIBE_EMAIL") or "").strip()
    if mailto:
        unsubscribe_parts.append(f"<mailto:{mailto}?subject=Unsubscribe>")
    url = os.getenv("UNSUBSCRIBE_URL", "").strip()
    if url:
        unsubscribe_parts.append(f"<{url}>")
    if unsubscribe_parts:
        message.header = Header("List-Unsubscribe", ", ".join(unsubscribe_parts))

    response = sg_client.send(message)
    if response.status_code not in (200, 201, 202):
        raise RuntimeError(f"SendGrid API error {response.status_code}: {response.body}")


def send_email(
    to_email: str, subject: str, body: str, recipient_first_name: str = ""
) -> tuple[str, str]:
    """Send one email via SendGrid.

    Returns (sender_address, formatted_body) — the exact body that was delivered,
    including greeting, signature, and unsubscribe footer.
    Raises on unrecoverable failure.
    """
    if is_suppressed(to_email):
        raise ValueError(f"Not sending: address is on suppression list ({to_email.strip().lower()})")

    body_out  = _ensure_recipient_greeting(body, recipient_first_name)
    sender    = _next_sender()

    body_with_sig = append_signature_block(body_out, sender_email=sender)
    body_final    = append_unsubscribe_footer(body_with_sig)

    _sendgrid_deliver(sender, to_email, subject, body_final)

    with _pool_lock:
        _sender_state[sender]["daily"] += 1

    if SEND_DELAY_SECONDS > 0:
        time.sleep(SEND_DELAY_SECONDS)

    log.info("pushed to=%s from=%s via=sendgrid", to_email, sender)
    return sender, body_final


# ── Admin seed email ───────────────────────────────────────────────────────

def send_seed_email(subject: str, body: str) -> None:
    """Send an internal run-summary email to SEED_EMAIL_RECIPIENT.

    Bypasses pool rotation and rate-limit tracking — for admin use only.
    Uses the first sender pool account.
    """
    if not _pool:
        log.warning("Seed email skipped: SENDER_POOL is empty")
        return

    sender = _pool[0]
    domain = sender.split("@", 1)[1] if "@" in sender else ""
    try:
        sg_client, pool_name = _sg_for_domain(domain)
    except ValueError as e:
        log.warning("Seed email skipped: %s", e)
        return

    display = FROM_DISPLAY or _full_name_from_email(sender)

    message = Mail(
        from_email=Email(sender, display),
        to_emails=SEED_EMAIL_RECIPIENT,
        subject=subject,
        plain_text_content=body,
    )
    if pool_name:
        message.ip_pool_name = IpPoolName(pool_name)

    try:
        response = sg_client.send(message)
        log.info(
            "Seed email sent to %s (status=%s)", SEED_EMAIL_RECIPIENT, response.status_code
        )
    except Exception as e:
        log.warning("Seed email failed: %s", e)
