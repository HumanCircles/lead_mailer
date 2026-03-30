"""
BD Outreach Agent — batch CLI
Reads prospects.csv → generates emails via OpenAI → sends via SMTP → logs to sent_log.csv
Run: python agent.py
"""

import csv
import json
import os
import re
import smtplib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from openai import OpenAI, RateLimitError

from core.deliverability import append_unsubscribe_footer, apply_list_unsubscribe_headers, is_suppressed
from core.prospect_csv import canonicalize_prospect_row

load_dotenv()

# ── Config from env ───────────────────────────────────────────────────────────

OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
MODEL              = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "20"))
LLM_MAX_CONCURRENT = int(os.getenv("LLM_MAX_CONCURRENT", "5"))
PROSPECTS_FILE     = os.getenv("PROSPECTS_FILE", "prospects.csv")
SENT_LOG_FILE      = os.getenv("SENT_LOG_FILE", "sent_log.csv")

SMTP_HOST    = os.environ["SMTP_HOST"]
SMTP_PORT    = int(os.getenv("SMTP_PORT", "465"))
FROM_NAME    = os.getenv("FROM_NAME", "")
DAILY_LIMIT  = int(os.getenv("DAILY_LIMIT", "150"))

def _parse_sender_pool() -> list[tuple[str, str]]:
    raw = os.getenv("SENDER_POOL", "")
    result = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            email, password = entry.split(":", 1)
            result.append((email.strip(), password.strip()))
    return result

_senders      = _parse_sender_pool()
_sender_idx   = 0
_sender_counts: dict[str, int] = {}
_sender_lock  = threading.Lock()

_openai   = OpenAI(api_key=OPENAI_API_KEY)
_llm_sem  = threading.Semaphore(LLM_MAX_CONCURRENT)
_log_lock = threading.Lock()

# ── Messaging guide ───────────────────────────────────────────────────────────

with open("MESSAGING_README.md") as _f:
    MESSAGING_GUIDE = _f.read()

# ── Logging ───────────────────────────────────────────────────────────────────

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error",
]


def _init_log():
    if not os.path.exists(SENT_LOG_FILE):
        with open(SENT_LOG_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()


def _load_sent_emails() -> set[str]:
    if not os.path.exists(SENT_LOG_FILE):
        return set()
    with open(SENT_LOG_FILE) as f:
        return {
            row["prospect_email"].strip().lower()
            for row in csv.DictReader(f)
            if row.get("status") == "pushed"
        }


def _append_log(row: dict):
    with _log_lock:
        with open(SENT_LOG_FILE, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writerow(row)


def _log_row(prospect: dict, subject: str, status: str, error: str = "") -> dict:
    return {
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "prospect_email": prospect["email"],
        "prospect_name":  f"{prospect['first_name']} {prospect['last_name']}",
        "company":        prospect.get("company", ""),
        "subject":        subject,
        "status":         status,
        "error":          error,
    }

# ── Prospects ─────────────────────────────────────────────────────────────────

def _load_prospects() -> list[dict]:
    with open(PROSPECTS_FILE, encoding="utf-8", newline="") as f:
        return [canonicalize_prospect_row(row) for row in csv.DictReader(f)]

# ── Generation ────────────────────────────────────────────────────────────────

def _parse_json(raw: str) -> dict:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw.strip())
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"Could not parse JSON:\n{raw[:300]}")


def _generate(prospect: dict) -> dict:
    name     = f"{prospect['first_name']} {prospect['last_name']}"
    company  = prospect.get("company", "")
    title    = prospect.get("title", "")
    platform = prospect.get("hcm_platform", "") or company

    prompt = f"""Prospect:
- Name: {name}
- Title: {title}
- Company: {company}
- Platform: {platform}

Write the email following the playbook exactly. Return JSON only — no markdown, no preamble:
{{
  "subject": "...",
  "body": "..."
}}"""

    messages = [
        {"role": "system", "content": MESSAGING_GUIDE},
        {"role": "user",   "content": prompt},
    ]

    with _llm_sem:
        for attempt in range(2):
            for rl_attempt in range(5):
                try:
                    resp = _openai.chat.completions.create(
                        model=MODEL,
                        max_tokens=1024,
                        messages=messages,
                    )
                    break
                except RateLimitError:
                    if rl_attempt == 4:
                        raise
                    wait = 2 ** rl_attempt
                    print(f"[rate_limit] sleeping {wait}s before retry {rl_attempt + 1}/4")
                    time.sleep(wait)

            raw = (resp.choices[0].message.content or "").strip()
            if raw:
                try:
                    return _parse_json(raw)
                except ValueError:
                    if attempt == 0:
                        continue
                    raise ValueError("Invalid JSON after retry")

    raise ValueError("Model returned empty response.")

# ── SMTP sending ──────────────────────────────────────────────────────────────

def _next_sender() -> tuple[str, str]:
    global _sender_idx
    with _sender_lock:
        for _ in range(len(_senders)):
            sender = _senders[_sender_idx % len(_senders)]
            _sender_idx += 1
            if _sender_counts.get(sender[0], 0) < DAILY_LIMIT:
                return sender
    raise ValueError("Daily send limit reached for all senders")


def _sender_first_name(email: str) -> str:
    local = email.split("@")[0]
    parts = re.split(r"[._\-]", local)
    return parts[0].capitalize()


def _push(prospect: dict, subject: str, body: str):
    from_email, password = _next_sender()
    to_email = prospect["email"]

    first_name = _sender_first_name(from_email)
    body = append_unsubscribe_footer(body.rstrip())
    body = body + f"\n\n{first_name}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{FROM_NAME} <{from_email}>" if FROM_NAME else from_email
    msg["To"]      = to_email

    msg.attach(MIMEText(body, "plain"))
    apply_list_unsubscribe_headers(msg)

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.login(from_email, password)
        smtp.sendmail(from_email, to_email, msg.as_string())

    with _sender_lock:
        _sender_counts[from_email] = _sender_counts.get(from_email, 0) + 1

# ── Per-prospect pipeline ─────────────────────────────────────────────────────

def _process(prospect: dict) -> dict:
    if is_suppressed(prospect["email"]):
        row = _log_row(prospect, "", "skipped_suppressed", "")
        _append_log(row)
        return row

    try:
        ec = _generate(prospect)
    except Exception as e:
        row = _log_row(prospect, "", "failed_generation", str(e))
        _append_log(row)
        return row

    try:
        _push(prospect, ec["subject"], ec["body"])
    except Exception as e:
        row = _log_row(prospect, ec["subject"], "failed_api", str(e))
        _append_log(row)
        return row

    row = _log_row(prospect, ec["subject"], "pushed")
    _append_log(row)
    return row

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    _init_log()
    sent = _load_sent_emails()
    all_prospects = _load_prospects()

    has_email  = [p for p in all_prospects if p.get("email", "").strip()]
    no_email   = len(all_prospects) - len(has_email)
    pending    = [p for p in has_email if p["email"].strip().lower() not in sent]
    duplicates = [p for p in has_email if p["email"].strip().lower() in sent]

    print(f"Loaded {len(all_prospects)} prospects — {no_email} skipped (no email), "
          f"{len(duplicates)} already pushed, {len(pending)} to process")

    for p in duplicates:
        _append_log(_log_row(p, "", "skipped_duplicate"))

    counts: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process, p): p for p in pending}
        for future in as_completed(futures):
            row    = future.result()
            status = row["status"]
            counts[status] = counts.get(status, 0) + 1
            print(f"[{status}] {row['prospect_name']} — {row.get('subject') or row.get('error', '')}")

    print(
        f"\nDone — pushed: {counts.get('pushed', 0)}, "
        f"skipped_suppressed: {counts.get('skipped_suppressed', 0)}, "
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}"
    )


if __name__ == "__main__":
    main()
