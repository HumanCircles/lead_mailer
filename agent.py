"""
BD Outreach Agent — batch CLI
Reads prospects.csv → generates emails via OpenAI → sends via SMTP → logs to sent_log.csv
Run: python agent.py
"""

import csv
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from openai import OpenAI, RateLimitError

from core.deliverability import (
    append_unsubscribe_footer,
    apply_list_unsubscribe_headers,
    is_suppressed,
    smtp_from_header,
    strip_control_chars,
)
from core.prospect_csv import canonicalize_prospect_row
from core.smtp_sender import smtp_deliver

load_dotenv()

# ── Config from env ───────────────────────────────────────────────────────────

OPENAI_API_KEY      = os.environ["OPENAI_API_KEY"]
OPENAI_TIMEOUT      = float(os.getenv("OPENAI_TIMEOUT", "30"))
OPENAI_MAX_RETRIES  = int(os.getenv("OPENAI_MAX_RETRIES", "2"))
MODEL               = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
MAX_WORKERS         = int(os.getenv("MAX_WORKERS", "5"))
LLM_MAX_CONCURRENT  = int(os.getenv("LLM_MAX_CONCURRENT", "10"))
CONCURRENT_SENDS    = int(os.getenv("CONCURRENT_SENDS", "3"))
PROSPECTS_FILE      = os.getenv("PROSPECTS_FILE", "prospects.csv")
SENT_LOG_FILE       = os.getenv("SENT_LOG_FILE", "sent_log.csv")
PROSPECTS_MAX       = int(os.getenv("PROSPECTS_MAX", "0"))

_fn_raw     = os.getenv("FROM_NAME")
FROM_NAME   = ("HireQuotient" if _fn_raw is None else _fn_raw).strip()
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "150"))
HOURLY_LIMIT = int(os.getenv("HOURLY_LIMIT", "100"))
ACCOUNT_HOURLY_CAP = int(os.getenv("ACCOUNT_HOURLY_CAP", "400"))


class _SendFailed(Exception):
    def __init__(self, cause: Exception, from_email: str):
        super().__init__(str(cause))
        self.cause = cause
        self.from_email = from_email


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
_sender_daily: dict[str, int] = {}
_sender_hourly: dict[str, int] = {}
_sender_lock  = threading.Lock()
_send_sem     = threading.Semaphore(max(1, CONCURRENT_SENDS))
_hour_window_start = time.time()

_account_sent_this_hour = 0
_account_hour_start = time.time()
_account_lock = threading.Lock()

_log_lock = threading.Lock()
_openai_local = threading.local()


def _openai_client() -> OpenAI:
    """One OpenAI client per worker thread (avoids shared httpx state under parallel LLM calls)."""
    c = getattr(_openai_local, "client", None)
    if c is None:
        _openai_local.client = OpenAI(
            api_key=OPENAI_API_KEY,
            timeout=OPENAI_TIMEOUT,
            max_retries=OPENAI_MAX_RETRIES,
        )
    return _openai_local.client


# ── Messaging guide ───────────────────────────────────────────────────────────

with open("MESSAGING_README.md", encoding="utf-8") as _f:
    MESSAGING_GUIDE = strip_control_chars(_f.read())

# ── Logging ───────────────────────────────────────────────────────────────────

_LOG_HEADERS = [
    "timestamp",
    "prospect_email",
    "prospect_name",
    "company",
    "subject",
    "status",
    "error",
    "from_email",
]


def _migrate_sent_log_add_from_email() -> None:
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
    padded = []
    for r in body:
        r = list(r)
        while len(r) < len(new_header):
            r.append("")
        padded.append(r[: len(new_header)])
    with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(new_header)
        w.writerows(padded)


def _load_sender_usage_from_log() -> tuple[dict[str, int], dict[str, int], int]:
    if not os.path.isfile(SENT_LOG_FILE):
        return {}, {}, 0
    today_utc = datetime.now(timezone.utc).date().isoformat()
    now = time.time()
    hourly_start = now - 3600
    daily_counts: dict[str, int] = {}
    hourly_counts: dict[str, int] = {}
    account_hourly = 0
    with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") != "pushed":
                continue
            ts = row.get("timestamp") or ""
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if not ts.startswith(today_utc):
                continue
            fe = (row.get("from_email") or "").strip().lower()
            if fe:
                daily_counts[fe] = daily_counts.get(fe, 0) + 1
            sent_epoch = dt.timestamp()
            if sent_epoch >= hourly_start:
                account_hourly += 1
                if fe:
                    hourly_counts[fe] = hourly_counts.get(fe, 0) + 1
    return daily_counts, hourly_counts, account_hourly


def _init_sender_counts_from_log() -> None:
    global _sender_daily, _sender_hourly, _account_sent_this_hour, _account_hour_start
    _sender_daily = {e: 0 for e, _ in _senders}
    _sender_hourly = {e: 0 for e, _ in _senders}
    daily_counts, hourly_counts, account_hourly = _load_sender_usage_from_log()
    for addr, n in daily_counts.items():
        for pool_email, _ in _senders:
            if pool_email.lower() == addr:
                _sender_daily[pool_email] = n
                break
    for addr, n in hourly_counts.items():
        for pool_email, _ in _senders:
            if pool_email.lower() == addr:
                _sender_hourly[pool_email] = n
                break
    _account_sent_this_hour = account_hourly
    _account_hour_start = time.time()


def _init_log():
    if not os.path.exists(SENT_LOG_FILE):
        with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()


def _load_sent_emails() -> set[str]:
    if not os.path.exists(SENT_LOG_FILE):
        return set()
    with open(SENT_LOG_FILE, encoding="utf-8") as f:
        return {
            row["prospect_email"].strip().lower()
            for row in csv.DictReader(f)
            if row.get("status") == "pushed"
        }


def _append_log(row: dict):
    with _log_lock:
        with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writerow(row)


def _log_row(
    prospect: dict,
    subject: str,
    status: str,
    error: str = "",
    from_email: str = "",
) -> dict:
    return {
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "prospect_email": prospect["email"],
        "prospect_name":  f"{prospect['first_name']} {prospect['last_name']}",
        "company":        prospect.get("company", ""),
        "subject":        subject,
        "status":         status,
        "error":          error,
        "from_email":     from_email,
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
    name     = strip_control_chars(f"{prospect['first_name']} {prospect['last_name']}".strip())
    company  = strip_control_chars(str(prospect.get("company", "") or ""))
    title    = strip_control_chars(str(prospect.get("title", "") or ""))
    platform = strip_control_chars(str(prospect.get("hcm_platform", "") or company or ""))

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

    for attempt in range(2):
        for rl_attempt in range(5):
            try:
                resp = _openai_client().chat.completions.create(
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
    global _sender_idx, _hour_window_start
    while True:
        with _sender_lock:
            if time.time() - _hour_window_start >= 3600:
                for k in _sender_hourly:
                    _sender_hourly[k] = 0
                _hour_window_start = time.time()
                print("[rate] New hour window — resetting hourly sender counters", flush=True)

            for _ in range(len(_senders)):
                email, pwd = _senders[_sender_idx % len(_senders)]
                _sender_idx += 1
                daily_ok = _sender_daily.get(email, 0) < DAILY_LIMIT
                hourly_ok = _sender_hourly.get(email, 0) < HOURLY_LIMIT
                if daily_ok and hourly_ok:
                    return email, pwd
            wait = max(1, 3600 - (time.time() - _hour_window_start))
        print(f"[rate] All senders at hourly/daily cap. Sleeping {wait / 60:.1f} min...", flush=True)
        time.sleep(wait + 1)


def _check_account_cap() -> float:
    global _account_sent_this_hour, _account_hour_start
    with _account_lock:
        elapsed = time.time() - _account_hour_start
        if elapsed >= 3600:
            _account_sent_this_hour = 0
            _account_hour_start = time.time()
            return 0.0
        if _account_sent_this_hour >= ACCOUNT_HOURLY_CAP:
            return max(1.0, 3600 - elapsed)
        _account_sent_this_hour += 1
        return 0.0


def _sender_first_name(email: str) -> str:
    local = email.split("@")[0]
    parts = re.split(r"[._\-]", local)
    return parts[0].capitalize()


def _push(prospect: dict, subject: str, body: str) -> str:
    wait = _check_account_cap()
    if wait > 0:
        print(
            f"[rate] Account cap hit ({ACCOUNT_HOURLY_CAP}/hr). Waiting {wait / 60:.1f} min...",
            flush=True,
        )
        time.sleep(wait + 1)

    from_email, password = _next_sender()
    to_email = prospect["email"]

    first_name = _sender_first_name(from_email)
    body_out = append_unsubscribe_footer(body.rstrip())
    body_out = body_out + f"\n\n{first_name}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_from_header(FROM_NAME, from_email)
    msg["To"]      = to_email

    msg.attach(MIMEText(body_out, "plain"))
    apply_list_unsubscribe_headers(msg)

    try:
        with _send_sem:
            smtp_deliver(from_email, password, to_email, msg.as_string())
    except Exception as e:
        raise _SendFailed(e, from_email) from e

    with _sender_lock:
        _sender_daily[from_email] = _sender_daily.get(from_email, 0) + 1
        _sender_hourly[from_email] = _sender_hourly.get(from_email, 0) + 1
    return from_email

# ── Two-phase pipeline: LLM generation, then SMTP send ────────────────────────

def _phase_generate(prospect: dict) -> dict:
    if is_suppressed(prospect["email"]):
        row = _log_row(prospect, "", "skipped_suppressed", "")
        _append_log(row)
        return {"kind": "logged", "row": row}
    try:
        ec = _generate(prospect)
    except Exception as e:
        row = _log_row(prospect, "", "failed_generation", str(e))
        _append_log(row)
        return {"kind": "logged", "row": row}
    return {"kind": "draft", "prospect": prospect, "ec": ec}


def _phase_send(draft: dict) -> dict:
    prospect = draft["prospect"]
    ec = draft["ec"]
    try:
        fe = _push(prospect, ec["subject"], ec["body"])
    except _SendFailed as e:
        row = _log_row(prospect, ec["subject"], "failed_api", str(e.cause), from_email=e.from_email)
        _append_log(row)
        return row
    except Exception as e:
        row = _log_row(prospect, ec["subject"], "failed_api", str(e))
        _append_log(row)
        return row
    row = _log_row(prospect, ec["subject"], "pushed", from_email=fe)
    _append_log(row)
    return row

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("BD Outreach agent starting...", flush=True)
    _init_log()
    _migrate_sent_log_add_from_email()
    _init_sender_counts_from_log()
    sent = _load_sent_emails()
    all_prospects = _load_prospects()

    has_email  = [p for p in all_prospects if p.get("email", "").strip()]
    no_email   = len(all_prospects) - len(has_email)
    pending    = [p for p in has_email if p["email"].strip().lower() not in sent]
    duplicates = [p for p in has_email if p["email"].strip().lower() in sent]

    if PROSPECTS_MAX > 0:
        pending = pending[:PROSPECTS_MAX]

    print(
        f"Loaded {len(all_prospects)} prospects — {no_email} skipped (no email), "
        f"{len(duplicates)} already pushed, {len(pending)} to process"
        + (f" (PROSPECTS_MAX={PROSPECTS_MAX})" if PROSPECTS_MAX else "")
        + f" — LLM_MAX_CONCURRENT={LLM_MAX_CONCURRENT}, MAX_WORKERS={MAX_WORKERS}, "
        f"CONCURRENT_SENDS={CONCURRENT_SENDS}, HOURLY_LIMIT={HOURLY_LIMIT}, "
        f"ACCOUNT_HOURLY_CAP={ACCOUNT_HOURLY_CAP}",
        flush=True,
    )

    for p in duplicates:
        _append_log(_log_row(p, "", "skipped_duplicate"))

    counts: dict[str, int] = {}
    drafts: list[dict] = []

    total = len(pending)
    done = 0
    print(f"[gen] Starting LLM generation for {total} prospects...", flush=True)

    with ThreadPoolExecutor(max_workers=LLM_MAX_CONCURRENT) as llm_pool:
        futures = {llm_pool.submit(_phase_generate, p): p for p in pending}
        for future in as_completed(futures):
            r = future.result()
            done += 1
            if r["kind"] == "draft":
                drafts.append(r)
                name = f"{r['prospect']['first_name']} {r['prospect']['last_name']}"
                print(f"[generated] ({done}/{total}) {name}", flush=True)
            else:
                row = r["row"]
                st = row["status"]
                counts[st] = counts.get(st, 0) + 1
                print(
                    f"[{st}] ({done}/{total}) {row['prospect_name']} — "
                    f"{row.get('subject') or row.get('error', '')}",
                    flush=True,
                )

    print(
        f"[gen] Done — {len(drafts)} drafted, {total - len(drafts)} failed/skipped",
        flush=True,
    )

    total_send = len(drafts)
    sent_done = 0
    print(f"[send] Starting SMTP for {total_send} drafts...", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as smtp_pool:
        futures = {smtp_pool.submit(_phase_send, d): d for d in drafts}
        for future in as_completed(futures):
            row = future.result()
            sent_done += 1
            status = row["status"]
            counts[status] = counts.get(status, 0) + 1
            print(
                f"[{status}] ({sent_done}/{total_send}) {row['prospect_name']} — "
                f"{row.get('subject') or row.get('error', '')}",
                flush=True,
            )

    print(
        f"\nDone — pushed: {counts.get('pushed', 0)}, "
        f"skipped_suppressed: {counts.get('skipped_suppressed', 0)}, "
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
