"""
BD Outreach Agent
- Reads prospects from prospects.csv
- Generates personalized emails via OpenAI (MESSAGING_README.md framework)
- Pushes to Instantly or Smartlead
- Logs all outcomes to sent_log.csv
"""

import csv
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from openai import OpenAI

# ── Config ────────────────────────────────────────────────────────────────────

with open("config.json") as _f:
    _cfg = json.load(_f)

PLATFORM           = _cfg.get("platform", "instantly")
INSTANTLY_API_KEY  = _cfg.get("instantly_api_key", "")
INSTANTLY_CAMPAIGN = _cfg.get("instantly_campaign_id", "")
SMARTLEAD_API_KEY  = _cfg.get("smartlead_api_key", "")
SMARTLEAD_CAMPAIGN = _cfg.get("smartlead_campaign_id", "")
MAX_WORKERS        = int(_cfg.get("max_workers", 20))
LLM_MAX_CONCURRENT = int(_cfg.get("llm_max_concurrent", 40))
PROSPECTS_FILE     = _cfg.get("prospects_file", "prospects.csv")
SENT_LOG_FILE      = _cfg.get("sent_log_file", "sent_log.csv")
MODEL              = _cfg.get("openai_model", "gpt-4.1-mini")

_openai   = OpenAI(api_key=_cfg["openai_api_key"])
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
            row["prospect_email"]
            for row in csv.DictReader(f)
            if row.get("status") == "pushed"
        }


def _append_log(row: dict):
    with _log_lock:
        with open(SENT_LOG_FILE, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writerow(row)


def _make_log_row(prospect: dict, subject: str, status: str, error: str = "") -> dict:
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
    with open(PROSPECTS_FILE) as f:
        return list(csv.DictReader(f))

# ── Claude generation ─────────────────────────────────────────────────────────

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


def _generate_email(prospect: dict) -> dict:
    name     = f"{prospect['first_name']} {prospect['last_name']}"
    company  = prospect.get("company", "")
    title    = prospect.get("title", "")
    platform = prospect.get("hcm_platform", "") or company

    prompt = f"""Prospect:
- Name: {name}
- Title: {title}
- Company: {company}
- Platform: {platform}

Use your knowledge about this company, their industry, and the person's role to craft a sharp email following the 5-beat framework. For Beat 1, draw on any publicly known signal about the prospect or company — a product launch, a press mention, a known initiative, or a clear industry-level observation tied to their specific segment.

Write the email. Return JSON only — no markdown, no preamble:
{{
  "subject": "...",
  "body": "..."
}}

Rules:
- Body is plain text. No markdown, no bold, no bullet points, no HTML.
- Every sentence must map to one of the five beats in the playbook.
- If a beat cannot be written with confidence, compress the email rather than write a filler sentence.
- Subject must follow exactly: Invite for discussion | [3-4 word pain point hook]"""

    with _llm_sem:
        for attempt in range(2):
            resp = _openai.chat.completions.create(
                model=MODEL,
                max_tokens=1024,
                messages=[
                    {"role": "system", "content": MESSAGING_GUIDE},
                    {"role": "user",   "content": prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw:
                try:
                    return _parse_json(raw)
                except ValueError:
                    if attempt == 0:
                        continue
                    raise ValueError("Invalid JSON after retry")

    raise ValueError("Model returned empty response.")

# ── Platform sending ──────────────────────────────────────────────────────────

def _api_call(method: str, url: str, **kwargs):
    backoffs = [5, 15, 45]
    last_err = None
    for attempt in range(len(backoffs) + 1):
        if attempt:
            time.sleep(backoffs[attempt - 1])
        try:
            resp = requests.request(method, url, timeout=30, **kwargs)
        except requests.ConnectionError as e:
            last_err = e
            if attempt == 0:
                time.sleep(10)
            continue

        if resp.status_code in (200, 201):
            return
        if resp.status_code == 400:
            raise ValueError(f"400 Bad Request: {resp.text[:200]}")
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            if attempt < 3:
                time.sleep(wait)
                continue
            raise ValueError(f"429 rate limited after {attempt + 1} retries")
        if resp.status_code >= 500:
            last_err = ValueError(f"{resp.status_code} server error: {resp.text[:200]}")
            continue
        resp.raise_for_status()

    raise last_err or ValueError("API call failed after retries")


def _push_instantly(prospect: dict, subject: str, body: str):
    _api_call(
        "POST",
        "https://api.instantly.ai/api/v1/lead/add",
        json={
            "api_key":              INSTANTLY_API_KEY,
            "campaign_id":          INSTANTLY_CAMPAIGN,
            "skip_if_in_workspace": True,
            "leads": [{
                "email":        prospect["email"],
                "first_name":   prospect["first_name"],
                "last_name":    prospect["last_name"],
                "company_name": prospect.get("company", ""),
                "custom_variables": {
                    "custom_subject": subject,
                    "custom_body":    body,
                },
            }],
        },
    )


def _push_smartlead(prospect: dict, subject: str, body: str):
    _api_call(
        "POST",
        "https://server.smartlead.ai/api/v1/leads",
        params={"api_key": SMARTLEAD_API_KEY},
        json={
            "campaign_id": SMARTLEAD_CAMPAIGN,
            "lead_list": [{
                "email":        prospect["email"],
                "first_name":   prospect["first_name"],
                "last_name":    prospect["last_name"],
                "company_name": prospect.get("company", ""),
                "custom_fields": {
                    "custom_subject": subject,
                    "custom_body":    body,
                },
            }],
        },
    )


def _push(prospect: dict, subject: str, body: str):
    if PLATFORM == "instantly":
        _push_instantly(prospect, subject, body)
    else:
        _push_smartlead(prospect, subject, body)

# ── Per-prospect pipeline ─────────────────────────────────────────────────────

def _process(prospect: dict) -> dict:
    # Step 1: Claude generation
    try:
        email_content = _generate_email(prospect)
    except Exception as e:
        row = _make_log_row(prospect, "", "failed_generation", str(e))
        _append_log(row)
        return row

    subject = email_content["subject"]
    body    = email_content["body"]

    # Step 2: Push to platform
    try:
        _push(prospect, subject, body)
    except Exception as e:
        row = _make_log_row(prospect, subject, "failed_api", str(e))
        _append_log(row)
        return row

    row = _make_log_row(prospect, subject, "pushed")
    _append_log(row)
    return row

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    _init_log()
    sent_emails = _load_sent_emails()

    all_prospects = _load_prospects()
    pending    = []
    duplicates = []

    for p in all_prospects:
        email = p.get("email", "").strip()
        if not email:
            continue
        if email in sent_emails:
            duplicates.append(p)
        else:
            pending.append(p)

    print(
        f"Loaded {len(all_prospects)} prospects — "
        f"{len(duplicates)} already pushed, "
        f"{len(pending)} to process"
    )

    for p in duplicates:
        _append_log(_make_log_row(p, "", "skipped_duplicate"))

    counts: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process, p): p for p in pending}
        for future in as_completed(futures):
            row    = future.result()
            status = row["status"]
            counts[status] = counts.get(status, 0) + 1
            detail = row.get("subject") or row.get("error", "")
            print(f"[{status}] {row['prospect_name']} — {detail}")

    print(
        f"\nDone — "
        f"pushed: {counts.get('pushed', 0)}, "
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}"
    )


if __name__ == "__main__":
    main()
