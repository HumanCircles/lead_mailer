"""
BD Outreach Agent — batch CLI
Reads prospects.csv → generates emails via OpenAI → pushes to Instantly/Smartlead → logs to sent_log.csv
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

import requests
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ── Config from env ───────────────────────────────────────────────────────────

OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
MODEL              = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "20"))
LLM_MAX_CONCURRENT = int(os.getenv("LLM_MAX_CONCURRENT", "40"))
PROSPECTS_FILE     = os.getenv("PROSPECTS_FILE", "prospects.csv")
SENT_LOG_FILE      = os.getenv("SENT_LOG_FILE", "sent_log.csv")

# Sending platform — set PLATFORM=instantly or PLATFORM=smartlead in .env
PLATFORM           = os.getenv("PLATFORM", "instantly")
INSTANTLY_API_KEY  = os.getenv("INSTANTLY_API_KEY", "")
INSTANTLY_CAMPAIGN = os.getenv("INSTANTLY_CAMPAIGN_ID", "")
SMARTLEAD_API_KEY  = os.getenv("SMARTLEAD_API_KEY", "")
SMARTLEAD_CAMPAIGN = os.getenv("SMARTLEAD_CAMPAIGN_ID", "")

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
            row["prospect_email"]
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
    with open(PROSPECTS_FILE) as f:
        return list(csv.DictReader(f))

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

# ── Platform push ─────────────────────────────────────────────────────────────

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
            last_err = ValueError(f"{resp.status_code}: {resp.text[:200]}")
            continue
        resp.raise_for_status()
    raise last_err or ValueError("API call failed after retries")


def _push(prospect: dict, subject: str, body: str):
    if PLATFORM == "smartlead":
        _api_call(
            "POST", "https://server.smartlead.ai/api/v1/leads",
            params={"api_key": SMARTLEAD_API_KEY},
            json={
                "campaign_id": SMARTLEAD_CAMPAIGN,
                "lead_list": [{
                    "email": prospect["email"],
                    "first_name": prospect["first_name"],
                    "last_name": prospect["last_name"],
                    "company_name": prospect.get("company", ""),
                    "custom_fields": {"custom_subject": subject, "custom_body": body},
                }],
            },
        )
    else:
        _api_call(
            "POST", "https://api.instantly.ai/api/v1/lead/add",
            json={
                "api_key": INSTANTLY_API_KEY,
                "campaign_id": INSTANTLY_CAMPAIGN,
                "skip_if_in_workspace": True,
                "leads": [{
                    "email": prospect["email"],
                    "first_name": prospect["first_name"],
                    "last_name": prospect["last_name"],
                    "company_name": prospect.get("company", ""),
                    "custom_variables": {"custom_subject": subject, "custom_body": body},
                }],
            },
        )

# ── Per-prospect pipeline ─────────────────────────────────────────────────────

def _process(prospect: dict) -> dict:
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

    pending    = [p for p in all_prospects if p.get("email", "").strip() not in sent]
    duplicates = [p for p in all_prospects if p.get("email", "").strip() in sent]

    print(f"Loaded {len(all_prospects)} prospects — {len(duplicates)} already pushed, {len(pending)} to process")

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
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}"
    )


if __name__ == "__main__":
    main()
