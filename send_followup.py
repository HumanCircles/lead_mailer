"""
Send follow-up emails to prospects who received the initial outreach but have not replied.

Logic:
  1. Read sent_log.csv — find emails with status=pushed sent N days ago (default: 3-5 days).
  2. Read replies CSV — build a set of emails that already responded.
  3. Subtract replied + suppressed → these get a follow-up.
  4. Generate a short follow-up via OpenAI and send via the existing SendGrid pipeline.
  5. Log results to sent_log.csv with status tag "followup_pushed".

Usage:
    python send_followup.py                            # follows up on day 3-5 sends
    python send_followup.py --days-min 4 --days-max 6
    python send_followup.py --replies data/all_real_replies.csv
    python send_followup.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

SENT_LOG_FILE = os.getenv("SENT_LOG_FILE", "sent_log.csv")
_LOG_HEADERS  = ["timestamp", "prospect_email", "prospect_name", "company",
                 "subject", "status", "error", "from_email"]

# ── Follow-up drafter ─────────────────────────────────────────────────────────

_FOLLOWUP_SYSTEM = """You write brief, human-sounding follow-up emails for cold outreach.

Rules:
- 2-3 short sentences only. No greetings, no sign-off, no signature.
- Reference the original subject/topic naturally — do not repeat it verbatim.
- Acknowledge they are busy. Do not apologise for following up.
- End with one direct, low-friction ask: reply yes/no, or suggest a time.
- No dashes (em dash, en dash, double hyphen). Use periods only.
- Plain text only. No bullet points, bold, or formatting.
- Return JSON only: {"body": "..."}
"""


def _draft_followup(original_subject: str, name: str, company: str) -> str:
    """Generate a short follow-up body via OpenAI."""
    import json, re
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    model  = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    prompt = (
        f"Original subject: {original_subject}\n"
        f"Prospect: {name} at {company}\n\n"
        "Write a 2-3 sentence follow-up. Return JSON only: {{\"body\": \"...\"}}"
    )
    resp = client.chat.completions.create(
        model=model, max_tokens=256,
        messages=[
            {"role": "system", "content": _FOLLOWUP_SYSTEM},
            {"role": "user",   "content": prompt},
        ],
    )
    raw = (resp.choices[0].message.content or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw.strip()).get("body", "")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Send follow-up emails to non-responders")
    p.add_argument("--days-min",  type=int, default=3,
                   help="Minimum days since initial send (default: 3)")
    p.add_argument("--days-max",  type=int, default=5,
                   help="Maximum days since initial send (default: 5)")
    p.add_argument("--replies",   default="data/all_real_replies.csv",
                   help="CSV of real replies (from clean_inboxes.py)")
    p.add_argument("--workers",   type=int, default=5,
                   help="Concurrent sends (default: 5)")
    p.add_argument("--dry-run",   action="store_true",
                   help="Generate follow-ups but do not send")
    p.add_argument("--limit",     type=int, default=0,
                   help="Cap number of follow-ups (0 = unlimited)")
    return p.parse_args()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _load_replied_emails(replies_csv: str) -> set[str]:
    if not os.path.isfile(replies_csv):
        return set()
    with open(replies_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # "from" field in the replies CSV is the prospect's email
    replied: set[str] = set()
    for r in rows:
        addr = r.get("from", "")
        # Extract address from "Name <email>" format
        if "<" in addr:
            addr = addr.split("<")[-1].rstrip(">")
        replied.add(addr.strip().lower())
    return replied


def _load_suppression() -> set[str]:
    from core.deliverability import is_suppressed
    # Reuse the module-level suppression check
    return set()  # is_suppressed() called per-row below


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    now_utc  = datetime.now(timezone.utc)
    cutoff_old = now_utc - timedelta(days=args.days_max)
    cutoff_new = now_utc - timedelta(days=args.days_min)

    # Load sent_log
    if not os.path.isfile(SENT_LOG_FILE):
        print(f"No sent log found at {SENT_LOG_FILE}")
        sys.exit(0)

    with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    # Find initial pushes in the time window
    initial_sends: dict[str, dict] = {}   # prospect_email → row (keep latest per email)
    for row in all_rows:
        if row.get("status") != "pushed":
            continue
        ts = _parse_ts(row.get("timestamp", ""))
        if ts is None:
            continue
        if not (cutoff_old <= ts <= cutoff_new):
            continue
        email = (row.get("prospect_email") or "").strip().lower()
        if not email:
            continue
        initial_sends[email] = row   # last push in window wins

    print(f"Initial sends in window (day {args.days_min}-{args.days_max}): {len(initial_sends)}")

    # Find who already followed up (skip)
    already_followedup = {
        (row.get("prospect_email") or "").strip().lower()
        for row in all_rows
        if row.get("status", "").startswith("followup")
    }

    # Load replies
    replied = _load_replied_emails(args.replies)
    print(f"Already replied : {len(replied)}")
    print(f"Already followed: {len(already_followedup)}")

    from core.deliverability import is_suppressed

    # Build follow-up list
    candidates = [
        row for email, row in initial_sends.items()
        if email not in replied
        and email not in already_followedup
        and not is_suppressed(email)
    ]

    if args.limit > 0:
        candidates = candidates[:args.limit]

    print(f"Follow-ups to send: {len(candidates)}")
    if not candidates:
        print("Nothing to do.")
        return

    if args.dry_run:
        print("\n[DRY RUN] Would follow up:")
        for r in candidates[:20]:
            print(f"  {r.get('prospect_email')} ({r.get('prospect_name')}) — orig: {r.get('subject')}")
        return

    # Send
    from core.sendgrid_sender import send_email, send_seed_email
    CONCURRENT = args.workers
    log_lock   = threading.Lock()
    log_rows: list[dict] = []

    def _followup_one(row: dict) -> dict:
        email   = (row.get("prospect_email") or "").strip()
        name    = (row.get("prospect_name") or "").strip()
        company = (row.get("company") or "").strip()
        orig_subject = (row.get("subject") or "").strip()
        first_name = name.split()[0] if name else ""

        try:
            body = _draft_followup(orig_subject, name, company)
            if not body:
                raise ValueError("Empty follow-up body generated")
            subject = f"Re: {orig_subject}"
            from_addr, _ = send_email(email, subject, body, first_name)
            return {
                "timestamp":      datetime.now(timezone.utc).isoformat(),
                "prospect_email": email,
                "prospect_name":  name,
                "company":        company,
                "subject":        subject,
                "status":         "followup_pushed",
                "error":          "",
                "from_email":     from_addr,
            }
        except Exception as e:
            return {
                "timestamp":      datetime.now(timezone.utc).isoformat(),
                "prospect_email": email,
                "prospect_name":  name,
                "company":        company,
                "subject":        f"Re: {orig_subject}",
                "status":         "followup_failed",
                "error":          str(e),
                "from_email":     "",
            }

    done_n = [0]

    with ThreadPoolExecutor(max_workers=CONCURRENT) as pool:
        futures = {pool.submit(_followup_one, row): row for row in candidates}
        for fut in as_completed(futures):
            result = fut.result()
            with log_lock:
                log_rows.append(result)
                done_n[0] += 1
                status = result["status"]
                print(
                    f"[{done_n[0]:>4}/{len(candidates)}]  "
                    f"{result['prospect_email']:<40}  {status}"
                )

    # Append to sent_log
    write_header = not os.path.isfile(SENT_LOG_FILE)
    with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_LOG_HEADERS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(log_rows)

    pushed = sum(1 for r in log_rows if r["status"] == "followup_pushed")
    failed = len(log_rows) - pushed
    print(f"\nDone — {pushed} sent, {failed} failed → appended to {SENT_LOG_FILE}")

    send_seed_email(
        subject=f"[BD Outreach] Follow-up run complete — {now_utc.strftime('%Y-%m-%d %H:%M UTC')}",
        body=(
            f"Follow-up run complete.\n\n"
            f"Window: day {args.days_min}-{args.days_max} after initial send\n"
            f"Candidates: {len(candidates)}\n"
            f"Sent: {pushed}\n"
            f"Failed: {failed}\n"
        ),
    )


if __name__ == "__main__":
    main()
