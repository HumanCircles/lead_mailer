"""
Fetch replies from all sender inboxes via IMAP.

Account sources (pick one):
  • SENDER_POOL in .env — comma-separated email:password (default)
  • --csv PATH [...] — SuperchargedAI roster exports; columns Email + Password
    (header names may differ between files; first matching columns are used)

IMAP defaults to mail.<email-domain>:993 (SSL), which matches:
  smartrecruitai.us, hirewithinsight.us, aihiringagents.us, smart-sight-hr.com
  (all use Incoming mail.<domain> IMAP 993 / Outgoing SMTP 465.)

Writes results to inbox_replies.csv (or --output path). Post-process with clean_inboxes.py.

Usage:
    python fetch_inboxes.py                          # last 7 days, from SENDER_POOL
    python fetch_inboxes.py --csv a.csv b.csv      # from roster CSVs only
    python fetch_inboxes.py --since 2026-03-25
    python fetch_inboxes.py --since 2026-03-25 --until 2026-04-01
    python fetch_inboxes.py --since 2026-03-25 --output my_replies.csv
    python fetch_inboxes.py --workers 40             # parallel IMAP connections
"""

from __future__ import annotations

import argparse
import csv
import imaplib
import os
import socket
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime

from dotenv import load_dotenv

load_dotenv()

IMAP_PORT    = 993
TIMEOUT_SECS = 40
OUTPUT_CSV   = "inbox_replies.csv"
FIELDNAMES   = ["inbox", "from", "subject", "date", "body"]

# Explicit IMAP host overrides (when mail.<domain> is wrong).
# New HireQuotient domains use mail.<domain> :993 by default (no entry needed).
IMAP_HOST_MAP = {
    "superchargedai.org": "gvam1039.siteground.biz",
}

_print_lock = threading.Lock()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch inbox replies from all sender accounts")
    p.add_argument(
        "--since", default="",
        metavar="YYYY-MM-DD",
        help="Fetch emails on or after this date (default: 7 days ago)",
    )
    p.add_argument(
        "--until", default="",
        metavar="YYYY-MM-DD",
        help="Fetch emails on or before this date (default: today)",
    )
    p.add_argument(
        "--output", default=OUTPUT_CSV,
        metavar="PATH",
        help=f"Output CSV path (default: {OUTPUT_CSV})",
    )
    p.add_argument(
        "--workers", type=int, default=10,
        metavar="N",
        help="Parallel IMAP connections (default: 10)",
    )
    p.add_argument(
        "--fresh", action="store_true",
        help="Overwrite output file instead of appending/resuming",
    )
    p.add_argument(
        "--domain", default="",
        metavar="DOMAIN",
        help="Only fetch inboxes for this domain (e.g. easygrowth.us)",
    )
    p.add_argument(
        "--csv", action="append", default=[], metavar="PATH",
        help="Roster CSV with Email + Password columns; repeat for multiple files (uses CSV only, not SENDER_POOL)",
    )
    return p.parse_args()


# ── Pool loading ──────────────────────────────────────────────────────────────

def _load_sender_pool() -> list[tuple[str, str]]:
    """Parse SENDER_POOL env var → list of (email, password)."""
    raw = os.getenv("SENDER_POOL", "").strip()
    if not raw:
        print("ERROR: SENDER_POOL is not set in .env")
        sys.exit(1)
    pairs = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            email, pwd = entry.split(":", 1)
            if "@" in email:
                pairs.append((email.strip(), pwd.strip()))
        elif "@" in entry:
            # email only — no password, IMAP won't work but keep for visibility
            pairs.append((entry.strip(), ""))
    return pairs


def _normalise_header_key(h: str) -> str:
    return (h or "").strip().lower().replace("\ufeff", "")


def _find_col(fieldnames: list[str], *candidates: str) -> str | None:
    """Return original header name for first column whose normalised name is in candidates."""
    want = {c.lower() for c in candidates}
    for name in fieldnames:
        if _normalise_header_key(name) in want:
            return name
    return None


def _load_accounts_from_csvs(paths: list[str]) -> list[tuple[str, str]]:
    """Load (email, password) from SuperchargedAI-style roster CSVs; later rows/files override same email."""
    order: list[str] = []
    by_email: dict[str, tuple[str, str]] = {}
    for path in paths:
        if not path.strip():
            continue
        if not os.path.isfile(path):
            print(f"ERROR: CSV not found: {path}")
            sys.exit(1)
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                print(f"ERROR: no header row in {path}")
                sys.exit(1)
            email_col = _find_col(reader.fieldnames, "email")
            pwd_col = _find_col(reader.fieldnames, "password")
            if not email_col or not pwd_col:
                print(
                    f"ERROR: {path} needs Email and Password columns "
                    f"(got {reader.fieldnames!r})"
                )
                sys.exit(1)
            for row in reader:
                email = (row.get(email_col) or "").strip()
                pwd = (row.get(pwd_col) or "").strip()
                if "@" not in email or not pwd:
                    continue
                key = email.lower()
                if key not in by_email:
                    order.append(key)
                by_email[key] = (email, pwd)
    return [by_email[k] for k in order]


# ── Date helpers ──────────────────────────────────────────────────────────────

def _to_imap_date(dt: datetime) -> str:
    """Format datetime as IMAP SINCE/BEFORE string: DD-Mon-YYYY."""
    return dt.strftime("%-d-%b-%Y")   # e.g. 1-Apr-2026


def _parse_date_header(raw: str) -> datetime | None:
    if not (raw or "").strip():
        return None
    try:
        dt = parsedate_to_datetime(raw.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _in_range(date_raw: str, since: datetime, until: datetime) -> bool:
    dt = _parse_date_header(date_raw)
    if dt is None:
        return False
    return since <= dt <= until


# ── Header / body helpers ─────────────────────────────────────────────────────

def _decode_hdr(raw) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            out.append(chunk.decode(charset or "utf-8", errors="replace"))
        else:
            out.append(chunk)
    return "".join(out).strip()


def _body_snippet(msg: Message, max_chars: int = 400) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and not part.get("Content-Disposition"):
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace").strip()[:max_chars]
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace").strip()[:max_chars]
    return ""


# ── IMAP fetch ────────────────────────────────────────────────────────────────

def fetch_inbox(
    account_email: str,
    password: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """Connect to IMAP and return messages in the [since, until] range."""
    domain = account_email.split("@", 1)[1]
    host   = IMAP_HOST_MAP.get(domain, f"mail.{domain}")
    rows: list[dict] = []

    socket.setdefaulttimeout(TIMEOUT_SECS)
    try:
        ctx = ssl.create_default_context()
        with imaplib.IMAP4_SSL(host, IMAP_PORT, ssl_context=ctx) as imap:
            imap.login(account_email, password)
            imap.select("INBOX", readonly=True)

            imap_since = _to_imap_date(since)
            _, data = imap.search(None, "SINCE", imap_since)
            msg_ids = data[0].split() if data[0] else []

            if not msg_ids:
                return rows

            id_range = f"{msg_ids[0].decode()}:{msg_ids[-1].decode()}"
            _, msg_data = imap.fetch(
                id_range,
                "(BODY[HEADER.FIELDS (FROM SUBJECT DATE)] BODY.PEEK[TEXT]<0.500>)"
            )

            i = 0
            while i < len(msg_data):
                item = msg_data[i]
                if not isinstance(item, tuple):
                    i += 1
                    continue
                raw_headers = item[1] or b""
                raw_body    = b""
                if i + 1 < len(msg_data) and isinstance(msg_data[i + 1], tuple):
                    raw_body = msg_data[i + 1][1] or b""
                    i += 2
                else:
                    i += 1

                msg    = message_from_bytes(raw_headers + b"\r\n" + raw_body)
                date_h = _decode_hdr(msg.get("Date", ""))

                if not _in_range(date_h, since, until):
                    continue

                rows.append({
                    "inbox":   account_email,
                    "from":    _decode_hdr(msg.get("From",    "")),
                    "subject": _decode_hdr(msg.get("Subject", "")),
                    "date":    date_h,
                    "body":    raw_body.decode("utf-8", errors="replace").strip()[:400],
                })

    except imaplib.IMAP4.error as e:
        with _print_lock:
            print(f"  [AUTH] {account_email}: {e}")
    except (OSError, TimeoutError) as e:
        with _print_lock:
            print(f"  [NET]  {account_email}: {e}")
    except Exception as e:
        with _print_lock:
            print(f"  [ERR]  {account_email}: {e}")

    # newest first
    rows.sort(key=lambda r: -(_parse_date_header(r["date"]) or datetime.min.replace(tzinfo=timezone.utc)).timestamp())
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    now_utc = datetime.now(timezone.utc)
    since = (
        datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        if args.since else
        (now_utc - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    )
    until = (
        datetime.fromisoformat(args.until).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        if args.until else
        now_utc
    )

    if args.csv:
        accounts = _load_accounts_from_csvs(args.csv)
        if not accounts:
            print("ERROR: no email:password rows found in given CSV file(s)")
            sys.exit(1)
        print(f"Loaded {len(accounts)} account(s) from {len(args.csv)} CSV file(s)")
    else:
        accounts = _load_sender_pool()

    if args.domain:
        accounts = [(e, p) for e, p in accounts if e.split("@", 1)[-1].lower() == args.domain.lower()]
        if not accounts:
            print(f"No accounts found for domain: {args.domain}")
            return
        print(f"Filtering to {len(accounts)} account(s) matching @{args.domain}")

    # Resume: skip accounts already in output unless --fresh
    done: set[str] = set()
    if not args.fresh and os.path.isfile(args.output):
        with open(args.output, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row.get("inbox", ""))
        print(f"Resuming — {len(done)} accounts already fetched, skipping.")

    pending = [(e, p) for e, p in accounts if e not in done]
    if not pending:
        print("Nothing to fetch.")
        return

    print(
        f"\nFetching {len(pending)} inboxes  [{since.date()} → {until.date()}]"
        f"  ({args.workers} parallel connections)\n"
    )

    write_header = args.fresh or not os.path.isfile(args.output)
    out_file = open(args.output, "w" if args.fresh else "a", newline="", encoding="utf-8")
    writer   = csv.DictWriter(out_file, fieldnames=FIELDNAMES)
    if write_header:
        writer.writeheader()

    total    = 0
    done_n   = [0]
    lock     = threading.Lock()

    def _fetch_one(email: str, pwd: str) -> tuple[str, list[dict]]:
        rows = fetch_inbox(email, pwd, since, until)
        return email, rows

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_fetch_one, e, p): e for e, p in pending}
        for fut in as_completed(futures):
            email, rows = fut.result()
            with lock:
                done_n[0] += 1
                writer.writerows(rows)
                out_file.flush()
                total += len(rows)
                print(
                    f"[{done_n[0]:>3}/{len(pending)}]  {email:<45}  {len(rows)} messages",
                    flush=True,
                )

    out_file.close()
    print(f"\nDone — {total} messages written → {args.output}")


if __name__ == "__main__":
    main()
