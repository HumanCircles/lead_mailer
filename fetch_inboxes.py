"""
Fetch replies from all sender inboxes via IMAP.

Reads sender pool directly from SENDER_POOL in .env — no separate CSV needed.
Writes results to inbox_replies.csv (or --output path).

Usage:
    python fetch_inboxes.py                          # last 7 days
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
TIMEOUT_SECS = 20
OUTPUT_CSV   = "inbox_replies.csv"
FIELDNAMES   = ["inbox", "from", "subject", "date", "body"]

# Explicit IMAP host overrides (when mail.<domain> is wrong)
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
        "--workers", type=int, default=30,
        metavar="N",
        help="Parallel IMAP connections (default: 30)",
    )
    p.add_argument(
        "--fresh", action="store_true",
        help="Overwrite output file instead of appending/resuming",
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

    accounts = _load_sender_pool()

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
