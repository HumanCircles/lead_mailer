"""
Fetch inbox emails from all sender accounts via IMAP and write to inbox_replies.csv.

Usage:
    python fetch_inboxes.py

Reads:  sender_inboxes.csv  (columns: email, password, domain)
Writes: inbox_replies.csv
"""

import csv
import imaplib
import os
import socket
import ssl
import sys
import time
from datetime import datetime, timezone
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime

socket.setdefaulttimeout(TIMEOUT_SECS := 15)

IMAP_PORT  = 993
INPUT_CSV  = "sender_inboxes.csv"
OUTPUT_CSV = "inbox_replies.csv"

# IMAP SINCE (RFC 3501 dd-Mon-yyyy). Narrow this + post-filter so exports are not full-year noise.
IMAP_SINCE = os.environ.get("INBOX_IMAP_SINCE", "30-Mar-2026")
# Inclusive floor for the message Date: header, interpreted then compared in UTC (ISO YYYY-MM-DD).
INBOX_MIN_DATE_UTC = os.environ.get("INBOX_SINCE_DATE", "2026-03-30")


def _min_utc() -> datetime:
    y, m, d = map(int, INBOX_MIN_DATE_UTC.strip().split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc)


def _date_header_utc(date_header: str) -> datetime | None:
    if not (date_header or "").strip():
        return None
    try:
        dt = parsedate_to_datetime(date_header.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _keep_row(date_header: str) -> bool:
    dt = _date_header_utc(date_header)
    if dt is None:
        return False
    return dt >= _min_utc()

# Explicit host overrides for domains where mail.<domain> is wrong
IMAP_HOST_MAP = {
    "superchargedai.org": "gvam1039.siteground.biz",
}


def _decode_header_value(raw) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            try:
                out.append(chunk.decode(charset or "utf-8", errors="replace"))
            except (LookupError, UnicodeDecodeError):
                out.append(chunk.decode("utf-8", errors="replace"))
        else:
            out.append(chunk)
    return "".join(out).strip()


def _get_body_snippet(msg: Message, max_chars: int = 300) -> str:
    """Extract plain-text body snippet from a MIME message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and not part.get("Content-Disposition"):
                payload = part.get_payload(decode=True)
                if payload:
                    text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                    return text.strip()[:max_chars]
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            text = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
            return text.strip()[:max_chars]
    return ""


def fetch_inbox(account_email: str, password: str) -> list[dict]:
    """Connect via IMAP and return list of message dicts from INBOX."""
    domain = account_email.split("@", 1)[1]
    host   = IMAP_HOST_MAP.get(domain, f"mail.{domain}")
    rows   = []

    try:
        ctx = ssl.create_default_context()
        with imaplib.IMAP4_SSL(host, IMAP_PORT, ssl_context=ctx) as imap:
            imap.login(account_email, password)
            imap.select("INBOX", readonly=True)

            _, data = imap.search(None, "SINCE", IMAP_SINCE)
            msg_ids = data[0].split() if data[0] else []

            if msg_ids:
                id_range = f"{msg_ids[0].decode()}:{msg_ids[-1].decode()}"
                _, msg_data = imap.fetch(id_range, "(BODY[HEADER.FIELDS (FROM SUBJECT DATE)] BODY.PEEK[TEXT]<0.400>)")
                i = 0
                while i < len(msg_data):
                    item = msg_data[i]
                    if not isinstance(item, tuple):
                        i += 1
                        continue
                    raw_headers = item[1] if item[1] else b""
                    # body peek is the next tuple element or next item
                    raw_body = b""
                    if i + 1 < len(msg_data) and isinstance(msg_data[i + 1], tuple):
                        raw_body = msg_data[i + 1][1] or b""
                        i += 2
                    else:
                        i += 1
                    msg = message_from_bytes(raw_headers + b"\r\n" + raw_body)
                    date_h = _decode_header_value(msg.get("Date", ""))
                    if not _keep_row(date_h):
                        continue
                    rows.append({
                        "inbox":   account_email,
                        "from":    _decode_header_value(msg.get("From", "")),
                        "subject": _decode_header_value(msg.get("Subject", "")),
                        "date":    date_h,
                        "body":    (raw_body.decode("utf-8", errors="replace").strip())[:300],
                    })
    except imaplib.IMAP4.error as e:
        print(f"  [AUTH ERROR] {account_email}: {e}")
    except (OSError, TimeoutError) as e:
        print(f"  [TIMEOUT/NET] {account_email}: {e}")
    except Exception as e:
        print(f"  [ERROR] {account_email}: {e}")

    def _sort_key(r: dict) -> float:
        dt = _date_header_utc(r.get("date", ""))
        return -(dt.timestamp() if dt else 0.0)

    rows.sort(key=_sort_key)
    return rows


def main():
    if not os.path.isfile(INPUT_CSV):
        print(f"ERROR: {INPUT_CSV} not found. Run from the lead_mailer directory.")
        sys.exit(1)

    print(
        f"IMAP SINCE {IMAP_SINCE!r}; keeping rows with Date >= {_min_utc().date().isoformat()} UTC "
        f"(set INBOX_IMAP_SINCE / INBOX_SINCE_DATE to override).\n"
    )

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        accounts = list(csv.DictReader(f))

    # Load already-done inboxes so restarts skip them
    done: set[str] = set()
    fieldnames = ["inbox", "from", "subject", "date", "body"]
    if os.path.isfile(OUTPUT_CSV):
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["inbox"])
        print(f"Resuming — {len(done)} accounts already saved, skipping.\n")

    write_header = not os.path.isfile(OUTPUT_CSV)
    out_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    total = 0
    try:
        for i, acct in enumerate(accounts, 1):
            acct_email = acct["email"].strip()
            password   = acct["password"].strip()
            if acct_email in done:
                print(f"[{i:>3}/{len(accounts)}] {acct_email} ... skipped")
                continue
            print(f"[{i:>3}/{len(accounts)}] {acct_email}", end=" ... ", flush=True)
            rows = fetch_inbox(acct_email, password)
            print(f"{len(rows)} messages")
            writer.writerows(rows)
            out_file.flush()
            total += len(rows)
            time.sleep(0.5)
    finally:
        out_file.close()

    print(f"\nDone. {total} new messages written → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
