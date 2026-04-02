"""
Re-fetch full message bodies for rows produced by clean_inboxes.py (e.g. real_replies.csv).

For each row, connects to the recipient inbox (same account as fetch_inboxes.py), searches
by From address + date window, matches Subject, then downloads RFC822 and extracts the
best available plain-text body (text/plain, else stripped text/html).

Credentials: same as fetch_inboxes.py — SENDER_POOL in .env or --csv roster file(s).

Usage:
    python fetch_full_real_replies.py --input real_replies.csv --output full_real_replies.csv \\
        --csv roster1.csv roster2.csv
    python fetch_full_real_replies.py -i real_replies.csv -o full_real_replies.csv --workers 15
"""

from __future__ import annotations

import argparse
import csv
import imaplib
import re
import socket
import ssl
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parseaddr

from dotenv import load_dotenv

from clean_inboxes import FIELDNAMES, read_inbox_csv
from fetch_inboxes import (
    IMAP_HOST_MAP,
    IMAP_PORT,
    TIMEOUT_SECS,
    _decode_hdr,
    _load_accounts_from_csvs,
    _load_sender_pool,
    _parse_date_header,
    _to_imap_date,
)

load_dotenv()

_print_lock = threading.Lock()
_HTML_TAG_RE = re.compile(r"<[^>]+>", re.DOTALL)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch full IMAP bodies for cleaned reply CSV rows")
    p.add_argument("--input", "-i", default="real_replies.csv", help="Input CSV (default: real_replies.csv)")
    p.add_argument("--output", "-o", default="full_real_replies.csv", help="Output CSV (default: full_real_replies.csv)")
    p.add_argument(
        "--csv", nargs="*", default=[], metavar="PATH",
        help="Roster CSV(s) with Email + Password; if omitted, uses SENDER_POOL from .env",
    )
    p.add_argument("--workers", type=int, default=12, help="Parallel inbox connections (default: 12)")
    return p.parse_args()


def _password_map_from_accounts(accounts: list[tuple[str, str]]) -> dict[str, str]:
    return {email.strip().lower(): pwd for email, pwd in accounts if pwd.strip() and "@" in email}


def _from_search_email(from_header: str) -> str:
    _, addr = parseaddr(from_header or "")
    return (addr or "").strip().lower()


def _normalize_subject(s: str) -> str:
    x = (s or "").strip().lower()
    while x.startswith("re:"):
        x = x[4:].lstrip()
    return " ".join(x.split())


def _subject_matches(want: str, got: str) -> bool:
    a, b = _normalize_subject(want), _normalize_subject(got)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def _strip_html(html: str) -> str:
    t = _HTML_TAG_RE.sub(" ", html or "")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extract_full_body(msg: Message) -> str:
    if msg.is_multipart():
        plain: str | None = None
        html: str | None = None
        for part in msg.walk():
            disp = (part.get_content_disposition() or "").lower()
            if disp == "attachment":
                continue
            ctype = part.get_content_type() or ""
            if ctype == "text/plain" and not disp:
                payload = part.get_payload(decode=True)
                if payload:
                    plain = payload.decode(part.get_content_charset() or "utf-8", errors="replace").strip()
            elif ctype == "text/html" and not disp and html is None:
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
        if plain:
            return plain
        if html:
            return _strip_html(html)
        return ""
    payload = msg.get_payload(decode=True)
    if not payload:
        return ""
    charset = msg.get_content_charset() or "utf-8"
    text = payload.decode(charset, errors="replace").strip()
    if (msg.get_content_type() or "").lower() == "text/html":
        return _strip_html(text)
    return text


def _date_window_for_search(dt: datetime) -> tuple[str, str]:
    """IMAP SINCE (day-1) / BEFORE (day+2) in local date parts of dt (UTC)."""
    d = dt.astimezone(timezone.utc).date()
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) - timedelta(days=1)
    end = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=2)
    return _to_imap_date(start), _to_imap_date(end)


def _fetch_rfc822(imap: imaplib.IMAP4_SSL, uid: bytes) -> bytes:
    typ, data = imap.uid("fetch", uid, "(RFC822)")
    if typ != "OK" or not data:
        return b""
    for item in data:
        if isinstance(item, tuple) and len(item) >= 2:
            return item[1] or b""
    return b""


def _peek_headers(imap: imaplib.IMAP4_SSL, uid: bytes) -> Message | None:
    typ, data = imap.uid("fetch", uid, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])")
    if typ != "OK" or not data:
        return None
    for item in data:
        if isinstance(item, tuple) and len(item) >= 2:
            raw = item[1] or b""
            return message_from_bytes(raw)
    return None


def fetch_full_body_for_row(
    imap: imaplib.IMAP4_SSL,
    row: dict[str, str],
) -> str:
    """Return full plain-ish body or '' if not found."""
    target_dt = _parse_date_header(row.get("date", ""))
    if target_dt is None:
        return ""

    addr = _from_search_email(row.get("from", ""))
    if not addr or "@" not in addr:
        return ""

    want_subj = row.get("subject", "")
    since_d, before_d = _date_window_for_search(target_dt)
    # Quote FROM for IMAP (escape backslashes and double quotes)
    safe_addr = addr.replace("\\", "\\\\").replace('"', '\\"')
    crit = f'(SINCE {since_d} BEFORE {before_d} FROM "{safe_addr}")'
    typ, data = imap.uid("search", None, crit)
    if typ != "OK" or not data or not data[0]:
        return ""

    uids = data[0].split()
    if not uids:
        return ""

    candidates: list[tuple[bytes, datetime, str]] = []
    for uid in reversed(uids):
        mini = _peek_headers(imap, uid)
        if mini is None:
            continue
        subj = _decode_hdr(mini.get("Subject", ""))
        if not _subject_matches(want_subj, subj):
            continue
        date_h = _decode_hdr(mini.get("Date", ""))
        dh = _parse_date_header(date_h)
        candidates.append((uid, dh or target_dt, subj))

    if not candidates:
        return ""

    def dist(c: tuple[bytes, datetime, str]) -> float:
        return abs((c[1] - target_dt).total_seconds())

    best_uid = min(candidates, key=dist)[0]
    raw = _fetch_rfc822(imap, best_uid)
    if not raw:
        return ""
    msg = message_from_bytes(raw)
    return _extract_full_body(msg)


def _open_imap(account_email: str, password: str) -> imaplib.IMAP4_SSL:
    domain = account_email.split("@", 1)[1]
    host = IMAP_HOST_MAP.get(domain, f"mail.{domain}")
    socket.setdefaulttimeout(TIMEOUT_SECS)
    ctx = ssl.create_default_context()
    imap = imaplib.IMAP4_SSL(host, IMAP_PORT, ssl_context=ctx)
    imap.login(account_email, password)
    imap.select("INBOX", readonly=True)
    return imap


def _process_inbox_rows(
    inbox_email: str,
    password: str,
    indexed_rows: list[tuple[int, dict[str, str]]],
) -> list[tuple[int, dict[str, str]]]:
    out: list[tuple[int, dict[str, str]]] = []
    if not password:
        for idx, row in indexed_rows:
            out.append((idx, dict(row)))
        return out

    try:
        imap = _open_imap(inbox_email, password)
    except Exception as e:
        with _print_lock:
            print(f"  [SKIP] {inbox_email}: cannot connect/login — {e}")
        for idx, row in indexed_rows:
            out.append((idx, dict(row)))
        return out

    try:
        for idx, row in indexed_rows:
            full = fetch_full_body_for_row(imap, row)
            new_row = dict(row)
            new_row["body"] = full if full.strip() else row.get("body", "")
            out.append((idx, new_row))
            if not full.strip():
                with _print_lock:
                    print(f"  [MISS] {inbox_email}  subj={row.get('subject', '')[:60]!r}")
    finally:
        try:
            imap.logout()
        except Exception:
            pass

    return out


def main() -> None:
    args = _parse_args()
    rows = read_inbox_csv(args.input)
    if not rows:
        print(f"No rows in {args.input}")
        sys.exit(1)

    if args.csv:
        accounts = _load_accounts_from_csvs(list(args.csv))
        if not accounts:
            print("ERROR: no accounts from --csv")
            sys.exit(1)
        print(f"Loaded {len(accounts)} credential(s) from {len(args.csv)} CSV file(s)")
    else:
        accounts = _load_sender_pool()

    pwd_map = _password_map_from_accounts(accounts)

    by_inbox: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    for i, row in enumerate(rows):
        by_inbox[row["inbox"].strip().lower()].append((i, row))

    print(f"\nFetching full bodies for {len(rows)} row(s) across {len(by_inbox)} inbox(es)\n")

    merged: dict[int, dict[str, str]] = {i: dict(rows[i]) for i in range(len(rows))}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = {}
        for inbox_key, indexed in by_inbox.items():
            canon_inbox = indexed[0][1]["inbox"].strip()
            pw = pwd_map.get(inbox_key, "")
            fut = pool.submit(_process_inbox_rows, canon_inbox, pw, indexed)
            futs[fut] = canon_inbox

        done = 0
        for fut in as_completed(futs):
            inbox = futs[fut]
            done += 1
            try:
                chunks = fut.result()
            except Exception as e:
                print(f"  [ERR] {inbox}: {e}")
                print(f"[{done:>3}/{len(futs)}]  {inbox}  (failed)", flush=True)
                continue
            for idx, r in chunks:
                merged[idx] = r
            print(f"[{done:>3}/{len(futs)}]  {inbox}  ({len(chunks)} message(s))", flush=True)

    ordered = [merged[i] for i in range(len(rows))]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(ordered)

    filled = sum(1 for r in ordered if len((r.get("body") or "").strip()) > 500)
    print(f"\nDone → {args.output}  ({len(ordered)} rows; ~{filled} with long bodies)")


if __name__ == "__main__":
    main()
