"""
Fetch full inboxes (no body truncation) for 3 specific accounts.
Messages after 30-Mar-2026 only. Writes to three_inboxes_full.csv.
"""

import csv
import imaplib
import ssl
from email import message_from_bytes
from email.header import decode_header
from email.message import Message

ACCOUNTS = [
    ("tyler.murphy@recruitagents.net",  "HireQuotient@1234"),
    ("blake.robinson@recruitagents.net", "HireQuotient@1234"),
    ("morgan.miller@recruitagents.net",  "HireQuotient@1234"),
]
SINCE_DATE = "30-Mar-2026"
OUTPUT_CSV = "three_inboxes_full.csv"
IMAP_HOST  = "mail.recruitagents.net"
IMAP_PORT  = 993


def _decode(raw) -> str:
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


def _full_body(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and not part.get("Content-Disposition"):
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace").strip()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace").strip()
    return ""


def fetch_full(account_email: str, password: str) -> list[dict]:
    rows = []
    ctx = ssl.create_default_context()
    try:
        with imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, ssl_context=ctx) as imap:
            imap.login(account_email, password)
            imap.select("INBOX", readonly=True)

            _, data = imap.search(None, "SINCE", SINCE_DATE)
            msg_ids = data[0].split() if data[0] else []
            print(f"  {len(msg_ids)} messages since {SINCE_DATE}")

            for msg_id in msg_ids:
                _, raw = imap.fetch(msg_id, "(RFC822)")
                if not raw or not isinstance(raw[0], tuple):
                    continue
                msg = message_from_bytes(raw[0][1])
                rows.append({
                    "inbox":   account_email,
                    "from":    _decode(msg.get("From", "")),
                    "subject": _decode(msg.get("Subject", "")),
                    "date":    _decode(msg.get("Date", "")),
                    "body":    _full_body(msg),
                })
    except imaplib.IMAP4.error as e:
        print(f"  [AUTH ERROR] {e}")
    except Exception as e:
        print(f"  [ERROR] {e}")
    return rows


def main():
    fieldnames = ["inbox", "from", "subject", "date", "body"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        total = 0
        for acct_email, password in ACCOUNTS:
            print(f"\n{acct_email}")
            rows = fetch_full(acct_email, password)
            writer.writerows(rows)
            f.flush()
            total += len(rows)
            print(f"  -> {len(rows)} rows written")
    print(f"\nDone. {total} total messages → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
