"""
Clean inbox_replies.csv — remove bounces, auto-replies, OOO, and delivery failures.
Writes real human replies to real_replies.csv.

Usage:
    python clean_inboxes.py
"""

import csv
import re

INPUT_CSV  = "inbox_replies.csv"
OUTPUT_CSV = "real_replies.csv"

# Sender addresses that are never real humans
JUNK_SENDERS = re.compile(r"""
    mailer-daemon | postmaster | no-reply | noreply |
    do-not-reply | donotreply | bounce | sendgrid\.net |
    mail-noreply | notifications@ | daemon@
""", re.IGNORECASE | re.VERBOSE)

# Subject patterns that indicate automated / system messages
JUNK_SUBJECTS = re.compile(r"""
    undelivered\ mail | delivery\ status | delivery\ failure |
    mail\ delivery\ failed | returned\ to\ sender | undeliverable |
    automatic\ reply | auto-reply | auto\ reply | out\ of\ office |
    \bOOO\b | vacation\ (reply|auto) | away\ from\ (the\ )?office |
    failed\ delivery | NDR | bounce | MAILER-DAEMON |
    do\ not\ reply | read\ receipt | receipt\ notification |
    FW:.*unsubscribe | subscription\ confirm
""", re.IGNORECASE | re.VERBOSE)

# Body patterns that reveal automated messages
JUNK_BODY = re.compile(r"""
    this\ is\ an\ auto(matic(ally)?)?[ -]generated |
    do\ not\ reply\ to\ this | this\ message\ was\ sent\ automatically |
    unable\ to\ be\ delivered | intended\ recipient |
    smtp\ error | permanent\ error | temporary\ error |
    Content-Type:\ (text|multipart) |   # raw MIME in body = bad parse
    -+\d+\s*Content-Type               # MIME boundary artifacts
""", re.IGNORECASE | re.VERBOSE)


def is_junk(row: dict) -> bool:
    sender  = row.get("from", "")
    subject = row.get("subject", "")
    body    = row.get("body", "")
    return (
        bool(JUNK_SENDERS.search(sender))
        or bool(JUNK_SUBJECTS.search(subject))
        or bool(JUNK_BODY.search(body))
    )


def main():
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total  = len(rows)
    kept   = [r for r in rows if not is_junk(r)]
    removed = total - len(kept)

    print(f"Total rows : {total:,}")
    print(f"Removed    : {removed:,}  (bounces / auto-replies / OOO / system)")
    print(f"Real replies: {len(kept):,}")

    fieldnames = ["inbox", "from", "subject", "date", "body"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    print(f"\nSaved → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
