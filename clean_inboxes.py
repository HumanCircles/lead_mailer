"""
Clean inbox reply exports — remove bounces, auto-replies, OOO, and delivery failures.

Default: inbox_replies.csv → real_replies.csv

Usage:
    python clean_inboxes.py
    python clean_inboxes.py --input april_replies.csv --output april_replies.csv
"""

from __future__ import annotations

import argparse
import csv
import re

DEFAULT_INPUT  = "inbox_replies.csv"
DEFAULT_OUTPUT = "real_replies.csv"

FIELDNAMES = ["inbox", "from", "subject", "date", "body"]


def _norm_header_cell(s: str) -> str:
    return (s or "").strip().lower().replace("\ufeff", "")


def _is_header_row(cells: list[str]) -> bool:
    if len(cells) < len(FIELDNAMES):
        return False
    return [_norm_header_cell(c) for c in cells[: len(FIELDNAMES)]] == [
        h.lower() for h in FIELDNAMES
    ]


def _row_to_record(cells: list[str]) -> dict[str, str] | None:
    """Map a csv.reader row to FIELDNAMES; handles ragged rows."""
    if not cells or not any((c or "").strip() for c in cells):
        return None
    row = list(cells)
    if len(row) < len(FIELDNAMES):
        row.extend([""] * (len(FIELDNAMES) - len(row)))
    elif len(row) > len(FIELDNAMES):
        row = row[:4] + [",".join(row[4:])]
    return dict(zip(FIELDNAMES, row, strict=True))


def read_inbox_csv(path: str) -> list[dict[str, str]]:
    """
    Load fetch_inboxes.py output. Uses csv.reader so multiline quoted bodies parse
    correctly. If the first row is the canonical header, it is skipped; if the file
    has no header (first row is data), all rows are loaded as data.
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        raw = list(csv.reader(f))
    if not raw:
        return []
    start = 1 if _is_header_row(raw[0]) else 0
    out: list[dict[str, str]] = []
    for cells in raw[start:]:
        rec = _row_to_record(cells)
        if rec is not None:
            out.append(rec)
    return out


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
    your\ message\ was\ not\ delivered | could\ not\ be\ delivered |
    automatic\ reply | auto-reply | auto\ reply | out\ of\ office |
    \bOOO\b | vacation\ (reply|auto) | away\ from\ (the\ )?office |
    failed\ delivery | NDR | bounce | MAILER-DAEMON |
    do\ not\ reply | read\ receipt | receipt\ notification |
    FW:.*unsubscribe | subscription\ confirm
""", re.IGNORECASE | re.VERBOSE)

# Body patterns that reveal automated / delivery messages (do not match generic
# Content-Type lines — real HTML replies contain those too.)
JUNK_BODY = re.compile(r"""
    this\ is\ an\ auto(matic(ally)?)?[ -]generated |
    this\ message\ was\ created\ automatically\ by\ mail\ delivery |
    automatically\ generated\ message\ from\ sendgrid |
    do\ not\ reply\ to\ this\ message |
    unable\ to\ be\ delivered | intended\ recipient |
    rejected\ your\ message\ to\ the\ following |
    smtp\ error | permanent\ failure | temporary\ failure |
    message\ that\ you\ sent\ could\ not\ be\ delivered |
    a\ message\ that\ you\ sent\ could\ not\ be\ delivered
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Remove bounces and system mail from inbox CSV exports.")
    ap.add_argument("--input", "-i", default=DEFAULT_INPUT, help=f"Input CSV (default: {DEFAULT_INPUT})")
    ap.add_argument("--output", "-o", default=DEFAULT_OUTPUT, help=f"Output CSV (default: {DEFAULT_OUTPUT})")
    args = ap.parse_args()
    input_csv = args.input
    output_csv = args.output

    rows = read_inbox_csv(input_csv)

    total  = len(rows)
    kept   = [r for r in rows if not is_junk(r)]
    removed = total - len(kept)

    print(f"Input       : {input_csv}")
    print(f"Total rows  : {total:,}")
    print(f"Removed     : {removed:,}  (bounces / auto-replies / OOO / system)")
    print(f"Real replies: {len(kept):,}")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(kept)

    print(f"\nSaved → {output_csv}")


if __name__ == "__main__":
    main()
