"""
BD Outreach Agent — batch CLI
Usage:
  python agent.py                    # process all pending prospects
  python agent.py --dry-run          # generate but do not send
  python agent.py --limit 20         # cap at 20 prospects this run
  python agent.py --file custom.csv  # override PROSPECTS_FILE env var
"""

import argparse
import csv
import os
import signal
import threading
import time
from datetime import datetime

from dotenv import load_dotenv

from core.logger import IST, get_logger
from core.pipeline import run_pipeline
from core.prospect_csv import canonicalize_prospect_row
from core.smtp_sender import send_seed_email

load_dotenv()

log = get_logger()

SENT_LOG_FILE  = os.getenv("SENT_LOG_FILE", "sent_log.csv")
PROSPECTS_FILE = os.getenv("PROSPECTS_FILE", "prospects.csv")

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error", "from_email",
]

_log_lock   = threading.Lock()
_stop_event = threading.Event()


def _init_log() -> None:
    if not os.path.exists(SENT_LOG_FILE):
        with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()


def _migrate_log() -> None:
    """Add from_email column to old sent_log.csv files that lack it."""
    if not os.path.isfile(SENT_LOG_FILE):
        return
    with open(SENT_LOG_FILE, encoding="utf-8") as f:
        first = f.readline()
    if "from_email" in first:
        return
    with open(SENT_LOG_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        return
    header, *body = rows
    new_header = header + ["from_email"]
    padded = [list(r) + [""] * (len(new_header) - len(r)) for r in body]
    with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(new_header)
        w.writerows(padded)


def _load_sent() -> set[str]:
    if not os.path.exists(SENT_LOG_FILE):
        return set()
    with open(SENT_LOG_FILE, encoding="utf-8") as f:
        return {
            row["prospect_email"].strip().lower()
            for row in csv.DictReader(f)
            if row.get("status") == "pushed"
        }


def _append_log(row: dict) -> None:
    with _log_lock:
        with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_LOG_HEADERS, extrasaction="ignore").writerow(row)


def _load_prospects(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return [canonicalize_prospect_row(r) for r in csv.DictReader(f)]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BD Outreach batch agent")
    p.add_argument("--dry-run", action="store_true", help="Generate emails but do not send")
    p.add_argument("--limit", type=int, default=0, metavar="N",
                   help="Process at most N prospects (0 = all)")
    p.add_argument("--file", default="", metavar="PATH",
                   help="Prospects CSV (overrides PROSPECTS_FILE env var)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    prospects_file = args.file or PROSPECTS_FILE

    log.info("BD Outreach agent starting — dry_run=%s limit=%s file=%s",
             args.dry_run, args.limit or "all", prospects_file)
    print(f"[{datetime.now(tz=IST).strftime('%H:%M:%S IST')}] BD Outreach agent starting...",
          flush=True)

    _init_log()
    _migrate_log()

    all_prospects = _load_prospects(prospects_file)
    already_sent  = _load_sent()

    pending = [
        p for p in all_prospects
        if str(p.get("email", "")).strip().lower() not in already_sent
    ]
    if args.limit > 0:
        pending = pending[:args.limit]
        # Rebuild full list: keep capped pending + already-sent (for duplicate marking)
        sent_prospects = [
            p for p in all_prospects
            if str(p.get("email", "")).strip().lower() in already_sent
        ]
        all_prospects = pending + sent_prospects

    total = len(pending)
    print(
        f"Loaded {len(all_prospects)} prospects — {total} pending"
        f" — {len(already_sent)} already sent"
        + (" [DRY RUN]" if args.dry_run else ""),
        flush=True,
    )

    def _handle_signal(sig, _frame):
        print(f"\n[signal {sig}] Stopping after in-flight work completes...", flush=True)
        log.warning("Received signal %s — setting stop_event", sig)
        _stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    counts: dict[str, int] = {}
    start_time = time.time()
    sample_emails: list[dict] = []  # capture up to 2 full formatted emails for seed

    def _on_result(row: dict) -> None:
        _append_log(row)
        st = row["status"]
        counts[st] = counts.get(st, 0) + 1
        if st == "pushed" and len(sample_emails) < 2:
            sample_emails.append(row)

    def _on_progress(done: int, total_: int) -> None:
        if total_ == 0:
            return
        elapsed = time.time() - start_time
        rate = done / elapsed * 60 if elapsed > 0 else 0
        eta = (total_ - done) / (done / elapsed) if elapsed > 0 and done > 0 else 0
        ts = datetime.now(tz=IST).strftime("%H:%M:%S IST")
        print(
            f"[{ts}] {done}/{total_}  {rate:.1f}/min  ETA ~{eta/60:.1f}min",
            flush=True,
        )

    run_pipeline(
        prospects=all_prospects,
        already_sent=already_sent,
        on_result=_on_result,
        on_progress=_on_progress,
        stop_event=_stop_event,
        dry_run=args.dry_run,
    )

    elapsed = time.time() - start_time
    summary = (
        f"Done — pushed: {counts.get('pushed', 0)}, "
        f"dry_run: {counts.get('dry_run', 0)}, "
        f"skipped_suppressed: {counts.get('skipped_suppressed', 0)}, "
        f"skipped_duplicate: {counts.get('skipped_duplicate', 0)}, "
        f"failed_generation: {counts.get('failed_generation', 0)}, "
        f"failed_api: {counts.get('failed_api', 0)}  "
        f"({elapsed:.0f}s)"
    )
    print(f"\n{summary}", flush=True)
    log.info(summary)

    # ── Seed email to admin ────────────────────────────────────────────────
    ts_label = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M IST")
    dry_tag  = " [DRY RUN]" if args.dry_run else ""
    seed_subject = f"[BD Outreach] Run complete{dry_tag} — {ts_label}"

    seed_body = (
        f"BD Outreach run completed{dry_tag}.\n\n"
        f"File: {prospects_file}\n"
        f"Total prospects: {len(all_prospects)}\n\n"
        f"{summary}"
    )

    if sample_emails:
        seed_body += "\n\n" + "=" * 50 + "\n"
        seed_body += f"SAMPLE EMAILS SENT ({len(sample_emails)} of {counts.get('pushed', 0)} pushed)\n"
        seed_body += "=" * 50
        for i, s in enumerate(sample_emails, 1):
            seed_body += (
                f"\n\n── Sample {i} ──\n"
                f"To:      {s['prospect_name']} <{s['prospect_email']}>\n"
                f"From:    {s['from_email']}\n"
                f"Company: {s['company']}\n"
                f"Subject: {s['subject']}\n"
                f"\n{s.get('body', '(body not captured)')}\n"
                f"\n{'─' * 40}"
            )

    send_seed_email(seed_subject, seed_body)


if __name__ == "__main__":
    main()
