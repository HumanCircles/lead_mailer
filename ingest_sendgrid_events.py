"""
Ingest SendGrid Event Webhook exports and enrich sent_log.csv delivery fields.

Updates these columns by message_id match:
  - delivery_status: accepted|delivered|bounced|dropped|blocked|deferred|opened|clicked|spam_report|unsubscribed
  - delivery_reason: reason/response text from SendGrid event payload

Input formats supported:
  - JSON array of events
  - JSON object (single event)
  - JSONL (one JSON event per line; line may also be an array)

Usage:
  python ingest_sendgrid_events.py --events sendgrid_events.json
  python ingest_sendgrid_events.py --events sendgrid_events.jsonl --log sent_log.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from datetime import datetime, timezone

DEFAULT_LOG = "sent_log.csv"
LOG_HEADERS = [
    "timestamp",
    "prospect_email",
    "prospect_name",
    "company",
    "subject",
    "status",
    "error",
    "from_email",
    "message_id",
    "delivery_status",
    "delivery_reason",
]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest SendGrid delivery events into sent_log.csv")
    p.add_argument("--events", required=True, help="Path to SendGrid events JSON or JSONL file")
    p.add_argument("--log", default=DEFAULT_LOG, help=f"sent_log path (default: {DEFAULT_LOG})")
    p.add_argument("--no-backup", action="store_true", help="Skip creating .bak backup before writing")
    return p.parse_args()


def _norm_msgid(s: str) -> str:
    """Normalize sg_message_id and x-message-id to comparable form."""
    x = (s or "").strip()
    if not x:
        return ""
    # SendGrid webhook sg_message_id often appends ".filterXXXX"
    return x.split(".", 1)[0].strip()


def _safe_ts(value) -> float:
    if value is None:
        return 0.0
    try:
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return 0.0
        if text.isdigit():
            return float(text)
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _load_events(path: str) -> list[dict]:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    text = open(path, encoding="utf-8").read().strip()
    if not text:
        return []

    # Try whole-file JSON first.
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            return [e for e in obj if isinstance(e, dict)]
        if isinstance(obj, dict):
            return [obj]
    except Exception:
        pass

    # Fallback: JSONL
    out: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
            elif isinstance(obj, list):
                out.extend([e for e in obj if isinstance(e, dict)])
    return out


def _map_event_name(name: str) -> str:
    n = (name or "").strip().lower()
    mapping = {
        "processed": "accepted",
        "delivered": "delivered",
        "bounce": "bounced",
        "dropped": "dropped",
        "blocked": "blocked",
        "deferred": "deferred",
        "open": "opened",
        "click": "clicked",
        "spamreport": "spam_report",
        "unsubscribe": "unsubscribed",
    }
    return mapping.get(n, n or "accepted")


def _event_reason(e: dict) -> str:
    for key in ("reason", "response", "status", "smtp-id"):
        v = e.get(key)
        if v:
            return str(v).strip()
    return ""


def _ensure_header_order(existing_headers: list[str]) -> list[str]:
    out = list(existing_headers or [])
    for h in LOG_HEADERS:
        if h not in out:
            out.append(h)
    return out


def main() -> None:
    args = _parse_args()

    if not os.path.isfile(args.log):
        raise FileNotFoundError(args.log)

    events = _load_events(args.events)
    if not events:
        print(f"No events parsed from {args.events}")
        return

    latest_by_msgid: dict[str, tuple[float, str, str]] = {}
    skipped_no_msgid = 0
    for e in events:
        msgid = _norm_msgid(str(e.get("sg_message_id", "") or ""))
        if not msgid:
            skipped_no_msgid += 1
            continue
        ts = _safe_ts(e.get("timestamp"))
        status = _map_event_name(str(e.get("event", "") or "processed"))
        reason = _event_reason(e)
        prev = latest_by_msgid.get(msgid)
        if prev is None or ts >= prev[0]:
            latest_by_msgid[msgid] = (ts, status, reason)

    with open(args.log, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = _ensure_header_order(reader.fieldnames or [])

    touched = 0
    for row in rows:
        msgid = _norm_msgid(row.get("message_id", "") or "")
        if not msgid:
            continue
        ev = latest_by_msgid.get(msgid)
        if ev is None:
            continue
        _, status, reason = ev
        old_status = (row.get("delivery_status") or "").strip()
        old_reason = (row.get("delivery_reason") or "").strip()
        if old_status != status or old_reason != reason:
            row["delivery_status"] = status
            row["delivery_reason"] = reason
            touched += 1

    if touched == 0:
        print(
            f"No sent_log rows updated. Parsed events={len(events)}, "
            f"with message_id={len(latest_by_msgid)}, missing message_id={skipped_no_msgid}"
        )
        return

    if not args.no_backup:
        bak = f"{args.log}.bak.events.{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        shutil.copyfile(args.log, bak)
        print(f"Backup written: {bak}")

    with open(args.log, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(
        f"Updated {touched} row(s) in {args.log} "
        f"from {len(latest_by_msgid)} distinct SendGrid message_id event stream."
    )


if __name__ == "__main__":
    main()
