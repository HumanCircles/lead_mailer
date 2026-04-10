"""
Generate Kunal's personalised email drafts for the SF/SJ prospect batch.

For each enriched prospect, calls OpenAI to write:
  • A sharp, specific subject line (3–5 words, names a pain point)
  • Para 1: 1–2 sentence personalised opener built from the LinkedIn research note

Then appends the fixed Para 2 & 3 verbatim as supplied by Kunal.

Usage:
    .venv/bin/python draft_kunal_batch.py \
        --input  kunal_sf_sj_enriched.csv \
        --output kunal_sf_sj_drafts.csv

    # Re-draft rows that already have drafts:
    .venv/bin/python draft_kunal_batch.py --input kunal_sf_sj_enriched.csv --fresh
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o")
CLIENT  = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    timeout=float(os.getenv("OPENAI_TIMEOUT", "30")),
    max_retries=int(os.getenv("OPENAI_MAX_RETRIES", "3")),
)

_print_lock = threading.Lock()

# ── Fixed body copy (verbatim from Kunal) ────────────────────────────────────

FIXED_PARA_2 = (
    "I'm Kunal, from the Founders Office at HireQuotient. Our CEO Smarthveer Sidana, "
    "who is based out of San Francisco (ex BCG, Forbes 30 Under 30 2024, Entrepreneur "
    "Magazine's 35 Under 35 2022, Royal Society of London Nominee), works closely with "
    "CXOs across the US as a trusted advisor on AI native transformation. He has been in "
    "a series of focused conversations with senior HR leaders driving culture and engagement "
    "at scale, and I believe your perspective would add a lot of depth to that exchange."
)

FIXED_PARA_3 = (
    "If calendars align, would you be open to a 15 minute coffee chat with him?"
)

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You draft hyper-personalised cold email components for BD outreach to senior HR leaders.

Your job is to produce exactly two things for each prospect:
1. SUBJECT — a 3–5 word subject line. It must name a specific, concrete pain point
   that is UNIQUE to this prospect's company, industry, or role context. Title case.
   No question marks or exclamation points. NEVER use generic phrases like
   "Scaling Employee Engagement", "Scaling People Strategy", "Coffee Chat",
   "Quick Call", "Introduction", or any phrase that could apply to all HR leaders.
   Think: what specific tension is this company or this industry facing RIGHT NOW?
   Name the problem domain precisely. Examples of good subjects:
     "Culture Drift After Hypergrowth"
     "Engineer Retention at Unicorn Scale"
     "M&A Integration People Risk"
     "Frontline Manager Bandwidth Gap"
     "HR Tech Stack Fragmentation"
   The subject must be unique to this prospect. If two different prospects could
   both receive it, it is too generic. Try again.

2. PARA1 — 1–2 sentences. A sharp, genuine observation about this specific prospect
   drawn from the research note if available. This is NOT a compliment. It is proof
   of attention. If a research note is provided, reference something concrete from it:
   a post they wrote, a stance they hold, a challenge in their summary. If no note is
   available, anchor on a real known challenge in their specific company or industry
   at their seniority level. End with one implication that bridges naturally to a
   conversation about culture, engagement, or AI-driven people strategy at scale.

Hard rules:
- No dashes of any kind (no —, –, or --). Use a period or rewrite.
- No stiff role labels ("as [full title]" or "in your role as").
- Do not restate the job title verbatim in the opener.
- Plain prose only. No bullet points, bold, or formatting.
- Do not write anything beyond SUBJECT and PARA1.
- Do not add a greeting ("Hi [name]") or sign-off.
- Every SUBJECT must be unique and specific to this individual. Generic is wrong.

Respond in this exact format — no extra text:
SUBJECT: <subject line>
PARA1: <one or two sentences>
"""


# ── OpenAI draft call ─────────────────────────────────────────────────────────

def _draft_one(row: dict) -> tuple[str, str]:
    """Return (subject, para1) for one prospect. Raises on hard failure."""
    first   = row.get("first_name", "").strip()
    last    = row.get("last_name",  "").strip()
    title   = row.get("title",      "").strip()
    company = row.get("company",    "").strip()
    note    = row.get("research_note", "").strip()

    user_msg = (
        f"Prospect: {first} {last}\n"
        f"Title: {title}\n"
        f"Company: {company}\n"
        f"Research note: {note if note else '(none available)'}\n"
    )

    resp = CLIENT.chat.completions.create(
        model=MODEL,
        temperature=0.7,
        max_tokens=200,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
    )
    text = resp.choices[0].message.content.strip()

    subject = ""
    para1   = ""
    for line in text.splitlines():
        if line.startswith("SUBJECT:"):
            subject = line[len("SUBJECT:"):].strip()
        elif line.startswith("PARA1:"):
            para1 = line[len("PARA1:"):].strip()

    return subject, para1


def _assemble_email(first_name: str, subject: str, para1: str) -> str:
    """Assemble the full email body from para1 + fixed paras."""
    greeting = f"Hi {first_name},"
    return "\n\n".join([greeting, para1, FIXED_PARA_2, FIXED_PARA_3])


# ── Row-level worker ──────────────────────────────────────────────────────────

def draft_row(row: dict) -> dict:
    subject, para1 = _draft_one(row)
    first = row.get("first_name", "").strip().title()
    body  = _assemble_email(first, subject, para1)
    out   = dict(row)
    out["subject"]    = subject
    out["para1"]      = para1
    out["email_body"] = body
    return out


# ── I/O helpers ───────────────────────────────────────────────────────────────

def _write(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate Kunal email drafts for SF/SJ batch")
    p.add_argument("--input",   "-i", default="kunal_sf_sj_enriched.csv")
    p.add_argument("--output",  "-o", default="kunal_sf_sj_drafts.csv")
    p.add_argument("--workers", "-w", type=int, default=5)
    p.add_argument("--fresh",   action="store_true", help="Re-draft already-drafted rows")
    p.add_argument("--checkpoint-every", type=int, default=10)
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    with open(args.input, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("Input CSV is empty.")
        sys.exit(0)

    for r in rows:
        r.setdefault("subject",    "")
        r.setdefault("para1",      "")
        r.setdefault("email_body", "")

    fieldnames = list(rows[0].keys())
    for col in ("subject", "para1", "email_body"):
        if col not in fieldnames:
            fieldnames.append(col)

    pending_idx = [
        i for i, r in enumerate(rows)
        if args.fresh or not r.get("subject", "").strip()
    ]

    print(f"Total rows   : {len(rows)}")
    print(f"To draft     : {len(pending_idx)}  (already done: {len(rows) - len(pending_idx)})")
    print(f"Workers      : {args.workers}  |  model: {MODEL}")
    print()

    if not pending_idx:
        print("Nothing to draft. Pass --fresh to re-draft all rows.")
        _write(args.output, fieldnames, rows)
        return

    done_n = [0]
    lock   = threading.Lock()
    checkpoint_every = max(0, args.checkpoint_every)

    def _work(i: int) -> tuple[int, dict]:
        return i, draft_row(rows[i])

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_work, i): i for i in pending_idx}
        for fut in as_completed(futures):
            try:
                i, drafted = fut.result()
                rows[i] = drafted
                with lock:
                    done_n[0] += 1
                    name    = f"{drafted.get('first_name','')} {drafted.get('last_name','')}".strip()
                    subject = drafted.get("subject", "")[:60]
                    with _print_lock:
                        print(f"[{done_n[0]:>4}/{len(pending_idx)}]  {name:<32}  {subject}")
                    if checkpoint_every > 0 and done_n[0] % checkpoint_every == 0:
                        _write(args.output, fieldnames, rows)
                        print(f"  ↳ checkpoint saved ({done_n[0]}/{len(pending_idx)}) → {args.output}")
            except Exception as exc:
                i = futures[fut]
                name = f"{rows[i].get('first_name','')} {rows[i].get('last_name','')}".strip()
                with _print_lock:
                    print(f"  ERROR  {name}: {exc}")

    _write(args.output, fieldnames, rows)
    drafted_count = sum(1 for r in rows if r.get("subject", "").strip())
    print(f"\nDrafted: {drafted_count}/{len(rows)}")
    print(f"Saved → {args.output}")


if __name__ == "__main__":
    main()
