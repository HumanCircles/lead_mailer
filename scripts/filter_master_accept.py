"""
Filter MASTER_ACCEPT-style exports into:
  1) HR/TA senior leadership (+ CEO / founder / CxO), excluding IC/manager noise
  2) HR/TA VP / Director / Head only — no CEO, founder, or CxO / chief-officer titles

Usage:
  python scripts/filter_master_accept.py \\
    --input "/path/to/MASTER_ACCEPT (1).csv" \\
    --out-all prospects_master_senior_leadership.csv \\
    --out-vp-dir prospects_ta_hr_vp_director_only.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

_HR_TA = re.compile(
    r"""
    human\s+resources|\bhr\b|people\s+(&|and)\s+|\bpeople\b|chief\s+people|chief\s+human|chro
    | talent|recruit|workforce|personnel|employee\s+relations|people\s+officer
    | learning\s+(&|and)\s+development|\bld\b|organizational\s+development|diversity
    | compensation|benefits|payroll|hris|hrbp|hr\s+business\s+partner
    """,
    re.I | re.X,
)

_IC_WORD = re.compile(
    r"\b(manager|specialist|coordinator|analyst|intern)\b",
    re.I,
)

_RECRUITER_JR = re.compile(r"\brecruiter\b", re.I)

_SENIOR_TOKEN = re.compile(
    r"""
    \b(ceo|co[\s\-]?ceo|chief\s+executive\s+officer)\b
    | \bco[\s\-]?founder\b
    | \bfounder\b
    | \b(c[tfdmio]o|chro|cpo|cdo|cso|clo|cro|cco)\b
    | \bchief\s+[^,\n]{1,70}?\bofficer\b
    | \b(svp|evp|vp|vice\s+president)\b
    | \b(associate\s+)?(senior\s+)?director\b
    | \bhead\s+of\b
    | \b(managing\s+)?president\b
    """,
    re.I | re.X,
)

_CXO_CEO_FOUNDER = re.compile(
    r"""
    \b(ceo|co[\s\-]?ceo|chief\s+executive\s+officer)\b
    | \bco[\s\-]?founder\b
    | \bfounder\b
    | \b(c[tfdmio]o|chro|cpo|cdo|cso|clo|cro|cco)\b
    | \bchief\s+[^,\n]{1,70}?\bofficer\b
    """,
    re.I | re.X,
)

_NOISE = re.compile(
    r"""
    executive\s+assistant|administrative\s+assistant|office\s+assistant
    | receptionist|internship|^\s*assistant\s+to\b
    """,
    re.I | re.X,
)


def is_ic_only(title: str) -> bool:
    t = title or ""
    if _NOISE.search(t):
        return True
    if _IC_WORD.search(t) and not _SENIOR_TOKEN.search(t):
        return True
    if _RECRUITER_JR.search(t) and not _SENIOR_TOKEN.search(t):
        return True
    if re.search(r"\bmanager\b", t, re.I) and not re.search(
        r"\b(director|svp|evp|vp|vice\s+president|chief|head\s+of|founder|ceo|co[\s\-]?ceo|c[tfdmio]o|chro|president)\b",
        t,
        re.I,
    ):
        return True
    return False


def is_hr_ta_senior(title: str) -> bool:
    t = (title or "").strip()
    if not t or _NOISE.search(t):
        return False
    if is_ic_only(t):
        return False
    if not _HR_TA.search(t):
        return False
    if not _SENIOR_TOKEN.search(t):
        return False
    return True


def is_exec_founder_cxo(title: str) -> bool:
    t = (title or "").strip()
    if not t or _NOISE.search(t):
        return False
    if is_ic_only(t):
        return False
    return bool(_CXO_CEO_FOUNDER.search(t))


def is_clean_master(title: str) -> bool:
    return is_hr_ta_senior(title) or is_exec_founder_cxo(title)


def is_ta_hr_vp_director_no_cxo(title: str) -> bool:
    t = (title or "").strip()
    if not t or _NOISE.search(t):
        return False
    if _CXO_CEO_FOUNDER.search(t):
        return False
    if is_ic_only(t):
        return False
    if not _HR_TA.search(t):
        return False
    if not re.search(
        r"\b(svp|evp|vp|vice\s+president)\b|\b(associate\s+)?(senior\s+)?director\b|\bhead\s+of\b",
        t,
        re.I,
    ):
        return False
    return True


def main() -> None:
    p = argparse.ArgumentParser(description="Filter MASTER_ACCEPT CSV by senior HR/TA roles")
    p.add_argument("--input", "-i", required=True, help="Source MASTER_ACCEPT CSV")
    p.add_argument("--out-all", default="prospects_master_senior_leadership.csv", help="All senior HR/TA + exec")
    p.add_argument("--out-vp-dir", default="prospects_ta_hr_vp_director_only.csv", help="VP/Director/Head, no CEO/CXO")
    p.add_argument(
        "--slice-1k",
        action="append",
        metavar="PATH:OFFSET",
        help='Optional: write 1000 rows to PATH starting at OFFSET (e.g. "batch2.csv:1000")',
    )
    args = p.parse_args()

    src = Path(args.input)
    with src.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    clean_a = [r for r in rows if is_clean_master(r.get("title", ""))]
    clean_b = [r for r in rows if is_ta_hr_vp_director_no_cxo(r.get("title", ""))]

    def _write(path: Path, data: list[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(data)

    _write(Path(args.out_all), clean_a)
    _write(Path(args.out_vp_dir), clean_b)

    for spec in args.slice_1k or []:
        if ":" not in spec:
            raise SystemExit(f"Invalid --slice-1k (want PATH:OFFSET): {spec}")
        out_s, off_s = spec.split(":", 1)
        offset = int(off_s)
        chunk = clean_b[offset : offset + 1000]
        _write(Path(out_s), chunk)

    print(f"Input rows     : {len(rows)}")
    print(f"Senior (A)     : {len(clean_a)}  → {args.out_all}")
    print(f"VP/Dir no CXO (B): {len(clean_b)}  → {args.out_vp_dir}")


if __name__ == "__main__":
    main()
