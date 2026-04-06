"""
Enrich a prospects CSV with LinkedIn research notes for Beat 1 of the email playbook.

For each prospect, fetches their LinkedIn profile via RapidAPI and extracts the most
useful recent signal (latest post, headline, about summary) into a `research_note` field.
The drafter then uses this to write a genuine, specific Beat 1 observation.

Usage:
    python enrich_linkedin.py --input data/main.csv --output prospects_enriched.csv
    python enrich_linkedin.py --input prospects.csv --workers 5

The script resumes automatically — already-enriched rows (non-empty research_note) are
skipped unless --fresh is passed.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = "fresh-linkedin-profile-data.p.rapidapi.com"
BASE_URL      = f"https://{RAPIDAPI_HOST}"

HEADERS = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

_print_lock = threading.Lock()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich prospects with LinkedIn research notes")
    p.add_argument("--input",   "-i", default="prospects.csv",          help="Input CSV")
    p.add_argument("--output",  "-o", default="prospects_enriched.csv", help="Output CSV")
    p.add_argument("--workers", "-w", type=int, default=3,              help="Parallel API calls (default: 3)")
    p.add_argument("--fresh",   action="store_true",                    help="Re-enrich all rows, even those with existing notes")
    p.add_argument("--delay",   type=float, default=0.5,                help="Seconds between API calls per worker (default: 0.5)")
    p.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="Persist output every N completed rows (default: 25, 0 = end only)",
    )
    return p.parse_args()


# ── LinkedIn URL normalisation ────────────────────────────────────────────────

def _clean_linkedin_url(raw: str) -> str:
    """Normalise to https://www.linkedin.com/in/slug form."""
    url = (raw or "").strip().rstrip("/")
    if not url or "linkedin.com" not in url:
        return ""
    if url.startswith("linkedin.com"):
        url = "https://www." + url
    elif url.startswith("www.linkedin.com"):
        url = "https://" + url
    elif not url.startswith("http"):
        url = "https://www.linkedin.com/in/" + url.lstrip("/")
    # Normalise http→https, linkedin.com → www.linkedin.com
    url = url.replace("http://", "https://").replace("https://linkedin.com", "https://www.linkedin.com")
    return url


def _find_linkedin_url(row: dict) -> str:
    """Try several common column name variants."""
    candidates = [
        "linkedin_url", "linkedin", "linkedInProfileUrl", "Person Linkedin Url",
        "profileUrl", "defaultProfileUrl", "li_url", "LinkedIn URL",
    ]
    for col in candidates:
        val = (row.get(col) or "").strip()
        if val and "linkedin.com/in/" in val:
            return _clean_linkedin_url(val)
    return ""


# ── RapidAPI calls ────────────────────────────────────────────────────────────

def _api_get(endpoint: str, params: dict) -> dict | None:
    """GET any RapidAPI endpoint, unwrap {"data": ..., "message": "ok"} envelope."""
    try:
        resp = requests.get(
            f"{BASE_URL}/{endpoint}",
            headers=HEADERS,
            params=params,
            timeout=15,
        )
        if resp.status_code == 200:
            body = resp.json()
            return body.get("data") or body
        if resp.status_code == 429:
            time.sleep(5)
        return None
    except Exception:
        return None


def _fetch_by_url(linkedin_url: str) -> dict | None:
    """
    Fetch profile + latest post for a LinkedIn URL.
    Makes two API calls:
      1. /enrich-lead          → awards, publications, about, skills, headline
      2. /get-profile-posts    → most recent post text (strongest Beat-1 signal)

    Returns a merged dict so _extract_research_note can pick the best signal.
    """
    profile = _api_get("enrich-lead", {
        "linkedin_url":               linkedin_url,
        "include_skills":             "true",
        "include_certifications":     "false",
        "include_profile_status":     "false",
        "include_company_public_url": "false",
    })
    if profile is None:
        profile = {}

    # Speed path: if enrich-lead already yields a usable signal
    # (about/headline/skills/publications/awards), avoid a second API call.
    if _extract_research_note(profile):
        return profile

    # Otherwise fetch latest post separately — returns a list of posts under "data"
    post_data = _api_get("get-profile-posts", {
        "linkedin_url": linkedin_url,
        "type":         "posts",
    })
    if post_data:
        first_post = post_data[0] if isinstance(post_data, list) else post_data
        if isinstance(first_post, dict) and first_post.get("text"):
            profile["_latest_post"] = first_post

    return profile if profile else None


def _search_by_name_company(first_name: str, last_name: str, company: str) -> dict | None:
    """Search LinkedIn via RapidAPI by name + company."""
    keyword = f"{first_name} {last_name}".strip()
    if not keyword:
        return None
    try:
        resp = requests.get(
            f"{BASE_URL}/search-employees",
            headers=HEADERS,
            params={"company_name": company or "", "keyword": keyword},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            employees = data.get("employees") or data.get("data") or []
            if employees:
                profile_url = (employees[0].get("linkedin_url") or
                               employees[0].get("profile_url") or "")
                if profile_url:
                    return _fetch_by_url(_clean_linkedin_url(profile_url))
        return None
    except Exception:
        return None


def _google_find_linkedin_url(first_name: str, last_name: str,
                               company: str, email: str) -> str:
    """
    Tier-3 fallback: Google search to discover a LinkedIn profile URL.

    Uses the email domain as an extra signal (e.g. hirequotient.com) to
    narrow results without needing a last name.
    Returns a linkedin.com/in/... URL string, or "".
    """
    import re as _re
    name    = f"{first_name} {last_name}".strip()
    domain  = email.split("@")[-1] if "@" in email else ""
    # Build a tight Google query
    parts = [f'site:linkedin.com/in', f'"{name}"' if name else "", f'"{company}"' if company else ""]
    if domain and domain not in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com"):
        parts.append(domain)
    query = " ".join(p for p in parts if p)

    try:
        search_resp = requests.get(
            "https://www.google.com/search",
            params={"q": query, "num": 3},
            headers={"User-Agent": "Mozilla/5.0 (compatible; enrichbot/1.0)"},
            timeout=10,
        )
        if search_resp.status_code != 200:
            return ""
        # Extract linkedin.com/in/slug URLs from response HTML
        urls = _re.findall(
            r'linkedin\.com/in/([\w\-]+)',
            search_resp.text,
        )
        if urls:
            slug = urls[0]
            return f"https://www.linkedin.com/in/{slug}"
    except Exception:
        pass
    return ""


# ── Research note extraction ──────────────────────────────────────────────────

def _extract_research_note(profile: dict) -> str:
    """
    Pull the most useful Beat-1 signal from a LinkedIn /enrich-lead response.

    Priority:
      1. Latest post         — what they're actively thinking about right now
      2. Recent publication  — establishes thought-leadership angle
      3. Top award           — strong credibility anchor
      4. About / summary     — their own framing of what they do
      5. Top skills          — professional focus areas
      6. Headline            — last resort fallback
    """
    if not profile:
        return ""

    # 1. Latest post (from /get-profile-posts — strongest Beat-1 signal)
    post = profile.get("_latest_post") or {}
    post_text = (post.get("text") or "").strip()
    if post_text and len(post_text) > 30:
        snippet   = post_text[:280].rsplit(" ", 1)[0]
        posted_on = (post.get("posted") or "")[:10]   # "2025-04-09"
        date_tag  = f" ({posted_on})" if posted_on else ""
        return f"Recent LinkedIn post{date_tag}: \"{snippet}...\""

    # 2. Most recent publication
    pubs = profile.get("publications") or []
    if pubs:
        pub = pubs[0] if isinstance(pubs[0], str) else (pubs[0].get("title") or "")
        if pub:
            return f"Published: \"{pub}\""

    # 3. Top award
    awards = profile.get("awards") or []
    if awards:
        award = awards[0] if isinstance(awards[0], str) else (awards[0].get("title") or "")
        if award:
            return f"Award: \"{award}\""

    # 4. About / summary
    about = (profile.get("about") or profile.get("summary") or "").strip()
    if about and len(about) > 40:
        return f"LinkedIn about: \"{about[:240].rsplit(' ', 1)[0]}...\""

    # 5. Top skills — API returns either a plain string or a list of strings/dicts
    skills_raw = profile.get("skills") or []
    if isinstance(skills_raw, str):
        skill_names = [s.strip() for s in skills_raw.split(",") if s.strip()]
    else:
        skill_names = [
            (s if isinstance(s, str) else s.get("name") or s.get("title") or "")
            for s in skills_raw
        ]
        skill_names = [s for s in skill_names if s]
    if skill_names:
        return f"Top skills: {', '.join(skill_names[:5])}"

    # 6. Headline
    headline = (profile.get("headline") or "").strip()
    if headline:
        return f"LinkedIn headline: \"{headline}\""

    return ""


# ── Per-row enrichment ────────────────────────────────────────────────────────

def enrich_row(row: dict, delay: float = 0.5) -> dict:
    """
    Enrich one prospect row with a LinkedIn research note.

    Three-tier fallback:
      1. LinkedIn URL from CSV  →  direct profile fetch (fastest, most accurate)
      2. Name + company         →  RapidAPI employee search (costs API credits)
      3. Google search          →  find LinkedIn slug from name + email domain (free, slower)
    """
    first   = (row.get("first_name") or "").strip()
    last    = (row.get("last_name")  or "").strip()
    company = (row.get("company")    or "").strip()
    email   = (row.get("email")      or "").strip()

    li_url  = _find_linkedin_url(row)
    profile = None
    did_lookup = False

    # Tier 1 — direct URL
    if li_url:
        did_lookup = True
        profile = _fetch_by_url(li_url)

    # Tier 2 — RapidAPI name+company search
    if profile is None and (first or last):
        did_lookup = True
        profile = _search_by_name_company(first, last, company)

    # Tier 3 — Google to discover the URL, then fetch profile
    if profile is None:
        discovered_url = _google_find_linkedin_url(first, last, company, email)
        if discovered_url:
            did_lookup = True
            profile = _fetch_by_url(discovered_url)

    note = _extract_research_note(profile) if profile else ""
    if did_lookup and delay > 0:
        time.sleep(delay)

    enriched = dict(row)
    enriched["research_note"] = note
    return enriched


def _write_output(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    with open(args.input, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("Input CSV is empty.")
        sys.exit(0)

    # Ensure research_note column exists
    for r in rows:
        r.setdefault("research_note", "")

    fieldnames = list(rows[0].keys())
    if "research_note" not in fieldnames:
        fieldnames.append("research_note")

    # Skip already-enriched rows unless --fresh
    pending_idx = [
        i for i, r in enumerate(rows)
        if args.fresh or not r.get("research_note", "").strip()
    ]

    print(f"Total rows  : {len(rows)}")
    print(f"To enrich   : {len(pending_idx)}  (already done: {len(rows) - len(pending_idx)})")
    print(f"Workers     : {args.workers}  |  delay: {args.delay}s per call")
    print()

    if not pending_idx:
        print("Nothing to enrich. Pass --fresh to re-enrich all rows.")
        _write_output(args.output, fieldnames, rows)
    else:
        done_n = [0]
        lock   = threading.Lock()
        checkpoint_every = max(0, args.checkpoint_every)

        def _work(i: int) -> tuple[int, dict]:
            enriched = enrich_row(rows[i], delay=args.delay)
            return i, enriched

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_work, i): i for i in pending_idx}
            for fut in as_completed(futures):
                i, enriched = fut.result()
                rows[i] = enriched
                with lock:
                    done_n[0] += 1
                    note_short = (enriched.get("research_note") or "")[:60]
                    name = f"{enriched.get('first_name','')} {enriched.get('last_name','')}".strip()
                    with _print_lock:
                        print(
                            f"[{done_n[0]:>4}/{len(pending_idx)}]  {name:<30}  "
                            + (f'"{note_short}..."' if note_short else "(no note found)")
                        )
                    if checkpoint_every > 0 and done_n[0] % checkpoint_every == 0:
                        _write_output(args.output, fieldnames, rows)
                        print(f"  ↳ checkpoint saved ({done_n[0]}/{len(pending_idx)}) → {args.output}")

    _write_output(args.output, fieldnames, rows)

    enriched_count = sum(1 for r in rows if r.get("research_note", "").strip())
    print(f"\nEnriched rows with notes: {enriched_count}/{len(rows)}")
    print(f"Saved → {args.output}")


if __name__ == "__main__":
    main()
