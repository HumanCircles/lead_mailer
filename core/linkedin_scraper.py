"""
linkedin_scraper.py
Step 1: Fresh LinkedIn Profile Data (RapidAPI)  — real structured profile
Step 2: gemini-2.0-flash + Google Search        — recent activity, hiring signals
Step 3: merge into final dict for email_drafter
"""

import os, re, json, requests
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

_client       = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
RAPIDAPI_KEY  = os.getenv("FRESH_LINKEDIN_PROFILE_API_KEY", os.getenv("RAPIDAPI_KEY", ""))
RAPIDAPI_HOST = "fresh-linkedin-profile-data.p.rapidapi.com"
ENRICH_URL    = f"https://{RAPIDAPI_HOST}/enrich-lead"


def _fetch_profile(linkedin_url: str) -> dict:
    resp = requests.get(
        ENRICH_URL,
        headers={
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key": RAPIDAPI_KEY,
        },
        params={
            "linkedin_url": linkedin_url,
            "include_skills": "true",
            "include_company_public_url": "true",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _research_signals(linkedin_url: str, name: str, company: str) -> str:
    response = _client.models.generate_content(
        model="gemini-2.0-flash",
        contents=(
            f"Research {name} at {company}. LinkedIn: {linkedin_url}\n\n"
            "Find ONLY:\n"
            "- Recent LinkedIn posts/comments/shares\n"
            "- Hiring signals: open roles, team growth, scaling\n"
            "- Recent company news, funding, product launches\n\n"
            "Plain text, 3-5 sentences. No JSON."
        ),
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=1.0,
        ),
    )
    if response.text:
        return response.text.strip()
    try:
        parts = response.candidates[0].content.parts
        return "".join(p.text for p in parts if getattr(p, "text", None)).strip()
    except Exception:
        return ""


def scrape_linkedin(linkedin_url: str, fresh: bool = True) -> dict:
    data = _fetch_profile(linkedin_url)
    data = data.get("data", data)

    experiences = data.get("experiences") or []
    exp_strings = [
        f"{e.get('title', '')} at {e.get('company', '')}".strip(" at")
        for e in experiences[:4]
    ]
    edu_strings = [
        f"{e.get('degree', '')} at {e.get('school', '')}".strip(" at")
        for e in (data.get("education") or [])[:2]
    ]
    skill_strings = [
        (s.get("name") or s) if isinstance(s, dict) else str(s)
        for s in (data.get("skills") or [])[:10]
    ]

    full_name = data.get("full_name") or data.get("fullName") or ""
    company_name = experiences[0].get("company") if experiences else ""

    signals = ""
    try:
        signals = _research_signals(
            linkedin_url,
            full_name,
            company_name or data.get("headline", ""),
        )
    except Exception:
        pass

    return {
        "full_name": full_name,
        "headline": data.get("headline", ""),
        "summary": data.get("summary", ""),
        "location": data.get("location", ""),
        "current_role": exp_strings[0] if exp_strings else "",
        "experience": exp_strings,
        "education": edu_strings,
        "skills": skill_strings,
        "industry": data.get("industry", ""),
        "interests": [],
        "recent_activity": signals,
        "hiring_signals": signals,
        "recent_news": signals,
    }
