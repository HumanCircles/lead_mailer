import os
from typing import Dict, Any

import requests
from dotenv import load_dotenv

load_dotenv()

ENRICHLAYER_ENDPOINT = "https://enrichlayer.com/api/v2/profile"


def scrape_linkedin(linkedin_url: str, fresh: bool = True) -> Dict[str, Any]:
    """
    Fetch structured LinkedIn profile data via EnrichLayer People API.

    - fresh=True  -> use_cache=if-recent (fresh data ≤29 days, costs +1 credit)
    - fresh=False -> use_cache=if-present (cached only, free)
    """
    api_key = os.getenv("ENRICHLAYER_API_KEY")
    if not api_key:
        raise RuntimeError("ENRICHLAYER_API_KEY is not set in the environment.")

    use_cache = "if-recent" if fresh else "if-present"

    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "url": linkedin_url,
        "use_cache": use_cache,
        "fallback_to_cache": "on-error",
        "skills": "include",
        "extra": "include",  # industry, interests, etc. (free)
    }

    resp = requests.get(ENRICHLAYER_ENDPOINT, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Extract only what the AI needs
    experiences = [
        f"{e.get('title','')} at {e.get('company','')}"
        for e in (data.get("experiences") or [])[:4]
    ]
    education = [e.get("summary", "") for e in (data.get("education") or [])[:2]]

    return {
        "full_name": data.get("fullName", "") or data.get("full_name", ""),
        "headline": data.get("headline", ""),
        "summary": data.get("summary", ""),
        "location": data.get("location", ""),
        "current_role": experiences[0] if experiences else "",
        "experience": experiences,
        "education": education,
        "skills": (data.get("skills") or [])[:10],
        "industry": data.get("industry", ""),
        "interests": (data.get("interests") or [])[:10],
    }

