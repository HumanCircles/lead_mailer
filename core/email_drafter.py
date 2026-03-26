import os
import re
import json
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

_GUIDE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "MESSAGING_README.md")
with open(_GUIDE_PATH) as _f:
    MESSAGING_GUIDE = _f.read()


def _parse_json(raw: str) -> dict:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw.strip())
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise ValueError(f"Could not parse JSON:\n{raw[:300]}")


def draft_email(lead_data: dict) -> dict:
    name     = lead_data.get("name", "")
    company  = lead_data.get("company", "")
    title    = lead_data.get("title", "")
    platform = lead_data.get("hcm_platform", "") or company

    prompt = f"""Prospect:
- Name: {name}
- Title: {title}
- Company: {company}
- Platform: {platform}

Write the email following the playbook exactly. Return JSON only — no markdown, no preamble:
{{
  "subject": "...",
  "body": "..."
}}"""

    for attempt in range(2):
        resp = _client.chat.completions.create(
            model=MODEL,
            max_tokens=1024,
            messages=[
                {"role": "system", "content": MESSAGING_GUIDE},
                {"role": "user",   "content": prompt},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        if raw:
            try:
                return _parse_json(raw)
            except ValueError:
                if attempt == 0:
                    continue
                raise

    raise ValueError("Model returned empty response.")
