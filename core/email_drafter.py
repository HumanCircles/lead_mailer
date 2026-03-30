import os
import re
import json
from openai import OpenAI
from dotenv import load_dotenv

from core.deliverability import strip_control_chars

load_dotenv()

OPENAI_TIMEOUT     = float(os.getenv("OPENAI_TIMEOUT", "30"))
OPENAI_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "2"))

_client = OpenAI(
    api_key=os.environ["OPENAI_API_KEY"],
    timeout=OPENAI_TIMEOUT,
    max_retries=OPENAI_MAX_RETRIES,
)
MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

_GUIDE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "MESSAGING_README.md")
with open(_GUIDE_PATH, encoding="utf-8") as _f:
    MESSAGING_GUIDE = strip_control_chars(_f.read())


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
    name     = strip_control_chars(str(lead_data.get("name", "") or ""))
    company  = strip_control_chars(str(lead_data.get("company", "") or ""))
    title    = strip_control_chars(str(lead_data.get("title", "") or ""))
    platform = strip_control_chars(str(lead_data.get("hcm_platform", "") or company or ""))

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
