import os, re, json
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
MODEL = "claude-haiku-4-5-20251001"

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
        raise ValueError(f"Could not parse email JSON:\n{raw[:300]}")


def draft_email(lead_data: dict) -> dict:
    name         = lead_data.get("name", "")
    company      = lead_data.get("company", "")
    title        = lead_data.get("title", "")
    platform     = lead_data.get("hcm_platform", "") or company

    prompt = f"""Prospect:
- Name: {name}
- Title: {title}
- Company: {company}
- Platform: {platform}

Use your knowledge about this company, their industry, and the person's role to craft a sharp email following the 5-beat framework. For Beat 1, draw on any publicly known signal about the prospect or company — a product launch, a press mention, a known initiative, or a clear industry-level observation tied to their specific segment.

Write the email. Return JSON only — no markdown, no preamble:
{{
  "subject": "...",
  "body": "..."
}}

Rules:
- Body is plain text. No markdown, no bold, no bullet points, no HTML.
- Every sentence must map to one of the five beats in the playbook.
- If a beat cannot be written with confidence, compress the email rather than write a filler sentence.
- Subject must follow exactly: Invite for discussion | [3-4 word pain point hook]"""

    for attempt in range(2):
        response = _client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=MESSAGING_GUIDE,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if raw:
            try:
                return _parse_json(raw)
            except ValueError:
                if attempt == 0:
                    continue
                raise

    raise ValueError("Model returned empty response.")
