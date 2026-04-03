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


def _format_body_for_plaintext(body: str) -> str:
    """
    Ensure readable plain-text email formatting with short paragraphs.
    If model output comes as one dense block, split it into short sections.
    """
    text = strip_control_chars(body or "").replace("\r\n", "\n").strip()
    if not text:
        return ""

    # Normalize whitespace inside lines while preserving existing paragraph breaks.
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    # If already paragraphized, keep it mostly as-is.
    if "\n\n" in text:
        return text

    # Greeting line stays on its own when present.
    greeting = ""
    remainder = text
    first_line_split = re.split(r"\n", text, maxsplit=1)
    first_line = first_line_split[0].strip()
    if re.match(r"^(hi|hello)\b", first_line, flags=re.IGNORECASE):
        greeting = first_line
        remainder = first_line_split[1].strip() if len(first_line_split) > 1 else ""

    source = remainder or text
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", source) if s.strip()]
    if len(sentences) <= 2:
        return f"{greeting}\n\n{source}".strip() if greeting else source

    paras: list[str] = []
    if greeting:
        paras.append(greeting)

    i = 0
    while i < len(sentences):
        # Keep paragraphs concise: 1-2 sentences each.
        take = 2 if i < len(sentences) - 1 else 1
        paras.append(" ".join(sentences[i:i + take]).strip())
        i += take

    return "\n\n".join(p for p in paras if p).strip()


def draft_email(lead_data: dict) -> dict:
    name          = strip_control_chars(str(lead_data.get("name", "") or ""))
    company       = strip_control_chars(str(lead_data.get("company", "") or ""))
    title         = strip_control_chars(str(lead_data.get("title", "") or ""))
    platform      = strip_control_chars(str(lead_data.get("hcm_platform", "") or company or ""))
    research_note = strip_control_chars(str(lead_data.get("research_note", "") or ""))

    research_block = (
        f"\n- Research note (use this verbatim for Beat 1): {research_note}"
        if research_note else
        "\n- Research note: none — infer Beat 1 from their title, company, and platform context"
    )

    prompt = f"""Prospect:
- Name: {name}
- Title: {title}
- Company: {company}
- Platform: {platform}{research_block}

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
                parsed = _parse_json(raw)
                parsed["body"] = _format_body_for_plaintext(str(parsed.get("body", "") or ""))
                return parsed
            except ValueError:
                if attempt == 0:
                    continue
                raise

    raise ValueError("Model returned empty response.")
