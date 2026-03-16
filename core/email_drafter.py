import os
import re
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

MODEL_RESEARCH = "gemini-2.0-flash"
MODEL_DRAFT = "gemini-2.5-pro"
MODEL_DRAFT_FALLBACK = "gemini-2.0-flash"

RESEARCH_PROMPT = """Use Google Search to find recent news, achievements, posts, or company updates about this person or their company. Return a short plain-text research summary (a few sentences) that would help personalize a cold outreach email. No JSON, no formatting — just the summary."""

SYSTEM_PROMPT_DRAFT = """
You are an expert B2B sales copywriter for HireQuotient — an AI-powered hiring platform 
that helps companies assess and hire top talent faster with automated skill assessments.

Your task: Write a hyper-personalized cold outreach email using the research summary and LinkedIn profile below.

Rules:
- Subject line must be punchy and < 9 words, referencing something specific about them
- Opening line must reference their SPECIFIC role, company, or recent achievement — NEVER generic
- Body: 3-4 short sentences max. Focus on ONE pain point relevant to their role
- CTA: Single, low-friction ask (15-min call or reply to learn more)
- Tone: Warm, peer-to-peer, NOT salesy. Like one professional emailing another
- Sign off as: Team HireQuotient
- Output ONLY valid JSON in this format (no markdown):
{
  "subject": "...",
  "body": "..."
}
"""


def _extract_text(response) -> str:
    """
    Get response text whether Gemini returned .text (normal) or used
    candidates[0].content.parts (grounding/thinking models).
    """
    if response.text is not None:
        return response.text.strip()
    if not response.candidates:
        return ""
    c0 = response.candidates[0]
    content = getattr(c0, "content", None)
    if content is None:
        return ""
    parts = getattr(content, "parts", None) or []
    texts = [p.text for p in parts if hasattr(p, "text") and p.text]
    return "".join(texts).strip()


def _parse_json(raw: str) -> dict:
    """Parse JSON from raw string; use regex fallback if wrapped in prose."""
    raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        raise


def _research_lead(lead_data: dict, profile: dict) -> str:
    """
    Step 1: Use Flash + Google Search to get a plain-text research summary.
    Grounding works reliably with Flash; avoids empty responses from Pro+tools.
    """
    name = lead_data.get("name", "")
    company = profile.get("current_role", "") or profile.get("headline", "")
    grounding_tool = types.Tool(google_search=types.GoogleSearch())
    config = types.GenerateContentConfig(
        tools=[grounding_tool],
    )
    user_message = f"{RESEARCH_PROMPT}\n\nPerson: {name}. Company/role: {company}."
    response = client.models.generate_content(
        model=MODEL_RESEARCH,
        contents=user_message,
        config=config,
    )
    return _extract_text(response) or "(No additional research found.)"


def _draft_from_context(
    research_summary: str, lead_data: dict, profile: dict, model: str
) -> str:
    """
    Step 2: Draft email JSON from research + profile. No tools = guaranteed text output.
    """
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT_DRAFT,
        temperature=1.0,
    )
    user_message = f"""
Research summary (from web search):
{research_summary}

Lead:
- Name: {lead_data["name"]}
- Email: {lead_data["email"]}
- LinkedIn: {lead_data["linkedin_url"]}

LinkedIn profile:
{json.dumps(profile, indent=2)}

Return ONLY the JSON object with "subject" and "body".
"""
    response = client.models.generate_content(
        model=model,
        contents=user_message,
        config=config,
    )
    return _extract_text(response)


def draft_email(lead_data: dict, profile: dict) -> dict:
    """
    Two-step flow: (1) Research with Flash + Search, (2) Draft with Pro (no tools).
    If Pro returns empty, fallback to Flash for the draft. Returns {"subject": str, "body": str}.
    """
    research_summary = _research_lead(lead_data, profile)

    raw = _draft_from_context(
        research_summary, lead_data, profile, MODEL_DRAFT
    )
    if not raw:
        raw = _draft_from_context(
            research_summary, lead_data, profile, MODEL_DRAFT_FALLBACK
        )
    if not raw:
        raise ValueError("Gemini returned no text from both draft models")

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    return _parse_json(raw)
