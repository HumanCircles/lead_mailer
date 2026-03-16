import os
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

SYSTEM_PROMPT = """
You are an expert B2B sales copywriter for HireQuotient — an AI-powered hiring platform 
that helps companies assess and hire top talent faster with automated skill assessments.

Your task: Write a hyper-personalized cold outreach email to the lead below.

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


def draft_email(lead_data: dict, profile: dict) -> dict:
    """
    Uses Gemini 2.5 Pro with Google Search Grounding to:
    1. Research the person further on the web
    2. Draft a hyper-personalized email
    Returns {"subject": str, "body": str}
    """
    grounding_tool = types.Tool(google_search=types.GoogleSearch())
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=[grounding_tool],
    )

    user_message = f"""
Lead Details:
- Name: {lead_data["name"]}
- Email: {lead_data["email"]}
- LinkedIn: {lead_data["linkedin_url"]}

LinkedIn Profile Data:
{json.dumps(profile, indent=2)}

Instructions:
1. Use Google Search to find any recent news, articles, or posts about {lead_data["name"]} 
   or their current company: {profile.get("current_role", "")}
2. Use those insights to make the email feel like you did your homework
3. Return ONLY the JSON object with subject and body
"""

    response = client.models.generate_content(
        model="gemini-2.5-pro-preview-03-25",
        contents=user_message,
        config=config,
    )
    raw = response.text.strip()

    # Strip markdown code fences if model adds them
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    return json.loads(raw.strip())
