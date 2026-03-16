import os, base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _get_gmail_service():
    creds = None
    token_file = os.getenv("GMAIL_TOKEN_FILE", "gmail_token.json")
    creds_file = os.getenv("GMAIL_OAUTH_CREDENTIALS", "gmail_credentials.json")

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def send_email(to_email: str, subject: str, body: str) -> str:
    """
    Sends an HTML email via Gmail API.
    Returns the Gmail message ID on success.
    """
    service = _get_gmail_service()
    sender = os.getenv("SENDER_EMAIL")

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = to_email

    # Plain text fallback + HTML version
    plain = MIMEText(body, "plain")
    html = MIMEText(
        f"<div style='font-family:Arial,sans-serif;font-size:15px;line-height:1.6'>"
        f"{body.replace(chr(10), '<br>')}</div>",
        "html",
    )
    message.attach(plain)
    message.attach(html)

    encoded = base64.urlsafe_b64encode(message.as_bytes()).decode()
    result = service.users().messages().send(userId="me", body={"raw": encoded}).execute()
    return result["id"]

