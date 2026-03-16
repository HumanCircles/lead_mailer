import os
import time, sys
from rich.console import Console
from rich.table import Table
from tenacity import retry, stop_after_attempt, wait_exponential

from core.sheets_reader   import get_leads, mark_sent
from core.linkedin_scraper import scrape_linkedin
from core.email_drafter   import draft_email
from core.gmail_sender    import send_email

console = Console()

DELAY_BETWEEN_EMAILS = 5  # seconds — avoid Gmail rate limits
ENRICHLAYER_FRESH_DATA = os.getenv("ENRICHLAYER_FRESH_DATA", "true").lower() == "true"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20))
def process_lead(lead: dict):
    name         = lead["name"]
    email        = lead["email"]
    linkedin_url = lead["linkedin_url"]

    console.print(f"\n[cyan]Processing:[/cyan] {name} ({email})")

    # Step 1: Scrape LinkedIn
    console.print(
        f"  [yellow]→[/yellow] Scraping LinkedIn profile "
        f"(fresh_data={'ON' if ENRICHLAYER_FRESH_DATA else 'OFF'})..."
    )
    profile = scrape_linkedin(linkedin_url, fresh=ENRICHLAYER_FRESH_DATA)
    console.print(f"  [green]✓[/green] Got profile: {profile.get('headline', 'N/A')}")

    # Step 2: Draft email with Gemini
    console.print("  [yellow]→[/yellow] Running deep research + drafting email...")
    email_content = draft_email(lead, profile)
    console.print(f"  [green]✓[/green] Subject: {email_content['subject']}")

    # Step 3: Send via Gmail
    console.print("  [yellow]→[/yellow] Sending email...")
    msg_id = send_email(email, email_content["subject"], email_content["body"])
    console.print(f"  [green]✓ SENT[/green] Gmail ID: {msg_id}")

    return email_content


def main():
    console.print("[bold magenta]HireQuotient — Personalized Lead Mailer[/bold magenta]")
    console.print("[dim]Reading leads from Google Sheet...[/dim]\n")

    leads = get_leads()
    console.print(f"[bold]Found {len(leads)} pending leads[/bold]")

    results = []
    for lead in leads:
        try:
            email_content = process_lead(lead)
            mark_sent(lead["row_index"])
            results.append({"name": lead["name"], "status": "✅ SENT", 
                            "subject": email_content["subject"]})
        except Exception as e:
            console.print(f"  [red]✗ FAILED[/red] {lead['name']}: {e}")
            results.append({"name": lead["name"], "status": "❌ FAILED", "subject": str(e)})

        time.sleep(DELAY_BETWEEN_EMAILS)

    # Final report
    table = Table(title="\nCampaign Results", show_lines=True)
    table.add_column("Name", style="cyan")
    table.add_column("Status")
    table.add_column("Subject / Error", style="dim")
    for r in results:
        table.add_row(r["name"], r["status"], r["subject"])
    console.print(table)

    sent = sum(1 for r in results if "SENT" in r["status"])
    console.print(f"\n[bold green]{sent}/{len(results)} emails sent successfully[/bold green]")


if __name__ == "__main__":
    main()
