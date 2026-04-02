 The core problem: your playbook says not to do what you're doing

  Your MESSAGING_README.md explicitly states:

  ▎ "Not scalable by volume alone. Sending 1,000 emails with generic research produces worse results than sending 50 with sharp research."
  ▎ "Beat 1 must be written from scratch for every recipient."
  ▎ "The checklist is not optional. An email written without completing it will not get a response."

  But the LLM is generating Beat 1 without any real prospect research — no LinkedIn posts, no articles, no public statements. So every email opens with a fabricated
  observation. Recipients can feel that. That's why even the recruitagents.net emails (which have better deliverability) are getting mostly unsubscribes.

  ---
  What will actually move the number:

  1. Fix the prospect data first (biggest lever)
  The LLM has name, company, title, hcm_platform — but no actual research material. Add a LinkedIn activity field or recent news field to your CSV before sending. Even one real
   data point per person changes the email completely.

  2. Segment and slow down
  Pick your best 200 prospects (right ICP — HR Directors/VPs at companies 200–2000 employees), do real research on each, let the LLM write from actual observations. 200 sharp
  emails will outperform 16,000 generic ones.

  3. The subject line is hurting opens
  Invite for discussion | onboarding friction in K-12 HR systems — the formula is visible. Every recipient in the same space gets a nearly identical-sounding subject. Try
  removing the Invite for discussion | prefix and just leading with the hook: Onboarding friction in K-12 HR systems is more direct and less templated-looking.

  4. Add a follow-up sequence
  Your playbook even notes "follow-ups are a separate playbook." Cold email reply rates nearly double with one well-timed follow-up (3–5 days later). Right now there's none.

  5. Don't send from superchargedai.org at all (already covered — those contacts haven't been burned yet)

  ---
  Quick wins you can do today in code:
  - Add a research_note column to prospect CSV and pass it to the LLM as context for Beat 1
  - Change the subject line prompt to drop the Invite for discussion | prefix for A/B testing
  - Cap sends to 50/day per domain while warming reputation, scale up based on reply rate