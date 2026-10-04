"""Send a short failure e-mail from a GitHub Actions job (only to the account owner).

Env: GMAIL_USER, GMAIL_APP_PASSWORD (GitHub secrets), NOTIFY_SUBJECT, NOTIFY_BODY (optional RUN_URL).
Does nothing when the credentials are not set.
"""
import os
import smtplib
import sys
from email.message import EmailMessage

user = os.environ.get("GMAIL_USER", "")
pw = os.environ.get("GMAIL_APP_PASSWORD", "")
if not user or not pw:
    sys.exit(0)

msg = EmailMessage()
msg["From"] = user
msg["To"] = user
msg["Subject"] = os.environ.get("NOTIFY_SUBJECT", "[taladnudbaan] job failed")
body = os.environ.get("NOTIFY_BODY", "A scheduled job failed.")
if os.environ.get("RUN_URL"):
    body += "\n\n" + os.environ["RUN_URL"]
msg.set_content(body)

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
    s.login(user, pw)
    s.send_message(msg)
print("failure e-mail sent")
