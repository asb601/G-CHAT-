"""
Minimal email sender using SMTP (works with Gmail App Passwords).

Configure in .env:
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=youremail@gmail.com
    SMTP_PASSWORD=your_app_password

To create a Gmail App Password:
    Google Account → Security → 2-Step Verification → App passwords
"""
from __future__ import annotations

import asyncio
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.core.logger import auth_logger


def _send_sync(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    to_email: str,
    subject: str,
    html_body: str,
) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_user
    msg["To"]      = to_email
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, to_email, msg.as_string())


async def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
) -> None:
    """Non-blocking email send. Logs and swallows errors so the main flow never fails."""
    if not smtp_user or not smtp_password:
        auth_logger.warning("email_skipped", reason="smtp_not_configured", to=to_email)
        return
    try:
        await asyncio.to_thread(
            _send_sync,
            smtp_host, smtp_port, smtp_user, smtp_password,
            to_email, subject, html_body,
        )
        auth_logger.info("email_sent", to=to_email, subject=subject)
    except Exception as exc:
        auth_logger.warning("email_failed", to=to_email, error=str(exc)[:200])


def access_request_admin_email(
    user_name: str, user_email: str, message: str | None, review_url: str
) -> str:
    """Email to admin: new access request received."""
    msg_block = (
        f"<p style='margin:12px 0;color:#555;'><b>Message:</b> {message}</p>"
        if message else ""
    )
    return f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:24px;">
  <h2 style="color:#111;margin-bottom:4px;">New Access Request</h2>
  <p style="color:#555;margin-top:0;">Someone wants to join your workspace.</p>
  <div style="background:#f5f5f5;border-radius:8px;padding:16px;margin:20px 0;">
    <p style="margin:0;font-size:15px;color:#111;"><b>{user_name or user_email}</b></p>
    <p style="margin:4px 0 0;font-size:13px;color:#666;">{user_email}</p>
    {msg_block}
  </div>
  <a href="{review_url}"
     style="display:inline-block;padding:10px 20px;background:#000;color:#fff;
            text-decoration:none;border-radius:6px;font-size:14px;">
    Review Request
  </a>
  <p style="margin-top:24px;font-size:12px;color:#999;">
    You can accept or decline from the Users tab.
  </p>
</div>
"""


def access_approved_user_email(user_name: str, app_url: str) -> str:
    """Email to user: access approved."""
    return f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:24px;">
  <h2 style="color:#111;margin-bottom:4px;">Access Approved ✓</h2>
  <p style="color:#555;">Hi {user_name or 'there'},<br><br>
     Your access request has been approved. You can now sign in and start using the platform.</p>
  <a href="{app_url}"
     style="display:inline-block;padding:10px 20px;background:#000;color:#fff;
            text-decoration:none;border-radius:6px;font-size:14px;">
    Go to App
  </a>
</div>
"""


def access_declined_user_email(user_name: str) -> str:
    """Email to user: access declined."""
    return f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:24px;">
  <h2 style="color:#111;margin-bottom:4px;">Access Request Declined</h2>
  <p style="color:#555;">Hi {user_name or 'there'},<br><br>
     Your access request was reviewed and unfortunately was not approved at this time.
     Please contact your administrator if you believe this is a mistake.</p>
</div>
"""
