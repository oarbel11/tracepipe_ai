"""
Email Sender
============
Sends peer review reports and alerts via SMTP.

Configuration is read from config/config.yml under the 'email' key.

Example config.yml:

    email:
      smtp_host: "smtp.gmail.com"
      smtp_port: 587
      smtp_user: "you@gmail.com"
      smtp_password: "your-app-password"
      from_address: "you@gmail.com"
      use_tls: true
"""

import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional, Union

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _load_email_config() -> dict:
    """Load email settings from config/config.yml."""
    try:
        import yaml
        config_path = PROJECT_ROOT / "config" / "config.yml"
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("email", {})
    except Exception as e:
        raise RuntimeError(f"Could not load email config: {e}")


class EmailSender:
    """Send emails via SMTP using settings from config.yml."""

    def __init__(self, config: Optional[dict] = None):
        self.config = config or _load_email_config()
        self._validate_config()

    def _validate_config(self):
        required = ["smtp_host", "smtp_user", "smtp_password", "from_address"]
        missing = [k for k in required if not self.config.get(k)]
        if missing:
            raise ValueError(
                f"Email config is missing required fields: {', '.join(missing)}. "
                "Please set them in config/config.yml under the 'email' key."
            )

    def send(
        self,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
    ) -> None:
        """
        Send an email.

        Args:
            to: Recipient address or list of addresses.
            subject: Email subject line.
            body: Plain-text body.
            html_body: Optional HTML body (sent as alternative part).
        """
        recipients = [to] if isinstance(to, str) else to

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.config["from_address"]
        msg["To"] = ", ".join(recipients)

        msg.attach(MIMEText(body, "plain"))
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        smtp_host = self.config["smtp_host"]
        smtp_port = int(self.config.get("smtp_port", 587))
        use_tls = self.config.get("use_tls", True)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if use_tls:
                server.ehlo()
                server.starttls()
                server.ehlo()
            server.login(self.config["smtp_user"], self.config["smtp_password"])
            server.sendmail(self.config["from_address"], recipients, msg.as_string())

    def send_peer_review_report(
        self,
        report_text: str,
        to: Union[str, List[str]],
        risk_level: str = "",
    ) -> None:
        """
        Send a formatted peer review report via email.

        Args:
            report_text: The formatted peer review output.
            to: Recipient address or list of addresses.
            risk_level: Risk level string ('GREEN', 'YELLOW', 'RED').
        """
        risk_tag = f"[{risk_level}] " if risk_level else ""
        subject = f"{risk_tag}Tracepipe AI - Peer Review Report"

        # Plain-text body is just the report
        body = report_text

        # Simple HTML version with colour-coded risk header
        colour_map = {"GREEN": "#2e7d32", "YELLOW": "#f57f17", "RED": "#c62828"}
        colour = colour_map.get(risk_level, "#333333")

        html_body = (
            "<html><body>"
            f"<h2 style='color:{colour};'>Peer Review Report — {risk_level or 'N/A'}</h2>"
            "<pre style='font-family:monospace;font-size:13px;'>"
            + report_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            + "</pre></body></html>"
        )

        self.send(to=to, subject=subject, body=body, html_body=html_body)
