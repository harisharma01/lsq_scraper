import imaplib
import email
from email.header import decode_header
import re
from datetime import datetime, timedelta
import os

class FetchOTP:
    def __init__(self, debug_logger, info_logger, critical_logger) -> None:
        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger

        self.c_ops_id = os.getenv('EMAIL_ID')
        self.c_ops_pass = os.getenv('EMAIL_PASSWORD')

    def connect_to_gmail_imap(self):
        imap_url = 'imap.gmail.com'
        try:
            mail = imaplib.IMAP4_SSL(imap_url)
            mail.login(self.c_ops_id, self.c_ops_pass)
            mail.select('inbox')  # Connect to the inbox.
            return mail
        except Exception as e:
            # self.debug_logger.debug("Connection failed: {}".format(e))
            raise

    def get_latest_otp(self, mail):
        # Search for emails from the last 24 hours
        date = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
        search_criteria = f'(FROM "noreply@leadsquared.com" SUBJECT "LeadSquared - One Time Password" SINCE "{date}")'
        
        resp, items = mail.search(None, search_criteria)
        items = items[0].split()

        if not items:
            self.info_logger.info("No recent OTP emails found.")
            return None

        # Get the latest email
        latest_email_id = items[-1]
        resp, data = mail.fetch(latest_email_id, "(RFC822)")
        email_body = data[0][1]
        email_message = email.message_from_bytes(email_body)

        # Extract OTP from email body
        for part in email_message.walk():
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True).decode()
                print(body)
                otp_match = re.search(r'Your OTP for LeadSquared application is <b>(\d+)</b>', body)
                if otp_match:
                    otp = otp_match.group(1)
                    return otp

        self.info_logger.info("No OTP found in the latest email.")
        return None