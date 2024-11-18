from utils import HelperFunctions
from paths import ElementsPaths
import os, sys
import time
from getmails import FetchOTP
import asyncio

class LoginNPF:
    def __init__(self, page, debug_logger, info_logger, critical_logger):

        self.page = page
        self.email = os.getenv('LSQ_USERID')
        self.password = os.getenv('LSQ_USERPASSWORD')
        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger

        self.hf = HelperFunctions(self.page, self.debug_logger, self.info_logger, self.critical_logger)
        self.xp = ElementsPaths()
        self.fetch_opt = FetchOTP(self.debug_logger, self.info_logger, self.critical_logger)

    async def login(self):
        self.info_logger.info('Login function started')
        
        async def attempt_action(action, error_message):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await action()
                    return True
                except Exception as e:
                    self.debug_logger.warning(f"{error_message} (Attempt {attempt + 1}/{max_retries}): {str(e)}")
                    if attempt == max_retries - 1:
                        self.debug_logger.error(f"Failed after {max_retries} attempts: {error_message}")
                        return False
                await asyncio.sleep(2)  # Short delay between retries
        
        self.debug_logger.debug('Next is input email')
        # Input email
        if not await attempt_action(
            lambda: self.hf.text_box(self.xp.fill_email_id_xp, self.email),
            "Failed to input email"
        ):
            return False

        # Click login button
        if not await attempt_action(
            lambda: self.hf.click(self.xp.click_login_button_xp),
            "Failed to click login button after email"
        ):
            return False

        self.debug_logger.debug('Next is input password')
        # Input password
        if not await attempt_action(
            lambda: self.hf.text_box(self.xp.fill_password_xp, self.password),
            "Failed to input password"
        ):
            return False

        # Click login button again
        if not await attempt_action(
            lambda: self.hf.click(self.xp.click_login_button_xp),
            "Failed to click login button after password"
        ):
            return False

        await asyncio.sleep(30)

        #fetching the otp from the central ops email
        mail = self.fetch_opt.connect_to_gmail_imap()

        fetched_otp = self.fetch_opt.get_latest_otp(mail)
        print(f'Fetched OTP = {fetched_otp}')

        # Input OTP
        # if not await attempt_action(
        #     lambda: self.hf.text_box(self.xp.full_otp_container, fetched_otp),
        #     "Failed to input OTP"
        # ):
        #     return False

        self.debug_logger.debug('Next is input otp')

        # Input OTP
        if not await attempt_action(
            lambda: self.hf.text_box(self.xp.otp_input_xpath, fetched_otp),
            "Failed to input OTP"
        ):
            return False

        self.debug_logger.debug('Next is clicking login')

        # Final login button click
        if not await attempt_action(
            lambda: self.hf.click(self.xp.click_login_button_xp),
            "Failed to click final login button"
        ):
            return False

        self.info_logger.info('Login successful')
        return self.page