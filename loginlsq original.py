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
        self.info_logger.info('How many time is login function running')

        try:
            await self.hf.text_box(self.xp.fill_email_id_xp, self.email)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                

        try:
            await self.hf.click(self.xp.click_login_button_xp)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                

        # Input password
        try:
            await self.hf.text_box(self.xp.fill_password_xp, self.password)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                

        try:
            await self.hf.click(self.xp.click_login_button_xp)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                

        await asyncio.sleep(30)

        #fetching the otp from the central ops email
        mail = self.fetch_opt.connect_to_gmail_imap()

        fetched_otp = self.fetch_opt.get_latest_otp(mail)
        print(f'Fetched OTP = {fetched_otp}')

        try:
            await self.hf.text_box(self.xp.full_otp_container, fetched_otp)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                

        await asyncio.sleep(1)
        
        try:
            await self.hf.click(self.xp.click_login_button_xp)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')                                


        return self.page