from pyppeteer import launch
from pyppeteer_stealth import stealth
import sys, os

class ChromeBrowser:
    def __init__(self, debug_logger, info_logger, critical_logger):
        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger
        self.url = os.getenv('LSQ_URL')

    async def chrome_browser(self):
        try:
            chrome_path = '/usr/bin/google-chrome'
            self.browser = await launch({'headless': True,
                                         'executablePath' : chrome_path,
                                         'args':['--no-sandbox']}) #by default headless=True
            
            self.page = await self.browser.newPage()
            print(self.page)
            await self.page.goto(self.url)
            await stealth(self.page)
            return self.page

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured in launching browser: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    async def close_browser_session(self):
        await self.browser.close()