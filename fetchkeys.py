import json
from pyppeteer.network_manager import Request, Response
import asyncio
import nest_asyncio
nest_asyncio.apply()
import sys, os

class FetchParams:
    def __init__(self, page, debug_logger, info_logger, critical_logger):

        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger
        self.page = page
        self.post_params = []

    async def handle_network_event(self, event):
        try:
            if isinstance(event, Request):
                if 'https://publisherapi.customui.leadsquared.com/api/getLeads/?' in event.url:
                    self.post_params.append({
                        'url': event.url
                    })
                    print(f"Event URL = {event.url}")
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
            return None, None

    async def event_listeners(self):
        self.page.on('response', lambda res: asyncio.create_task(self.handle_network_event(res)))
        # self.page.on('request', lambda req: asyncio.create_task(self.handle_network_event(req)))

    async def authentication_params(self):
        try:
            if self.post_params:
                print(self.post_params)
            else:
                print('self.post_params is empty')
                # return None, None
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
            return None, None