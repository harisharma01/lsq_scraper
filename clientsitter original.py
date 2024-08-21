import asyncio
import aiohttp
import tqdm
import pandas as pd
import numpy
import time
from prettytable import PrettyTable
import sys, os
import json
import logging
from loggers import configure_logger
from scrap import ScraperCall
from proxies import PROXY_LIST

class DataManager:
    
    def __init__(self) -> None:

        self.headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'content-type': 'application/json',
    'origin': 'https://publisherportal.customui.leadsquared.com',
    'priority': 'u=1, i',
    'referer': 'https://publisherportal.customui.leadsquared.com/',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}

        self.cidnames_headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'origin': 'https://publisherportal.customui.leadsquared.com',
    'priority': 'u=1, i',
    'referer': 'https://publisherportal.customui.leadsquared.com/',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
        
        self.payload = {
    "SearchText": "",
    "Filters": {
        "PublisherNumber": 3,
        "DateRangeFrom": None, #"2024-07-31 18:30:00"
        "DateRangeTo": None, #"2024-08-05 06:14:25"
        "LeadTypes": [],
        "IsVerifiedLead": None,
        "ApplicationStatus": [],
        "IncludeSummary": True
    },
    "Paging": {
        "PageIndex": None,
        "PageSize": None
    },
    "filtertype": 0,
    "Sorting": {
        "ColumnName": None
    }
}
        
        self.debug_logger = configure_logger(logging.DEBUG, 'debug')
        self.info_logger = configure_logger(logging.INFO, 'info')
        self.ss_call = ScraperCall(self.debug_logger, self.info_logger, self.payload, self.headers)
        self.prox = PROXY_LIST(self.info_logger, self.debug_logger)
    
    def fetching_proxies(self):
        self.proxy_list = self.prox.get_proxies()
    
    def fetching_colleges(self):
        try:
            self.college_id_names = self.ss_call.scrap_colleges(self.cidnames_headers)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
    
    async def link_redirect_looping(self):
        
        start_time_for_college = time.time()
        semaphore = asyncio.Semaphore(20)
        start_date = '2024-01-01 00:00:00' #yyyy-mm-dd hh:mm:ss
        end_date = '2024-12-31 23:59:59'

        for cd_cust_id, college_name in self.college_id_names[0:2]:
            try:
                leads_data, summary_data, total_leads = await self.ss_call.main(cd_cust_id, college_name, start_date, end_date, semaphore, self.proxy_list)
                if leads_data is None:
                    print(f'Returned None for {college_name}')
                    continue
                
                college_leads = [lead for data in leads_data if data is not None for lead in data['DataSet']]
                college_leads_df = pd.DataFrame(college_leads)
                print(f'For {college_name}=>\nTotal Leads:{total_leads} | Scraped Leads:{college_leads_df.shape[0]}')

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')