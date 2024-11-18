# storing data in database

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
import numpy as np
from LSQdatabase import lsqDB
from browser import ChromeBrowser
from loginlsq import LoginNPF
from fetchkeys import FetchParams
from fetchingkeys import ExtractKeys

class DataManager:
    
    def __init__(self) -> None:

        self.headers = {
    # 'Connection':'keep-live',
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
        self.critical_logger = configure_logger(logging.CRITICAL, 'critical')

        self.ss_call = ScraperCall(self.debug_logger, self.info_logger, self.critical_logger, self.payload, self.headers)
        self.prox = PROXY_LIST(self.info_logger, self.debug_logger)
        self.lsq_database_obj = lsqDB(self.debug_logger, self.info_logger)
        self.conn = self.lsq_database_obj.connect_sql()

        self.pretty_table = PrettyTable()
        self.pretty_table.field_names=['College Id','College Name','Total Leads','LeadsDf_Shape','SummaryDf_Shape','Time Taken', 'Time Taken fillDB']

    async def fetching_keys(self):

        browser = ChromeBrowser(self.debug_logger, self.info_logger, self.critical_logger)
        self.page = await browser.chrome_browser()

        lg = LoginNPF(self.page, self.debug_logger, self.info_logger, self.critical_logger)
        self.page = await lg.login()
        
        await asyncio.sleep(4)
        # fetch_key = FetchParams(self.page, self.debug_logger, self.info_logger, self.critical_logger)
        # await fetch_key.event_listeners()
        # await fetch_key.authentication_params()

        fetch_key = ExtractKeys(self.page, self.debug_logger, self.info_logger, self.critical_logger)
        self.access_key, self.secret_key = await fetch_key.secretkeys()

    def fetching_colleges(self):
        try:
            self.college_id_names = self.ss_call.scrap_colleges(self.cidnames_headers, self.access_key, self.secret_key)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
    
    async def link_redirect_looping(self):
        semaphore = asyncio.Semaphore(20)
        start_date = '2024-01-01 00:00:00' #yyyy-mm-dd hh:mm:ss
        end_date = '2024-12-31 23:59:59'

        for cd_cust_id, college_name in self.college_id_names:
            # await asyncio.sleep(2)
            start_time_for_college = time.time()
            try:
                leads_data, summary_data, total_leads = await self.ss_call.main(cd_cust_id, college_name, start_date, end_date, semaphore)
                if leads_data is None:
                    self.lsq_database_obj.db_req_data(college_id= cd_cust_id, college_name= college_name, leads_data=None, summary_data=None)  #calling data base function to send college data
                    self.time_taken_fld = self.lsq_database_obj.handle_no_leads_colleges()
                    self.lsq_database_obj.handle_no_summary_colleges()
                    self.lsq_database_obj.fill_college_id_name()

                    self.pretty_table.add_row([cd_cust_id, college_name, total_leads, leads_data, 0,np.round(time.time()-start_time_for_college,2), self.time_taken_fld])
                    # print(f'Returned None for {college_name}')
                    continue
                
                college_leads_df = pd.DataFrame(leads_data)
                college_leads_df = college_leads_df.drop_duplicates()
                summary_df = pd.json_normalize(summary_data)

                self.lsq_database_obj.db_req_data(college_id= cd_cust_id, college_name=college_name, leads_data=college_leads_df, summary_data=summary_df)
                self.lsq_database_obj.headers_check_lsq()
                self.lsq_database_obj.headers_check_summary()
                self.time_taken_fld = self.lsq_database_obj.fill_leadsdata_db()
                self.lsq_database_obj.fill_summary_data()
                self.lsq_database_obj.fill_college_id_name()

                self.pretty_table.add_row([cd_cust_id, college_name, total_leads, college_leads_df.shape, summary_df.shape, np.round(time.time()-start_time_for_college,2), self.time_taken_fld])

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

        self.info_logger.info(f'\n{self.pretty_table}')