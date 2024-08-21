import json
import aiohttp
import aiohttp_socks
import asyncio
import pandas as pd
import math
import time
import sys, os
from datetime import datetime
from dotenv import load_dotenv
import requests
import numpy as np

class ScraperCall:
    def __init__(self, debug_logger, info_logger, payload, headers) -> None:
        self.debug_logger = debug_logger
        self.info_logger = info_logger

        self.access_key = os.getenv('ACCESS_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        self.url = os.getenv('URL')
        self.leads_api = os.getenv('LEADS_API')
        self.client_api = os.getenv('CLIENT_API')
        self.new_user_thing = os.getenv('NEWUSER')
        self.payload = payload
        self.headers = headers

    def scrap_colleges(self, headers):
        try:
            self.get_colleges_api = f'{self.url}/api/{self.client_api}/?accessKey={self.access_key}&secretKey={self.secret_key}&isNewUser={self.new_user_thing}'
            response = requests.get(self.get_colleges_api, headers=headers)
            json_res = response.json()
            cid_cname = [[value['CustomerId'], value['CustomerName']] for value in json_res['Data']]
            return cid_cname
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for fetching Colleges ID and Names: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    async def fetch_leads(self, leads_api_url, payload_page_num, college_name, payload_data, semaphore, delay, proxy_list):
        proxy = proxy_list[np.random.randint(0, len(proxy_list))]
        connector = aiohttp_socks.ProxyConnector.from_url(proxy)
        await asyncio.sleep(delay)
        payload_data['Paging']['PageIndex'] = payload_page_num
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with semaphore, session.post(leads_api_url, headers=self.headers, data=json.dumps(payload_data)) as response:
                    self.info_logger.info(response.status)
                    if response.status == 200:
                        response_data = await response.text()
                        data_list = json.loads(response_data)
                        return data_list
                    else:
                        return None
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    async def main(self, cd_cust_id, college_name, start_date, end_date, semaphore, proxy_list):
        tasks = []
        try:
            self.leads_api = f'{self.url}/api/{self.leads_api}/?accessKey={self.access_key}&secretKey={self.secret_key}&customerId={cd_cust_id}'
            self.payload['Filters']['DateRangeFrom'] = start_date
            self.payload['Filters']['DateRangeTo'] = end_date
            self.payload['Paging']['PageSize'] = 100

            # Calculate total pages here by sending a single page request and extracting total leads
            async with aiohttp.ClientSession() as session:
                initial_payload = self.payload.copy()
                initial_payload['Paging'] = {"PageIndex": 1, "PageSize": 1}
                async with session.post(self.leads_api, headers=self.headers, data=json.dumps(initial_payload)) as response:
                    if response.status == 200:
                        response_data = await response.text()
                        data_list = json.loads(response_data)
                        if data_list and 'Info' in data_list and 'TotalLeads' in data_list['Info']:
                            summary_data = data_list['Info']
                            total_leads = data_list['Info']['TotalLeads']
                        else:
                            self.debug_logger.critical(f'Failed initial request for {college_name} with status {response.status}')
                            return None, None, None
                    else:
                        self.debug_logger.critical(f'Failed initial request for {college_name} with status {response.status}')
                        return None, None, None

                total_pages = math.floor(total_leads / self.payload['Paging']['PageSize'])
                start_page = 1
                delay = 5

                for payload_page_num in range(start_page, total_pages):
                    tasks.append(self.fetch_leads(self.leads_api, payload_page_num, college_name, self.payload, semaphore, delay, proxy_list))
                leads_data_per_page = await asyncio.gather(*tasks)
            return leads_data_per_page, summary_data, total_leads
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')