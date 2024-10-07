import json
import aiohttp
import asyncio
import pandas as pd
import sys, os
from dotenv import load_dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class ScraperCall:
    def __init__(self, debug_logger, info_logger, critical_logger, payload, headers) -> None:
        
        load_dotenv()
        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger

        self.access_key = os.getenv('ACCESS_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        self.url = os.getenv('API_URL')
        self.leads_api = os.getenv('LEADS_API')
        self.client_api = os.getenv('CLIENT_API')
        self.new_user_thing = os.getenv('NEWUSER')
        self.payload = payload
        self.headers = headers

    def scrap_colleges(self,headers):

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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, ValueError))
    )
    async def fetch_leads(self, leads_api_url, session, payload_page_num, college_name, semaphore):
        payload_data = self.payload.copy()
        payload_data['Paging']['PageIndex'] = payload_page_num
        try:
            async with semaphore:
                async with session.post(leads_api_url, headers=self.headers, data=json.dumps(payload_data), timeout=30) as response:
                    if response.status == 200:
                        response_data = await response.text()
                        data_list = json.loads(response_data)
                        if 'DataSet' not in data_list or not isinstance(data_list['DataSet'], list):
                            raise ValueError(f"Invalid response data for page {payload_page_num}")
                        await asyncio.sleep(1)  # Add a small delay between requests
                        return data_list
                    else:
                        self.debug_logger.warning(f"Failed to fetch page {payload_page_num} for {college_name}. Status: {response.status}")
                        raise aiohttp.ClientResponseError(response.request_info, response.history, status=response.status)
        except Exception as e:
            self.debug_logger.error(f"Error fetching page {payload_page_num} for {college_name}: {str(e)}")
            raise

    async def fetch_chunk(self, leads_api_url, session, start_page, end_page, college_name, semaphore):
        tasks = [self.fetch_leads(leads_api_url, session, page, college_name, semaphore) for page in range(start_page, end_page + 1)]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def main(self, cd_cust_id, college_name, start_date, end_date, semaphore):
        self.critical_logger.info(f'\nScraping for {college_name}\n')
        try:
            leads_api_url = f'{self.url}/api/{self.leads_api}/?accessKey={self.access_key}&secretKey={self.secret_key}&customerId={cd_cust_id}'
            self.payload['Filters']['DateRangeFrom'] = start_date
            self.payload['Filters']['DateRangeTo'] = end_date 
            self.payload['Paging']['PageSize'] = 1000

            async with aiohttp.ClientSession() as session:
                # Fetch initial data to get total pages
                initial_data = await self.fetch_leads(leads_api_url, session, 1, college_name, semaphore)
                if not initial_data or 'Info' not in initial_data or 'TotalLeads' not in initial_data['Info']:
                    self.debug_logger.critical(f'Failed to get initial data for {college_name}')
                    return None, None, None

                total_leads = initial_data['Info']['TotalLeads']
                total_pages = -(-total_leads // self.payload['Paging']['PageSize'])  # Ceiling division
                
                chunk_size = 10  # Adjust this value based on your needs
                chunks = [range(i, min(i + chunk_size, total_pages + 1)) for i in range(1, total_pages + 1, chunk_size)]
                
                all_leads_data = []
                for chunk in chunks:
                    chunk_data = await self.fetch_chunk(leads_api_url, session, chunk.start, chunk.stop - 1, college_name, semaphore)
                    all_leads_data.extend(chunk_data)

                # Check for and handle any exceptions
                valid_leads_data = []
                for result in all_leads_data:
                    if isinstance(result, Exception):
                        self.debug_logger.error(f"Error occurred while fetching data for {college_name}: {str(result)}")
                    else:
                        valid_leads_data.append(result)

                # Validate that we have all the pages
                if len(valid_leads_data) != total_pages:
                    self.debug_logger.warning(f"Missing pages for {college_name}. Expected {total_pages}, got {len(valid_leads_data)}")
                
                # Combine all leads data
                combined_leads_data = []
                for data in valid_leads_data:
                    if 'DataSet' in data:
                        combined_leads_data.extend(data['DataSet'])

                # self.info_logger.info(f'\ncombined_leads_data=>\n{combined_leads_data}\n, \ninitial_data=>\n{init_data}\n, \ntotal_leads=>\n{total_leads}\n')
                return combined_leads_data, initial_data['Info'], total_leads

        except Exception as e:
            self.debug_logger.critical(f'An Exception Occurred for {college_name}: {str(e)}')
            return None, None, None