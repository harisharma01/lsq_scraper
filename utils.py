import nest_asyncio
nest_asyncio.apply()
import asyncio
import sys, os
from pyppeteer.errors import NetworkError, PageError

class HelperFunctions:
    def __init__(self, page, debug_logger, info_logger, critical_logger):

        self.page = page
        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.critical_logger = critical_logger

    async def click(self, path_name):
        for i in range(3):
            try:
                await self.page.waitForXPath(path_name)
                page_ = await self.page.xpath(path_name)
                await page_[0].click()
                return

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    # async def text_box(self, path_name, input_text):
    #     max_retries = 3
    #     for attempt in range(max_retries):
    #         try:
    #             await self.page.waitForXPath(path_name, timeout=10000)  # 10 seconds timeout
    #             elements = await self.page.xpath(path_name)
    #             if not elements:
    #                 raise Exception(f"No element found with XPath: {path_name}")
                
    #             await elements[0].type(input_text)
    #             self.info_logger.info(f"Successfully entered text in element with XPath: {path_name}")
    #             return
    #         except NetworkError as e:
    #             self.debug_logger.error(f"Network error on attempt {attempt + 1}: {str(e)}")
    #             if "Inspected target navigated or closed" in str(e):
    #                 self.debug_logger.warning("Page may have navigated. Attempting to refocus.")
    #                 try:
    #                     await self.page.bringToFront()
    #                 except Exception as front_error:
    #                     self.debug_logger.error(f"Failed to bring page to front: {str(front_error)}")
    #         except PageError as e:
    #             self.debug_logger.error(f"Page error on attempt {attempt + 1}: {str(e)}")
    #         except Exception as e:
    #             self.debug_logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            
    #         if attempt < max_retries - 1:
    #             await asyncio.sleep(2)  # Wait before retrying
        
    #     raise Exception(f"Failed to enter text after {max_retries} attempts")

    async def text_box(self, path_name, input_text):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self.page.waitForXPath(path_name, timeout=10000)  # 10 seconds timeout
                elements = await self.page.xpath(path_name)
                if not elements:
                    raise Exception(f"No element found with XPath: {path_name}")
                
                await elements[0].type(input_text)
                return
            except NetworkError as e:
                self.debug_logger.error(f"Network error on attempt {attempt + 1}: {str(e)}")
                if "Inspected target navigated or closed" in str(e):
                    self.debug_logger.warning("Page may have navigated. Attempting to refocus.")
                    try:
                        await self.page.waitForNavigation({'waitUntil': 'networkidle0'})
                        self.debug_logger.info("Page navigation complete. Retrying action.")
                    except Exception as nav_error:
                        self.debug_logger.error(f"Failed to wait for navigation: {str(nav_error)}")
            except PageError as e:
                self.debug_logger.error(f"Page error on attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                self.debug_logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2)  # Wait before retrying
        
        raise Exception(f"Failed to enter text after {max_retries} attempts")
    

    async def waitfor_page_navigation(self):
        await self.page.waitForNavigation({'waitUntil': 'networkidle0'})

    # async def text_box(self, path_name, input_text):
        # for i in range(3):
            # try:
                # await self.page.waitForXPath(path_name)
                # page_ = await self.page.xpath(path_name)
                # await page_[0].type(input_text)
                # return
# 
            # except Exception as e:
                # exc_type, exc_obj, exc_tb = sys.exc_info()
                # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                # print(exc_type, fname, exc_tb.tb_lineno)
                # self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    # async def text_box(self, path_name, input_text):
    #     for i in range(3):
    #         try:
    #             await self.page.waitForXPath(path_name)
    #             page_ = await self.page.xpath(path_name)
    #             await page_.type(input_text)
    #             return
            
    #         except Exception as e:
    #             exc_type, exc_obj, exc_tb = sys.exc_info()
    #             fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #             print(exc_type, fname, exc_tb.tb_lineno)
    #             self.debug_logger.critical(f'An Exception Occured for: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
