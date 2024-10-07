import nest_asyncio
nest_asyncio.apply()
import asyncio

class HelperFunctions:

    def __init__(self, page):
        self.page = page
        
    async def click(self, path_name):
        for i in range(3):
            try:
                await self.page.waitForXPath(path_name)
                page_ = await self.page.xpath(path_name)
                await page_[0].click()
                return

            except Exception as e:
                await asyncio.sleep(1)

    async def text_box(self, path_name, input_text):
        for i in range(3):
            try:
                await self.page.waitForXPath(path_name)
                page_ = await self.page.xpath(path_name)
                await page_[0].type(input_text)
                return

            except Exception as e:
                await asyncio.sleep(1)