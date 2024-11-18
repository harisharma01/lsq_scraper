from bs4 import BeautifulSoup
import re

class ExtractKeys:

    def __init__(self, page, debug_logger, info_logger, critical_logger):
            self.debug_logger = debug_logger
            self.info_logger = info_logger
            self.critical_logger = critical_logger
            self.page = page

    async def secretkeys(self):

        html = await self.page.content()
        soup = BeautifulSoup(html, features="html.parser")

        iframe = soup.find('iframe', id = 'ReportViewer')
        if iframe and iframe.has_attr('src'):
            src_url = iframe['src']

            access_key_pattern = r'accessKey=([a-zA-Z0-9$]+)'
            secret_key_pattern = r'secretKey=([a-zA-Z0-9]+)'
    
            access_key_match = re.search(access_key_pattern, src_url)
            secret_key_match = re.search(secret_key_pattern, src_url)
    
            access_key = access_key_match.group(1) if access_key_match else None
            secret_key = secret_key_match.group(1) if secret_key_match else None

            # self.info_logger.info(f'access_key = {access_key}, secret_key = {secret_key}')
            return access_key, secret_key
        
        else:
            print("Iframe with the required src attribute not found.")