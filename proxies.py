import requests
import json
import os, sys

class PROXY_LIST:
    def __init__(self, info_logger, debug_logger) -> None:
        self.info_logger = info_logger
        self.debug_logger = debug_logger

    def get_proxies(self):
        
        try:
            proxy_url = 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc'
            response  = requests.get(proxy_url)
            self.info_logger.info(f'PROXY status:{response.status_code}')
            proxies = json.loads(response.text)
            proxy_list = [''.join(proxy['protocols'][0]) + '://' + proxy['ip'] + ':' + proxy['port'] for proxy in proxies['data']]
            return proxy_list
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured while fetching proxies: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')