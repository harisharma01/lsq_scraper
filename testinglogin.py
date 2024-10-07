## import asyncio
from pyppeteer import launch
import nest_asyncio
from pyppeteer_stealth import stealth
import asyncio

# Apply nest_asyncio to allow nested event loops in Jupyter
nest_asyncio.apply()

xpath_username = '//*[@id="EmailID"]' 
xpath_password = '//*[@id="Password"]'
xpath_next = '//button[@class="btn-primary btn lsq-signin-button"]'

username = 'central.operations@collegedunia.com'
password = ' e$70ZOzq6J07'

# async def fetch_payload(request):
    
#     await asyncio.sleep(20)
    
#     if 'https://px.ads.linkedin.com/wa/' in request.url:
#         print(f'\nUrl on email entering page{request.url}\n')
#     if 'https://analytics.google.com/g/collect?' in request.url:
#         print(f'\nUrl on page before login {request.url}\n')
#     if 'https://px.ads.linkedin.com/attribution_trigger?' in request.url:
#         print(f'\nUrl on password entering page {request.url}\n')
#     if 'https://login-in21.leadsquared.com/Home/VerifyPassword' in request.url:
#         print(f'\nUrl on OTP page {request.url}\n')
#     if 'https://login-in21.leadsquared.com/Home/SendOTP' in request.url:
#         print(f'\nUrl on OTP page {request.url}\n')
#     if 'https://publisherapi.customui.leadsquared.com/api/getLeads' in request.url:
#         print(f'\nUrl after login page {request.url}\n')

async def login():
    
    chrome_path = '/usr/bin/google-chrome'
    browser = await launch({'executablePath':chrome_path,
                            'args':['--no-sandbox']})
    
    page = await browser.newPage()
    await page.goto('https://login.leadsquared.com')
    await stealth(page)
            
    # Enter username
    to_enter_username = await page.waitForXPath(xpath_username)
    await to_enter_username.type(username)
    
    #Click next
    to_next = await page.waitForXPath(xpath_next)
    await to_next.click()
    
    await asyncio.sleep(3)
    
#     Enter password
    to_enter_password = await page.waitForXPath(xpath_password)
    await to_enter_password.type(password)
    
    # Click next
    to_next = await page.waitForXPath(xpath_next)
    await to_next.click()
    
    # Get cookies
    cookies = await page.cookies()
    print(cookies, '\n')
    return page

# Run the login function and attach event listener properly
page = asyncio.get_event_loop().run_until_complete(login())