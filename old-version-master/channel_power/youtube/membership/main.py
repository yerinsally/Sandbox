import os
import gspread
from playwright.sync_api import Playwright, sync_playwright, expect
import onetimepass as otp

import pickle

import json
from datetime import datetime, timedelta
import time

from dotenv import load_dotenv
load_dotenv(".env")

gc = gspread.service_account(filename=os.environ.get("GOOGLE_JSON_KEY_PATH_IP_STRATEGY"))
sheet_url = "https://docs.google.com/spreadsheets/d/1fcwmHaLls-6pwH4hq6nKTip-ZhsM6f190TXTwo_WjSY/edit#gid=1083675111"

secretKey = os.environ.get("GOOGLE_OTP_SECRET_KEY_IP_STRATEGY") # otp를위한 secret key
gmail = os.environ.get("GOOGLE_LOGIN_ID_IP_STRATEGY")
gmail_password = os.environ.get("GOOGLE_LOGIN_PWD_IP_STRATEGY")


# os.chdir("C:/Users/SANDBOX/Documents/GitHub/sandbox_gaming_datamanagement/channel_power/youtube/membership")
# from module.make_cookies import run as make_cookies

def get_code():
    rt = otp.get_totp(secretKey)
    rt = str(rt)
    rt = rt.zfill(6)
    return rt

def make_cookies(playwright: Playwright)-> None:
    
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.youtube.com/")
    page.get_by_role("link", name="로그인").click()
    page.get_by_role("textbox", name="이메일 또는 휴대전화").fill(gmail)
    page.get_by_role("button", name="다음").click()
    page.get_by_role("textbox", name="비밀번호 입력").fill(gmail_password)
    page.get_by_role("textbox", name="비밀번호 입력").press("Enter")
    page.get_by_role("button", name="다른 방법 시도").click()
    page.get_by_role("link", name="Google OTP 앱에서 인증 코드 받기").click()
    # page.get_by_role("textbox", name="코드 입력").fill("99906")
    # page.get_by_role("textbox", name="코드 입력").press("PageDown")
    page.get_by_role("textbox", name="코드 입력").fill(get_code())
    page.get_by_role("button", name="다음").click()
    page.get_by_role("link", name="YouTube 홈").click()
    cookies = context.cookies()
    f = open('C:/Users/SANDBOX/Documents/GitHub/sandbox_gaming_datamanagement/channel_power/youtube/membership/Data/cookies.json', 'w')
    f.write(json.dumps(cookies))



def get_CR_list():
    sheet_name = "유료화_크롤링_채널목록"
    sh = gc.open_by_url(sheet_url).worksheet(sheet_name)
    data = sh.get_all_values()
    return data[1:]


def gender_stats(page, date, CID, CMS_ID):
    start_date = date
    end_date = date + timedelta(days=6)

    start_date = int(datetime(year=start_date.year, month=start_date.month, day=start_date.day).timestamp() * 1000) + 86400000
    end_date = int(datetime(year=end_date.year, month=end_date.month, day=end_date.day).timestamp() * 1000) + 172800000

    basic_stats_url = f"https://studio.youtube.com/channel/{CID}/analytics/tab-overview/period-lifetime/explore?o={CMS_ID}&entity_type=CHANNEL&entity_id={CID}&time_period={start_date}%2C{end_date}&explore_type=TABLE_AND_CHART&metric=TRANSACTION_COUNT&granularity=DAY&t_metrics=TRANSACTION_COUNT&t_metrics=TRANSACTION_EARNINGS_ALL&dimension=TRANSACTION_BUSINESS_MODEL&o_column=TRANSACTION_COUNT&o_direction=ANALYTICS_ORDER_DIRECTION_DESC"

    page.goto(basic_stats_url)

    list = page.wait_for_selector('.style-scope.yta-explore-table').inner_text().splitlines()
    list = list[6:]

    result = []
    for i in range(0, len(list), 5):
        list_final = list[i:i + 5]
        list_final = [list_final[0], list_final[1], list_final[3]]
        result.append([str(date), CID] + list_final)

    return result



def run(date):

    with sync_playwright() as playwright:
        make_cookies(playwright)

    date = datetime(year=int(date.split("-")[0]), month=int(date.split("-")[1]), day=int(date.split("-")[2]))
    target_date = date + timedelta(days=6)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False, timeout=300000)
        context = browser.new_context()
        cookie_file = open('C:/Users/SANDBOX/Documents/GitHub/sandbox_gaming_datamanagement/channel_power/youtube/membership/Data/cookies.json')
        cookies = json.load(cookie_file)
        context.add_cookies(cookies)
        page = context.new_page()

        CR_list = get_CR_list()

        for i in CR_list:
            time.sleep(0.5)
            CID = i[0]
            CMS_ID = i[1]

            try:
                data = gender_stats(page, date, CID, CMS_ID)
            except:
                pass
            else:
                print(data)
                if data != []:
                    data = [[target_date.strftime("%Y-%m-%d")] + data[0][1:]]
                    print(data)
                    sheet_name = "유튜브 유료화 RAW"
                    sh = gc.open_by_url(sheet_url).worksheet(sheet_name)
                    sh.append_rows(data, value_input_option='USER_ENTERED')

        context.close()
        browser.close()
