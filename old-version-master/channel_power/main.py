import sys, getopt, json, os, time , datetime, random, argparse, traceback

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine, upload_df
from sa_package.my_selenium.webdriver import MyChromeDriver

from sa_package.platform.afreecatv import AfreecaTVDriver

from youtube.membership.main import run as membership

from twitch.twitch_stat import run as tstat
from twitch.twitch_subs import run as tsubs
from afreecatv.afreecatv_stat import run as astat

from cr_weekly.live_update import run as lupdate
from cr_weekly.yt_follower_update import run as yfupdate

from slack_message import slack_message

from dotenv import load_dotenv
load_dotenv(".env")

json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
slack_token = os.getenv("SLACK_TOKEN")

postgresql_engine = get_engine(
        dbms="postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database="sandbox"
    )


parser = argparse.ArgumentParser(description="Test opetions")

parser.add_argument("--type", required=True, help="operation type")
parser.add_argument("--chs", required=False, help="channel id list")
parser.add_argument("--ch", required=False, help="channel id")
parser.add_argument("--start", required=False, help="crawling start date / YYYY-MM-DD 형태")
parser.add_argument("--date", required=False, help="membership date / YYYY-MM-DD 형태")

parser.add_argument("--vid", required=False, help="video id")
parser.add_argument(
    "--vids", required=False, help="video ids, vids vid1,vid2,vid3 format"
)
parser.add_argument(
    "--sec",
    required=False,
    help="time format convert to sec",
    action="store_true",
)

args = parser.parse_args()


def get_ch_id_list(platform):
    ch_follower_sh_id = os.getenv("CH_FOLLOWER_SH_ID")
    
    sheet = GspreadConnection(json_key_path).get_spreadsheet(ch_follower_sh_id)

    twitch_df = sheet.get_df_from_gspread(worksheet_name="채널목록", read_range="A1:E")
    twitch_df = twitch_df.loc[twitch_df['플랫폼'] == platform]

    return twitch_df['ID'].values.tolist()


def get_twitchtracker_driver():

    driver = MyChromeDriver()

    # 최근 30일로 날짜 설정
    driver.get('https://twitchtracker.com/handongsuk/statistics')
    WebDriverWait(driver, 10).until(lambda x: x.find_element(By.CSS_SELECTOR, '#timeframe'))
    driver.find_element(By.CSS_SELECTOR, '#timeframe').click()
    time.sleep(3)
    driver.execute_script(
            'return arguments[0].shadowRoot', 
            driver.find_element(By.CSS_SELECTOR, '#timeframe > span.easepick-wrapper')
        ).find_element(By.CSS_SELECTOR, 'div > main > div > button:nth-child(2)').click() # 최근 30일
    
    return driver


def get_streamscharts_driver():

    file_path = os.getenv("SCHARTS_COOKIE_PATH")
    with open(file_path, 'r') as file: 
        cookies = json.load(file).get('cookies')
    
    default_url = "https://streamscharts.com"
    driver = MyChromeDriver()
    driver.get(default_url)

    cookies = cookies.split('; ')
    print(cookies)

    saved_cookies = []
    for cookie in cookies:
        k = cookie.split('=')[0]
        v = "".join(cookie.split('=')[1:])

        saved_cookies.append({
            'domain':'.streamscharts.com',
            'name':k,
            'value':v
        })
    time.sleep(1)

    driver.get(default_url)

    for cookie in saved_cookies:
        driver.add_cookie(cookie)

    print(driver.get_cookies())
    driver.get(default_url)
    WebDriverWait(driver, timeout=30).until(lambda x: x.find_element(By.CSS_SELECTOR, 'body > nav > div > div.menu-logo.sm-max\:hidden > a > div > img.hidden-light.max-w-none'))

    return driver


def update_youtube_membership():

    if args.date:
        membership_date = args.date
    else:
        # 그 전주 수요일
        today = datetime.date.today()
        membership_date = today - datetime.timedelta(days=5+today.weekday())
        membership_date = membership_date.strftime("%Y-%m-%d")

    try:
        membership(membership_date)

    except:
        slack_message(
            title="멤버십 스탯 업데이트 실패",
            err_msg=traceback.format_exc()
        )


def update_daily_twitch_stat():
    
    chs = get_ch_id_list('트위치')

    if args.chs:
        chs = args.chs
    if args.ch:
        chs = [args.ch]

    start = datetime.date.today() - datetime.timedelta(days=8)
    if args.start:
        start = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
    
    tt_driver = get_twitchtracker_driver()
    
    stat_result = {'성공':[], '실패':[]}    
    
    for ch in chs:

        try:
            stat_df = tstat(ch, start, tt_driver)
            upload_df(
                dbms='postgres', engine=postgresql_engine, df=stat_df, 
                pk_list=['날짜', '채널ID'], scheme='channel_power', 
                table='twitch_platform_power')
            
            stat_result['성공'].append(ch)
            
        except Exception as e:
            print(e)
            stat_result['실패'].append(ch)

        
        time.sleep(random.randint(10,15))

    tt_driver.close()

    
    slack_message(
            title="트위치 스탯 업데이트 성공",
            err_msg=''
        )


def update_daily_twitch_subs():
    
    chs = get_ch_id_list('트위치')

    if args.chs:
        chs = args.chs
    if args.ch:
        chs = [args.ch]

    sc_driver = get_streamscharts_driver()

    subs_result = {'성공':[], '실패':[]}  
    
    for ch in chs:

        try:
            subs_df = tsubs(ch, sc_driver)
            upload_df(
                dbms='postgres', engine=postgresql_engine, df=subs_df, 
                pk_list=['채널ID', '날짜'], scheme='channel_power', 
                table='twitch_subscription')

            subs_result['성공'].append(ch)
            

        except Exception as e:
            print(e)
            subs_result['실패'].append(ch)

        
        time.sleep(random.randint(10,15))

    sc_driver.close()

    
    slack_message(
            title="트위치 구독 업데이트 성공",
            err_msg=''
        )   


def update_daily_afreeca_stat():
    
    chs = get_ch_id_list('아프리카')

    if args.chs:
        chs = args.chs
    if args.ch:
        chs = [args.ch]

    start = datetime.date.today() - datetime.timedelta(days=8)
    if args.start:
        start = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()

    result = {'성공':[], '실패':[]}    
    for ch in chs:

        try:
            df = astat(ch, start)
            upload_df(
            dbms='postgres', engine=postgresql_engine, df=df, pk_list=['채널ID', '날짜'], 
            scheme='channel_power', table='afreecatv_platform_power')
            
            result['성공'].append(ch)

        except Exception as e:
            print(e)
            result['실패'].append(ch)

    slack_message(
            title="아프리카 스탯 업데이트 성공",
            err_msg=''
        )   


def update_weekly_live_sheet():
    lupdate()


def update_weekly_yt_follower_sheet():
    yfupdate()


def update_monthly_live_sheet():
    pass

def main():
    
    if args.type == "membership":
        update_youtube_membership()

    elif args.type == "tstat":
        update_daily_twitch_stat()

    elif args.type == "tsubs":
        update_daily_twitch_subs()

    elif args.type == "astat":
        update_daily_afreeca_stat()

    elif args.type == "weekly":
        update_weekly_live_sheet()
        update_weekly_yt_follower_sheet()

    elif args.type == "month":
        update_monthly_live_sheet()
    else:
        print("No type argument")


if __name__ == "__main__":
    main()
