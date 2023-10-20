import datetime
import pandas as pd

from selenium.webdriver.common.by import By


from dotenv import load_dotenv
load_dotenv(".env")

sub_types = ['신규구독', '재구독', '선물']
sub_tiers = ['프라임', '티어1', '티어2', '티어3']

sub_type_infos = {}
sub_tier_infos = {}

default_url = "https://streamscharts.com"


def run(twitch_id, driver) -> pd.DataFrame():
    
    url = f"{default_url}/channels/{twitch_id}/subscribers"
        
    driver.get(url)

    sections = driver.find_elements(By.CSS_SELECTOR, 'section')
    if sections and len(sections) >= 4:

        # 구독 타입
        divs = sections[2].find_elements(By.CSS_SELECTOR, 'div.pt-4 > div')
        for div_idx, div in enumerate(divs):
            if div_idx >= len(sub_types):
                break

            span = div.find_elements(By.CSS_SELECTOR, 'span.text-xs')
            if len(span) > 0:
                text = span[0].text
                sub_info = text.split(': ')[1].split('(')[0].replace(' ', '')
            else:
                sub_info = 0

            sub_type_infos[sub_types[div_idx]] = sub_info

        # 구독 형태
        divs = sections[3].find_elements(By.CSS_SELECTOR, 'div.pt-4 > div')
        for div_idx, div in enumerate(divs):
            if div_idx >= len(sub_tiers):
                break

            span = div.find_elements(By.CSS_SELECTOR, 'span.text-xs')
            if len(span) > 0:
                text = span[0].text
                sub_info = text.split(': ')[1].split('(')[0].replace(' ', '')
            else:
                sub_info = 0

            sub_tier_infos[sub_tiers[div_idx]] = sub_info

    basic_info = {
        '채널ID':twitch_id,
        '날짜':datetime.date.today().strftime("%Y-%m-%d")
        # '날짜':'2023-07-04'
    }
    sub_info = {**sub_type_infos, **sub_tier_infos}

    data = {**basic_info, **sub_info}
    df = pd.DataFrame(data=data, index=[0])
    
    return df

