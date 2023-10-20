import random
import time
import datetime
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

class MyChromeDriver(webdriver.Chrome):

    def __init__(self, headless=False, maximize=True):

        options = webdriver.ChromeOptions()
        if headless:    
            options.add_argument('--headless')   
        
        webdriver.Chrome.__init__(self, service=ChromeService(ChromeDriverManager().install()), options=options)

        if maximize:
            self.maximize_window()


def run(twitch_id, start, driver=None) -> pd.DataFrame():

    df = pd.DataFrame()

    need_to_close = False
    if driver is None: 
        driver = MyChromeDriver()
        driver.get("https://twitchtracker.com")
        need_to_close = True
    
    url = f"https://twitchtracker.com/{twitch_id}/statistics"
    driver.get(url)
    driver.implicitly_wait(random.randint(5, 10))
    driver.execute_script("document.body.style.zoom='100%'")
    driver.execute_script("window.scrollTo(0, 400)")


    # 추가 데이터 선택
    for unselected in driver.find_elements(By.CSS_SELECTOR, 'div.pge.unselected'):
        if unselected.find_element(By.CSS_SELECTOR, 'div > div.pge-t').text in ['Hours Watched', 'Peak viewers']:
            unselected.click()
            time.sleep(0.5)
    
    a = ActionChains(driver)

    try:
        WebDriverWait(driver, timeout=10).until(lambda x: x.find_element(By.CSS_SELECTOR, 'svg > g.highcharts-series-group > g.highcharts-series.highcharts-series-4.highcharts-column-series.highcharts-tracker > rect'))
    except:
        if need_to_close:
            driver.close()
        return None

    for rect in driver.find_elements(By.CSS_SELECTOR, 'svg > g.highcharts-series-group > g.highcharts-series.highcharts-series-4.highcharts-column-series.highcharts-tracker > rect')[::-1]:
        a.move_to_element(rect).perform()
        time.sleep(0.5)
        a.move_to_element(rect).perform()
        tooltip = driver.find_element(By.CSS_SELECTOR, 'div.highcharts-label.highcharts-tooltip.highcharts-color-undefined')

        tds = tooltip.find_elements(By.CSS_SELECTOR, 'span > table > tbody > tr > td')
        target_date = datetime.datetime.strptime(tds[0].text.replace(' ',''), "%d/%b/%Y")

        if target_date.date() < start-datetime.timedelta(days=1):
            break
    
        data = {}
        data['채널ID'] = twitch_id
        data['날짜'] = target_date

        for td_idx, td in enumerate(tds[1:]):
            if td_idx == len(tds)-2:
                pass
            elif td_idx % 2 == 0:
                column_name = td.text
            else:
                data[column_name] = int(td.text.replace(' ', ''))
        df = pd.concat([df, pd.DataFrame(data, index=[0])], ignore_index=True)
    
    for col in ['Followers', 'Hours Streamed', 'Hours Watched']:
        df[col+'_'] = df[col].shift(-1)
        df[col] = df[col]-df[col+'_']

    df = df.rename(columns={
        'Followers':'followers_gained', 
        'Hours Streamed':'hours_streamed',
        'Hours Watched':'hours_watched',
        'Average Viewers':'average_viewers',
        'Peak Viewers':'peak_viewers'
    })
    df = df.drop(columns=['Followers_', 'Hours Streamed_', 'Hours Watched_'])
    df = df[:-1]

    if need_to_close:
        driver.close()

    return df