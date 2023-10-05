# 트위치 ACCV 코드 고도화 작업
from selenium import webdriver
import time
from bs4 import BeautifulSoup
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import math
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from selenium.webdriver.common.by import By
import json
import csv
import pygsheets
from google.oauth2 import service_account
import platform
import datetime
from dateutil.relativedelta import relativedelta
import sys
from oauth2client.service_account import ServiceAccountCredentials
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

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

if driver is None:
        my_driver = MyChromeDriver()
        time.sleep(10)
else:
    my_driver = driver

try :
    cr_total_len = int( input('수집할 크리에이터 수는 총 몇명입니까? (최소값:50): '))
    month = int(input('수집할 월은 언제입니까?'))
except ValueError :
    cr_total_len = 50
    print('최소값인 50 명으로 수집을 진행합니다.')

url = 'https://sullygnome.com/channels/watched?language=ko'
my_driver.get(url)
time.sleep(3)
act = ActionChains(my_driver)

# More 클릭
time.sleep(3)
my_driver.find_element(By.XPATH,'//*[@id="pageSubHeaderMiddle"]/div[8]/div[1]/a').click()

# 월 클릭
time.sleep(2)
month_xpath = ('//*[@id="AdditionalRangePickerMiddle"]/div/div[2]/div['+str(month+1)+']/a')
my_driver.find_element(By.XPATH,month_xpath).click()
time.sleep(4)
# //*[@id="AdditionalRangePickerMiddle"]/div/div[4]/div[13]/a
# //*[@id="AdditionalRangePickerMiddle"]/div/div[3]/div[11]/a 22년10월
# //*[@id="AdditionalRangePickerMiddle"]/div/div[3]/div[12]/a 22년11월
# //*[@id="AdditionalRangePickerMiddle"]/div/div[3]/div[13]/a 12월
# //*[@id="AdditionalRangePickerMiddle"]/div/div[2]/div[2]/a 23년 1월


# ACCV 내림차순
my_driver.find_element(By.XPATH,'//*[@id="tblControl"]/thead/tr/th[7]').click()      
time.sleep(3)

# 100명으로 크롤링할 때
# driver.find_element(By.XPATH, '//*[@id="tblControl_length"]/label/select/option[4]')

channel_id_list = []
accv_list = []
cr_len_plus = 0

while True :
    
    cr_len = int(driver.find_element(By.CLASS_NAME, 'dataTables_info').text.split('to')[1].split('of')[0].replace(',',''))
    
    for i in range(1,51) :

        id_xpath = ('//*[@id="tblControl"]/tbody/tr['+str(i)+']/td[3]/a')
        channel_id = my_driver.find_element(By.XPATH,id_xpath).text
        channel_id_list.append(channel_id)
        
        time.sleep(1)

        accv_xpath = ('//*[@id="tblControl"]/tbody/tr['+str(i)+']/td[7]/div/div[1]/div[1]')
        accv = my_driver.find_element(By.XPATH,accv_xpath).text
        accv_list.append(accv)

        print(channel_id, accv)

    if cr_len >= cr_total_len :
        print('크롤링 끝')
        break
        
    time.sleep(5)
    next_page = my_driver.find_element(By.XPATH, '//*[@id="tblControl_next"]') 
    act.click(next_page).perform()
    time.sleep(7)

# 트위치 ACCV 시트 업로드
twitch_accv_df = pd.DataFrame()
twitch_accv_df['channel_id'] = pd.Series(channel_id_list)
twitch_accv_df['accv'] = pd.Series(accv_list)
gc = pygsheets.authorize(service_account_file='creds.json')
sheetname = '크롤링 시도'
sh = gc.open(sheetname)
wks = sh.add_worksheet(title='트위치 23.'+str(month))
wks.set_dataframe(twitch_accv_df, 'A1', index=False)

# sheetname= '트위치 accv 크롤링 (TOP 3000)'
# sh = gc.open(sheetname)
# wks = sh.add_worksheet(title='23.'+str(month))
# wks.set_dataframe(twitch_accv_df, 'A1', index=False)