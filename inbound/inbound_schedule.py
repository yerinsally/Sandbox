# 해당 파이썬 파일 직접 실행
# python inbound_schedule.py

import subprocess
import schedule
import time

def run_notebook():
    # Jupyter Notebook 파일 실행
    subprocess.run(['jupyter', 'nbconvert', '--execute', 'inbound_analysis_raw_ver3.ipynb'])

# 매주 월요일 15:00에 실행하도록 스케줄링
schedule.every().monday.at("15:00").do(run_notebook)

while True:
    schedule.run_pending()
    time.sleep(1)