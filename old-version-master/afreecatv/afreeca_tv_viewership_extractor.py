import re
import os
import datetime
import traceback
import pandas as pd

from sa_package.mydatabase import get_engine
from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.convert.time_format import convert_sec_to_hhmmss_format
from sa_package.platform.afreecatv import AfreecaTVDriver, get_bj_nick

from afreecatv_lib import update_bj_viewership_data

from slack_message import slack_message

from dotenv import load_dotenv
load_dotenv(".env")

afreeca_sh_id = os.getenv("AFREECA_SH_ID")
json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")

postgresql_engine = get_engine(
        dbms="postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database='sandbox'
    )


def extract_afreeca_viewer_data(start_date, end_date):

    afreeca_spreadsheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=afreeca_sh_id)

    bj_df = afreeca_spreadsheet.get_df_from_gspread(
        worksheet_name="여기에 적어주세요",
        read_range="A3:A"
    )
    
    if len(bj_df) == 0:
        return

    driver = AfreecaTVDriver(login_id=os.getenv("AFREECA_LOGIN_ID"), login_pwd=os.getenv("AFREECA_LOGIN_PWD"))

    for idx in bj_df.index:
        
        try:
        
            bj_id = bj_df.loc[idx, 'URL'].split("/")[-1]
            bj_nick = get_bj_nick(bj_id, driver)
            
            #특수문자 제외한 BJ NICK
            word_only_regex = re.compile("[\d\w]")
            cleanse_bj_nick = word_only_regex.findall(bj_nick)
            cleanse_bj_nick = "".join(cleanse_bj_nick)

            afreeca_spreadsheet.clear_values(
                worksheet_name="여기에 적어주세요",
                clear_range=f"A{idx+4}"
            )

        except Exception as e:
            continue

        try:
            afreeca_spreadsheet.duplicate_worksheet(
                worksheet_name=cleanse_bj_nick,
                dup_sheet_name="다시보기 데이터 양식"
            )

            afreeca_spreadsheet.update_worksheet_properties(
                worksheet_name=cleanse_bj_nick,
                properties={
                    "index": 3,
                    "hidden": False
                }
            )

        except Exception as e:
            afreeca_spreadsheet.update_worksheet_properties(
                worksheet_name=cleanse_bj_nick,
                properties={
                    "index": 3,
                    "hidden": False
                }
            )

            afreeca_spreadsheet.clear_values(
                worksheet_name=cleanse_bj_nick,
                clear_range="A3:J"
            )
        
        afreeca_spreadsheet.write_values_to_sh(
            worksheet_name=cleanse_bj_nick,
            start_cell="A1",
            values=[["데이터 불러오는 중"]]
        )
        

        ## 데이터 업데이트하기
        try:
            update_bj_viewership_data(bj_id, start_date, end_date, driver=driver)   
        except Exception as e:
            
            slack_message(
                title="아프리카 데이터 크롤링 실패",
                err_msg=traceback.format_exc()[:2500] if len(traceback.format_exc()) > 2500 else traceback.format_exc()
            )


        ## 불러오기
        sql = f"""SELECT * FROM afreecatv_data."afreecatv_vod_data" WHERE "bj_id" = '{bj_id}' AND "start_time" >= '{start_date.strftime('%Y-%m-%d')}' ORDER BY "start_time" DESC"""
        vod_df = pd.read_sql(sql, con=postgresql_engine)
        if len(vod_df) == 0:
            afreeca_spreadsheet.write_values_to_sh(
                values=[["최근 데이터 없음"]],
                worksheet_name=bj_nick,
                start_cell="A3"
            )

        else:
            vod_df['vod_time_hhmmss'] = vod_df['vod_time'].apply(lambda x: convert_sec_to_hhmmss_format(x))
            vod_df['link'] = vod_df['vod_id'].apply(lambda x: "https://vod.afreecatv.com/player/"+x)

            vod_df = vod_df[["link", "title", "start_time", "vod_time_hhmmss", "accv", "pccv", "chat", "category"]]   

            afreeca_spreadsheet.write_df_to_sh(
                worksheet_name=cleanse_bj_nick,
                df=vod_df,
                datetime_columns=["start_time"],
                include_header=False,
                start_cell="A3"
            )

        update_msg = f"데이터 불러오기 완료 - {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
        afreeca_spreadsheet.write_values_to_sh(
            values=[[update_msg]],
            worksheet_name=cleanse_bj_nick,
            start_cell="A1"
        )



if __name__ == "__main__":
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=40)

    extract_afreeca_viewer_data(start_date=start_date, end_date=end_date)