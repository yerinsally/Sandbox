import os
import datetime
import pandas as pd

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine
from sa_package.platform.afreecatv import AfreecaTVDriver, get_bj_info

from afreecatv_lib import update_bj_viewership_data

from dotenv import load_dotenv
load_dotenv(".env")

afreeca_sh_id = os.getenv("AFREECA_SH_ID")
json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")

connection_engine = get_engine(
    dbms="postgresql",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"), 
    host=os.getenv("POSTGRES_HOST"), 
    port=int(os.getenv("POSTGRES_PORT")), 
    database=os.getenv("POSTGRES_DB")
)


def update_top_bj_info():

    afreeca_spreadsheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=afreeca_sh_id)
    driver = AfreecaTVDriver(login_id=os.getenv("AFREECA_LOGIN_ID"), login_pwd=os.getenv("AFREECA_LOGIN_PWD"))

    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=30)

    top_bj_df = afreeca_spreadsheet.get_df_from_gspread(
        worksheet_name="주요 BJ",
        read_range="A3:B"
    )

    for idx in top_bj_df.index:
        bj_id = top_bj_df.loc[idx, "BJ ID"]

        bj_info = get_bj_info(bj_id, driver=driver)
        bj_nick = bj_info['nick']
        fav_num = bj_info['favor_num']

        update_bj_viewership_data(bj_id=bj_id, start_date=start_date, end_date=end_date, driver=driver)

        sql = f"""SELECT * FROM afreecatv_data."afreecatv_vod_data" WHERE "bj_id" = '{bj_id}' AND "start_time" >= '{start_date.strftime('%Y-%m-%d')}' ORDER BY "start_time" DESC;"""
        vod_df = pd.read_sql(sql, con=connection_engine)
        
        
        top_bj_df.loc[idx, "방송국명"] = bj_nick
        top_bj_df.loc[idx, "애청자"] = fav_num
        # top_bj_df.loc[idx, "평균 ACCV"] = round(vod_df["accv"].mean(), -2)
        # top_bj_df.loc[idx, "평균 PCCV"] = round(vod_df["pccv"].mean(), -2)
        top_bj_df.loc[idx, "평균 ACCV"] = vod_df["accv"].mean()
        top_bj_df.loc[idx, "평균 PCCV"] = vod_df["pccv"].mean()

    print(top_bj_df)
    driver.close()

    afreeca_spreadsheet.write_df_to_sh(
        df=top_bj_df,
        worksheet_name="주요 BJ",
        include_header=True,
        start_cell="A3"
    )

    update_msg = f"업데이트 날짜 - {datetime.date.today().strftime('%Y-%m-%d')}"
    afreeca_spreadsheet.write_values_to_sh(
        values=[[update_msg]],
        worksheet_name="주요 BJ",
        start_cell="A1"
    )

if __name__ == "__main__":

    update_top_bj_info()