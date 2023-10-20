import os
import time
import datetime
import pandas as pd
import numpy as np

from sa_package.platform.afreecatv import get_vod_list_month_range, get_vod_viewer_info
from sa_package.mydatabase import get_engine, upload_df

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


def update_bj_viewership_data(bj_id, start_date, end_date, driver):

    """
    crawl certain BJ's afreeca tv vod viewership data
    for certain period (start_date ~ end_date)
    except already crawled ones

    Parameters
    ----------
    bj_id: str

    start_date: `datetime.date`
    
    end_date: `datetime.date`
    """

    start_month = f"{start_date.year}{start_date.month:02}"
    end_month = f"{end_date.year}{end_date.month:02}"


    # 특정 기간 동안의 다시보기 리스트 BJ 방송국에서 읽어오기
    vod_df = get_vod_list_month_range(driver=driver, bj_id=bj_id, start_month=start_month, end_month=end_month)
    vod_df['date_'] = pd.to_datetime(vod_df['date'])
    vod_df = vod_df.loc[vod_df['date_'] >= datetime.datetime(start_date.year, start_date.month, start_date.day)]

    print(vod_df)

    if len(vod_df) > 0:
        
        # 데이터베이스에서 뷰어십 데이터 불러오기
        sql = f"""SELECT * FROM afreecatv_data."afreecatv_vod_data" WHERE bj_id = '{bj_id}';"""
        sql_df = pd.read_sql(sql, con=postgresql_engine)
        
        # 뷰어십 데이터 저장되어 있지 않은 케이스 (새로 읽어와야 하는 케이스)
        sql_vod_list = sql_df["vod_id"].values.tolist()
        new_vod_df = vod_df.loc[~vod_df["vod_id"].isin(sql_vod_list)]

        if len(new_vod_df) > 0:

            print(new_vod_df)

            # ACCV, PCCV, CHAT 데이터 불러오기
            for vod_idx in new_vod_df.index:
                vod_id = new_vod_df.loc[vod_idx, 'vod_id']
                vod_time = new_vod_df.loc[vod_idx, 'vod_time']

                vod_info_df = get_vod_viewer_info(vod_id, vod_time, driver)

                new_vod_df.loc[vod_idx, "accv"] = None if vod_info_df.loc[0, "accv"] == "데이터 없음" else vod_info_df.loc[0, "accv"]
                new_vod_df.loc[vod_idx, "pccv"] = None if vod_info_df.loc[0, "pccv"] == "데이터 없음" else vod_info_df.loc[0, "pccv"]
                new_vod_df.loc[vod_idx, "chat"] = None if vod_info_df.loc[0, "chat"] == "데이터 없음" else vod_info_df.loc[0, "chat"]

                if vod_info_df.loc[0, "방송시간"] is np.nan:
                    new_vod_df.loc[vod_idx, "start_time"] = None
                else:
                    new_vod_df.loc[vod_idx, "start_time"] = vod_info_df.loc[0, "방송시간"].split(" ~ ")[0]
                
                if vod_info_df.loc[0, "카테고리"] is np.nan:
                    new_vod_df.loc[vod_idx, "category"] = None
                else:
                    new_vod_df.loc[vod_idx, "category"] = vod_info_df.loc[0, "카테고리"]

                # 새로 읽어온 값 데이터베이스에 저장
                to_be_upload_df = new_vod_df.loc[[vod_idx]]

                to_be_upload_df["bj_id"] = bj_id
                to_be_upload_df = to_be_upload_df.drop(columns=["date", "date_"])
                to_be_upload_df = to_be_upload_df.replace("", None)
                upload_df(
                    dbms="postgres",
                    engine=postgresql_engine,
                    df=to_be_upload_df,
                    pk_list=["vod_id"],
                    scheme="afreecatv_data",
                    table="afreecatv_vod_data"
                )

                    


    # 못 읽어온 값 다시 확인하기
    for _ in range(2):
        sql = f"""SELECT * FROM afreecatv_data."afreecatv_vod_data"
                    WHERE "bj_id" = '{bj_id}' 
                        AND "start_time" >= '{start_date.strftime('%Y-%m-%d')}' 
                        AND "accv" IS NULL 
                    ORDER BY "start_time" DESC"""
        vod_df = pd.read_sql(sql, con=postgresql_engine)

        if len(vod_df) > 0:
            for vod_idx in vod_df.index:
                vod_id = vod_df.loc[vod_idx, 'vod_id']
                vod_time = vod_df.loc[vod_idx, 'vod_time']

                vod_info_df = get_vod_viewer_info(vod_id, vod_time, driver)

                vod_df.loc[vod_idx, "accv"] = None if vod_info_df.loc[0, "accv"] == "데이터 없음" else vod_info_df.loc[0, "accv"]
                vod_df.loc[vod_idx, "pccv"] = None if vod_info_df.loc[0, "pccv"] == "데이터 없음" else vod_info_df.loc[0, "pccv"]
                vod_df.loc[vod_idx, "chat"] = None if vod_info_df.loc[0, "chat"] == "데이터 없음" else vod_info_df.loc[0, "chat"]


                # 새로 읽어온 값 데이터베이스에 저장
                to_be_upload_df = vod_df.loc[[vod_idx]]

                to_be_upload_df["bj_id"] = bj_id
                to_be_upload_df = to_be_upload_df.replace("", None)
                upload_df(
                        dbms="postgres",
                        engine=postgresql_engine,
                        df=to_be_upload_df,
                        pk_list=["vod_id"],
                        scheme="afreecatv_data",
                        table="afreecatv_vod_data"
                    )

        else:
            break