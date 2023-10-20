
import os
import pandas as pd

from sa_package.mydatabase import get_engine, upload_df

def run(bj_id:str, start) -> pd.DataFrame():

    postgresql_engine = get_engine(
        dbms="postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database="sandbox"
    )

    sql = f"""SELECT * FROM afreecatv_data."afreecatv_vod_data" WHERE "bj_id" = '{bj_id}' AND "start_time" >= to_date('{start.strftime("%Y-%m-%d")}', 'YYYY-MM-DD');"""
    vod_df = pd.read_sql(sql, con=postgresql_engine)
    vod_df["start_time"] = pd.to_datetime(vod_df["start_time"])
    vod_df["날짜"] = vod_df["start_time"].dt.date
    vod_df = vod_df.dropna(subset=["날짜"])

    accv_groupby_df = vod_df.groupby(["bj_id", "날짜"])["accv"].mean().round(-1).reset_index()
    accv_groupby_df = accv_groupby_df.rename(columns={
        "bj_id":"채널ID"
    })

    pccv_groupby_df = vod_df.groupby(["bj_id", "날짜"])["pccv"].max().round(-1).reset_index()
    pccv_groupby_df = pccv_groupby_df.rename(columns={
        "bj_id":"채널ID"
    })

    afreeca_channel_power_df = pd.merge(accv_groupby_df, pccv_groupby_df[['채널ID', '날짜', 'pccv']], on=['채널ID', '날짜'])
    afreeca_channel_power_df = afreeca_channel_power_df.rename(columns={
        'accv':'average_viewers',
        'pccv':'peak_viewers'
    })

    return afreeca_channel_power_df
    
