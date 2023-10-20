
import os
import datetime
import numpy as np
import pandas as pd

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine, upload_df

from dotenv import load_dotenv
load_dotenv(".env")

key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
cr_weekly_sheet = GspreadConnection(key_path).get_spreadsheet(sh_id=os.getenv("CR_WEEKLY_SH_ID"))

postgresql_engine = get_engine(
        dbms="postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database="sandbox"
    )

finance_postgresql_engine = get_engine(
        dbms="postgresql",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database="sandbox_finance_data"
    )

def get_channel_data():
    channel_data = cr_weekly_sheet.get_df_from_gspread(worksheet_name="IMPORT: 1.채널정보3.0", read_range="A1:L")
    channel_data = channel_data[['통합CRID', '통합CRID명', '플랫폼', '채널ID', '채널명']]

    return channel_data


def get_agenda_data():

    # creator_dashboard에서 22년 12월부터의 데이터 가져오기
    sql = f"""SELECT "통합CRID", "통합CRID명", "소속", "계약상태코드", "년", "월" 
            FROM "1_data_warehouse"."creator_dashboard" 
            WHERE ("년" = 2023) OR ("년" = 2022 AND "월" = 12);"""
    agenda_data = pd.read_sql(sql, con=finance_postgresql_engine)
    agenda_data['년'] = agenda_data['년'].astype(str)
    agenda_data['월'] = agenda_data['월'].astype(str)

    # 최근 월은 데이터가 안 올라가 있어서 채널정보3.0 데이터 그대로 읽기
    recent_agenda_data = cr_weekly_sheet.get_df_from_gspread(worksheet_name="IMPORT: 1.채널정보3.0", read_range="A1:D")
    recent_agenda_data['년'] = str(datetime.date.today().year)
    recent_agenda_data['월'] = str(datetime.date.today().month)
    recent_agenda_data = recent_agenda_data[['통합CRID', '통합CRID명', '소속', '계약상태코드', '년', '월']]

    agenda_data = pd.concat([agenda_data, recent_agenda_data], ignore_index=True)
    agenda_data = agenda_data.drop_duplicates(subset=['통합CRID', '년', '월'])

    return agenda_data


def tuesday(x):
        if x.weekday() <= 1: return x + datetime.timedelta(days=1-x.weekday()) 
        else: return x + datetime.timedelta(days=8-x.weekday())

def run():

    channel_data = get_channel_data()
    agenda_data = get_agenda_data()


    ## 트위치
    sql = """SELECT 
                COALESCE(tpp."채널ID", COALESCE(f."채널ID", sub."채널ID")) AS "채널ID",
                COALESCE(tpp."날짜", COALESCE(f."날짜", sub."날짜")) AS "날짜", 
                tpp.followers_gained, 
                tpp.hours_watched, 
                tpp.average_viewers, 
                tpp.peak_viewers, 
                tpp.hours_streamed, 
                sub."유료구독자", 
                f."팔로워"
            FROM channel_power.twitch_platform_power AS tpp

            FULL OUTER JOIN (SELECT "채널ID", "날짜", "팔로워"
                        FROM channel_power.followers WHERE "플랫폼" = '트위치' AND "채널ID" IS NOT NULL) AS f
                    ON f."채널ID" = tpp."채널ID" AND f."날짜" = tpp."날짜"

            FULL OUTER JOIN (SELECT "채널ID", "날짜", "신규구독"+"재구독"+"선물" AS "유료구독자" 
                        FROM channel_power.twitch_subscription) AS sub
                    ON sub."채널ID" = tpp."채널ID" AND sub."날짜" = tpp."날짜"

            ORDER BY "날짜" DESC;"""

    twitch_power_data = pd.read_sql(sql, con=postgresql_engine)

    
    twitch_power_data['매주화요일'] = twitch_power_data['날짜'].apply(lambda x: tuesday(x))
    twitch_power_data = twitch_power_data.drop(columns=['날짜']).rename(columns={'매주화요일':'날짜'})

    twitch_power_data['년'] = twitch_power_data['날짜'].apply(lambda x: str(x.year))
    twitch_power_data['월'] = twitch_power_data['날짜'].apply(lambda x: str(x.month))

    twitch_data = twitch_power_data[['채널ID', '날짜', '년', '월']].drop_duplicates()
    

    # followers_gained
    followers_groupby_df = np.round(twitch_power_data.pivot_table(index=['날짜', '채널ID'], values='followers_gained', aggfunc='sum').reset_index())
    twitch_data = pd.merge(twitch_data, followers_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1')

    # accv
    accv_groupby_df = np.round(twitch_power_data.pivot_table(index=['날짜', '채널ID'], values='average_viewers', aggfunc='mean').reset_index())
    twitch_data = pd.merge(twitch_data, accv_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1') 

    # pccv
    pccv_groupby_df = np.round(twitch_power_data.pivot_table(index=['날짜', '채널ID'], values='peak_viewers', aggfunc='max').reset_index())
    twitch_data = pd.merge(twitch_data, pccv_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1') 

    # 유료구독자
    subs_groupby_df = np.round(twitch_power_data.pivot_table(index=['날짜', '채널ID'], values='유료구독자', aggfunc='mean').reset_index())
    twitch_data = pd.merge(twitch_data, subs_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1') \

    # 팔로워
    followers_groupby_df = np.round(twitch_power_data.pivot_table(index=['날짜', '채널ID'], values='팔로워', aggfunc='mean').reset_index())
    twitch_data = pd.merge(twitch_data, followers_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1') 


    twitch_data = pd.merge(twitch_data, channel_data.loc[channel_data['플랫폼'] == '트위치'], on='채널ID', how='left', validate='m:1')
    twitch_data = pd.merge(twitch_data, agenda_data.drop(columns=['통합CRID명']), on=['통합CRID', '년', '월'], how='left', validate='m:1')
    twitch_data = twitch_data.sort_values(by='날짜')


    ## 아프리카
    sql = """
        SELECT app."채널ID", app."날짜", app."average_viewers", app."peak_viewers", 
                app."hours_streamed", f."팔로워", app."유료구독자"

            FROM channel_power.afreecatv_platform_power AS app
                LEFT JOIN (SELECT "채널ID", "날짜", "팔로워" FROM channel_power.followers WHERE "플랫폼" = '아프리카') AS f
                    ON f."채널ID" = app."채널ID" AND f."날짜" = app."날짜" 

            ORDER BY app."날짜" DESC;"""

    afreeca_power_data = pd.read_sql(sql, con=postgresql_engine)

    afreeca_power_data['매주화요일'] = afreeca_power_data['날짜'].apply(lambda x: tuesday(x))
    afreeca_power_data = afreeca_power_data.drop(columns=['날짜']).rename(columns={'매주화요일':'날짜'})

    afreeca_power_data['년'] = afreeca_power_data['날짜'].apply(lambda x: str(x.year))
    afreeca_power_data['월'] = afreeca_power_data['날짜'].apply(lambda x: str(x.month))

    afreeca_data = afreeca_power_data[['채널ID', '날짜', '년', '월']].drop_duplicates()


    # follower
    followers_groupby_df = np.round(afreeca_power_data.pivot_table(index=['날짜', '채널ID'], values='팔로워', aggfunc='mean').reset_index())

    df = pd.DataFrame()
    for cr in followers_groupby_df['채널ID'].unique():
        tmp_df = followers_groupby_df.loc[followers_groupby_df['채널ID'] == cr]
        tmp_df = tmp_df.sort_values(by='날짜')
        tmp_df['전주'] = tmp_df['팔로워'].shift(1)
        tmp_df['followers_gained'] = tmp_df['팔로워'] - tmp_df['전주']
        tmp_df['followers_gained'].fillna(0, inplace=True)

        df = pd.concat([df, tmp_df.drop(columns=['전주'])])

    afreeca_data = pd.merge(afreeca_data, df[['채널ID', '날짜', 'followers_gained']], on=['채널ID', '날짜'], how='outer', validate='1:1')

    # accv
    accv_groupby_df = np.round(afreeca_power_data.pivot_table(index=['날짜', '채널ID'], values='average_viewers', aggfunc='mean').reset_index())
    afreeca_data = pd.merge(afreeca_data, accv_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1')

    # pccv
    pccv_groupby_df = np.round(afreeca_power_data.pivot_table(index=['날짜', '채널ID'], values='peak_viewers', aggfunc='max').reset_index())
    afreeca_data = pd.merge(afreeca_data, pccv_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1')

    # 유료구독자
    subs_groupby_df = np.round(afreeca_power_data.pivot_table(index=['날짜', '채널ID'], values='유료구독자', aggfunc='mean').reset_index())
    afreeca_data = pd.merge(afreeca_data, subs_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1')

    # 팔로워
    followers_groupby_df = np.round(afreeca_power_data.pivot_table(index=['날짜', '채널ID'], values='팔로워', aggfunc='mean').reset_index())
    afreeca_data = pd.merge(afreeca_data, followers_groupby_df, on=['채널ID', '날짜'], how='outer', validate='1:1') 

    afreeca_data = pd.merge(afreeca_data, channel_data.loc[channel_data['플랫폼'] == '아프리카'], on='채널ID', how='left')
    afreeca_data = pd.merge(afreeca_data, agenda_data.drop(columns=['통합CRID명']), on=['통합CRID', '년', '월'], how='left')
    afreeca_data = afreeca_data.sort_values(by='날짜')






    live_data = pd.concat([twitch_data, afreeca_data]).sort_values(by='날짜')
    live_data = live_data[['채널ID', '날짜', '통합CRID명', '소속', '계약상태코드', '플랫폼', 'average_viewers', 'peak_viewers', 'followers_gained', '유료구독자', '팔로워']]
    live_data = live_data.loc[live_data['계약상태코드'] == 'A. Active']

    cr_weekly_sheet.clear_values(worksheet_name='LIVE', clear_range="A2:K")
    cr_weekly_sheet.write_df_to_sh(df=live_data, worksheet_name='LIVE', include_header=False, date_columns=['날짜'])


if __name__ == '__main__':
    run()