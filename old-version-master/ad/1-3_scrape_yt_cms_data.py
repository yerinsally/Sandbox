import os
import datetime
import pandas as pd
import argparse

from sa_package.mydatabase import get_engine, upload_df

from yt_cms_lib import get_yt_studio_drive, crawl_cms_data

from dotenv import load_dotenv
load_dotenv(".env")


google_login_id = os.getenv("GOOGLE_LOGIN_ID_IP_STRATEGY")
google_login_pwd = os.getenv("GOOGLE_LOGIN_PWD_IP_STRATEGY")
google_otp_secret_key = os.getenv("GOOGLE_OTP_SECRET_KEY_IP_STRATEGY")

managed_auth_id = os.getenv("MANAGED_AUTH_ID")
affiliate_auth_id = os.getenv("AFFILIATE_AUTH_ID")


postgresql_engine = get_engine(
    dbms="postgresql",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database='sandbox'
)



def crawl_cms_data_with_video_id(video_id, driver, period="first_week"):

    
    # video_id 로 channel_id, auth_id 가져오기
    sql = f"SELECT video_id, cms_id, channel_id FROM metadata.video_list WHERE video_id = '{video_id}'"
    vod_info_df = pd.read_sql(sql, con=postgresql_engine)

    channel_id = vod_info_df.loc[0, "channel_id"]
    cms_id = vod_info_df.loc[0, "cms_id"]
    spare_auth_id = os.getenv("MANAGED_AUTH_ID") if cms_id == os.getenv("AFFILIATE_AUTH_ID") else os.getenv("AFFILIATE_AUTH_ID")

    
    # 크롤링
    df_dict = crawl_cms_data(
        driver=driver,
        video_id=video_id,
        channel_id=channel_id,
        auth_id=cms_id,
        spare_auth_id=spare_auth_id,
        period=period
    )

    # '아직' 데이터 없는 경우
    if df_dict['overview'].loc[video_id, 'views'] is None:
        return None

    # 데이터가 CMS에 없는 경우
    if df_dict['overview'].loc[video_id, 'views'] == "-":
        df_dict['overview'].loc[video_id, 'views'] = None

    return df_dict



def update_yt_cms_data(from_date, channel_id=None, period:str="first_week", ad_only=False, cms_update=False):


    """
    Parameter
    ---------

    period: {first_week, first_4_weeeks}
    """

    assert period in ["first_week", "first_4_weeks"], "잘못된 period"


    log_msg = ""
    msg_type = "log"

    if period == "first_week":
        period_num = 7
    else:
        period_num = 28


    if channel_id is None:
        # 성현님 DB에서 비디오 목록 불러오기
        sql = f"""SELECT vl.video_id, vl.channel_id, vl.time_published, vl.video_title, vl.video_length, vi.paid_promotion, vi.shorts
                        FROM metadata.video_list AS vl
                    LEFT JOIN metadata.video_info as vi
                        ON vl.video_id = vi.video_id
                    WHERE vl.time_published IS NOT NULL 
                        AND vl.time_published >= '{from_date.strftime('%Y-%m-%d')}'
                        AND vl.time_published <= '{(datetime.date.today() - datetime.timedelta(days=period_num)).strftime('%Y-%m-%d')}';
                """
    else:
        sql = f"""SELECT vl.video_id, vl.channel_id, vl.time_published, vl.video_title, vl.video_length, vi.paid_promotion, vi.shorts
                        FROM metadata.video_list AS vl
                    LEFT JOIN metadata.video_info as vi
                        ON vl.video_id = vi.video_id
                    WHERE vl.time_published IS NOT NULL
                        AND vl.channel_id = '{channel_id}' 
                        AND vl.time_published >= '{from_date.strftime('%Y-%m-%d')}'
                        AND vl.time_published <= '{(datetime.date.today() - datetime.timedelta(days=period_num)).strftime('%Y-%m-%d')}';
                """

    vod_list_df = pd.read_sql(sql, con=postgresql_engine)
    new_vod_list = vod_list_df["video_id"].values.tolist()

    if not cms_update:

        # DB에 이미 cms 저장된 비디오 목록 불러오기
        # existing_vod_list = pd.read_sql(sql="SELECT video_id, views FROM youtube_cms", con=mysql_engine)["video_id"].values.tolist()          
        
        existing_vod_list = pd.read_sql(sql=f'SELECT video_id, views FROM data_crawling_cms."video_stats_{period_num}D"', con=postgresql_engine)["video_id"].values.tolist()


        # 새로 크롤링할 비디오 id 리스트
        new_vod_list = [x for x in new_vod_list if x not in existing_vod_list]


    if ad_only:

        # DB에서 광고 태그 붙은 비디오 목록 불러오기
        ad_vod_list_df = vod_list_df.loc[vod_list_df["paid_promotion"] == True]
        ad_vod_list = ad_vod_list_df["video_id"].values.tolist()

        # DB에서 #achievement에 올라온 비디오 목록 불러오기
        sql = """SELECT video_id FROM ad_vod_info."sb_slack_achievement";"""
        ad_vod_list_df = pd.read_sql(sql, con=postgresql_engine)
        ad_vod_list += ad_vod_list_df["video_id"].values.tolist()
        
        ad_vod_list = list(set(ad_vod_list))

        new_vod_list = [x for x in new_vod_list if x in ad_vod_list]


    ## 크롤링 시작
    
    driver_try_num = 0
    while driver_try_num < 3:
        try:
            driver = get_yt_studio_drive(google_login_id, google_login_pwd, google_otp_secret_key, managed_auth_id)
            break

        except Exception as e:
            driver_try_num += 1
            
    if driver is None:
        log_msg += f"driver 열기 실패"
        msg_type = "error"

    else:

        success_num = 0
        not_yet_num = 0
        fail_num = 0
        fail_list = []

        for video_id in new_vod_list:
            try:
                df_dict = crawl_cms_data_with_video_id(
                    video_id=video_id,
                    driver=driver,
                    period=period
                )

                # 아직 데이터가 CMS에 없는 경우
                if df_dict is None:
                    not_yet_num += 1
                    continue


                # 크롤링 CMS 데이터 DB에 업로드
                upload_df(
                    dbms="postgres",
                    engine=postgresql_engine,
                    df=df_dict['overview'].fillna("").drop(columns=["channel_id", "title", "upload_date"]),
                    pk_list=["video_id"],
                    scheme="data_crawling_cms",
                    table=f"video_stats_{period_num}D"
                )

                upload_df(
                    dbms="postgres",
                    engine=postgresql_engine,
                    df=df_dict['additional'].fillna("").drop(columns=["channel_id"]),
                    pk_list=["video_id", "data_type", "data_name"],
                    scheme="data_crawling_cms",
                    table=f"video_stats_{period_num}D_additional"
                )

                success_num += 1

            except Exception as e:
                print(type(e), e)
                fail_num += 1
                fail_list.append("https://www.youtube.com/watch?v="+video_id)


        log_msg = f"""
        크롤링 시작 날짜     : {from_date.strftime("%Y-%m-%d")}
        크롤링 CMS PERIOD   : {period}
        광고만 크롤링 여부   : {ad_only}
        CMS 업데이트 여부    : {cms_update}

        =======================================================================
        크롤링 시도 영상 개수 : {len(new_vod_list)}개
        성공 개수 : {success_num}개
        보류 개수 : {not_yet_num}개
        실패 개수 : {fail_num}개
        """


        driver.close()
        postgresql_engine.dispose()



if __name__ == "__main__":


    parser = argparse.ArgumentParser(description="")

    parser.add_argument('--startdate', type=str, default=None, help="insert startdate in format yyyymmdd")
    parser.add_argument('--period', type=str, default='7D', help="insert cms period type (7D, 28D)")
    parser.add_argument('--channel_id', type=str, default=None, help="insert channel id to crawl vod data / default value is None(crawl all)")
    parser.add_argument('--ad_only', type=str, default='False', help="insert True or False")
    parser.add_argument('--cms_update', type=str, default='False', help="insert True or False")
    
    args = parser.parse_args()

    assert args.period in ['7D', '28D']
    assert args.ad_only in ['True', 'False']

    if args.startdate is None:
        if args.period == '7D':
            crawling_start_date = datetime.date.today() - datetime.timedelta(days=14)
        elif args.period == '28D':
            crawling_start_date = datetime.date.today() - datetime.timedelta(days=50)
        
    else:
        crawling_start_date = datetime.datetime.strptime(args.startdate, "%Y%m%d")
    
    period = "first_week" if args.period == '7D' else "first_4_weeks"
    ad_only = True if args.ad_only == 'True' else False
    cms_update = True if args.cms_update == 'True' else False
    
    update_yt_cms_data(from_date=crawling_start_date, channel_id=args.channel_id, ad_only=ad_only, period=period, cms_update=cms_update)
    

    # crawling_start_date = datetime.date(2023,2,5)
    # update_yt_cms_data(from_date=crawling_start_date, channel_id=None, ad_only=False, period="first_week")
    # update_yt_cms_data(from_date=crawling_start_date, channel_id=None, ad_only=False, period="first_4_weeks")

    # crawling_start_date = TODAY - datetime.timedelta(days=50)
    # update_yt_cms_data(from_date=crawling_start_date, channel_id=None, ad_only=False, period="first_4_weeks")

    # video_id = "2K_q9U_PYOY"
    # video_id = "z7aqlNvn2uA"
    # driver = get_yt_studio_drive(google_login_id, google_login_pwd, google_otp_secret_key, managed_auth_id)
    # df_dict = crawl_cms_data_with_video_id(video_id, driver, period="first_week")
    # print(df_dict)

    # upload_df(
    #     dbms="mysql",
    #     engine=mysql_engine,
    #     df=df_dict['overview'].fillna(""),
    #     pk_list=["video_id"],
    #     scheme="sandbox",
    #     table="youtube_cms"
    # )

    # upload_df(
    #     dbms="mysql",
    #     engine=mysql_engine,
    #     df=df_dict['additional'].fillna(""),
    #     pk_list=["video_id", "data_type", "data_name"],
    #     scheme="sandbox",
    #     table="youtube_cms_additional_data"
    # )

    # upload_df(
    #     dbms="postgres",
    #     engine=postgresql_engine,
    #     df=df_dict['overview'].fillna("").drop(columns=["channel_id", "title", "upload_date"]),
    #     pk_list=["video_id"],
    #     scheme="data_crawling_cms",
    #     table="video_stats_7D"
    # )

    # upload_df(
    #     dbms="postgres",
    #     engine=postgresql_engine,
    #     df=df_dict['additional'].fillna(""),
    #     pk_list=["video_id", "data_type", "data_name"],
    #     scheme="data_crawling_cms",
    #     table="video_stats_7D_additional"
    # )


    # 민쩌미
    # crawling_start_date = datetime.date(2021,7,1)
    # update_yt_cms_data(from_date=crawling_start_date, channel_id="UCCLD41Qs7YnqLsXhWjkX8qQ", ad_only=False, period="first_week")
    # update_yt_cms_data(from_date=crawling_start_date, channel_id="UCCLD41Qs7YnqLsXhWjkX8qQ", ad_only=False, period="first_4_weeks")


    # video_id = "nnfNhmLgiCE"
    # driver = get_yt_studio_drive(login_id=google_login_id, login_pwd=google_login_pwd, otp_secret_key=google_otp_secret_key, auth_id=managed_auth_id)
    # crawl_cms_data_with_video_id(video_id, driver)