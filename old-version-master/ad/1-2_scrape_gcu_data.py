import os
import traceback
import pandas as pd

from slack_bolt import App

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.google_api.my_youtube import YoutubeApi

from sa_package.mydatabase import get_engine, upload_df

from sa_package.convert.time_format import convert_hhmmss_format_to_sec

from slack_message import slack_message

from dotenv import load_dotenv
load_dotenv(".env")


json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
ad_gcu_sh_id = os.getenv("AD_GCU_SH_ID")
ad_gcu_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=ad_gcu_sh_id)


postgresql_engine = get_engine(
    dbms="postgresql",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database='sandbox'
)

youtube_data_api = os.getenv("YOUTUBE_DATA_API")
youtube_api = YoutubeApi(youtube_data_api)

json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
yt_sub_sh_id = os.getenv("YT_SUB_SH_ID")
yt_sub_spreadsheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=yt_sub_sh_id)


## SLACK
app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"), # Features > OAuth & Permissions > Bot User OAuth Access Token
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET") # Features > Basic Information > Signing Secret
)

# =====================================================================================
# GCU 데이터 DB에 업로드
# =====================================================================================

def yt_link_to_video_id(url):

    if 'watch?v=' in url:
        video_id = url.split('watch?v=')[-1]

        if len(video_id) == 11:
            return video_id
        else:
            video_id = video_id.split('&')[0]
            if len(video_id) == 11:
                return video_id
    
    else:
        video_id = url.split('/')[-1]

        if len(video_id) == 11:
            return video_id
        else:
            video_id = video_id.split('?')[0]
            if len(video_id) == 11:
                return video_id

    return None


# 1️⃣ 외부 크리에이터 CMS 데이터 수집 시트에 추가된 내용 DB에 업로드
def scrape_gcu_data():

    age_columns = ["만 13–17세", "만 18–24세", "만 25–34세", "만 35–44세", "만 45–54세", "만 55–64세", "만 65세 이상"]
    gender_columns = ["남성", "여성", "사용자가 지정"]
    subscribe_columns = ["구독 중",	"구독 안함"]

    try:
        gcu_df = ad_gcu_sheet.get_df_from_gspread(worksheet_name="데이터 수집", read_range="B1:AC")
        gcu_df['video_id'] = gcu_df['광고 영상 링크(Youtube)'].apply(lambda x: yt_link_to_video_id(x))

    except Exception as e:
        
        slack_message(
            title=f"GCU 데이터 크롤링 실패",
            err_msg=traceback.format_exc()[:1000] if len(traceback.format_exc()) > 1000 else traceback.format_exc(),
            link="https://docs.google.com/spreadsheets/d/1K5_aPJr23hyeNzSsP1gdSEaodBJSB7RhA3SkslkAUK4/edit#gid=407491652"
        )

        title = "GCU 데이터 크롤링 문제 발생"
        message_blocks = [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": title,
                            "emoji": True
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type":"section",
                        "text":{
                            "type": "mrkdwn",
                            "text": f"*Traceback*: {traceback.format_exc()[:1000] if len(traceback.format_exc()) > 1000 else traceback.format_exc()}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"시트 확인하러 가기"
                        },
                        "accessory": {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "바로가기",
                                "emoji": True
                            },
                            "value": "click_me_123",
                            "url": "https://docs.google.com/spreadsheets/d/1K5_aPJr23hyeNzSsP1gdSEaodBJSB7RhA3SkslkAUK4/edit#gid=1181310685",
                            "action_id": "button-action"
                        }
                    }
                ]
        
        app.client.chat_postMessage(
                channel=os.getenv('SLACK_STRATEGY_DATA_OPS_ID'),
                blocks=message_blocks,
                text=title
            )



    for idx in gcu_df.index:
        video_id = gcu_df.loc[idx, 'video_id']
        sql = f"""SELECT * FROM ad_vod_info."gcu_vod_info" WHERE video_id = '{video_id}'"""
        db_df = pd.read_sql(sql=sql, con=postgresql_engine)
        if len(db_df) > 0:
            gcu_df.drop(idx, inplace=True)


    if len(gcu_df) > 0:

        try:
            # 누락 데이터 처리
            for col in gcu_df.columns:
                gcu_df[col] = gcu_df[col].apply(lambda x: "" if x == "-" else x)

            gcu_df['avg_watch_time(sec)'] = gcu_df['c. 평균 시청 지속 시간'].apply(lambda x: convert_hhmmss_format_to_sec(x))
            gcu_df.rename(columns={
                    '크리에이터명':'cr_name',
                    'a. 시청 시간(단위: 시간)':'watch_time',
                    'b. 조회수':'views',
                    'd. 평균 조회율':'avg_watch_percentage',
                    'e. 구독자':'subscribers',
                    'f. 노출수':'impressions',
                    'g. 노출 클릭률':'click_through_rate',
                    'h. 순 시청자수':'unique_viewers',
                    'i. 시청자당 평균 조회수':'avg_views_per_viewer',
                    'j. 좋아요':'likes',
                    'k. 싫어요':'dislikes',
                    'l. 공유':'sharings',
                    'm. 추가된 댓글 수':'comments'
                }, inplace=True)
            gcu_df.drop(columns=['캠페인명 (GCU BD 수동 기입)', '광고 영상 링크(Youtube)', 'c. 평균 시청 지속 시간'], inplace=True)


            # ==============
            # 영상 기본 정보
            # ==============
            for idx in gcu_df.index:
                video_id = gcu_df.loc[idx, 'video_id']
                video_detail = youtube_api.get_video_detail(video_id)

                # vod data 가져오기
                for col in ['title', 'upload_date', 'channel_id', 'channel_name', 'length']:
                    gcu_df.loc[idx, col] = video_detail[col]
                
                # game_tag = get_video_tag(video_id=video_id, driver=driver)
                # gcu_df.loc[idx, 'game_tag'] = game_tag



            # ===============
            # 기본 CMS 데이터
            # ===============

            gcu_cms_df = gcu_df.drop(columns=['cr_name', 'title', 'upload_date', 'channel_id', 'channel_name', 'length'] + age_columns + gender_columns + subscribe_columns)
            upload_df(
                dbms='postgres',
                engine=postgresql_engine,
                df=gcu_cms_df,
                pk_list=['video_id'],
                scheme='data_crawling_cms',
                table='video_stats_7D'
            )


            # =====================
            # 시청자 연령 데모그래픽
            # =====================
            gcu_age_df = gcu_df[age_columns + ['video_id']]

            gcu_age_df = pd.melt(gcu_age_df, id_vars=['video_id'])
            gcu_age_df.loc[gcu_age_df.index, 'data_type'] = 'age_views'
            gcu_age_df.rename(columns={
                    'variable':'data_name'
                }, inplace=True)
            gcu_age_df['value'] = gcu_age_df['value'].apply(lambda x: "" if x == '-' else x)
            gcu_age_df = pd.merge(gcu_age_df, gcu_df[['video_id']], on='video_id', how='left')

            upload_df(
                dbms='postgres',
                engine=postgresql_engine,
                df=gcu_age_df,
                pk_list=['video_id', 'data_type', 'data_name'],
                scheme='data_crawling_cms',
                table='video_stats_7D_additional'
            )


            # =====================
            # 시청자 성별 데모그래픽
            # =====================
            gcu_gender_df = gcu_df[gender_columns + ['video_id']]

            gcu_gender_df = pd.melt(gcu_gender_df, id_vars=['video_id'])
            gcu_gender_df.loc[gcu_gender_df.index, 'data_type'] = 'gender_views'
            gcu_gender_df.rename(columns={
                    'variable':'data_name'
                }, inplace=True)
            gcu_gender_df['value'] = gcu_gender_df['value'].apply(lambda x: "" if x == '-' else x)
            gcu_gender_df = pd.merge(gcu_gender_df, gcu_df[['video_id']], on='video_id', how='left')

            upload_df(
                dbms='postgres',
                engine=postgresql_engine,
                df=gcu_gender_df,
                pk_list=['video_id', 'data_type', 'data_name'],
                scheme='data_crawling_cms',
                table='video_stats_7D_additional'
            )


            # =====================
            # 시청자 구독 데모그래픽
            # =====================
            gcu_subscribe_df = gcu_df[subscribe_columns + ['video_id']]

            gcu_subscribe_df = pd.melt(gcu_subscribe_df, id_vars=['video_id'])
            gcu_subscribe_df.loc[gcu_subscribe_df.index, 'data_type'] = 'subscribe_views'
            gcu_subscribe_df.rename(columns={
                    'variable':'data_name'
                }, inplace=True)
            gcu_subscribe_df['value'] = gcu_subscribe_df['value'].apply(lambda x: "" if x == '-' else x)
            gcu_subscribe_df = pd.merge(gcu_subscribe_df, gcu_df[['video_id']], on='video_id', how='left')

            upload_df(
                dbms='postgres',
                engine=postgresql_engine,
                df=gcu_subscribe_df,
                pk_list=['video_id', 'data_type', 'data_name'],
                scheme='data_crawling_cms',
                table='video_stats_7D_additional'
            )
            
            
            
            gcu_vod_df = gcu_df[['video_id', 'cr_name', 'title', 'channel_id', 'channel_name', 'upload_date', 'length']]
            gcu_vod_df['length'] = gcu_vod_df['length'].astype(int)
            upload_df(
                'postgres',
                engine=postgresql_engine,
                df=gcu_vod_df,
                pk_list=['video_id'],
                scheme='ad_vod_info',
                table='gcu_vod_info'
            )

        except Exception as e:

            slack_message(
                title=f"GCU 데이터 크롤링 실패",
                err_msg=traceback.format_exc()[:1000] if len(traceback.format_exc()) > 1000 else traceback.format_exc(),
                link="https://docs.google.com/spreadsheets/d/1K5_aPJr23hyeNzSsP1gdSEaodBJSB7RhA3SkslkAUK4/edit#gid=407491652"
            )


if __name__ == "__main__":

    scrape_gcu_data()
