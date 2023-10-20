import os
import time
import datetime
import traceback
import pandas as pd

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine

from slack_message import slack_message

from dotenv import load_dotenv
load_dotenv(".env")

json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")


postgresql_engine = get_engine(
    dbms="postgresql",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database='sandbox'
)

# =====================================================================================
# Raw 데이터 시트 업데이트
# =====================================================================================

def update_vod_data(start_date:datetime.date, end_date:datetime.date, sh_id:str, worksheet_name:str, agenda:str=None, channel_id:str=None, period:str="7D"):


    """
    Parameter
    ---------

    agenda : {"Gaming", "Ent", "F&A", "Emerging", "제작1CP", "제작2CP", "Global", "Membership", "기타"}
    
    period : {"7D", "28D"}

    """

    # 올바른 스프레드시트 아이디인지 확인
    spread_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=sh_id)
    if spread_sheet is None:
        return

    sb_agenda_list = ["Gaming", "Ent", "F&A", "Emerging", "제작1CP", "제작2CP", "Global", "Membership", "기타"]
    assert agenda in [None] + sb_agenda_list, "잘못된 아젠다입니다"


    age_columns = ["만 13–17세", "만 18–24세", "만 25–34세", "만 35–44세", "만 45–54세", "만 55–64세", "만 65세 이상"]
    gender_columns = ["남성", "여성", "사용자가 지정"]
    subscribe_columns = ["구독 중",	"구독 안함"]
    traffic_source_columns = ['Shorts 피드', 'YouTube 검색', 'YouTube 광고', '기타', '기타 YouTube 기능', '동영상 카드 및 특수효과',
    '알림', '외부', '재생목록', '재생목록 페이지', '직접 입력 또는 알 수 없음', '채널 페이지', '최종 화면', '추천 동영상', '탐색 기능', '해시태그 페이지']


    default_sql = f"""
                    SELECT 
                        vs."video_id" AS "비디오 ID", 
                        CONCAT('https://www.youtube.com/watch?v=', vs."video_id") AS "링크",
                        vl."channel_id" AS "채널 ID",
                        cl."CRID_name" AS "크리에이터명", 
                        cl."channel_name" AS "채널명", 
                        vl."video_title" AS "제목", 
                        vl."time_published" AS "업로드 날짜", 
                        vl."video_length" AS "영상 길이(sec)",
                        vi."paid_promotion" AS "광고 표기 여부",
                        vs."views" AS "조회수", 
                        vs."impressions" AS "노출수",
                        vs."click_through_rate" AS "노출클릭률",
                        vs."unique_viewers" AS "순 시청자수",  
                        vs."watch_time" AS "총 시청 시간(h)",
                        vs."avg_watch_time(sec)" AS "평균 시청 지속 시간(sec)",
                        vs."avg_watch_percentage" AS "평균 시청 지속률",
                        vs."avg_views_per_viewer" AS "시청자당 조회수",
                        vs."subscribers" AS "구독자수 증감",
                        vs."subscribers_gained" AS "구독자 증가수",
                        vs."subscribers_lost" AS "구독자 감소수",
                        vs."comments" AS "댓글수",
                        vs."likes" AS "좋아요수",
                        vs."dislikes" AS "싫어요수",
                        vs."sharings" AS "공유수"
                    FROM data_crawling_cms."video_stats_{period}" as vs
                        LEFT JOIN metadata."video_list" AS vl
                            ON vs.video_id = vl.video_id
                        LEFT JOIN metadata."channel_list" AS cl
                            ON vl.channel_id = cl.channel_id
                        LEFT JOIN metadata."video_info" AS vi
                            ON vs.video_id = vi.video_id
                    WHERE vs.views IS NOT NULL
                        AND vl."time_published" <= '{end_date.strftime('%Y-%m-%d')}'
                        AND vl."time_published" >= '{start_date.strftime('%Y-%m-%d')}'
                        """
        
    try:
        if agenda is None:

            # 전체 크리에이터
            if channel_id is None:    
                sql = default_sql
                
            else:
                sql = default_sql + f""" AND vl."channel_id" = '{channel_id}';"""

        else:
            
            # agenda 소속 CR 채널 리스트 불러오기
            ad_data_sh_id = os.getenv("AD_DATA_SH_ID")
            ad_data_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=ad_data_sh_id)

            channel_df = ad_data_sheet.get_df_from_gspread(
                worksheet_name="IMPORT:1.채널정보3.0",
                read_range="A1:K"
            )
            
            channel_df = channel_df.loc[(channel_df["계약상태코드"] == "A. Active") & (channel_df["플랫폼"] == "유튜브") & (channel_df["소속"] == agenda)]
            channel_df = channel_df.dropna(subset=["채널링크"])
            channel_df["channel_id"] = channel_df["채널링크"].apply(lambda x: x.split("/")[-1])
            
            channel_list = channel_df["channel_id"].values.tolist()
            channel_list = [channel_id for channel_id in channel_list if len(channel_id) == 24 and channel_id[:2] == "UC"]



            sql = default_sql + f""" AND vl.channel_id in ('{"', '".join(channel_list)}');"""
            
        vod_df = pd.read_sql(sql=sql, con=postgresql_engine)
        vod_df = vod_df.sort_values(by="업로드 날짜", ascending=False)
            


        # SQL에서 영상 데모그래픽 가져오기 (youtube_cms_additional_data)
        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period}_additional" as vsad;
            """

        vod_add_df = pd.read_sql(sql=sql, con=postgresql_engine).drop_duplicates()



        # 조회수 기준 비중
        views_add_df = vod_add_df.copy()
        views_add_df['check'] = views_add_df['data_type'].apply(lambda x: True if 'views' in x else False)
        views_add_df = views_add_df.loc[views_add_df['check']]
        views_add_df.drop(columns=['check'], inplace=True)

        views_pivot_df = views_add_df.pivot_table(values='value', index='비디오 ID', columns='data_name')
        views_pivot_df = views_pivot_df[age_columns + gender_columns + subscribe_columns]


        # 평균 조회율
        percentage_add_df = vod_add_df.copy()
        percentage_add_df['check'] = percentage_add_df['data_type'].apply(lambda x: True if 'watch_percentage' in x else False)
        percentage_add_df = percentage_add_df.loc[percentage_add_df['check']]
        percentage_add_df.drop(columns=['check'], inplace=True)

        percentage_pivot_df = percentage_add_df.pivot_table(values='value', index='비디오 ID', columns='data_name')
        percentage_pivot_df = percentage_pivot_df[age_columns + gender_columns + subscribe_columns]
        percentage_pivot_df = percentage_pivot_df.add_suffix(' 평균 조회율')

        # -------------------------------------------------------------------------------------------------------

        # 주요 지표 및 데모그래픽 데이터 합치기
        vod_merge_df = pd.merge(vod_df, views_pivot_df, on="비디오 ID", how="left")
        vod_merge_df = pd.merge(vod_merge_df, percentage_pivot_df, on="비디오 ID", how="left")


        # 평균 시청 지속시간
        for col in age_columns + gender_columns + subscribe_columns:
            vod_merge_df[col+" 평균 시청 지속 시간(sec)"] = round(vod_merge_df['영상 길이(sec)']*vod_merge_df[col+" 평균 조회율"]/100)


        # -------------------------------------------------------------------------------------------------------


        # 트래픽 소스
        traffic_source_df = vod_add_df.copy()
        traffic_source_df['check'] = traffic_source_df['data_type'].apply(lambda x: True if x == 'traffic_source' else False)
        traffic_source_df = traffic_source_df.loc[traffic_source_df['check']]
        traffic_source_df.drop(columns=['check'], inplace=True)

        traffic_source_pivot_df = traffic_source_df.pivot_table(values='value', index='비디오 ID', columns='data_name')
        for traffic_source in traffic_source_columns:
            traffic_source_pivot_df.loc['test', traffic_source] = None

        traffic_source_pivot_df = traffic_source_pivot_df[traffic_source_columns]
        traffic_source_pivot_df = traffic_source_pivot_df.drop('test')


        vod_merge_df = pd.merge(vod_merge_df, traffic_source_pivot_df, on='비디오 ID', how="left")
        vod_merge_df.fillna("", inplace=True)


        # -------------------------------------------------------------------------------------------------------

        # 시트에 업데이트

        if len(vod_merge_df) == 0:
            return
        else:
            print(len(vod_merge_df))
        

        # 시트가 이미 있는 경우
        if spread_sheet.worksheet_exists(worksheet_name):
            print(sh_id, worksheet_name, "워크시트 존재")
            spread_sheet.clear_values(
                worksheet_name=worksheet_name,
                clear_range="A1:AY"
            )
            
            cur_sheet_row_count = spread_sheet.get_worksheet(worksheet_name=worksheet_name).row_count
            if  cur_sheet_row_count < len(vod_merge_df):
                spread_sheet.append_rows(worksheet_name=worksheet_name, append_row_num=len(vod_merge_df) + 1 - cur_sheet_row_count)

        else:
            spread_sheet.create_worksheet(
                worksheet_name=worksheet_name,
                row_count=len(vod_merge_df)+1,
                col_count=len(vod_merge_df.columns),
                frozen_row_count=1
            )

        spread_sheet.set_cell_format(
            worksheet_name=worksheet_name,
            set_range="A1:F",
            number_format="TEXT"
        )

        spread_sheet.set_cell_format(
            worksheet_name=worksheet_name,
            set_range="A1:BX",
            fontsize=8,
            wrap_strategy="CLIP"
        )
        
        spread_sheet.set_cell_format(
            worksheet_name=worksheet_name,
            set_range="A1:1",
            bold=True,
            fontsize=8
        )

        spread_sheet.write_df_to_sh(
            df=vod_merge_df,
            worksheet_name=worksheet_name,
            date_columns=['업로드 날짜'],
            include_header=True,
        )


    except:
        slack_message(
            title='RAW 데이터 업데이트 실패',
            err_msg=traceback.format_exc(),
            link=None
        )


if __name__ == "__main__":


    gaming_cms_raw_sh_id = os.getenv("CMS_RAW_SH_ID")
    gaming_cms_raw_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=gaming_cms_raw_sh_id)


    sb_all_cms_raw_sh_id = os.getenv("SB_ALL_CMS_RAW_SH_ID")
    sb_all_cms_raw_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=sb_all_cms_raw_sh_id)
    

    sheet_link_df = sb_all_cms_raw_sheet.get_df_from_gspread(
        worksheet_name="데이터 링크",
        read_range="A1:B"
    )

    for idx in sheet_link_df.index:

        year = int(sheet_link_df.loc[idx, "연도"])
        link = sheet_link_df.loc[idx, "링크"]

        if year in [2021, 2022]:
            continue
        
        sh_id = link.split("/d/")[1].split("/edit")[0]

        for agenda in ["Gaming", "Ent", "F&A", "Emerging", "제작1CP", "제작2CP", "Membership"]:

            for period in ["7D", "28D"]:
                
                update_vod_data(
                    start_date=datetime.date(year, 1, 1),
                    end_date=datetime.date(year, 12, 31),
                    sh_id=sh_id,
                    worksheet_name=f"전체_{period}" if agenda is None else f"{agenda}_{period}",
                    agenda=agenda,
                    period=period
                )

                print(year, agenda, period, "완료")

                time.sleep(10)
                
    

    gp3_raw_sh_id = "1PB_xiwXjQ8SW2KpSo1Q2UZ3HqMwrABxhmdYLi1juBgM"
    gp_raw_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=gp3_raw_sh_id)

    channelid_list_df = gp_raw_sheet.get_df_from_gspread(worksheet_name="채널목록", read_range="A2:B")
    for idx in channelid_list_df.index:
        cr_name = channelid_list_df.loc[idx, '크리에이터명']
        channel_id = channelid_list_df.loc[idx, '채널ID']

        update_vod_data(
            start_date=datetime.date(2022,1,1),
            end_date=datetime.date.today(),
            sh_id=gp3_raw_sh_id,
            worksheet_name=cr_name,
            channel_id=channel_id,
            period='7D'
        )
        
        time.sleep(10)