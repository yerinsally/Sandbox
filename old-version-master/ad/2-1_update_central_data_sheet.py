import os
import datetime
import traceback
import pandas as pd

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine

from slack_message import slack_message

from dotenv import load_dotenv
load_dotenv(".env")

ad_data_sh_id = os.getenv("AD_DATA_SH_ID")
json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
ad_data_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=ad_data_sh_id)


postgresql_engine = get_engine(
    dbms="postgresql",
    username=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    database='sandbox'
)

slack_token = os.getenv("SLACK_TOKEN")

# =====================================================================================
# `13:00` BDC 중앙 데이터 시트 업데이트
# =====================================================================================

# 1️⃣ DB에 있는 VOD 광고건 BDC 중앙 데이터 시트에 업데이트하기
def update_ad_vod_data(start_date:datetime.date, end_date:datetime.date, period_type:str="7D"):

    """
    Parameter
    ---------

    period_type : {"7D", "28D"}

    """


    age_columns = ["만 13–17세", "만 18–24세", "만 25–34세", "만 35–44세", "만 45–54세", "만 55–64세", "만 65세 이상"]
    gender_columns = ["남성", "여성", "사용자가 지정"]
    subscribe_columns = ["구독 중",	"구독 안함"]
    traffic_source_columns = ['Shorts 피드', 'YouTube 검색', 'YouTube 광고', '기타', '기타 YouTube 기능', '동영상 카드 및 특수효과',
    '알림', '외부', '재생목록', '재생목록 페이지', '직접 입력 또는 알 수 없음', '채널 페이지', '최종 화면', '추천 동영상', '탐색 기능', '해시태그 페이지']

        
    try:
        # 내부 cr 데이터
        # sql = f"""
        #     SELECT 
        #         vs."video_id" AS "비디오 ID", 
        #         vl."channel_id" AS "채널 ID",
        #         cl."channel_name" AS "채널명", 
        #         cl."CRID_name" AS "크리에이터명", 
        #         vl."video_title" AS "제목", 
        #         vl."time_published" AS "업로드 날짜", 
        #         vl."video_length" AS "영상 길이",
        #         '' AS "게임 태그",
        #         '' AS "IP",
        #         '' AS "광고 단가",
        #         '' AS "광고 요약",
        #         vs."views" AS "조회수", 
        #         vs."impressions" AS "노출수",
        #         vs."unique_viewers" AS "순 시청자수",  
        #         vs."comments" AS "댓글수",
        #         vs."likes" AS "좋아요수",
        #         vs."dislikes" AS "싫어요수",
        #         vs."sharings" AS "공유수",
        #         vs."click_through_rate" AS "노출클릭률",
        #         vs."avg_watch_percentage" AS "평균 조회율", 
        #         vs."watch_time" AS "시청 시간(h)",
        #         vs."subscribers" AS "구독자수 증감",
        #         vs."subscribers_gained" AS "구독자 증가수",
        #         vs."subscribers_lost" AS "구독자 감소수"
        #     FROM data_crawling_cms."video_stats_{period_type}" as vs
        #         LEFT JOIN metadata."video_list" AS vl
        #             ON vs.video_id = vl.video_id
        #         LEFT JOIN metadata."channel_list" AS cl
        #             ON vl.channel_id = cl.channel_id
        #     WHERE vs.views IS NOT NULL
        #         AND vl."time_published" <= '{end_date.strftime('%Y-%m-%d')}'
        #         AND vl."time_published" >= '{start_date.strftime('%Y-%m-%d')}'
        #     ORDER BY vl."time_published" DESC, cl."CRID_name";
        #     """
        sql = f"""
            SELECT 
                vs."video_id" AS "비디오 ID", 
                vl."channel_id" AS "채널 ID",
                cl."channel_name" AS "채널명", 
                cl."CRID_name" AS "크리에이터명", 
                vl."video_title" AS "제목", 
                vl."time_published" AS "업로드 날짜", 
                vl."video_length" AS "영상 길이",
                '' AS "게임 태그",
                '' AS "IP",
                '' AS "광고 단가",
                '' AS "광고 요약",
                vs."views" AS "조회수", 
                vs."impressions" AS "노출수",
                vs."unique_viewers" AS "순 시청자수",  
                vs."comments" AS "댓글수",
                vs."likes" AS "좋아요수",
                vs."dislikes" AS "싫어요수",
                vs."sharings" AS "공유수",
                vs."click_through_rate" AS "노출클릭률",
                vs."avg_watch_percentage" AS "평균 조회율", 
                vs."watch_time" AS "시청 시간(h)",
                vs."subscribers" AS "구독자수 증감",
                vs."subscribers_gained" AS "구독자 증가수",
                vs."subscribers_lost" AS "구독자 감소수"
            FROM data_crawling_cms."video_stats_{period_type}" as vs
                LEFT JOIN metadata."video_list" AS vl
                    ON vs.video_id = vl.video_id
                LEFT JOIN metadata."channel_list" AS cl
                    ON vl.channel_id = cl.channel_id
                LEFT JOIN metadata."video_info" AS vi
                    ON vs.video_id = vi.video_id
            WHERE vi."paid_promotion" = True
                AND vs.views IS NOT NULL
                AND vl."time_published" <= '{end_date.strftime('%Y-%m-%d')}'
                AND vl."time_published" >= '{start_date.strftime('%Y-%m-%d')}'
            ORDER BY vl."time_published" DESC, cl."CRID_name";
            """
        ad_list_df = pd.read_sql(sql=sql, con=postgresql_engine)


        # gcu 데이터
        sql = f"""
            SELECT 
                gv."video_id" AS "비디오 ID", 
                gv."channel_id" AS "채널 ID",
                gv."channel_name" AS "채널명", 
                gv."cr_name" AS "크리에이터명", 
                gv."title" AS "제목", 
                gv."upload_date" AS "업로드 날짜", 
                gv."length" AS "영상 길이",
                '' AS "게임 태그",
                '' AS "IP",
                '' AS "광고 단가",
                '' AS "광고 요약",
                vs."views" AS "조회수", 
                vs."impressions" AS "노출수",
                vs."unique_viewers" AS "순 시청자수",  
                vs."comments" AS "댓글수",
                vs."likes" AS "좋아요수",
                vs."dislikes" AS "싫어요수",
                vs."sharings" AS "공유수",
                vs."click_through_rate" AS "노출클릭률",
                vs."avg_watch_percentage" AS "평균 조회율", 
                vs."watch_time" AS "시청 시간(h)",
                vs."subscribers" AS "구독자수 증감",
                vs."subscribers_gained" AS "구독자 증가수",
                vs."subscribers_lost" AS "구독자 감소수"
            FROM ad_vod_info."gcu_vod_info" as gv
                LEFT JOIN data_crawling_cms."video_stats_7D" AS vs
                    ON gv.video_id = vs.video_id
            WHERE vs.views IS NOT NULL
                AND gv."upload_date" <= '{end_date.strftime('%Y-%m-%d')}'
                AND gv."upload_date" >= '{start_date.strftime('%Y-%m-%d')}'
            ORDER BY gv."upload_date" DESC, gv."cr_name";
            """
        gcu_ad_list_df = pd.read_sql(sql=sql, con=postgresql_engine)

        ad_list_df = pd.concat([ad_list_df, gcu_ad_list_df], axis=0)
        ad_list_df = ad_list_df.sort_values(by="업로드 날짜", ascending=False)

        # cost_df = get_data(sql="SELECT * FROM cr_bdc_cost_history")

        # def get_quarter(x):
        #     year = x.year
        #     q = ((x.month - 1) // 3) + 1

        #     return f"{year}-{q}Q"

        # ad_list_df['quarter'] = ad_list_df['업로드 날짜'].apply(lambda x: get_quarter(x))

        # def get_cost(x):
        #     try:
        #         # print(x['채널 ID'], x['quarter'])
        #         return cost_df.loc[(cost_df['channel_id'] == x['채널 ID']) & (cost_df['quarter'] == x['quarter']), 'cost'].values[0]
        #     except Exception as e:
        #         return ""

        # ad_list_df['광고 단가'] = ad_list_df.apply(lambda x: get_cost(x), axis=1)
        # ad_list_df.drop(columns=['quarter'], inplace=True)


        # -------------------------------------------------------------------------------------------------------


        # SQL에서 광고 영상 데모그래픽 가져오기 (youtube_cms_additional_data)
        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period_type}_additional" as vsad
            WHERE data_type in ('age_views', 'gender_views', 'subscribe_views', 'traffic_source');
            """

        add_data_df = pd.read_sql(sql=sql, con=postgresql_engine).drop_duplicates()

        pivot_df = add_data_df.pivot_table(values='value', index='비디오 ID', columns='data_name')
        for traffic_source in traffic_source_columns:
            pivot_df.loc['test', traffic_source] = None

        pivot_df = pivot_df[age_columns + gender_columns + subscribe_columns + traffic_source_columns]
        pivot_df = pivot_df.drop('test')


        # -------------------------------------------------------------------------------------------------------


        # 대표 성별 및 연령 정보 가져오기 (youtube_cms_additional_data)
        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name" AS "성별", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period_type}_additional" as vsad
                INNER JOIN 
                    (SELECT "video_id", MAX("value") AS "max_value"
                        FROM data_crawling_cms."video_stats_{period_type}_additional"
                    WHERE "data_type" = 'gender_views'
                    GROUP BY "video_id") AS groupedad
                ON vsad."video_id" = groupedad."video_id"
                    AND vsad."value" = groupedad."max_value"
                WHERE vsad."data_type" = 'gender_views';
            """
        max_gender_df = pd.read_sql(sql=sql, con=postgresql_engine)


        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name" AS "연령", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period_type}_additional" as vsad
                INNER JOIN 
                    (SELECT "video_id", MAX("value") AS "max_value"
                        FROM data_crawling_cms."video_stats_{period_type}_additional"
                    WHERE "data_type" = 'age_views'
                    GROUP BY "video_id") AS groupedad
                ON vsad."video_id" = groupedad."video_id"
                    AND vsad."value" = groupedad."max_value"
                WHERE vsad."data_type" = 'age_views';
            """
        max_age_df = pd.read_sql(sql=sql, con=postgresql_engine)


        gender_age_df = pd.merge(max_gender_df[['비디오 ID', '성별']], max_age_df[['비디오 ID', '연령']], on='비디오 ID')
        

        # 주요 지표 및 데모그래픽 데이터 합치기
        ad_merge_df = pd.merge(ad_list_df, pivot_df, on="비디오 ID", how="left")
        ad_merge_df = pd.merge(ad_merge_df, gender_age_df, on="비디오 ID", how="left")


        # column 순서 조정하기
        new_col = []
        for col in ad_merge_df.columns:
            if col == '영상 길이':
                continue
            new_col.append(col)
        new_col.append('영상 길이')
        ad_merge_df = ad_merge_df[new_col]


        # NA 처리
        ad_merge_df.fillna("", inplace=True)


        # -------------------------------------------------------------------------------------------------------

        if period_type == "7D":
            sheet_name = f"VOD 광고 데이터"
        else:
            sheet_name = f"VOD 광고 데이터_28D"
        

        # 시트에 업데이트
        ad_data_sheet.clear_values(
            worksheet_name=sheet_name,
            clear_range="A6:BB"
        )
        ad_data_sheet.write_df_to_sh(
            df=ad_merge_df,
            worksheet_name=sheet_name,
            start_cell="A5",
            date_columns=['업로드 날짜'],
            to_str_columns=['광고 단가'],
            include_header=True
        )

        # 업데이트 완료 메시지 기록
        update_msg = f"업데이트 시기: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]],
            worksheet_name=sheet_name,
            start_cell="A1"
        )

        update_msg = f"데이터 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]],
            worksheet_name=sheet_name,
            start_cell="A2"
        )



    except:
        # 실패시 실패 메시지 기록
        update_msg = f"업데이트 실패 - {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]],
            worksheet_name=sheet_name,
            start_cell="A1"
        )

        update_msg = f"{traceback.format_exc()}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]],
            worksheet_name=sheet_name,
            start_cell="A2"
        )

        slack_message(
            title=f"{period_type} VOD 광고 데이터 업데이트 실패",
            err_msg=traceback.format_exc(),
            link="https://docs.google.com/spreadsheets/d/1-IvArH_lb3qllr0cfkyIViCciyCsVHH_FEPq5KVYHQM/edit#gid=1123710452"
        )


# 2️⃣ DB에 있는 VOD 비광고건 BDC 중앙 데이터 시트에 업데이트하기
def update_none_ad_vod_data(start_date:datetime.date, end_date:datetime.date, period_type:str="7D"):

    if period_type == "7D":
        sheet_name = f"VOD 비광고 데이터"
    else:
        sheet_name = f"VOD 비광고 데이터_28D"


    try:
        # SQL에서 비광고 영상 주요 지표 가져오기
        sql = f"""
            SELECT 
                vs."video_id" AS "비디오 ID", 
                vl."channel_id" AS "채널 ID",
                cl."channel_name" AS "채널명", 
                cl."CRID_name" AS "크리에이터명", 
                vl."video_title" AS "제목", 
                vl."time_published" AS "업로드 날짜", 
                '' AS "게임 태그",
                vs."views" AS "조회수", 
                vs."likes" AS "좋아요수",
                vs."comments" AS "댓글수"
            FROM data_crawling_cms."video_stats_{period_type}" as vs
                LEFT JOIN metadata."video_list" AS vl
                    ON vs.video_id = vl.video_id
                LEFT JOIN metadata."channel_list" AS cl
                    ON vl.channel_id = cl.channel_id
                LEFT JOIN metadata."video_info" AS vi
                    ON vs.video_id = vi.video_id
            WHERE vi."paid_promotion" = False
                AND vs.views IS NOT NULL
                AND vl."time_published" <= '{end_date.strftime('%Y-%m-%d')}'
                AND vl."time_published" >= '{start_date.strftime('%Y-%m-%d')}'
            ORDER BY vl."time_published" DESC, cl."CRID_name";
            """
        none_ad_list_df = pd.read_sql(sql=sql, con=postgresql_engine)


        # 대표 성별 및 연령 정보 가져오기 (youtube_cms_additional_data)
        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name" AS "성별", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period_type}_additional" as vsad
                INNER JOIN 
                    (SELECT "video_id", MAX("value") AS "max_value"
                        FROM data_crawling_cms."video_stats_{period_type}_additional"
                    WHERE "data_type" = 'gender_views'
                    GROUP BY "video_id") AS groupedad
                ON vsad."video_id" = groupedad."video_id"
                    AND vsad."value" = groupedad."max_value"
                WHERE vsad."data_type" = 'gender_views';
            """
        max_gender_df = pd.read_sql(sql=sql, con=postgresql_engine)

        sql = f"""
            SELECT 
                vsad."video_id" AS "비디오 ID", 
                vsad."data_type", 
                vsad."data_name" AS "연령", 
                vsad."value"
            FROM data_crawling_cms."video_stats_{period_type}_additional" as vsad
                INNER JOIN 
                    (SELECT "video_id", MAX("value") AS "max_value"
                        FROM data_crawling_cms."video_stats_{period_type}_additional"
                    WHERE "data_type" = 'age_views'
                    GROUP BY "video_id") AS groupedad
                ON vsad."video_id" = groupedad."video_id"
                    AND vsad."value" = groupedad."max_value"
                WHERE vsad."data_type" = 'age_views';
            """
        max_age_df = pd.read_sql(sql=sql, con=postgresql_engine)

        gender_age_df = pd.merge(max_gender_df[['비디오 ID', '성별']], max_age_df[['비디오 ID', '연령']], on='비디오 ID')


        # 주요 지표 및 데모그래픽 데이터 합치기
        nonead_merge_df = pd.merge(none_ad_list_df, gender_age_df, on="비디오 ID", how="left").sort_values(by="업로드 날짜", ascending=False)
        

        # -------------------------------------------------------------------------------------------------------


        # 시트에 업데이트 
        ad_data_sheet.clear_values(
            worksheet_name=sheet_name,
            clear_range="A5:L"
        )
        ad_data_sheet.write_df_to_sh(
            df=nonead_merge_df,
            worksheet_name=sheet_name,
            include_header=False,
            date_columns=['업로드 날짜'],
            start_cell="A5"
        )

        # 업데이트 완료 메시지 기록
        update_msg = f"업데이트 시기: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]], 
            worksheet_name=sheet_name, 
            start_cell="A1"
            )

        update_msg = f"데이터 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]], 
            worksheet_name=sheet_name, 
            start_cell="A2"
            )
            

    except:
        # 실패시 실패 메시지 기록
        update_msg = f"업데이트 실패 - {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]], 
            worksheet_name=sheet_name, 
            start_cell="A1"
            )

        update_msg = f"{traceback.format_exc()}"
        ad_data_sheet.write_values_to_sh(
            values=[[update_msg]], 
            worksheet_name=sheet_name, 
            start_cell="A2"
            )
      

        slack_message(
            title=f"VOD 비광고 데이터 업데이트 실패",
            err_msg=traceback.format_exc(),
            link="https://docs.google.com/spreadsheets/d/1-IvArH_lb3qllr0cfkyIViCciyCsVHH_FEPq5KVYHQM/edit#gid=1138560569"
        )


if __name__ == "__main__":

    TODAY = datetime.date.today()

    update_ad_vod_data(
        start_date=datetime.date(2022,1,1),
        end_date=TODAY - datetime.timedelta(days=7),
        period_type="7D"
    )
    update_ad_vod_data(
        start_date=datetime.date(2022,1,1),
        end_date=TODAY - datetime.timedelta(days=28),
        period_type="28D"
    )

    update_none_ad_vod_data(
        start_date=TODAY - datetime.timedelta(days=98),
        end_date=TODAY - datetime.timedelta(days=8)
    )
