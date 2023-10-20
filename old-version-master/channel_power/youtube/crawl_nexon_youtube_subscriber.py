import os
import datetime
import traceback

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.google_api.my_youtube import YoutubeApi
from sa_package.mydatabase import upload_df, get_engine
from sa_package.platform.slack import send_slack_message_formatted

from dotenv import load_dotenv
load_dotenv(".env")


slack_token = os.getenv("SLACK_TOKEN")

# =====================================================================================
# 넥슨 유튜브 채널 데이터 업데이트
# =====================================================================================

def update_sb_channel_detail():

    youtube_data_api = os.getenv("YOUTUBE_DATA_API")
    youtube_api = YoutubeApi(youtube_data_api)

    json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")
    nexon_sub_sh_id = os.getenv("NEXON_SUB_SH_ID")
    nexon_sub_sheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=nexon_sub_sh_id)

    update_channel_df = nexon_sub_sheet.get_df_from_gspread(worksheet_name="channel_list", read_range="A2:A")

    for idx in update_channel_df.index:
        channel_id = update_channel_df.loc[idx, '채널 ID']

        channel_info = youtube_api.get_channel_detail(channel_id)

        update_channel_df.loc[idx, '채널명'] = channel_info['channel_name']
        update_channel_df.loc[idx, '구독자수'] = channel_info['subscribers']
        update_channel_df.loc[idx, '누적조회수'] = channel_info['views']
        update_channel_df.loc[idx, '누적업로드수'] = channel_info['videos']

    # 시트에 기록
    nexon_sub_sheet.clear_values(
        worksheet_name="channel_list", 
        clear_range="A3:E"
        )

    nexon_sub_sheet.write_df_to_sh(
        df=update_channel_df, 
        worksheet_name="channel_list", 
        include_header=False, 
        start_cell="A3"
        )

    update_msg = f"마지막 업데이트: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    nexon_sub_sheet.write_values_to_sh(
        values=[[update_msg]], 
        worksheet_name="channel_list", 
        start_cell="A1"
        )

    

if __name__ == "__main__":
    try:
        update_sb_channel_detail()
    except:
        send_slack_message_formatted(
            token=slack_token,
            subject="넥슨 구독자수 업데이트 실패",
            body=traceback.format_exc(),
            link="https://docs.google.com/spreadsheets/d/1wIT64DbBdWl0iGVtaj_Rdj6iSHJ0-dEjg4TkheusCRQ/edit#gid=1408534070",
            msg_type="error"
        )
    