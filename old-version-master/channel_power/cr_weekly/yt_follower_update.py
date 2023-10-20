import os
import pandas as pd

from sa_package.mydatabase import get_engine

from sa_package.google_api.my_gspread import GspreadConnection
from sa_package.mydatabase import get_engine

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


def run():

    sql = """SELECT "날짜", "채널ID", "플랫폼", "채널명", "팔로워"
                    FROM channel_power.followers
                        WHERE "플랫폼" = '유튜브' 
                            AND EXTRACT(DOW from "날짜") = 2
                        ORDER BY "날짜";
    """
    followers = pd.read_sql(sql, con=postgresql_engine)

    cr_weekly_sheet.clear_values(
        worksheet_name='유튜브 구독자',
        clear_range='A1:E'
    )
    cr_weekly_sheet.write_df_to_sh(
        df=followers,
        worksheet_name="유튜브 구독자",
        include_header=True,
        date_columns=['날짜'],
        if_not_exist='create'
    )

