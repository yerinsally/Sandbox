import os
import time
import datetime

from sa_package.google_api.my_gspread import GspreadConnection

from dotenv import load_dotenv
load_dotenv(".env")

afreeca_sh_id = os.getenv("AFREECA_SH_ID")
json_key_path = os.getenv("GOOGLE_JSON_KEY_PATH")


def clean_up_old_sheet(day_limit):

    TODAY = datetime.date.today()

    afreeca_spreadsheet = GspreadConnection(key_path=json_key_path).get_spreadsheet(sh_id=afreeca_sh_id)

    worksheets = afreeca_spreadsheet.get_all_worksheets()

    for worksheet in worksheets:
        sheet_title = worksheet.title
        print(sheet_title)


        if sheet_title in ["여기에 적어주세요", "양식", "다시보기 데이터 양식", "주요 BJ"]:

            continue

        try:
            update_data = afreeca_spreadsheet.get_df_from_gspread(
                worksheet_name=sheet_title,
                read_range="A1",
                header=["col"]
            ).loc[0, "col"].split("데이터 불러오기 완료 - ")[1]

            update_date = datetime.datetime.strptime(update_data, "%Y-%m-%d %H:%M:%S").date()
            print(update_date)

            if update_date <= TODAY - datetime.timedelta(days=day_limit):
                afreeca_spreadsheet.delete_worksheet(worksheet_name=sheet_title)


        except Exception as e:
            print(e)
        
        finally:
        
            time.sleep(5)  

    
if __name__ == "__main__":
    clean_up_old_sheet(day_limit=5)