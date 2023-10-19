import logging
from playwright.sync_api import sync_playwright
import onetimepass as otp
from google.cloud import secretmanager
import os, json, time, hashlib, requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

class YouTubeAuthenticator:

    def __init__(self):
        self.project_id = os.environ.get("project_id")
        self.secret_id_cookies = os.environ.get("secret_id_cookies")
        self.secret_id_account = os.environ.get("secret_id_account")
        self.account_info = self.access_secret_account()
        self.secretKey = self.account_info["secret_key"]
        self.gmail = self.account_info["gmail"]
        self.gmail_password = self.account_info["gmail_password"]

    def get_code(self):
        rt = otp.get_totp(self.secretKey)
        rt = str(rt)
        rt = rt.zfill(6)
        return rt

    def get_secret_value(self, secret_id):
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    def set_cookies(self, secret_value):
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{self.project_id}"
        secret_name = f"{parent}/secrets/{self.secret_id_cookies}"

        # Create payload with current timestamp and secret value
        payload = json.dumps({
            "timestamp": time.time(),  # Save current timestamp
            "secret_value": secret_value
        })

        # Add new secret version
        version = client.add_secret_version(
            request={"parent": secret_name, "payload": {"data": payload.encode("UTF-8")}}
        )

        logging.info("쿠키 저장 완료")

    def access_secret_cookies(self):
        secret_value = self.get_secret_value(self.secret_id_cookies)
        secret_data = json.loads(secret_value)

        if time.time() - secret_data["timestamp"] > 60 * 60 * 24 * 14:  # Check if two weeks has passed
            logging.info("쿠키가 만료되었습니다")
            return None

        return secret_data["secret_value"]

    def arrange_cookies(self, cookies):
        result = {}
        for i in cookies:
            if i["name"] in ["SID", "HSID", "SSID", "APISID", "SAPISID"] and i["domain"] == ".youtube.com":
                result[i["name"]] = i["value"]
        return result

    def check_cookies(self):
        try:
            cookies = self.access_secret_cookies()

            ori = "https://studio.youtube.com"
            date = datetime.timestamp(datetime.now())

            hash_key = str(date)[:10] + " " + cookies['SAPISID'] + " " + ori
            hash_obj = hashlib.sha1(bytes(hash_key, 'utf-8'))
            hash_str = hash_obj.hexdigest()

            params = {"alt": "json", "key": "AIzaSyBUPetSUmoZL-OhlxA7wSac5XinrygCqMo"}
            headers = {
                "authorization": "",
                "cookie": "SID=; HSID=; SSID=; APISID=; SAPISID=;",
                "origin": "https://studio.youtube.com",
                "x-origin": "https://studio.youtube.com"
            }
            headers["origin"] = ori
            headers["x-origin"] = ori
            headers["authorization"] = "SAPISIDHASH " + str(date)[:10] + "_" + hash_str
            headers["cookie"] = "SID=" + cookies['SID'] + "; HSID=" + cookies['HSID'] + "; SSID=" + cookies[
                'SSID'] + "; APISID=" + cookies['APISID'] + "; SAPISID=" + cookies['SAPISID'] + ";"

            json_data = {"externalOwnerId": "8n9NT_QYS0-cG0bFXzovsA", "reportGroupFilter": [
                {"reportPageTypeIs": {"value": "REPORT_PAGE_TYPE_PAYMENT_SUMMARY"},
                 "reportFrequencyIs": {"value": "REPORT_FREQUENCY_MONTH"}, "pageInfo": {"pageSize": 1}}],
                         "context": {"client": {"clientName": 62, "clientVersion": "1.20230627.00.00"}}}

            data = requests.post("https://studio.youtube.com/youtubei/v1/cms/list_cms_report_groups", headers=headers,
                                 params=params, json=json_data)
            if data.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            logging.error(e)
            return False

    def access_secret_account(self):
        secret_value = self.get_secret_value(self.secret_id_account)
        return json.loads(secret_value)

    def login(self):
        try:
            logging.info("쿠키 생성 프로세스 시작")
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=True, args=["--headless=new", "--lang=ko-KR"])
            context = browser.new_context(viewport={"width": 1200, "height": 800}, locale="ko-KR")
            page = context.new_page()
            page.goto("https://www.youtube.com/")
            logging.info("로그인 페이지 이동")
            page.get_by_role("link", name="로그인").click()
            logging.info("이메일 입력")
            page.get_by_role("textbox", name="이메일 또는 휴대전화").fill(self.gmail)
            page.get_by_role("button", name="다음").click()

            logging.info("비밀번호 입력")
            page.get_by_role("textbox", name="비밀번호 입력").fill(self.gmail_password)
            page.get_by_role("textbox", name="비밀번호 입력").press("Enter")

            page.get_by_role("button", name="다른 방법 시도").click()
            logging.info("OTP 입력")
            time.sleep(1)
            page.get_by_role("link", name="Google OTP 앱에서 인증 코드 받기").click()
            page.get_by_role("textbox", name="코드 입력").fill(self.get_code())
            logging.info("로그인 버튼 클릭")

            page.get_by_role("button", name="다음").click()
            logging.info("로그인 완료")

            page.get_by_role("link", name="YouTube 홈").click()

            cookies = context.cookies()
            result = self.arrange_cookies(cookies)
            self.set_cookies(result)
            logging.info("쿠키 생성 프로세스 종료")
        except:
            logging.error(e)
            raise Exception("쿠키 생성 실패")

    def run(self):
        try:
            result = self.check_cookies()
            if result:
                logging.info("쿠키가 유효합니다")
            else:
                logging.info("쿠키가 유효하지 않습니다")
                self.login()
        except:
            logging.error(e)
            raise Exception("쿠키 생성 실패")

if __name__ == "__main__":
    YouTubeAuthenticator().run()
