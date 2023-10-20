import os
from slack_bolt import App

from dotenv import load_dotenv
load_dotenv(".env")

## SLACK
app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"), # Features > OAuth & Permissions > Bot User OAuth Access Token
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET") # Features > Basic Information > Signing Secret
)

def slack_message(title, err_msg, link):
    
    message_blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{title}*"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type":"section",
                    "text":{
                        "type": "mrkdwn",
                        "text": f"*Traceback*: {err_msg}"
                    }
                },
                {
                    "type": "divider"
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
                        "url": {link},
                        "action_id": "button-action"
                    }
                }
            ]
    
    app.client.chat_postMessage(
            channel=os.getenv('SLACK_STRATEGY_DATA_OPS_ID'),
            blocks=message_blocks,
            text=title
        )
    