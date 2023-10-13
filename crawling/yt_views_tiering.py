# pip install sbn-yt-pu-crawler --upgrade
# python crawling/yt_views_tiering.py

import pandas as pd
import pygsheets
from sbn_yt_pu_crawler import Video, Channel, Search

# Video.Info(video_id='xxxxxxxxxxx').extract() # withStats=True, withShorts=True
# Video.Comment(video_id='xxxxxxxxxxx').extract() # withStats=True, withReply=True, sithAuthor=True

# Channel.About(channel_id='UCCJ2b2lJE7M77cSuSHLcMOQ').extract()
data = Channel.Videos(channel_id='UCu9BCtGIEr73LXZsKmoujKw').extract() # limit=100, sort='newest' or 'oldest' or 'popular', target='3달 전' or ['3달 전', '4달 전']
df = pd.DataFrame(data)
print(data)
# Channel.Shorts(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract() # limit=100, sort='newest' or 'oldest' or 'popular'
# Channel.Streams(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract() # limit=100, sort='newest' or 'oldest' or 'popular', target='3달 전' or ['3달 전', '4달 전']

# Search.Videos(keyword='xxxxxxxxxxx').extract() # limit=100
# Search.Channels(keyword='xxxxxxxxxxx').extract() # limit=100


# 시트 업로드
# df = pd.DataFrame()
# df['channel_id'] = pd.Series(channel_id_list)
# df['accv'] = pd.Series(accv_list)
# gc = pygsheets.authorize(service_account_file='creds.json')
# sheetname = '크롤링 시도'
# sh = gc.open(sheetname)
# wks = sh.add_worksheet(title='트위치 23.'+str(month))
# wks.set_dataframe(df, 'A1', index=False)