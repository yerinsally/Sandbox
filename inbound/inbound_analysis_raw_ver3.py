import os
os.getcwd()
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import glob # 파일 리스트 추출
from datetime import datetime
from tqdm import tqdm # for문 진행 상황 체크
import platform

folder_path = 'C:/Users/SANDBOX/Desktop/Sandbox/inbound/Front_data(190601~)'
all_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
dataframes = [pd.read_csv(file) for file in all_files]
front = pd.concat(dataframes, ignore_index=True)
front.sort_values('Message date', inplace = True)
print('(행, 열): ', front.shape)
front

# 구글 시트 연동 - 크리에이터 정보
import json
import csv
import pygsheets
from google.oauth2 import service_account
from matplotlib import font_manager, rc
import platform
plt.rcParams['axes.unicode_minus'] = False
gc = pygsheets.authorize(service_account_file='creds.json')
sheetname= '인바운드raw'
# 오류 발생 시, worksheet의 행 수 늘려서 재실행
sh = gc.open(sheetname)
wks_tag1 = sh.worksheet_by_title('import_tag')
wks_tag2 = sh.worksheet_by_title('import_cptag')

# CR별 정보 시트 불러오기
creator_df = wks_tag1.get_as_df( has_header=True , index_column=None , start='A1', end='E5000' , numerize=True , empty_value=np.nan)
print('(행, 열): ', creator_df.shape)
creator_df

# CP의 크리에이터 관리 시트 불러오기
cp_creator_df = wks_tag2.get_as_df(has_header=True , index_column=None , start='A1', end='L10000' , numerize=True , empty_value=np.nan)
print('(행, 열): ', cp_creator_df.shape)
cp_creator_df

# 태그 내 Product 존재하면 Inbox를 'AD'로 교체 (틱톡 제외 - 이전에 태그명 겹쳤던 이슈 존재)
ad_index = front[(front['Tags'].str.contains('BDC/PPL|Brand Channel Appearance|Commercial Model|Content Supply|Education|Instagram|Lecture|License|Live Commerce|Offline Event|Others|Revenue Share|Shorts|Instream|Null(Product)') == True)].index
front.loc[ad_index, 'Inbox'] = 'AD'

inbox_count = front['Inbox'].value_counts()
inbox_count 

# Drop unnecessary columns
front.drop(['First response','Replies to resolve','Business hours','Handle time','Autoreply','Assignee','Attributed to',\
            'Final resolution','Reaction time','Resolution time', 'Response time', 'Status',\
            'Message API ID','Conversation API ID', 'Contact name','Cc','New Conversation','Account names', \
            'Inbox API ID', 'Tag API IDs'], axis=1, inplace=True) # Contact name, Contact handle 나중에 도움될 수도
# Drop unnecessary inbox
index_inbox = front[(front['Inbox'] == '겜브링') | (front['Inbox'] == '도티')| (front['Inbox']=='슈카월드')|\
                   (front['Inbox']=='유병재')|(front['Inbox'] == 'CX & DataLab')|(front['Inbox'] == 'Marketing')|\
                   (front['Inbox'] == 'Finance')|(front['Inbox'] == 'FA')|(front['Inbox'] == 'S&P')|\
                   (front['Inbox'] == 'HR')|(front['Inbox'] == 'Talent')|(front['Inbox'] == 'Ent-inbound')|\
                   (front['Inbox'] == 'Ent-business')|(front['Inbox'] == 'Other-fan')|(front['Inbox'] == 'Growth')|\
                   (front['Inbox'] == 'Commerce')|(front['Inbox'] == 'Contact')|(front['Inbox'] == 'Creator Partnerships')|\
                   (front['Inbox'] == 'Content-biz')|(front['Inbox'] == 'Global')|(front['Inbox'] == 'IP Business')|\
                   (front['Inbox'] == 'CX & CT')|(front['Inbox'] == 'Cp-Atype')|(front['Inbox'] == 'Creator')|\
                   (front['Inbox'] == 'Other-seeding')|(front['Inbox'] == 'Content Business')|(front['Inbox']=='AD_internal')|\
                   (front['Inbox'] == 'Kids & Animation - Business')|(front['Inbox'] == 'Live Commerce')|\
                   (front['Inbox'] == 'Global (Archived)')|(front['Inbox'] == 'IP Licensing Business')|\
                   (front['Inbox'] == 'Other-Misc') | (front['Inbox'] == 'AD-Shortform')].index
front.drop(index_inbox, inplace=True)
inbox_count = front['Inbox'].value_counts()
inbox_count

print('(행, 열): ', front.shape)
front.tail(3)

# 쓰레드가 outbound이며, 사내 메일인 경우 + inbound인 경우
front = front[(front['Direction'] == "Inbound") | ((front['Direction'] == "Outbound") & (front['Tags'].str.contains('YES_AD', na=False)))]
print('(행, 열): ', front.shape)
front.head(3)

# CP-Ent Biz는 삭제하지 말기
front['Inbox'].replace(['CP-tiktok','Cp-interest','Cp-talent','Cp-story','CP-production', 'Cp-music', 'Gaming-cp',\
                          'CP-virtual','Cp-gaming', 'CP-Entertainment','CP-Lifestyle P1','CP-Lifestyle P2',\
                          'CP-Lifestyle PD/BD','CP-Sports','CP-Music', 'Kids & Animation - CP','CP-Sports','Kids & Animation-CP',\
                          'Gaming-CP','IP & Family-CP', 'Marketing (Archived)', 'CP-Entertainment1-2', 'CP-Entertainment1-3', \
                          'CP-Entertainment 1-1', 'CP-Production5', 'CP-Entertainment PnC', 'CP-Entertainment2', \
                          'CP-Entertainment1-1', 'CP-Creator Membership (CM)', 'CP-Global'],'CP', inplace=True)

cp_index = front[front['Inbox'] == 'CP'].index
front.drop(cp_index, inplace=True)

inbox_count = front['Inbox'].value_counts()
inbox_count

front = front[front['Segment']==1]
print('(행, 열): ', front.shape)
front.head(5)

front = front.drop_duplicates(subset=['Conversation ID', 'Segment'], keep='first')
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

len(front['Conversation ID'].unique())

# 사내 메일(system@sandbox는 아니면서 YES_AD 태그가 없는 경우) 제거
sandbox_inbox_index = front[(front['Contact handle'] != 'system@sandbox.co.kr') & (front['Contact handle'].str.contains('@sandbox')) & (~front['Tags'].str.contains('YES_AD', na=False))].index
front.drop(sandbox_inbox_index, inplace=True)
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

# Message date 데이터를 YYYY-MM-DD로 변경하기
front["Message date"] = pd.to_datetime(front["Message date"]).dt.strftime('%Y-%m-%d')

# Subject, Extract, To 같은 데이터에 대해 첫 데이터만 남기기 : front_1
front_1 = front.drop_duplicates(subset=['Subject', 'Extract', 'To'], keep='first')
front_1 = front_1.reset_index(drop=True)
print('(행, 열): ', front_1.shape)
front_1.head(3)

# 중복 데이터 중 첫 번째 데이터 빼고 나머지 데이터 : front_2
front_2 = front[front.duplicated(subset=['Subject', 'Extract', 'To'], keep='first')]
front_2 = front_2.reset_index(drop=True)
print('(행, 열): ', front_2.shape)
front_2.head(3)

# 확인용 : 중복 처리되어 삭제된 데이터의 Subject의 Unique 값 확인하기
for i in range(len(set(front_2['Subject']))):
    print(i, list(set(front_2['Subject']))[i])

# 예외 처리 : Subject에 sandbox.co.kr이나 YOUHA가 들어간 경우, 개별 데이터로
exception_list = ['[sandbox.co.kr]', 'YOUHA'] # 중복 데이터 보고 추가 가능
# 유하나 샌드박스가 제목에 들어가지 않으면 삭제
for i in range(len(front_2)):
    if exception_list[0] not in str(front_2['Subject'][i]) and exception_list[1] not in str(front_2['Subject'][i]):
        front_2.iloc[i] = np.nan # nan 처리
front_2 = front_2.dropna(how='all')
front_2 = front_2.reset_index(drop=True)
print('(행, 열): ', front_2.shape)
front_2.head(3)

# 중복 데이터 삭제한 데이터와 예외 처리한 데이터를 합쳐서 2차 처리한 최종 dataframe 완성
front = pd.concat([front_1, front_2])
# Subject가 NaN값이면 삭제
front = front[front['Subject'].notnull()]
front = front.reset_index(drop=True)
# Message date로 재정렬
front = front.sort_values('Message date', ascending=True)
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

front = front[~front["Contact handle"].str.contains('noreply')]
front = front[~front["Contact handle"].str.contains('no-reply')]
front = front[front['Contact handle'] != 'wiseapp@ideaware.co.kr']
front = front[front['Contact handle'] != 'iaward@kipfa.or.kr']
front = front[front['Contact handle'] != 'mkt-kr@data.ai ']
front = front[front['Contact handle'] != 'notification@facebookmail.com']
front = front[front['Contact handle'] != 'help@youha.info']
front = front[front['To'] != 'sandbox-all@sandbox.co.kr']
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)

# Extract 확인을 통해 광고 문의인지 파악
del_subject = "키워드|비즈쿠폰|쿠폰|읽지 않은 알림|프로필|인기 게시물|서명요청|편집자 모집|공지사항|\
                신청서|정산|당첨자|면접|지원서 | 지원서|설문조사|이용약관|수상 안내|안내사항|주간쁘금|인증번호|\
                팬아트|포토샵|마감임박|이메일 인증|주소 인증|서명요청|와이즈앱|스폰서십 프로그램|암호 만료|팬아트|\
                소셜아이어워드|News Letter|계약서|개인정보 |발송 안내|전송 실패"
ett = list(set(front.loc[(front['Subject'].str.contains(del_subject)), 'Subject']))
for i in range(len(ett)):
    print(i, ett[i])

# Extract 확인을 통해 광고 문의인지 파악
del_extract_zombie = "좀비트립|좀비트랩"
del_subject_zombie = "[코리안좀비]"
ett = list(set(front.loc[(front['Extract'].str.contains(del_extract_zombie) & front['Subject'].str.contains(del_subject_zombie)), 'Subject']))
front.loc[(front['Subject'].str.contains(del_subject)) | ((front['Extract'].str.contains(del_extract_zombie) & front['Subject'].str.contains(del_subject_zombie)))] = np.nan
front = front.dropna(how='all')
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

front = front[~front['Tags'].str.contains('DROP|NO_AD(no_first)|NO_AD(first)|NO_AD', na=False)]
front = front.reset_index(drop=True)
front[front['Tags'].str.contains('YES_AD', na=False)]

# 1. Tags 컬럼에서 태그가 1개인 항목에 대해 각 컬럼 생성해서 데이터 기입
# 태그가 1개인 항목 : LeadSource, Industry
lead_source_list = ['Null(Lead Source)', 'creator', 'homepage', 'employee']
client_industry_list = ['App','Car','Home/Living', 'F&B', 'Leisure/Sports', 'Leisure','Music(Industry)','Film/Media', \
                        'Kids','Fashion/Beauty','Film/Broadcast','Financial', 'gov','Gov','Pet','Health/Medical','Game',\
                        'IT/Tech','Null(Industry)','Edu/Book','S&O3/Home/Living', 'Beauty','Fashion', 'etc', 'Film', \
                        'Leisure / Sports','Film / Media','Retail','Finance', 'BS2', 'Telecom']
# 태그가 여러 개인 항목 : Product
ad_product_list = ['BDC/PPL', 'Brand Channel Appearance', 'Commercial Model', 'Content Supply', 'Education', 'Instagram', \
                'Lecture', 'License', 'Live Commerce', 'Offline Event', 'Others', 'Revenue Share', 'Shorts', 'Tiktok', 'Null(Product)']

# Tags 데이터에서 마지막에 lead_source, industry 존재하는 경우, 추출이 안 되므로 맨 마지막에 ',' 추가
for i in range(len(front)):
    front['Tags'][i] = str(front['Tags'][i]) + ','

# LeadSource를 Tags 컬럼 데이터에서 뽑아서 'lead_source' 컬럼 생성 후 데이터 대입(',' 제거)
for i in range(len(lead_source_list)):
    front.loc[front['Tags'].str.contains(lead_source_list[i] + ",", na=False), 'Lead_source'] = lead_source_list[i].split(',')[0]
# Industry를 Tags 컬럼 데이터에서 뽑아서 'client_industry' 컬럼 생성 후 데이터 대입(',' 제거)
for i in range(len(client_industry_list)):
    front.loc[front['Tags'].str.contains(client_industry_list[i] + ",", na=False), 'Client_industry'] = client_industry_list[i].split(',')[0]
front.tail(3)

# LeadSource용 : 'To'가 샌드박스 메일이 아니면 'Bcc' 가져오기
bcc_index = front[~(front['To'].str.contains('@sand', na=False)) & (front['Bcc'].notnull())].index
front.loc[bcc_index, 'To'] = front.loc[bcc_index, 'Bcc']
len(front[front['Lead_source'].isnull()])

# LeadSource용 : 리드소스가 비어있는데, creator 메일이 수신자인 경우 'creator'로 입력하기
# CR 태그 - CP
cr_mail = list(cp_creator_df[cp_creator_df['그룹스 주소'] != 'x']['그룹스 주소'].unique())
cr_mails = 'CP태그'
for i in range(len(cr_mail)):
    cr_mails = cr_mails + '|' + str(cr_mail[i]).split('box')[0]
# CR 태그 - DS
cr_mail2 = list(creator_df['영문명'].unique())
for i in range(len(cr_mail2)):
    cr_mail2[i] = str(cr_mail2[i]) + '@sand'
cr_mails2 = 'DS태그'
for i in range(len(cr_mail2)):
    cr_mails2 = cr_mails2 + '|' + cr_mail2[i].split('box')[0]
cr_mails2 = cr_mails2.lower().replace(' ', '')
# CR 태그
front.loc[front[(front['To'].str.contains(cr_mails)) & (front['Lead_source'].isnull())].index, 'Lead_source'] = 'creator'
front.loc[front[(front['To'].str.contains(cr_mails2)) & (front['Lead_source'].isnull())].index, 'Lead_source'] = 'creator'

len(front[front.loc[:, 'Lead_source'].isnull()])

# 발신 : system@sandbox.co.kr -> 리드소스 : homepage
system_index = front[front['Contact handle'] == 'system@sandbox.co.kr'].index
front.loc[system_index, 'Lead_source'] = 'homepage'
print('(행, 열): ', front.shape)
front.head(3)

# inbox가 AD-BS4면서, 리드소스가 null이면 drop
bs4_index = front[(front['Inbox'] == 'AD-BS4') & front['Lead_source'].isnull()].index
front.drop(bs4_index, inplace=True)
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

# LS 추가 태깅 (homepage, employee)
homepage_index = front[(front['Lead_source'].isnull()) & front['To'].str.contains('contact@sandbox|business@sandbox|ad-inbound@sandbox')].index
front.loc[homepage_index, 'Lead_source'] = 'homepage'
employee1_index = front[(front['Lead_source'].isnull()) & front['To'].str.contains('sno1@sandbox|sno2@sandbox|sno3@sandbox|sno4@sandbox')].index
front.loc[employee1_index, 'Lead_source'] = 'employee'
employee2_index = front[(front['Lead_source'].isnull()) & front['To'].str.contains('@sandbox')].index
front.loc[employee2_index, 'Lead_source'] = 'employee'
print('(행, 열): ', front.shape)
front.tail(3)

front[(front['Lead_source'].isnull())]['To'].unique()

# LS, To가 null이면 drop
null_index = front[(front['Lead_source'].isnull())].index
front.drop(null_index, inplace=True)
front.reset_index(drop=True, inplace=True)
front['Lead_source'].value_counts()

# Music 인식하지 못할 수 있어서 다시 작업
for i in tqdm(range(len(front))):
    if 'Music(Industry),' in front.loc[i, 'Tags']:
        front.loc[i, 'Client_industry'] = 'Music(Industry)'

# NaN값을 임시적으로 'null(temporary)'로 바꾸기 -> 이후 nan값으로 대체
# 복수행을 생성하며 NaN값을 이전 데이터로 채우는데 이에 발생하는 오류를 없애기 위함
front = front.fillna('null(temporary)')
front['Client_industry'].unique()

# 산업이 null이면 추가 정제
len(front[front['Client_industry'] == 'null(temporary)'])

# industry 비어 있는데, 해당 키워드가 제목에 들어가면 해당 키워드에 맞게 industry 부여
str_fb = '치킨|청정원|맥주|피자|초콜릿|동서식품|투썸플레이스|족발|음료|오리온|밀키트|디저트|제과|농심|떡볶이'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_fb))].index , 'Client_industry'] = 'F&B'
str_hm = '성형외과|비타민|건강기능|영양제|유산균|구전녹용|피부과|치과|정관장'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_hm))].index , 'Client_industry'] = 'Health/Medical'
str_app = '어플|아만다'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_app))].index , 'Client_industry'] = 'App'
str_it = '삼성전자|LG전자|LG 전자|노트북|다이슨|로봇청소기|커피머신|LG오브제|캐논|공기청정기'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_it))].index , 'Client_industry'] = 'F&B'
str_car = '자동차'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_car))].index , 'Client_industry'] = 'Car'
str_bt = '화장품|아모레퍼시픽|라네즈|올리브영|코스메틱|바이오더마|비욘드|뷰티브랜드|스킨케어|AHC|에스트라'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_bt))].index , 'Client_industry'] = 'Beauty'
str_gov = '공익광고|서울시|관광공사|관광청|문화재단|정부'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_gov))].index , 'Client_industry'] = 'Gov'
str_fn = '은행|금융|부동산|우리카드|국민카드|롯데카드|현대카드'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_fn))].index , 'Client_industry'] = 'Finance'
str_tel = '엘지유플러스|LG U+|LGU+|유플러스|갤럭시|SK텔레콤'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_tel))].index , 'Client_industry'] = 'Telecom'
str_fm = '넷플릭스|왓챠|영화|JTBC|웹툰|웹예능|웹드라마'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_fm))].index , 'Client_industry'] = 'Film/Media'
str_hl = 'LG생활건강|섬유유연제|침구|주방'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_hl))].index , 'Client_industry'] = 'Home/Living'
str_game = 'E-SPORTS|e스포츠|블리자드|모바일게임'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_game))].index , 'Client_industry'] = 'Game'
str_fs = '파인드카푸어|아디다스|패션브랜드|아웃도어|의류|언더웨어|무신사'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_fs))].index , 'Client_industry'] = 'Fashion'
str_ret = '홈쇼핑|GS25|아울렛|롯데백화점'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_ret))].index , 'Client_industry'] = 'Retail'
str_eb = '패스트캠퍼스| 도서 |출판사'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_eb))].index , 'Client_industry'] = 'Edu/Book'
str_etc = '대학내일'
front.loc[front[(front['Client_industry'] == 'null(temporary)') & (front['Subject'].str.contains(str_etc))].index , 'Client_industry'] = 'etc'

len(front[front['Client_industry'] == 'null(temporary)'])

## 2. Creator 칼럼 복수 생성하기
# CP 시트에 있는데, DS 시트에 없는 새로운 CR 정보 불러오기
new_cr_df = cp_creator_df[(cp_creator_df['프론트 태그'].isin(list(creator_df['영문명'])) == False) & (cp_creator_df['통합CRID명'].isin(list(creator_df['크리에이터'])) == False) & (cp_creator_df['프론트 태그/룰 활성 여부'] == 'TRUE')]
new_cr_df = new_cr_df.reset_index(drop=True)
new_cr_df

# 새로운 CR 입력하기
new_cr_df = new_cr_df[['프론트 태그', '통합CRID명', '소속', '담당자(정)']]
new_cr_df = new_cr_df.rename(columns = {'프론트 태그' : '영문명', '통합CRID명':'크리에이터', '소속' : '아젠다', '담당자(정)':'매니저'})
new_cr_df['크리에이터(Subject)'] = new_cr_df['크리에이터']
print('(행, 열): ', new_cr_df.shape)
new_cr_df

# CP의 크리에이터 관리 시트 연동한 최종 CR df
creator_df = pd.concat([creator_df, new_cr_df])
creator_df = creator_df.reset_index(drop=True)
creator_df['크리에이터(Subject)'] = creator_df['크리에이터(Subject)'].fillna(creator_df['크리에이터'])
creator_df = creator_df.fillna('')
creator_df

# 새로운 CR 추가한 최종 리스트를 구글 시트로 내보내기
# 새로운 CR 추가한 최종 리스트를 구글 시트로 내보내기
plt.rcParams['axes.unicode_minus'] = False
gc = pygsheets.authorize(service_account_file='creds.json')
sheetname= '인바운드raw'
# 오류 발생 시, worksheet의 행 수 늘려서 재실행
sh = gc.open(sheetname)
wks_tag1 = sh.worksheet_by_title('import_tag')
wks_tag1.clear('A1','E')
wks_tag1.set_dataframe(creator_df, 'A1', index=False)

import warnings
warnings.filterwarnings("ignore")
len_front = len(front)
# Tags 컬럼에서 존재하는 CR명 모두 추출하는 함수
def creators_in_tags_fx(k):
    global creators_in_tags
    tags_list = str(front['Tags'][k]).split(',')[:-1]
    cr = [str(x) for x in list(creator_df['영문명']) if pd.isnull(x) == False] # 전체 CR명 리스트로 가져오기
    creators_in_tags = list(set(tags_list) & set(cr)) # 태그 리스트와 전체 CR들 중 겹치는 값만 최종 리스트로 저장
    return creators_in_tags
# 원본 row에 CR 추가 및 중복 row 추가 (중복 row 추가 시, 다른 열의 데이터는 모두 NaN)
for i in tqdm(range(len_front)):
    creators_in_tags = creators_in_tags_fx(i)
    if len(creators_in_tags) == 0: # CR 없으면 패스
        continue
    for k in range(len(creators_in_tags)):
        front.loc[i+k/(len(creators_in_tags)), 'Creator'] = creators_in_tags[k] # 새로운 행 추가, Creator 열에 CR명 삽입
front = front.sort_index() # 인덱스 재배열
front = front.reset_index(drop=True) # 인덱스 재설정
print('(행, 열): ', front.shape)
front.head(3)

# creator 컬럼 제외 nan값이 있으면 이전 행 데이터로 채우기 + creator 컬럼 합치기(nan 존재)
front = pd.concat([front.iloc[:,:-1].fillna(method = 'pad'), front.iloc[:,[-1]]], axis = 1)
print('(행, 열): ', front.shape)
front.head(3)

# 3. Subject 칼럼에서 크리에이터명 찾아서 대체하기 (복수처리)
def creators_in_subject_fx(k):
    global creators_in_subject
    creators_in_subject = []
    subject = str(front['Subject'][k])
    cr = [str(x) for x in list(creator_df['크리에이터(Subject)']) if pd.isnull(x) == False if x != '']
    cr = list(set(cr))
    # 님, [] 등이 으로 CR명 추출하기
    for i in range(len(cr)):
        if cr[i] + ' ' in subject or cr[i] +'님' in subject or '[' + cr[i] + ']' in subject:
            creators_in_subject.append(cr[i])
    if creators_in_subject:
        return creators_in_subject[0]
for i in tqdm(range(len(front))):
    creators_in_subject_fx(i)

import warnings
warnings.filterwarnings("ignore")
len_front = len(front)
cr_count = 0
# 원본 row에 CR 추가 및 중복 row 추가 (중복 row 추가 시, 다른 열의 데이터는 모두 NaN)
for i in tqdm(range(len_front)):
    creators_in_subject = creators_in_subject_fx(i)
    if str(front['Creator'][i]) == 'nan':
        if creators_in_subject:
            front.loc[i, 'Creator'] = creators_in_subject
front = front.sort_index() # 인덱스 재배열
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

# creator 컬럼 제외 nan값 이전 행 데이터 채우기 + creator 컬럼 합치기(nan 존재)
# front = pd.concat([front.iloc[:,:-1].fillna(method = 'pad'), front.iloc[:,[-1]]], axis = 1)
# front
# Creator 컬럼에 한글명으로 들어있으니 영문명으로 교체
for i in tqdm(range(len(creator_df))):
    creator_name = front['Creator'] == list(creator_df['크리에이터(Subject)'])[i]
    front.loc[creator_name, 'Creator'] = list(creator_df['영문명'])[i] # 영문명으로 교체
front

# 4. CR명 관련 아젠다, 매니저 컬럼 생성 후 매칭
for i in tqdm(range(len(creator_df))):
    creator_name = front['Creator'] == list(creator_df['영문명'])[i]
    front.loc[creator_name, 'Creator_type'] = list(creator_df['아젠다'])[i]
    front.loc[creator_name, 'Manager'] = list(creator_df['매니저'])[i]
    front.loc[creator_name, '크리에이터'] = list(creator_df['크리에이터'])[i] # 한글명
# 크리에이터명이 nan 값이면 'CR 미지정'으로 바꾸기
front['크리에이터'] = front['크리에이터'].fillna('CR 미지정')

# 5. Product 칼럼 복수 생성하기
# NaN값을 임시적으로 'null(temporary)'로 바꿔보기 -> 이후 nan값으로 대체
# 복수행을 생성하며 NaN값을 이전 데이터로 채우는데 이에 발생하는 오류를 없애기 위함
front = front.fillna('null(temporary)')
# Tags 컬럼에서 Product 모두 추출
def products_in_tags_fx(k):
    global products_in_tags
    tags_list = str(front['Tags'][k]).split(',')
    products_in_tags = [t for t in tags_list if any(p in t for p in ad_product_list)]
    return products_in_tags

warnings.filterwarnings("ignore")
len_front = len(front)
# 원본 데이터에 CR 열 추가 및 중복 row 추가 (중복 row 추가 시, 다른 열의 데이터는 모두 NaN)
for i in tqdm(range(len_front)):
    products_in_tags = products_in_tags_fx(i)
    for k in range(len(products_in_tags)):
        front.loc[i+k/(len(products_in_tags)), 'AD_product'] = products_in_tags[k] # 새로운 행 추가, Creator 열에 CR명 삽입
front = front.sort_index() # 인덱스 재배열
front = front.reset_index(drop=True)
print('(행, 열): ', front.shape)
front.head(3)

# ad_product 컬럼 제외 nan값 이전 행 데이터로 채우기 + ad_product 컬럼 합치기(nan 존재)
front = pd.concat([front.iloc[:,:-1].fillna(method = 'pad'), front.iloc[:,[-1]]], axis = 1)
pnull_index = front[front['Message date'] < '2022-01-01'].index
front.loc[pnull_index, 'AD_product'] = 'Null(Product)' # 1월 이전 데이터는 null(product) 기입

# 6. 데이터 정리 : 컬럼별 null값 채우기, 컬럼 삭제
null_dic = {'Author' : 'Null(Author)',
            'Extract' : 'Null(Extract)',
            'Tags' : 'Null(Tags)',
            'Lead_source' : 'Null(Lead Source)',
            'Client_industry' : 'Null(Client Industry)',
            'Creator' : 'Null(Creator Name)',
            'Creator_type' : 'Null(Creator Category)',
            'Manager' : 'Null(Manager)',
            'AD_product' : 'Null(Product)'}
# 컬럼별 데이터가 'null(temporary)'면 컬럼에 맞게 null값 넣기
for i in range(len(null_dic)):
    n_data = front[list(null_dic.keys())[i]].isin(['null(temporary)'])
    front.loc[n_data, list(null_dic.keys())[i]] = list(null_dic.values())[i]
front['AD_product'] = front['AD_product'].fillna('Null(Product)')

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
front.tail(3)

front['Lead_source'].unique()

front['Creator_type'].unique()

front['Client_industry'].unique() # Film/Broadcast, Film, Film / Media / gov / S&O3/Home/Living / Financial

front['Client_industry'].value_counts()

front['Manager'].unique()

front['AD_product'].unique()

front['AD_product'].value_counts()

front['Inbox'].unique()

# 이전 태깅 항목 대체
front['Client_industry'].replace(['Film/Broadcast', 'Film', 'Film / Media'],'Film/Media', inplace=True)
front['Client_industry'].replace('gov', 'Gov', inplace=True)
front['Client_industry'].replace('S&O3/Home/Living','Home/Living', inplace=True)
front['Client_industry'].replace('Finance','Financial', inplace=True)
front['Client_industry'].replace('it/Tech','IT/Tech', inplace=True)
front['Client_industry'].replace('Leisure','Leisure/Sports', inplace=True)
front['Client_industry'].replace('Leisure / Sports','Leisure/Sports', inplace=True)
front['Client_industry'].replace('Music(Industry)','Music', inplace=True)
# Front 확인 후 대체
front['Client_industry'].replace('BS2','IT/Tech', inplace=True)
pet_list = list(front[front['Client_industry'] == 'Pet'].index)
change_tag = ['F&B', 'F&B', 'IT/Tech', 'Beauty', 'F&B', 'F&B']
for i in range(len(pet_list)):
    front.loc[pet_list[i], 'Client_industry'] = change_tag[i]
front.loc[pet_list]

front['Client_industry'].unique()

# 새로운 컬럼 생성하여 추출한 문자열 저장
front['Contact handle_2'] = front['Contact handle'].str.extract(r"@(.+?)\.")
front

front['Contact handle_2'].nunique()

# 도메인 명에 알맞은 한국 회사명 매칭 : 인바운드raw - client 시트
# 오류 발생 시, worksheet의 행 수 늘려서 재실행
sh = gc.open(sheetname)
client_sh = sh.worksheet_by_title('client')
# 시트 불러오기
client_df = client_sh.get_as_df( has_header=True , index_column=None , start='A1', end='E5000' , numerize=True , empty_value=np.nan)
print('(행, 열): ', client_df.shape)
client_df

new_column = {'도매인': 'Contact handle_2', '문의 건 수':'inbound', '클라이언트':'client_kr'}
client_df.rename(columns=new_column, inplace=True)
client_df

# Contact_handle_2 & 클라이언트 매칭
front = front.merge(client_df[['Contact handle_2', 'client_kr']], on='Contact handle_2', how='left')
front

nan_count = front['client_kr'].isnull().sum()
nan_count

front['client_kr'].fillna("한글 회사명 미지정", inplace=True)
front

front['AD_product'].unique()

mapping = {'Others': '기타(공구/시딩 등)', 'Content Supply': '영상 납품', 'BDC/PPL' : 'BDC/PPL', 'Instagram': '인스타/숏폼',
           'Brand Channel Appearance' : '출연/행사', 'Commercial Model' : '광고 모델', 'Lecture' : '강연', 'Offline Event' : '출연/행사',
           'Tiktok' : '틱톡', 'Revenue Share' : '수익 쉐어', 'License' : '라이선스', 'Shorts' : '인스타/숏폼',
           'Live Commerce' : '라이브커머스', 'Education' : '교육'}
front['AD_product_kr'] = front['AD_product'].replace(mapping)
front.tail(1000)

front['AD_product_kr'].unique()

# Inbox 정리
# AD로 통일, Industry로 필터링하여 추이 확인 가능하게 구현
# CP로 통일, creator_type로 필터링하여 추이 확인 가능하게 구현

front['Inbox'].replace(['AD-S&O1','AD-S&O2','AD-S&O3','AD-BS1','AD-BS2','AD-BS3','AD-BS4','Gaming-ad','AD-Presales',\
                          'Tiktok','Advertising','Business','Brand Solution','Media Solution','Agency Solution',\
                          'Business Creative','Ad-inbound','AD-business','AD-offline','AD-Tiktok',\
                          'Gaming-Business','AD-Ops','AD-CU', 'AD-DnP','CP-Ent Biz','FnA-Business','GAD-게임','GAD-일반'\
                          'Biz-Planning','BS1','BS2','BS3','BS4','Presales','인바운드_etc',''],'AD', inplace=True)
front['Inbox'].unique()

front = front[front['Inbox'] == 'AD']
front = front.reset_index(drop=True)
front_copy = front.copy()

# 컬럼 재정렬 : Contact handle, Contact handle_2 저장
front = front[['Message date', 'Author', 'Conversation ID', 'Extract', 'Inbox', 'Message ID', 'Subject', 'To', 'Contact handle', 'Contact handle_2', 'Lead_source', 'Client_industry', 'Creator', 'Creator_type', 'Manager', 'AD_product', '크리에이터', 'client_kr', 'AD_product_kr']]
front

front.info()

gc = pygsheets.authorize(service_account_file='creds.json')
sheetname= '인바운드raw'
# 오류 발생 시, worksheet의 행 수 늘려서 재실행
sh = gc.open(sheetname)
wks = sh.worksheet_by_title('inbound_raw')
wks.clear('A1','O')
wks.set_dataframe(front, 'A1', index=False)