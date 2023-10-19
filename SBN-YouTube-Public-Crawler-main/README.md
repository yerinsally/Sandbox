# SANDBOX YouTube Public Crawler

## 소개

Requests만을 이용하여 YouTube의 Public Data를 수집하는 크롤러 패키지입니다.

## 설치

해당 패키지는 GCP 내 SBN-Master-Project 프로젝트 내에 있는 sbn-master-python-registry 레지스트리에 등록되어 있습니다.
해당 저장소에 접근 권한이 먼저 필요합니다.

1. 설치하려는 환경에서 gcloud 설치 및 초기화. [메뉴얼](https://cloud.google.com/sdk/docs/install?hl=ko)
2. pip를 통해 인증 패키지 설치.
    
    ```bash
   pip install twine keyrings.google-artifactregistry-auth
    ```

3. 설정 완료 후 아래 명령어를 통해 저장소 구성 설정.

    ```bash
    gcloud artifacts print-settings python \
        --project=sbn-master-project \
        --repository=sbn-master-python-registry \
        --location=asia-northeast3
    ```
5. pip를 통해 설치

    ```bash
    pip install sbn-yt-pu-crawler --extra-index-url https://asia-northeast3-python.pkg.dev/sbn-master-project/sbn-master-python-registry/simple/
    ```

## 사용법

```python
from sbn_yt_pu_crawler import Video, Channel, Search

Video.Info(video_id='xxxxxxxxxxx').extract() # withStats=True, withShorts=True
Video.Comment(video_id='xxxxxxxxxxx').extract() # withStats=True, withReply=True, sithAuthor=True

Channel.About(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract()
Channel.Videos(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract() # limit=100, sort='newest' or 'oldest' or 'popular', target='3달 전' or ['3달 전', '4달 전']
Channel.Shorts(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract() # limit=100, sort='newest' or 'oldest' or 'popular'
Channel.Streams(channel_id='xxxxxxxxxxxxxxxxxxxxxxxx').extract() # limit=100, sort='newest' or 'oldest' or 'popular', target='3달 전' or ['3달 전', '4달 전']

Search.Videos(keyword='xxxxxxxxxxx').extract() # limit=100
Search.Channels(keyword='xxxxxxxxxxx').extract() # limit=100
```