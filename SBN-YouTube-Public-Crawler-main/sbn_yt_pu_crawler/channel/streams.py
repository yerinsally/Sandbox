from typing import List, Union, Optional
from sbn_yt_pu_crawler._module.function import *
from sbn_yt_pu_crawler._module.variable import *
import re
import requests
import json

class Streams:
    VALID_SORTS = ['newest', 'oldest', 'popular']

    def __init__(self, channel_id: str, limit: int = 100, sort: str = 'newest', target=None):
        self._initialize_variables(channel_id, limit, sort, target)
        self._validate_inputs()
        self._load_and_extract_videos()

    def _initialize_variables(self, channel_id, limit, sort, target):
        self._channel_id = channel_id
        self._limit = limit
        self._sort = sort
        self._published = []
        self._videoList = []
        self._set_target(target)

    def _set_target(self, target):
        if isinstance(target, str):
            self._target = [target]
        elif isinstance(target, list):
            self._target = target
        elif target is None:
            self._target = None
        else:
            raise Exception('target은 str 또는 list 형식이어야 합니다.')

    def _validate_inputs(self):
        self._validate_channel_id()
        self._validate_sort()
        self._validate_target()

    def _validate_channel_id(self):
        if len(self._channel_id) != 24:
            raise Exception('올바른 채널 ID를 입력해주세요.')

    def _validate_sort(self):
        if self._sort not in self.VALID_SORTS:
            raise Exception(f'올바른 정렬 방식을 입력해주세요. {self.VALID_SORTS}')
        if self._sort == 'popular' and self._target:
            raise Exception('인기순 정렬은 target을 사용할 수 없습니다.')

    def _validate_target(self):
        if self._target:
            pattern = r"(?:분|시간|일|주|개월|년) 전"
            if any(not re.search(pattern, t) for t in self._target):
                raise Exception("target 목록의 각 아이템은 '분 전', '시간 전', '일 전', '주 전', '개월 전', '년 전'(이)가 포함 되어야 합니다.")

    def _load_and_extract_videos(self):
        self._load_initial_page()
        self._get_initial_videos()

        if self._limit is None:
            self._get_all_videos()
        else:
            self._get_limited_videos()

    def _load_initial_page(self):
        self._html, self._cookies = getInitPage(self._channel_id, 'channel_streams')
        self._iresult, self._config = getInitConfigs(self._html)
        if self._html is None:
            raise Exception('영상 목록을 불러오는 중 오류가 발생했습니다. (Exception: HTML을 불러올 수 없음)')

    def _get_initial_videos(self):
        if self._sort == 'newest':
            self._cresult, self._continuations, self._json = extractContinuation(self._html, type=f'channel_videos')
            self._get_videos(self._json, isFirst=True)
        else:
            self._cresult, self._continuations, self._json = extractContinuation(self._html, type=f'channel_videos_{self._sort}')

    def _get_all_videos(self):
        while self._cresult:
            self._fetch_next()
            self._get_videos(self._json)

    def _get_limited_videos(self):
        if self._target is None:
            while self._cresult and len(self._videoList) < self._limit:
                self._fetch_next()
                self._get_videos(self._json)
                self._videoList = self._videoList[:self._limit]
        else:
            while self._cresult and not self._check_target():
                self._fetch_next()
                self._get_videos(self._json)

    def _fetch_next(self):
        self._cresult, self._json, self._continuations, self._config = self._next(self._continuations, self._config)

    def _next(self, _continuations, _config):
        url, continuation, configs = self._prepare_next_params(_continuations, _config)
        res = self._post_request(url, continuation, configs)

        if res.status_code != 200 or len(res.text) <= 0:
            return False, None

        self._cresult, self._continuations, self._json = extractContinuation(res.text, type='channel_videos', isFirst=False)
        return self._cresult, json.loads(res.text), self._continuations, configs

    def _prepare_next_params(self, _continuations, _config):
        url = 'https://www.youtube.com' + _continuations.get('endpoint', {}).get('apiUrl', {}) + '?key=' + _config['apikey']
        continuation = _continuations.get('command', {}).get('token', {})
        return url, continuation, _config

    def _post_request(self, url, continuation, configs):
        headers = {
            'x-goog-authuser': '0',
            'x-origin': 'https://www.youtube.com',
            'x-youtube-client-name': str(configs['clientName']),
            'x-youtube-client-version': configs['clientVersion'],
            'content-type': 'application/json'
        }
        data = {'continuation': continuation, 'context': configs['context']}
        return requests.post(url, headers=headers, cookies=self._cookies, data=json.dumps(data))

    def _get_videos(self, ytData, isFirst=False):
        self._parse_video_data(ytData, isFirst=isFirst)

    def _parse_video_data(self, ytData, isFirst=False):
        contents = self._get_video_contents(ytData, isFirst=isFirst)

        for content in contents:
            video_data = self._extract_video_data_from_content(content)
            if video_data:
                self._videoList.append(video_data)
                self._published.append(video_data['published'])

    def _get_video_contents(self, ytData, isFirst):
        if isFirst:
            tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])
            for tab in tabs:
                if tab.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('contents', []):
                    return tab.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('contents', [])
        else:
            onResponseReceivedActions = ytData.get('onResponseReceivedActions', [])
            for action in onResponseReceivedActions:
                if action.get('appendContinuationItemsAction', {}).get('continuationItems', []):
                    return action.get('appendContinuationItemsAction', {}).get('continuationItems', [])
        return []

    def _extract_video_data_from_content(self, content):
        videoGrid = content.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {})
        if videoGrid:
            video_data = {
                'video_id': videoGrid.get('videoId', {}),
                'title': videoGrid.get('title', {}).get('runs', [])[0].get('text', {}),
                'published': videoGrid.get('publishedTimeText', {}).get('simpleText', "실시간"),
                'length': self._get_length(videoGrid.get('lengthText').get('simpleText')) if videoGrid.get('lengthText') else None,
                'views': int(re.sub(r"\D", "", videoGrid.get('viewCountText').get('simpleText'))) if "simpleText" in videoGrid.get('viewCountText') else None,
                'description': videoGrid.get('descriptionSnippet', {}).get('runs', [])[0].get('text', {}),
            }
            return video_data
        return None

    def _get_length(self, duration):
        duration_split = duration.split(':')
        if len(duration_split) == 3:
            return int(duration_split[0]) * 3600 + int(duration_split[1]) * 60 + int(duration_split[2])
        elif len(duration_split) == 2:
            return int(duration_split[0]) * 60 + int(duration_split[1])
        else:
            return int(duration_split[0])

    def _check_target(self):
        if self._target == []:
            return False

        for i in self._published:
            for p in self._target:
                if p in i:
                    return True

    def extract(self):
        return self._videoList