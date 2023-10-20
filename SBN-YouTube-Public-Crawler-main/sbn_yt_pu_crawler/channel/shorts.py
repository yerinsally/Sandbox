from typing import List, Union, Optional
from sbn_yt_pu_crawler._module.function import *
from sbn_yt_pu_crawler._module.variable import *
import re
import requests
import json

class Shorts:
    VALID_SORTS = ['newest', 'oldest', 'popular']

    def __init__(self, channel_id: str, limit: int = 100, sort: str = 'newest'):
        self._initialize_variables(channel_id, limit, sort)
        self._validate_inputs()
        self._load_and_extract_videos()

    def _initialize_variables(self, channel_id, limit, sort):
        self._channel_id = channel_id
        self._limit = limit
        self._sort = sort
        self._videoList = []

    def _validate_inputs(self):
        self._validate_channel_id()
        self._validate_sort()

    def _validate_channel_id(self):
        if len(self._channel_id) != 24:
            raise Exception('올바른 채널 ID를 입력해주세요.')

    def _validate_sort(self):
        if self._sort not in self.VALID_SORTS:
            raise Exception(f'올바른 정렬 방식을 입력해주세요. {self.VALID_SORTS}')

    def _load_and_extract_videos(self):
        self._load_initial_page()
        self._get_initial_videos()

        if self._limit is None:
            self._get_all_videos()
        else:
            self._get_limited_videos()

    def _load_initial_page(self):
        self._html, self._cookies = getInitPage(self._channel_id, 'channel_shorts')
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
        while self._cresult and len(self._videoList) < self._limit:
            self._fetch_next()
            self._get_videos(self._json)
            self._videoList = self._videoList[:self._limit]

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
        videoGrid = content.get('richItemRenderer', {}).get('content', {}).get('reelItemRenderer', {})
        if videoGrid:
            video_data = {
                'video_id': videoGrid.get('videoId', {}),
                'title': videoGrid.get('headline', {}).get('simpleText', {}),
                'length': self._get_length(videoGrid.get('accessibility').get('accessibilityData').get('label')),
                'views': self._get_views(videoGrid.get('viewCountText', {}).get('simpleText', {})),
            }
            return video_data
        return None

    def _get_length(self, duration):
        duration_split = duration.split('-')
        duration = duration_split[-2].replace('초', '')

        if '분' in duration:
            duration = duration.replace('분', '')
            duration = int(duration) + 60

        return int(duration)

    def _get_views(self, views):
        views = views.replace('조회수 ', '').replace('회', '')
        if views.isdigit():
            views = int(views)
        else:
            views_int = float(views[:-1].replace(',', ''))
            videoCount_unit = views[-1]
            if videoCount_unit == '천':
                views_int = views_int * 1000
            elif videoCount_unit == '만':
                views_int = views_int * 10000
            elif videoCount_unit == '억':
                views_int = views_int * 100000000
            else:
                views_int = views_int * 1

            views = int(views_int)

        return views

    def extract(self):
        return self._videoList