from typing import List, Union, Optional
from sbn_yt_pu_crawler._module.function import *
from sbn_yt_pu_crawler._module.variable import *
import re
import requests
import json

class Videos:

    def __init__(self, keyword: str, limit: int = 100):
        self._initialize_variables(keyword, limit)
        self._load_and_extract_videos()

    def _initialize_variables(self, keyword, limit):
        self._keyword = keyword
        self._limit = limit
        self._videoList = []

    def _load_and_extract_videos(self):
        self._load_initial_page()
        self._get_initial_videos()

        if self._limit is None:
            self._get_all_videos()
        else:
            self._get_limited_videos()

    def _load_initial_page(self):
        self._html, self._cookies = getInitPage(self._keyword, 'search_videos')
        self._iresult, self._config = getInitConfigs(self._html)
        if self._html is None:
            raise Exception('영상 목록을 불러오는 중 오류가 발생했습니다. (Exception: HTML을 불러올 수 없음)')

    def _get_initial_videos(self):
        self._cresult, self._continuations, self._json = extractContinuation(self._html, type='search_videos')
        self._get_videos(self._json, isFirst=True)

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

        self._cresult, self._continuations, self._json = extractContinuation(res.text, type='search', isFirst=False)
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
            tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('primaryContents', {}).get('sectionListRenderer', {}).get('contents', {})
            for tab in tabs:
                if tab.get('itemSectionRenderer', {}).get('contents', {}):
                    return tab.get('itemSectionRenderer', {}).get('contents', {})
        else:
            onResponseReceivedActions = ytData.get('onResponseReceivedCommands', {})
            for action in onResponseReceivedActions:
                if action.get('appendContinuationItemsAction', {}).get('continuationItems', [])[0].get('itemSectionRenderer', {}).get('contents', {}):
                    return action.get('appendContinuationItemsAction', {}).get('continuationItems', [])[0].get('itemSectionRenderer', {}).get('contents', {})
        return []

    def _extract_video_data_from_content(self, content):
        videoGrid = content.get('videoRenderer', {})
        if videoGrid:
            video_data = {
                'video_id': videoGrid.get('videoId', {}),
                'keyword': videoGrid.get('ownerText', {}).get('runs', [])[0].get('navigationEndpoint', {}).get('browseEndpoint', {}).get('browseId', {}),
                'title': videoGrid.get('title', {}).get('runs', [])[0].get('text', {}),
                'published': videoGrid.get('publishedTimeText', {}).get('simpleText', {}),
                'length': self._get_length(videoGrid.get('lengthText').get('simpleText')) if videoGrid.get('lengthText') else None,
                'views': int(re.sub(r"\D", "", videoGrid.get('viewCountText').get('simpleText'))) if videoGrid.get('viewCountText', {}) != {} and "simpleText" in videoGrid.get('viewCountText', {}) else None,
                'description': videoGrid.get('detailedMetadataSnippets', [])[0].get('snippetText', {}).get('runs', [])[0].get('text', {}) if videoGrid.get('detailedMetadataSnippets', {}) else None,
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

    def extract(self):
        return self._videoList