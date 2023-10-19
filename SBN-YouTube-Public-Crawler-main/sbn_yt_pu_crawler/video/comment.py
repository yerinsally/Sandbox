from sbn_yt_pu_crawler._module.function import *


class Comment:

    def __init__(self, video_id: str, limit=200, withStats: bool = False, withReply: bool = False, withAuthor: bool = False):

        self._video_id = video_id
        self._limit = limit
        self._withStats = withStats
        self._withReply = withReply
        self._withAuthor = withAuthor

        self._commentList = []

        if len(video_id) != 11:
            raise Exception('올바른 영상 ID를 입력해주세요.')

        self._html, self._cookies = getInitPage(self._video_id, 'video')

        if self._html is None:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: HTML을 불러올 수 없음)')

        self._cresult, self._continuations, self._json = extractContinuation(self._html, type='video')

        if self._cresult is False:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: Continuation을 불러올 수 없음)')

        self._iresult, self._config = getInitConfigs(self._html)

        if self._iresult is False:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: Config를 불러올 수 없음)')

        self._result, self._message = self._get_comments(self._config, self._continuations, True)

    def _get_replies(self, configs, nextParams, isFirst=False):
        def make_url(endpoint):
            return f'https://www.youtube.com{endpoint["apiUrl"]}?key={parse.quote(configs["apikey"])}'

        def make_request_body(command, clickTrackingParams):
            return {
                'continuation': command['token'],
                'context': {
                    'client': configs['context']['client'],
                    'user': configs['context']['user'],
                    'request': configs['context']['request'],
                    'clickTracking': {
                        'clickTrackingParams': clickTrackingParams
                    }
                }
            }

        def send_request(url, body):
            return requests.post(url=url,
                                 data=json.dumps(body),
                                 cookies=configs.get('cookies', {}),
                                 headers={
                                     'Content-Type': 'application/json',
                                     'x-youtube-client-name': str(configs['clientName']),
                                     'x-youtube-client-version': configs['clientVersion']
                                 })

        def parse_response(commentResponse):
            responseRecievedEndPoint = commentResponse.get('onResponseReceivedEndpoints')
            if isFirst:
                return responseRecievedEndPoint[0].get('appendContinuationItemsAction', {}).get('continuationItems', [])
            else:
                return responseRecievedEndPoint[0].get('appendContinuationItemsAction', {}).get('continuationItems', [])

        def get_props_in_continuation(continuationItems):
            comments = []

            nextEndpoint = None
            nextCommand = None
            nextClickTrack = None

            for item in continuationItems:
                commentRenderer = item.get('commentRenderer')
                continuationItemRenderer = item.get('continuationItemRenderer')

                if commentRenderer:
                    commentTextRuns = commentRenderer.get('contentText', {}).get('runs', [])
                    comment = ''.join([i.get('text', '') for i in commentTextRuns])
                    comment_json = {"comment": comment}

                    if self._withStats:
                        likes = commentRenderer.get('actionButtons').get('commentActionButtonsRenderer').get('likeButton').get('toggleButtonRenderer').get('accessibilityData').get('accessibilityData').get('label')
                        comment_json["likes"] = int(re.sub(r"\D", "", likes))

                    if self._withAuthor:
                        author = commentRenderer.get('authorText', {}).get('simpleText', '')
                        comment_json["author"] = author


                    comments.append(comment_json)

                elif continuationItemRenderer:
                    nextEndpoint = continuationItemRenderer.get('button', {}).get('buttonRenderer', {}).get('command', {}).get('commandMetadata', {}).get('webCommandMetadata')
                    nextCommand = continuationItemRenderer.get('button', {}).get('buttonRenderer', {}).get('command', {}).get('continuationCommand')
                    nextClickTrack = continuationItemRenderer.get('button', {}).get('buttonRenderer', {}).get('command', {}).get('clickTrackingParams')

            return comments, {'endpoint': nextEndpoint, 'command': nextCommand, 'clickTrackingParams': nextClickTrack}

        def extract_comments_and_next_params(continuationItems):
            comments = []

            comments_props, next_params = get_props_in_continuation(continuationItems)

            comments += comments_props

            return comments, next_params

        url = make_url(nextParams['endpoint'])
        body = make_request_body(nextParams['command'], nextParams['clickTrackingParams'])
        response = send_request(url, body)

        if response.status_code == 200:
            commentResponse = json.loads(response.text)
            continuationItems = parse_response(commentResponse)
            comments, next_params = extract_comments_and_next_params(continuationItems)

        return comments, next_params

    def _nextComment(self, configs, continuations):
        return self._get_comments(configs, continuations)

    def _get_comments(self, configs, nextParams, isFirst=False):

        def make_url(endpoint):
            return f'https://www.youtube.com{endpoint["apiUrl"]}?key={parse.quote(configs["apikey"])}'

        def make_request_body(command, clickTrackingParams):
            return {
                'continuation': command['token'],
                'context': {
                    'client': configs['context']['client'],
                    'user': configs['context']['user'],
                    'request': configs['context']['request'],
                    'clickTracking': {
                        'clickTrackingParams': clickTrackingParams
                    }
                }
            }

        def send_request(url, body):
            return requests.post(url=url,
                                 data=json.dumps(body),
                                 cookies=configs.get('cookies', {}),
                                 headers={
                                     'Content-Type': 'application/json',
                                     'x-youtube-client-name': str(configs['clientName']),
                                     'x-youtube-client-version': configs['clientVersion']
                                 })

        def parse_response(commentResponse):
            responseRecievedEndPoint = commentResponse.get('onResponseReceivedEndpoints')
            if isFirst:
                return responseRecievedEndPoint[1].get('reloadContinuationItemsCommand', {}).get('continuationItems',
                                                                                                 [])
            else:
                return responseRecievedEndPoint[0].get('appendContinuationItemsAction', {}).get('continuationItems', [])

        def get_props_in_continuation(continuationItems):
            comments = []

            nextEndpoint = None
            nextCommand = None
            nextClickTrack = None

            for item in continuationItems:
                commentThreadRenderer = item.get('commentThreadRenderer')
                continuationItemRenderer = item.get('continuationItemRenderer')

                if commentThreadRenderer:
                    commentRenderer = commentThreadRenderer.get('comment', {}).get('commentRenderer')
                    commentTextRuns = commentRenderer.get('contentText', {}).get('runs', [])
                    comment = ''.join([i.get('text', '') for i in commentTextRuns])
                    comment_json = {"comment": comment}

                    if self._withStats:
                        likes = commentRenderer.get('actionButtons').get('commentActionButtonsRenderer').get('likeButton').get('toggleButtonRenderer').get('accessibilityData').get('accessibilityData').get('label')
                        comment_json["likes"] = int(re.sub(r"\D", "", likes))
                        comment_json["reply"] = commentRenderer.get('replyCount') if commentRenderer.get('replyCount') else 0

                    if self._withAuthor:
                        author = commentRenderer.get('authorText', {}).get('simpleText', '')
                        comment_json["author"] = author

                    if self._withReply:
                        comment_json["reply"] = []

                        if commentThreadRenderer.get('replies', {}):
                            nextEndpoint = commentThreadRenderer.get('replies', {}).get('commentRepliesRenderer', {}).get('contents', [])[0].get('continuationItemRenderer', {}).get('continuationEndpoint').get('commandMetadata').get('webCommandMetadata')
                            nextCommand = commentThreadRenderer.get('replies', {}).get('commentRepliesRenderer', {}).get('contents', [])[0].get('continuationItemRenderer', {}).get('continuationEndpoint').get('continuationCommand')
                            nextClickTrack = commentThreadRenderer.get('replies', {}).get('commentRepliesRenderer', {}).get('contents', [])[0].get('continuationItemRenderer', {}).get('continuationEndpoint').get('clickTrackingParams')

                            comment, nextParams = self._get_replies(configs, {'endpoint': nextEndpoint, 'command': nextCommand, 'clickTrackingParams': nextClickTrack}, isFirst=True)
                            comment_json["reply"] += comment

                            while True:
                                if nextParams['clickTrackingParams'] is None:
                                    break
                                else:
                                    comment, nextParams = self._get_replies(configs, nextParams)
                                    comment_json["reply"] += comment

                        else:
                            comment_json["reply"] = []

                    comments.append(comment_json)

                elif continuationItemRenderer:
                    nextEndpoint = continuationItemRenderer.get('continuationEndpoint', {}).get('commandMetadata',{}).get('webCommandMetadata')
                    nextCommand = continuationItemRenderer.get('continuationEndpoint', {}).get('continuationCommand')
                    nextClickTrack = continuationItemRenderer.get('continuationEndpoint', {}).get('clickTrackingParams')

            return comments, {'endpoint': nextEndpoint, 'command': nextCommand, 'clickTrackingParams': nextClickTrack}

        def extract_comments_and_next_params(continuationItems):
            comments = []

            comments_props, next_params = get_props_in_continuation(continuationItems)

            comments += comments_props

            return comments, next_params

        url = make_url(nextParams['endpoint'])
        body = make_request_body(nextParams['command'], nextParams['clickTrackingParams'])
        response = send_request(url, body)

        if response.status_code == 200:
            commentResponse = json.loads(response.text)
            continuationItems = parse_response(commentResponse)
            comments, next_params = extract_comments_and_next_params(continuationItems)

            self._commentList += comments

            if len(self._commentList) >= self._limit or next_params['clickTrackingParams'] is None:
                self._commentList = self._commentList[0:self._limit]
                return True, 'find end'

            return self._nextComment(configs, next_params)
        else:
            return False, None

    def extract(self):
        return self._commentList