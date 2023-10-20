from sbn_yt_pu_crawler._module.function import *
from sbn_yt_pu_crawler._module.variable import *

class Info:

    def __init__(self, video_id: str, withStats: bool = False, isShorts: bool = None):

        self._video_id = video_id
        self._withStats = withStats
        self._isShorts = isShorts

        self.videoList = []

        if len(video_id) != 11:
            raise Exception('올바른 영상 ID를 입력해주세요.')

        self._html, self._cookies = getInitPage(self._video_id, 'video')

        if self._html is None:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: HTML을 불러올 수 없음)')

        self._microformat, self._videoDetails, self._ytPlayerData, self._ytInitData = self._extract_source_in_html(self._html)

        self._details = self._get_details(self._ytInitData)
        self._status = self._get_status(self._microformat, self._videoDetails, self._ytPlayerData)

        if self._status == 'PRIVATE':
            raise Exception('비공개 영상입니다.')

        if self._isShorts is None:
            self._isShorts = self._get_shorts_info()

        self._videoInfo = self._get_video_info(self._videoDetails, self._status, self._details, self._microformat, self._isShorts, self._ytInitData)


    def _extract_source_in_html(self, html):
        reCompYtPlayerInfo = re.compile(YOUTUBE_PLAYER_VARIABLE_NAME + ' = ({.*?});', re.DOTALL)
        searchCompYtPlayerInfo = reCompYtPlayerInfo.search(html)

        if searchCompYtPlayerInfo is None:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: ytPlayerResponse를 찾을 수 없음)')

        strYTPlayer = searchCompYtPlayerInfo.group(1)
        ytPlayerData = json.loads(strYTPlayer)

        reCompYtInitInfo = re.compile(YOUTUBE_INIT_VARIABLE_NAME + ' = ({.*?});', re.DOTALL)
        searchCompYtInitInfo = reCompYtInitInfo.search(html)

        if searchCompYtInitInfo is None:
            raise Exception('영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: ytInitialResponse를 찾을 수 없음)')

        strYTInit = searchCompYtInitInfo.group(1)
        ytInitData = json.loads(strYTInit)

        microformat = None

        if ytPlayerData.get('microformat', {}) is not None:
            microformat = ytPlayerData.get('microformat', {}).get('playerMicroformatRenderer')

        videoDetails = None

        if ytPlayerData.get('videoDetails', {}) is not None:
            videoDetails = ytPlayerData.get('videoDetails')

        return microformat, videoDetails, ytPlayerData, ytInitData

    def _get_status(self, microformat, videoDetails, ytData):

        if videoDetails is None:
            return 'PRIVATE'

        playabilityStatus = ytData.get('playabilityStatus').get('status')
        playabilityError = ytData.get('playabilityStatus').get('errorScreen', {}).get('playerLegacyDesktopYpcOfferRenderer', {}).get('itemTitle', {})

        if playabilityStatus == 'LOGIN_REQUIRED' and videoDetails.get('isPrivate') is False:
            return 'PUBLIC'
        elif playabilityStatus == 'LOGIN_REQUIRED' and videoDetails.get('isPrivate') is True:
            return 'PRIVATE'
        elif playabilityStatus == 'UNPLAYABLE' and playabilityError == '회원 전용 콘텐츠':
            return 'MEMBERSHIP'
        elif playabilityStatus == 'UNPLAYABLE':
            return 'UNPLAYABLE'
        elif playabilityStatus == 'ERROR':
            return 'ERROR'
        elif playabilityStatus == 'OK':
            if microformat is not None and microformat.get('isUnlisted'):
                return 'UNLISTED'
            elif videoDetails is not None and videoDetails.get('isPrivate'):
                return 'PRIVATE'
            else:
                return 'PUBLIC'
        else:
            return 'UNKNOWN'

    def _get_details(self, ytData):
        def get_likes(ytData):
            likes = None
            contents = ytData.get('contents', {}).get('twoColumnWatchNextResults', {}).get('results', {}).get('results',
                                                                                                              {}).get(
                'contents')

            for items in contents:
                if items.get('videoPrimaryInfoRenderer') is not None and items.get('videoPrimaryInfoRenderer', {}).get(
                        'videoActions') is not None:
                    topLevelButtons = items.get('videoPrimaryInfoRenderer', {}).get('videoActions').get('menuRenderer',
                                                                                                        {}).get(
                        'topLevelButtons', {})

                    if topLevelButtons is not None:
                        try:
                            for button in topLevelButtons:
                                if 'segmentedLikeDislikeButtonRenderer' in button:
                                    likes = int(button.get('segmentedLikeDislikeButtonRenderer').get('likeCount'))
                        except:
                            likes = None

            return likes

        def get_gameTags(ytData):
            gameTags = []

            contents = ytData.get('contents', {}).get('twoColumnWatchNextResults', {}).get('results', {}).get('results',
                                                                                                              {}).get(
                'contents')

            for items in contents:
                if items.get('videoSecondaryInfoRenderer') is not None and items.get('videoSecondaryInfoRenderer').get(
                        'metadataRowContainer', {}).get('metadataRowContainerRenderer').get('rows') is not None:
                    videoSecondaryInfoRendererRows = items.get('videoSecondaryInfoRenderer', {}).get(
                        'metadataRowContainer').get('metadataRowContainerRenderer').get('rows')

                    for row in videoSecondaryInfoRendererRows:
                        if row.get('richMetadataRowRenderer') is not None and \
                                row.get('richMetadataRowRenderer').get('contents') is not None:
                            MetadataContents = row.get('richMetadataRowRenderer').get('contents')

                            for content in MetadataContents:
                                tag = content.get('richMetadataRenderer').get('title').get('simpleText')

                                if tag is not None:
                                    gameTags.append(tag)

            return gameTags

        def get_musicTags(ytData):
            musicTags = []

            for panel in ytData.get('engagementPanels', {}):
                if panel.get('engagementPanelSectionListRenderer') is not None and \
                        panel.get('engagementPanelSectionListRenderer').get('content').get(
                            'structuredDescriptionContentRenderer') is not None:
                    rendererItems = panel.get('engagementPanelSectionListRenderer').get('content').get(
                        'structuredDescriptionContentRenderer').get('items')
                    if rendererItems is not None:
                        try:
                            for item in rendererItems:
                                if item.get('videoDescriptionMusicSectionRenderer') is not None:
                                    carouselLockups = item.get('videoDescriptionMusicSectionRenderer').get(
                                        'carouselLockups')
                                    for carouselLockup in carouselLockups:
                                        if carouselLockup.get('carouselLockupRenderer').get('infoRows')[0].get(
                                                'infoRowRenderer').get('title').get('simpleText') == '노래' and 'runs' in \
                                                carouselLockup.get('carouselLockupRenderer').get('infoRows')[0].get(
                                                        'infoRowRenderer').get('defaultMetadata'):
                                            musicTags.append(
                                                carouselLockup.get('carouselLockupRenderer').get('infoRows')[0].get(
                                                    'infoRowRenderer').get('defaultMetadata').get('runs')[0].get(
                                                    'navigationEndpoint').get('watchEndpoint').get('videoId'))
                                        elif carouselLockup.get('carouselLockupRenderer').get('infoRows')[0].get(
                                                'infoRowRenderer').get('title').get(
                                                'simpleText') == '노래' and 'runs' not in \
                                                carouselLockup.get('carouselLockupRenderer').get('infoRows')[0].get(
                                                        'infoRowRenderer').get('defaultMetadata'):
                                            musicTags.append(self._video_id)
                                        elif '노래' in carouselLockup.get('carouselLockupRenderer').get(
                                                'videoLockup').get('compactVideoRenderer').get('shortBylineText').get(
                                                'simpleText'):
                                            musicTags.append(
                                                carouselLockup.get('carouselLockupRenderer').get('videoLockup').get(
                                                    'compactVideoRenderer').get('navigationEndpoint').get(
                                                    'watchEndpoint').get('videoId'))
                                        else:
                                            pass
                        except:
                            pass

            return musicTags

        def get_subscribers(ytData):
            subscribers = None

            if ytData.get('contents').get('twoColumnWatchNextResults').get('results').get('results').get('contents'):
                data = ytData.get('contents').get('twoColumnWatchNextResults').get('results').get('results').get(
                    'contents')
                for item in data:
                    if item.get('videoSecondaryInfoRenderer') is not None and \
                            item.get('videoSecondaryInfoRenderer').get('owner').get('videoOwnerRenderer').get(
                                'subscriberCountText') is not None:
                        subscribers = item.get('videoSecondaryInfoRenderer').get('owner').get('videoOwnerRenderer').get(
                            'subscriberCountText').get('accessibility').get('accessibilityData').get('label')
                        subscribers = subscribers.replace('구독자 ', '').replace('명', '')
                        if subscribers.isdigit():
                            subscribers = int(subscribers)
                            break
                        else:
                            subscribers_int = float(subscribers[:-1].replace(',', ''))
                            subscribers_unit = subscribers[-1]
                            if subscribers_unit == '천':
                                subscribers_int = subscribers_int * 1000
                            elif subscribers_unit == '만':
                                subscribers_int = subscribers_int * 10000
                            elif subscribers_unit == '억':
                                subscribers_int = subscribers_int * 100000000
                            else:
                                subscribers_int = subscribers_int * 1

                            subscribers = int(subscribers_int)
                            break
                    else:
                        subscribers = None

            return subscribers

        try:
            return {
                'likes': get_likes(ytData),
                'gameTags': get_gameTags(ytData),
                'musicTags': get_musicTags(ytData),
                'subscribers': get_subscribers(ytData)
            }

        except Exception as e:
            return False, f'영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: {e})'

    def _get_shorts_info(self):
        try:
            video_url = SHORTS_URL + parse.quote(self._video_id) + '?gl=kr&hl=ko'
            response = requests.get(video_url, allow_redirects=False)

            if response.status_code == 200:
                return True
            elif response.status_code // 100 == 3:
                return False
            else:
                raise Exception(f'영상 정보를 불러오는 중 오류가 발생했습니다. (Status Code: {response.status_code})')

        except Exception as e:
            return False, f'영상 정보를 불러오는 중 오류가 발생했습니다. (Exception: {e})'

    def _get_comments(self, html):
        cresult, nextParams, json = extractContinuation(html, type='video')

        if cresult is False:
            return None
        else:
            iresult, configs = getInitConfigs(html)
            if iresult is False:
                return None
            else:
                endpoint = nextParams['endpoint']
                clickTrackingParams = nextParams['clickTrackingParams']
                command = nextParams['command']

                url = 'https://www.youtube.com{e}?key={q}'.format(e=endpoint.get('apiUrl'),
                                                                  q=parse.quote(configs['apikey']))
                body = {'continuation': command['token'],
                        'context': {
                            'client': configs['context']['client'],
                            'user': configs['context']['user'],
                            'request': configs['context']['request'],
                            'clickTracking': {
                                'clickTrackingParams': clickTrackingParams
                            }
                        }
                        }
                response = requests.post(url=url,
                                         data=json.dumps(body),
                                         cookies=self._cookies,
                                         headers={'Content-Type': 'application/json',
                                                  'x-youtube-client-name': str(configs['clientName']),
                                                  'x-youtube-client-version': configs['clientVersion']})


                if response.status_code == 200:
                    commentsResponse = json.loads(response.text)
                    onResponseReceivedEndpoints = commentsResponse.get('onResponseReceivedEndpoints', [])

                    for endpoint in onResponseReceivedEndpoints:
                        if endpoint.get('reloadContinuationItemsCommand') is not None and endpoint.get(
                                'reloadContinuationItemsCommand').get('continuationItems') is not None:
                            continuationItems = endpoint.get('reloadContinuationItemsCommand').get('continuationItems')

                            for items in continuationItems:

                                if items.get('commentsHeaderRenderer') is not None and items.get(
                                        'commentsHeaderRenderer').get('countText') is not None:
                                    commentsItems = items.get('commentsHeaderRenderer').get('countText').get('runs')

                                    for commentsitem in commentsItems:
                                        if commentsitem.get('text') is not None and commentsitem.get('text') not in [
                                            '댓글 ', '개']:
                                            comments = commentsitem.get('text')
                                            return int(comments.replace(',', ''))
                    return None

                else:
                    return None

    def _get_video_info(self, videoDetails, status, details, microformat, isShorts, ytInitData):
        VideoInfo = {}

        VideoInfo['video_id'] = videoDetails.get('videoId')
        VideoInfo['channel_id'] = videoDetails.get('channelId')
        VideoInfo['title'] = videoDetails.get('title')
        VideoInfo['description'] = videoDetails.get('shortDescription')
        VideoInfo['videoLength'] = int(videoDetails.get('lengthSeconds'))
        VideoInfo['category'] = microformat.get('category')
        VideoInfo['status'] = status
        VideoInfo['keywords'] = videoDetails.get('keywords') if videoDetails.get('keywords') is not None else []
        VideoInfo['gameTags'] = details.get('gameTags')
        VideoInfo['musicTags'] = details.get('musicTags')
        VideoInfo['isPaid'] = ytInitData.get('paidContentOverlay') is not None
        VideoInfo['isShorts'] = isShorts
        VideoInfo['isLive'] = videoDetails.get('isLiveContent')
        VideoInfo['liveDetails'] = microformat.get('liveBroadcastDetails') if microformat.get('liveBroadcastDetails') is not None and videoDetails.get('isLiveContent') is True else None
        VideoInfo['isPremiere'] = True if microformat.get('liveBroadcastDetails') is not None and videoDetails.get('isLiveContent') is False else False
        VideoInfo['premiereDetails'] = microformat.get('liveBroadcastDetails') if microformat.get('liveBroadcastDetails') is not None and videoDetails.get('isLiveContent') is False else None
        VideoInfo['publishDate'] = microformat.get('publishDate')
        VideoInfo['uploadDate'] = microformat.get('uploadDate')
        VideoInfo['allowRatings'] = videoDetails.get('allowRatings')

        if VideoInfo['liveDetails'] is not None:
            del VideoInfo['liveDetails']['isLiveNow']

        if self._withStats:
            VideoInfo['viewCount'] = videoDetails.get('viewCount')
            VideoInfo['likes'] = details.get('likes')
            VideoInfo['comments'] = self._get_comments(self._html)
            VideoInfo['subscribers'] = details.get('subscribers')

        return VideoInfo

    def extract(self):
        return self._videoInfo