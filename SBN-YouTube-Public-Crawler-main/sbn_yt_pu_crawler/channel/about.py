from sbn_yt_pu_crawler._module.function import *
from sbn_yt_pu_crawler._module.variable import *

class About:
    def __init__(self, channel_id: str):

        self._channel_id = channel_id

        if len(self._channel_id) != 24:
            raise Exception('올바른 채널 ID를 입력해주세요.')

        self._html, self._cookies = getInitPage(self._channel_id, 'channel_about')

        if self._html is None:
            raise Exception('채널 정보를 불러오는 중 오류가 발생했습니다. (Exception: HTML을 불러올 수 없음)')

        self.ytData = self._extract_source_in_html(self._html)

        if self.ytData is None:
            raise Exception('채널 정보를 불러오는 중 오류가 발생했습니다. (Exception: ytData를 추출할 수 없음)')

        self._stats = self._get_stats(self.ytData)

        self._channel_info = self._get_channel_info(self.ytData, self._stats)

        if self._channel_info is None:
            raise Exception('채널 정보를 불러오는 중 오류가 발생했습니다. (Exception: 채널 정보를 추출할 수 없음)')

    def _extract_source_in_html(self, html):
        reCompYtInfo = re.compile(YOUTUBE_INIT_VARIABLE_NAME + ' = ({.*?});', re.DOTALL)
        searchCompYtInfo = reCompYtInfo.search(html)
        if searchCompYtInfo is None: return False, 'can not found ytinitialdata variable'

        strYTInit = searchCompYtInfo.group(1)
        ytData = json.loads(strYTInit)

        return ytData

    def _get_links(self, ytData):
        links = []

        tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])

        for tab in tabs:
            contents = tab.get('tabRenderer', {}).get('content', {}).get('sectionListRenderer', {}).get('contents', [])

            for content in contents:
                items = content.get('itemSectionRenderer', {}).get('contents', [])

                for item in items:
                    link_props = item.get('channelAboutFullMetadataRenderer', {}).get('links', [])


        for link in link_props:

            links.append({'title' : link.get('channelExternalLinkViewModel', {}).get('title', {}).get('content', {}), 'link' : link.get('channelExternalLinkViewModel', {}).get('link', {}).get('content', {})})

        return links

    def _get_stats(self, ytData):

        def get_subscribers(ytData):
            subscribers = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('subscriberCountText', {}).get('accessibility', {}).get('accessibilityData', {}).get('label', {})
            subscribers = subscribers.replace('구독자 ', '').replace('명', '')
            if subscribers.isdigit():
                subscribers = int(subscribers)
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

            return subscribers

        def get_viewCount(ytData):
            tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', {})

            for tab in tabs:
                contents = tab.get('tabRenderer', {}).get('content', {}).get('sectionListRenderer', {}).get('contents',{})

                for content in contents:
                    items = content.get('itemSectionRenderer', {}).get('contents', {})

                    for item in items:
                        viewCount = item.get('channelAboutFullMetadataRenderer', {}).get('viewCountText', {}).get('simpleText',{})

            viewCount = int(viewCount.replace('조회수 ', '').replace('회', '').replace(',', ''))

            return viewCount

        def get_videoCount(ytData):
            videoCount = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('videosCountText', {}).get('runs', {})[1].get('text', {})
            videoCount = videoCount.replace('동영상 ', '').replace('개', '')
            if videoCount.isdigit():
                videoCount = int(videoCount)
            else:
                videoCount_int = float(videoCount[:-1].replace(',', ''))
                videoCount_unit = videoCount[-1]
                if videoCount_unit == '천':
                    videoCount_int = videoCount_int * 1000
                elif videoCount_unit == '만':
                    videoCount_int = videoCount_int * 10000
                elif videoCount_unit == '억':
                    videoCount_int = videoCount_int * 100000000
                else:
                    videoCount_int = videoCount_int * 1

                videoCount = int(videoCount_int)

            return videoCount

        def get_joinedDate(ytData):
            tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', {})

            for tab in tabs:
                contents = tab.get('tabRenderer', {}).get('content', {}).get('sectionListRenderer', {}).get('contents',{})

                for content in contents:
                    items = content.get('itemSectionRenderer', {}).get('contents', {})

                    for item in items:
                        joinedDate = item.get('channelAboutFullMetadataRenderer', {}).get('joinedDateText', {}).get('runs',{})[1].get('text', {})

            joinedDate = datetime.strptime(joinedDate, "%Y. %m. %d.")
            joinedDate = joinedDate.strftime("%Y-%m-%d")

            return joinedDate


        try:
            return {
                'subscribers': get_subscribers(ytData),
                'viewCount': get_viewCount(ytData),
                'videoCount': get_videoCount(ytData),
                'joinedDate': get_joinedDate(ytData),
            }

        except Exception as e:
            return False, f'채널 정보를 불러오는 중 오류가 발생했습니다. (Exception: {e})'

    def _get_country(self, ytData):

        tabs = ytData.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])

        for tab in tabs:
            contents = tab.get('tabRenderer', {}).get('content', {}).get('sectionListRenderer', {}).get('contents',[])

            for content in contents:
                items = content.get('itemSectionRenderer', {}).get('contents', [])

                for item in items:
                    country = item.get('channelAboutFullMetadataRenderer', {}).get('country', []).get('simpleText', {})

        return country

    def _get_channel_info(self, ytData, stats):
        ChannelInfo = {}

        ChannelInfo['channel_id'] = self._channel_id
        ChannelInfo['title'] = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('title', {})
        ChannelInfo['handle'] = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('channelHandleText', {}).get('runs', [])[0].get('text', {})
        ChannelInfo['description'] = ytData.get('metadata', {}).get('channelMetadataRenderer', {}).get('description', {})
        ChannelInfo['keywords'] = ytData.get('metadata', {}).get('channelMetadataRenderer', {}).get('keywords', {})
        ChannelInfo['thumbnail'] = ytData.get('metadata', {}).get('channelMetadataRenderer', {}).get('avatar', {}).get('thumbnails', [])[0].get('url', {})
        ChannelInfo['banner'] = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('banner', {}).get('thumbnails', [])[-1].get('url', {})
        ChannelInfo['verified'] = ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('badges', {})[0].get('metadataBadgeRenderer', {}).get('icon', {}).get('iconType', {}) if ytData.get('header', {}).get('c4TabbedHeaderRenderer', {}).get('badges', {}) else 'NONE'
        ChannelInfo['links'] = self._get_links(ytData)
        ChannelInfo['subscribers'] = stats['subscribers']
        ChannelInfo['viewCount'] = stats['viewCount']
        ChannelInfo['videoCount'] = stats['videoCount']
        ChannelInfo['joinedDate'] = stats['joinedDate']
        ChannelInfo['country'] = self._get_country(ytData)

        return ChannelInfo

    def extract(self):
        return self._channel_info