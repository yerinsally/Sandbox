from sbn_yt_pu_crawler._module.variable import *

import requests
import re
import json
from urllib import parse
from datetime import datetime

def getInitPage(query, type):
    if type == 'video':
        url = VIDEO_URL + parse.quote(query) + '?gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    elif type == 'channel_about':
        url = CHANNEL_URL + parse.quote(query) + '/about?gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    elif type == 'channel_videos':
        url = CHANNEL_URL + parse.quote(query) + '/videos?gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    elif type == 'channel_shorts':
        url = CHANNEL_URL + parse.quote(query) + '/shorts?gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    elif type == 'channel_streams':
        url = CHANNEL_URL + parse.quote(query) + '/streams?gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    elif type == 'search_videos' or type == 'search_channel':
        url = SEARCH_URL + parse.quote(query) + '&gl=kr&hl=ko'
        response = requests.get(url)
        if response.status_code == 200:
            cookies = response.cookies.get_dict()
            return response.text, cookies
        return None
    else:
        return None

def getInitConfigs(html):
    try:
        fitYtCfgInindex = html.find('ytcfg.set({"CLIENT_CANARY_STATE')
        if fitYtCfgInindex > 0:
            fitUtCfgText = html[fitYtCfgInindex:]
            reCompYtInfo = re.compile(YOUTUBE_CONFIG_VARIABLE_NAME + '\(({.*?})\);', re.DOTALL)
            searchConfig = reCompYtInfo.search(fitUtCfgText)
            strYTCfg = searchConfig.group(1)
            ytCfg = json.loads(strYTCfg)
            return True, {'token': ytCfg.get('XSRF_TOKEN'),
                          'apikey': ytCfg.get('INNERTUBE_API_KEY'),
                          'context': ytCfg.get('INNERTUBE_CONTEXT'),
                          'clientVersion': ytCfg['INNERTUBE_CLIENT_VERSION'],
                          'clientName': ytCfg['INNERTUBE_CONTEXT_CLIENT_NAME']}
    except Exception as e:
        print(e)
        return False, None

def extractContinuation(html, type, isFirst=True):
    if (type == 'channel_videos' or type == 'channel_streams' or type == 'search' or type == 'search_videos' or type == 'search_channel') and not isFirst:
        ytData = json.loads(html)
    else:
        reCompYtInfo = re.compile(YOUTUBE_INIT_VARIABLE_NAME + ' = ({.*?});', re.DOTALL)
        searchCompYtInfo = reCompYtInfo.search(html)
        if searchCompYtInfo is None: return False, 'can not found ytinitialdata variable', None

        strYTInit = searchCompYtInfo.group(1)
        ytData = json.loads(strYTInit)

    if type == 'video':

        PrimaryInfos = ytData.get('contents', {}). \
            get('twoColumnWatchNextResults', {}). \
            get('results', {}). \
            get('results', {}). \
            get('contents')

        ItemSection = None

        for items in PrimaryInfos:
            if items.get('itemSectionRenderer'):
                identifier = items.get('itemSectionRenderer', {}).get('sectionIdentifier')
                if identifier is not None and identifier == 'comment-item-section':
                    ItemSection = items.get('itemSectionRenderer')

        if ItemSection is None: return False, None, None

        contents = ItemSection.get('contents')
        if contents is not None and len(contents) > 0:
            continuationItem = contents[0].get('continuationItemRenderer')
            if continuationItem:
                endpoint = continuationItem.get('continuationEndpoint', {}). \
                    get('commandMetadata', {}). \
                    get('webCommandMetadata')
                command = continuationItem.get('continuationEndpoint', {}). \
                    get('continuationCommand', {})
                return True, {'endpoint': endpoint,
                              'command': command,
                              'clickTrackingParams': ItemSection['trackingParams']}, None
        return False, None, None

    elif type == 'channel_videos' and isFirst:

        PrimaryInfos = ytData.get('contents', {}). \
            get('twoColumnBrowseResultsRenderer', {}). \
            get('tabs', {})

        for PrimaryInfo in PrimaryInfos:
            if PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('contents', {}):
                contents = PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('contents', {})

                for content in contents:
                    if content.get('continuationItemRenderer', {}):

                        continuationItem = content.get('continuationItemRenderer', {})

                        endpoint = continuationItem.get('continuationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('continuationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, ytData

        return False, None, ytData

    elif type == 'channel_videos' and not isFirst:

        PrimaryInfos = ytData.get('onResponseReceivedActions', {})

        for PrimaryInfo in PrimaryInfos:
            if PrimaryInfo.get('appendContinuationItemsAction', {}).get('continuationItems', {}):
                contents = PrimaryInfo.get('appendContinuationItemsAction', {}).get('continuationItems', {})

                for content in contents:
                    if content.get('continuationItemRenderer', {}):

                        continuationItem = content.get('continuationItemRenderer', {})

                        endpoint = continuationItem.get('continuationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('continuationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, None

            if PrimaryInfo.get('reloadContinuationItemsCommand', {}).get('continuationItems', {}):
                contents = PrimaryInfo.get('reloadContinuationItemsCommand', {}).get('continuationItems', {})

                for content in contents:
                    if content.get('continuationItemRenderer', {}):

                        continuationItem = content.get('continuationItemRenderer', {})

                        endpoint = continuationItem.get('continuationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('continuationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, None

        return False, None, None


    elif type == 'channel_videos_popular':

        PrimaryInfos = ytData.get('contents', {}). \
            get('twoColumnBrowseResultsRenderer', {}). \
            get('tabs', {})

        for PrimaryInfo in PrimaryInfos:

            if PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('header', {}).get('feedFilterChipBarRenderer', {}).get('contents', {}):

                contents = PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('header', {}).get('feedFilterChipBarRenderer', {}).get('contents', {})

                for content in contents:
                    if content.get('chipCloudChipRenderer', {}) and content.get('chipCloudChipRenderer', {}).get('text', {}).get('simpleText', {}) == '인기순':

                        continuationItem = content.get('chipCloudChipRenderer', {})

                        endpoint = continuationItem.get('navigationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('navigationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, ytData

            # else:
            #     raise Exception('인기순 필터가 없습니다.')

    elif type == 'channel_videos_oldest':

        PrimaryInfos = ytData.get('contents', {}). \
            get('twoColumnBrowseResultsRenderer', {}). \
            get('tabs', {})

        for PrimaryInfo in PrimaryInfos:

            if PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get('header', {}).get(
                    'feedFilterChipBarRenderer', {}).get('contents', {}):

                contents = PrimaryInfo.get('tabRenderer', {}).get('content', {}).get('richGridRenderer', {}).get(
                    'header', {}).get('feedFilterChipBarRenderer', {}).get('contents', {})

                for content in contents:
                    if content.get('chipCloudChipRenderer', {}) and content.get('chipCloudChipRenderer', {}).get('text',{}).get('simpleText', {}) == '날짜순':
                        continuationItem = content.get('chipCloudChipRenderer', {})

                        endpoint = continuationItem.get('navigationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('navigationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, ytData

            # else:
            #     raise Exception('날짜순 필터가 없습니다.')


        return False, None, None

    elif type == 'search' and isFirst:

        PrimaryInfos = ytData.get('contents', {}). \
            get('twoColumnSearchResultsRenderer', {}). \
            get('primaryContents', {}). \
            get('sectionListRenderer', {}). \
            get('contents', {})

        for content in PrimaryInfos:
            if content.get('continuationItemRenderer', {}):

                continuationItem = content.get('continuationItemRenderer', {})

                endpoint = continuationItem.get('continuationEndpoint', {}). \
                    get('commandMetadata', {}). \
                    get('webCommandMetadata')
                command = continuationItem.get('continuationEndpoint', {}). \
                    get('continuationCommand', {})

                return True, {'endpoint': endpoint, 'command': command}, ytData

        return False, None, ytData

    elif type == 'search' and not isFirst:

        PrimaryInfos = ytData.get('onResponseReceivedCommands', {})

        for PrimaryInfo in PrimaryInfos:

            if PrimaryInfo.get('appendContinuationItemsAction', {}).get('continuationItems', {}):

                contents = PrimaryInfo.get('appendContinuationItemsAction', {}).get('continuationItems', {})

                for content in contents:

                    if content.get('continuationItemRenderer', {}):

                        continuationItem = content.get('continuationItemRenderer', {})

                        endpoint = continuationItem.get('continuationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')
                        command = continuationItem.get('continuationEndpoint', {}). \
                            get('continuationCommand', {})

                        return True, {'endpoint': endpoint, 'command': command}, ytData

        return False, None, None

    elif type == 'search_videos':

        PrimaryInfos = ytData.get('header', {}). \
            get('searchHeaderRenderer', {}). \
            get('searchFilterButton', {}). \
            get('buttonRenderer', {}). \
            get('command', {}). \
            get('openPopupAction', {}). \
            get('popup', {}). \
            get('searchFilterOptionsDialogRenderer', {}). \
            get('groups', {})

        for PrimaryInfo in PrimaryInfos:

            if PrimaryInfo.get('searchFilterGroupRenderer', {}).get('filters', {}):

                contents = PrimaryInfo.get('searchFilterGroupRenderer', {}).get('filters', {})

                for content in contents:
                    if content.get('searchFilterRenderer', {}) and content.get('searchFilterRenderer', {}).get('label', {}).get('simpleText', {}) == '동영상':

                        continuationItem = content.get('searchFilterRenderer', {})

                        endpoint = continuationItem.get('navigationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')

                        apiUrl = endpoint.get('url', {})

                        url = "https://www.youtube.com" + apiUrl

                        response = requests.get(url)
                        if response.status_code == 200:
                            return extractContinuation(response.text, 'search')

            # else:
            #     raise Exception('인기순 필터가 없습니다.')

    elif type == 'search_channel':

        PrimaryInfos = ytData.get('header', {}). \
            get('searchHeaderRenderer', {}). \
            get('searchFilterButton', {}). \
            get('buttonRenderer', {}). \
            get('command', {}). \
            get('openPopupAction', {}). \
            get('popup', {}). \
            get('searchFilterOptionsDialogRenderer'
                                                                                                                                                                                                           , {}). \
            get('groups', {})

        for PrimaryInfo in PrimaryInfos:

            if PrimaryInfo.get('searchFilterGroupRenderer', {}).get('filters', {}):

                contents = PrimaryInfo.get('searchFilterGroupRenderer', {}).get('filters', {})

                for content in contents:
                    if content.get('searchFilterRenderer', {}) and content.get('searchFilterRenderer', {}).get('label', {}).get('simpleText', {}) == '채널':

                        continuationItem = content.get('searchFilterRenderer', {})

                        endpoint = continuationItem.get('navigationEndpoint', {}). \
                            get('commandMetadata', {}). \
                            get('webCommandMetadata')

                        apiUrl = endpoint.get('url', {})

                        url = "https://www.youtube.com" + apiUrl

                        response = requests.get(url)
                        if response.status_code == 200:
                            return extractContinuation(response.text, 'search')

            # else:
            #     raise Exception('날짜순 필터가 없습니다.')


        return False, None, None