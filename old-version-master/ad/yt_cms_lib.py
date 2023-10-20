from __future__ import annotations

import os
import datetime
import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from sa_package.platform.google import get_signed_in_google_driver
from sa_package.convert.number_format import convert_hannum_to_num
from sa_package.convert.time_format import convert_hhmmss_format_to_sec


TIMEOUT = 15

OVERVIEW_COLUMNS = [
    'video_id',
    'title',
    'upload_date',

    # 개요
    'views',
    'impressions',
    'click_through_rate',
    'unique_viewers',
    'watch_time',
    'avg_watch_percentage',
    'avg_watch_time(sec)',
    'avg_views_per_viewer',
    'subscribers_gained',
    'subscribers_lost',
    'subscribers',
    'comments',
    'likes',
    'dislikes',
    'sharings'
]

ADDITIONAL_DATA_COLUMNS = [
    'video_id',
    'data_type',
    'data_name',
    'value',
    'channel_id',
]

MAX_TRY_NUM = 3


def get_yt_studio_drive(login_id:str, login_pwd:str, otp_secret_key:str, auth_id:str):

    driver = get_signed_in_google_driver(login_id, login_pwd, otp_secret_key)

    assert driver is not None, "로그인 실패"
    driver.get(f"https://studio.youtube.com/owner/{auth_id}?o={auth_id}")  # 예시

    return driver


def get_yt_studio_url(info_type:str, content_id:str, auth_id:str, period:str="first_week"):

    # content_type
    # - channel
    # - video

    # info_type
    # - overview: 개요
    # - reach_viewers: 도달범위
    # - interest_viewers: 참여도
    # - build_audience: 시청자층

    # period
    # - default: 지난 28일
    # - week: 지난 7일
    # - 4_weeks: 지난 28일
    # - quarter: 지난 90일
    # - year: 지난 365일
    # - lifetime: 전체
    # - current_year
    # - minus_1_year
    # - current_month
    # - minus_1_month
    # - minus_2_month
    # - first_week
    # - first_4_weeks
    
    if info_type == "overview":
        return f"""
        https://studio.youtube.com/video/{content_id}/analytics/tab-interest_viewers/period-{period}/explore?o={auth_id}&entity_type=VIDEO&entity_id={content_id}&time_period={period}&explore_type=TABLE_AND_CHART&metric=VIEWS&comparison_metric=VIDEO_THUMBNAIL_IMPRESSIONS&granularity=DAY&t_metrics=VIEWS&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS_VTR&t_metrics=ESTIMATED_UNIQUE_VIEWERS&t_metrics=WATCH_TIME&t_metrics=AVERAGE_WATCH_PERCENTAGE&t_metrics=AVERAGE_WATCH_TIME&t_metrics=AVERAGE_VIEWS_PER_VIEWER&t_metrics=SUBSCRIBERS_GAINED&t_metrics=SUBSCRIBERS_LOST&t_metrics=SUBSCRIBERS_NET_CHANGE&t_metrics=COMMENTS&t_metrics=RATINGS_LIKES&t_metrics=RATINGS_DISLIKES&t_metrics=SHARINGS&dimension=VIDEO&o_column=VIEWS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
        """

    elif info_type == "gender":
        return f"""
        https://studio.youtube.com/video/{content_id}/analytics/tab-build_audience/period-{period}/explore?o={auth_id}&entity_type=VIDEO&entity_id={content_id}&time_period={period}&explore_type=TABLE_AND_CHART&metric=VIEWS&granularity=DAY&t_metrics=VIEWS&t_metrics=AVERAGE_WATCH_PERCENTAGE&dimension=VIEWER_GENDER&o_column=VIEWER_GENDER&o_direction=ANALYTICS_ORDER_DIRECTION_ASC
        """
        
    elif info_type == "age":
        return f"""
        https://studio.youtube.com/video/{content_id}/analytics/tab-build_audience/period-{period}/explore?o={auth_id}&entity_type=VIDEO&entity_id={content_id}&time_period={period}&explore_type=TABLE_AND_CHART&metric=VIEWS&granularity=DAY&t_metrics=VIEWS&t_metrics=AVERAGE_WATCH_PERCENTAGE&dimension=VIEWER_AGE&o_column=VIEWER_AGE&o_direction=ANALYTICS_ORDER_DIRECTION_ASC
        """

    elif info_type == "subscribe":
        return f"""
        https://studio.youtube.com/video/{content_id}/analytics/tab-build_audience/period-{period}/explore?o={auth_id}&entity_type=VIDEO&entity_id={content_id}&time_period={period}&explore_type=TABLE_AND_CHART&metric=VIEWS&granularity=DAY&t_metrics=VIEWS&t_metrics=AVERAGE_WATCH_PERCENTAGE&dimension=SUBSCRIBED_TO_UPLOADER_STATE&o_column=VIEWS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
        """
    
    elif info_type == "traffic_source":
        return f"""
        https://studio.youtube.com/video/{content_id}/analytics/tab-overview/period-{period}/explore?o={auth_id}&entity_type=VIDEO&entity_id={content_id}&time_period={period}&explore_type=TABLE_AND_CHART&metric=VIEWS&granularity=DAY&t_metrics=VIEWS&dimension=TRAFFIC_SOURCE_TYPE&o_column=VIEWS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
        """
    
    else:
        return f"https://studio.youtube.com/video/{content_id}/analytics/tab-{info_type}/period-{period}?o={auth_id}"


def crawl_cms_data(driver, video_id:str, channel_id:str, auth_id:str, spare_auth_id:str, period:str="first_week"):

    df_dict = {
        'overview': pd.DataFrame(columns=OVERVIEW_COLUMNS),
        'additional': pd.DataFrame(columns=ADDITIONAL_DATA_COLUMNS)
    }

    id_changed = False

    # ====================개요====================
    try_num = 0
    while True:
        try:
            
            driver.get(get_yt_studio_url(
                info_type="overview",
                content_id=video_id,
                auth_id=auth_id,
                period=period
            ))
            
            WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="row-container"]'))
            
            upload_date = driver.find_element(By.XPATH, '//*[@id="trigger"]/ytcp-dropdown-trigger/div/div[2]/div/div').text.split('~')[0].replace('. ', '-')[:-1]
            plus_date = driver.find_element(By.XPATH, '//*[@id="trigger"]/ytcp-dropdown-trigger/div/div[2]/div/div').text.split(' ~ ')[1].replace('. ', '-')[:-1]
            
            if period == "first_week":
                check_time = 6
            elif period == "first_4_weeks":
                check_time = 27
        
            #TODO
            #추가 필요

            if datetime.datetime.strptime(upload_date, "%Y-%m-%d").date() + datetime.timedelta(days=check_time) != datetime.datetime.strptime(plus_date, "%Y-%m-%d").date():   
                df_dict['overview'].loc[video_id, 'views'] = None 
                return df_dict
            
            print("데이터 존재")
            
            
            # ===기본 정보===
            df_dict['overview'].loc[video_id, 'video_id'] = video_id
            df_dict['overview'].loc[video_id, 'channel_id'] = channel_id
            df_dict['overview'].loc[video_id, 'title'] = driver.find_element(By.XPATH, '//*[@id="entity-title-value"]').text
            
            df_dict['overview'].loc[video_id, 'upload_date'] = upload_date
                
            
            print("개요 데이터 시작")
            

            # ===개요 데이터===
            overview_metrics = [x.text for x in driver.find_element(By.XPATH, '//*[@id="row-container"]').find_elements(By.CSS_SELECTOR, ".value")]

            # 데이터가 없는 경우 -> 다른 계정으로 로그인 해보기
            if overview_metrics[0] == "—":
                if id_changed:
                    df_dict['overview'].loc[video_id, 'views'] = '-'
                    return df_dict

                else:
                    auth_id = spare_auth_id
                    id_changed = True
                    continue

            # 조회수
            df_dict['overview'].loc[video_id, 'views'] = convert_hannum_to_num(overview_metrics[0], is_int=True)

            # 노출수
            df_dict['overview'].loc[video_id, 'impressions'] = convert_hannum_to_num(overview_metrics[1], is_int=True)

            # 노출 클릭률
            df_dict['overview'].loc[video_id, 'click_through_rate'] = convert_hannum_to_num(overview_metrics[2])

            # 순시청자수
            df_dict['overview'].loc[video_id, 'unique_viewers'] = convert_hannum_to_num(overview_metrics[3], is_int=True)

            # 시청 시간(단위: 시간)
            df_dict['overview'].loc[video_id, 'watch_time'] = convert_hannum_to_num(overview_metrics[4])

            # 평균 조회율
            df_dict['overview'].loc[video_id, 'avg_watch_percentage'] = convert_hannum_to_num(overview_metrics[5])

            # 평균 시청 지속 시간
            df_dict['overview'].loc[video_id, 'avg_watch_time(sec)'] = convert_hhmmss_format_to_sec(overview_metrics[6])

            # 시청자당 평균 조회수
            df_dict['overview'].loc[video_id, 'avg_views_per_viewer'] = convert_hannum_to_num(overview_metrics[7])

            # 구독자 증가수
            df_dict['overview'].loc[video_id, 'subscribers_gained'] = convert_hannum_to_num(overview_metrics[8], is_int=True)

            # 구독자 감소수
            df_dict['overview'].loc[video_id, 'subscribers_lost'] = convert_hannum_to_num(overview_metrics[9], is_int=True)

            # 구독자 증감
            df_dict['overview'].loc[video_id, 'subscribers'] = convert_hannum_to_num(overview_metrics[10], is_int=True)

            # 추가된 댓글 수
            df_dict['overview'].loc[video_id, 'comments'] = convert_hannum_to_num(overview_metrics[11], is_int=True)

            # 좋아요
            df_dict['overview'].loc[video_id, 'likes'] = convert_hannum_to_num(overview_metrics[12], is_int=True)

            # 싫어요
            df_dict['overview'].loc[video_id, 'dislikes'] = convert_hannum_to_num(overview_metrics[13], is_int=True)

            # 공유
            df_dict['overview'].loc[video_id, 'sharings'] = convert_hannum_to_num(overview_metrics[14], is_int=True)

            break

        except TimeoutError as e:
            error_img = driver.find_elements(By.XPATH, '//*[@id="error-image"]')
            if len(error_img) > 0:
                df_dict['overview'].loc[video_id, 'views'] = '-'
                return df_dict

            if try_num >= MAX_TRY_NUM:
                return df_dict
            else: 
                continue

        except Exception as e:
            print(e)
            break

    # ====================시청자층====================
    while True:
        try:
            driver.get(get_yt_studio_url(
                info_type="build_audience",
                content_id=video_id,
                auth_id=auth_id,
                period=period
            ))
            WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="metric-total"]'))

            # 시청자층 데이터
            build_audience_metrics = [x.text for x in driver.find_elements(By.XPATH, '//*[@id="metric-total"]')]
            avg_views = convert_hannum_to_num(build_audience_metrics[1])

            if type(avg_views) is not str and avg_views > 0:
                # 시청자당 평균 조회수
                df_dict['overview']['avg_views_per_viewer'] = avg_views


            # 구독자
            try:
                driver.get(get_yt_studio_url(
                    info_type="subscribe",
                    content_id=video_id,
                    auth_id=auth_id,
                    period=period
                ))
                WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="row-container"]'))

                subscriber_metrics = driver.find_elements(By.XPATH, '//*[@id="row-container"]')
                for idx in [1, 2]:
                    for type_idx, data_type in enumerate(['views', 'avg_watch_percentage']):

                        value = subscriber_metrics[idx].find_elements(By.CSS_SELECTOR, ".value-container")[type_idx].text
                        if '\n' in value:
                            value = value.split('\n')[-1].replace("%", "")
                        else:
                            value = value.replace("%", "")
                        
                        if '—' in value:
                            value = ""

                        new_df = pd.DataFrame(data={
                                'video_id': video_id,
                                'data_type': 'subscribe_'+data_type,
                                'data_name': subscriber_metrics[idx].find_element(By.CSS_SELECTOR, "#title-cell").text,
                                'value': value,
                                'channel_id': channel_id
                            }, index=[0])
                        df_dict['additional'] = pd.concat([df_dict['additional'], new_df], ignore_index=True)

            except Exception as e:
                print(e)
                pass

            # 연령
            try:
                driver.get(get_yt_studio_url(
                    info_type="age",
                    content_id=video_id,
                    auth_id=auth_id,
                    period=period
                ))
                WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="row-container"]'))

                age_metrics = driver.find_elements(By.XPATH, '//*[@id="row-container"]')
                for idx in range(7):
                    for type_idx, data_type in enumerate(['views', 'avg_watch_percentage']):

                        value = age_metrics[idx].find_elements(By.CSS_SELECTOR, ".value-container")[type_idx].text
                        if '—' in value:
                            value = ""
                        else:
                            value = value.replace("%", "")
                        
                        new_df = pd.DataFrame(data={
                                'video_id': video_id,
                                'data_type': 'age_'+data_type,
                                'data_name': age_metrics[idx].find_element(By.CSS_SELECTOR, "#title-cell").text,
                                'value': value,
                                'channel_id': channel_id
                            }, index=[0])
                        df_dict["additional"] = pd.concat([df_dict["additional"], new_df], ignore_index=True)

            except Exception as e:
                print(e)
                pass


            # 성별
            try:
                driver.get(get_yt_studio_url(
                    info_type="gender",
                    content_id=video_id,
                    auth_id=auth_id,
                    period=period
                ))
                WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="row-container"]'))

                gender_metrics = driver.find_elements(By.XPATH, '//*[@id="row-container"]')
                for idx in range(3):
                    for type_idx, data_type in enumerate(['views', 'avg_watch_percentage']):

                        value = gender_metrics[idx].find_elements(By.CSS_SELECTOR, ".value-container")[type_idx].text
                        if '—' in value:
                            value = ""
                        else:
                            value = value.replace("%", "")

                        new_df = pd.DataFrame(data={
                            'video_id': video_id,
                            'data_type': 'gender_'+data_type,
                            'data_name': gender_metrics[idx].find_element(By.CSS_SELECTOR, "#title-cell").text,
                            'value': value,
                            'channel_id': channel_id
                        }, index=[0])
                        df_dict["additional"] = pd.concat([df_dict["additional"], new_df], ignore_index=True)
            
            except Exception as e:
                print(e)
                pass

        
            
            # 트래픽소스
            try:
                driver.get(get_yt_studio_url(
                    info_type="traffic_source",
                    content_id=video_id,
                    auth_id=auth_id,
                    period=period
                ))
                WebDriverWait(driver, timeout=TIMEOUT).until(lambda x: x.find_element(By.XPATH, '//*[@id="row-container"]'))

                traffic_source_metrics = driver.find_elements(By.XPATH, '//*[@id="row-container"]')

                for idx in range(len(traffic_source_metrics)):
                    data_name = traffic_source_metrics[idx].find_element(By.CSS_SELECTOR, "#title-cell").text
                    
                    if data_name == "합계":
                        continue

                    value = traffic_source_metrics[idx].find_element(By.CSS_SELECTOR, ".value-container").text.split("\n")[1]

                    if '—' in value:
                        value = ""
                    else:
                        value = value.replace("%", "")

                    new_df = pd.DataFrame(data={
                        'video_id': video_id,
                        'data_type': 'traffic_source',
                        'data_name': traffic_source_metrics[idx].find_element(By.CSS_SELECTOR, "#title-cell").text,
                        'value': value,
                        'channel_id': channel_id
                    }, index=[0])
                    df_dict["additional"] = pd.concat([df_dict["additional"], new_df], ignore_index=True)

            except Exception as e:
                print(e)
                pass


            break

        except TimeoutError as e:
            error_img = driver.find_elements(By.XPATH, '//*[@id="error-image"]')
            if len(error_img) > 0:
                continue

        except Exception as e:
            print(e)
            break
    
    df_dict["additional"] = df_dict["additional"].drop(df_dict["additional"].loc[df_dict["additional"]["value"] == ""].index)
    df_dict["overview"].fillna("", inplace=True)

    return df_dict
