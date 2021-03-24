#!/usr/bin/env python
# encoding: utf-8
"""
File Description: 
Author: nghuyong
Mail: nghuyong@163.com
Created Time: 2020/4/14
"""
import re
import time
import datetime
from lxml import etree
from scrapy import Spider
from scrapy.http import Request
from items import TweetItem
from spiders.utils import time_fix, extract_weibo_content


class TweetSpider(Spider):
    name = "tweet_spider"
    base_url = "https://weibo.cn"

    def __init__(self, keywords=[], date_start='', date_end='', user_ids=[], **kwargs):  # 2017-07-30
        super().__init__(**kwargs)
        self.keywords = keywords
        self.date_start = date_start
        self.date_end = date_end
        self.user_ids = user_ids

    def init_url_by_user_id(self):
        # crawl tweets post by users
        # user_ids = ['1087770692', '1699432410', '1266321801']
        urls = [f'{self.base_url}/{user_id}/profile?page=1' for user_id in self.user_ids]
        return urls

    def init_url_by_keywords(self):
        if self.date_start and self.date_end:
            # crawl tweets include keywords in a period, you can change the following keywords and date
            date_start = datetime.datetime.strptime(self.date_start, '%Y-%m-%d')
            date_end = datetime.datetime.strptime(self.date_end, '%Y-%m-%d')
            time_spread = datetime.timedelta(days=1)
            url_format_by_day = "https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&starttime={}&endtime={}&atten=1&sort=time&page=1"
            url_format_by_hour = "https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&advancedfilter=1&starttime={}&endtime={}&sort=time&atten=1&page=1"
            urls = []
            while date_start <= date_end:
                for keyword in self.keywords:
                    # 添加按日的url
                    day_string = date_start.strftime("%Y%m%d")
                    urls.append(url_format_by_day.format(keyword, day_string, day_string))
                    # 添加按小时的url
                    one_day_back = date_start - time_spread
                    # from today's 7:00-8:00am to 23:00-24:00am
                    for hour in range(7, 24):
                        # calculation rule of starting time: start_date 8:00am + offset:16
                        begin_hour = one_day_back.strftime("%Y%m%d") + "-" + str(hour + 16)
                        # calculation rule of ending time: (end_date+1) 8:00am + offset:-7
                        end_hour = one_day_back.strftime("%Y%m%d") + "-" + str(hour - 7)
                        urls.append(url_format_by_hour.format(keyword, begin_hour, end_hour))
                    two_day_back = one_day_back - time_spread
                    # from today's 0:00-1:00am to 6:00-7:00am
                    for hour in range(0, 7):
                        # note the offset change bc we are two-days back now
                        begin_hour = two_day_back.strftime("%Y%m%d") + "-" + str(hour + 40)
                        end_hour = two_day_back.strftime("%Y%m%d") + "-" + str(hour + 17)
                        urls.append(url_format_by_hour.format(keyword, begin_hour, end_hour))
                date_start = date_start + time_spread
        else:
            url_format = "https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&page=1"
            urls = []
            for keyword in self.keywords:
                urls.append(url_format.format(keyword))
            return urls
        return urls

    def start_requests(self):
        # select urls generation by the following code
        urls = self.init_url_by_keywords()
        for url in urls:
            yield Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, callback=self.parse, dont_filter=True, meta=response.meta)

        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for tweet_node in tweet_nodes:
            # try:
            tweet_item = TweetItem()
            # tweet_item['crawl_time'] = int(time.time())
            tweet_repost_url = tweet_node.xpath('.//a[contains(text(),"转发[")]/@href')[0]
            user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_repost_url)
            # tweet_item['weibo_url'] = 'https://weibo.com/{}/{}'.format(user_tweet_id.group(2),
            #                                                            user_tweet_id.group(1))
            tweet_item['weibo_url'] = '=HYPERLINK("https://weibo.com/{}/{}")'.format(user_tweet_id.group(2),
                                                                                     user_tweet_id.group(1))
            # tweet_item['user_id'] = user_tweet_id.group(2)
            tweet_item['user_id'] = '/u/' + user_tweet_id.group(2)
            tweet_item['_id'] = user_tweet_id.group(1)
            create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
            create_time_info = create_time_info_node.xpath('string(.)')
            if "来自" in create_time_info:
                tweet_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
                tweet_item['tool'] = create_time_info.split('来自')[1].strip()
            else:
                tweet_item['created_at'] = time_fix(create_time_info.strip())
                tweet_item['tool'] = 'Unknown'

            like_num = tweet_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
            tweet_item['like_num'] = int(re.search('\d+', like_num).group())

            repost_num = tweet_node.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
            tweet_item['repost_num'] = int(re.search('\d+', repost_num).group())

            comment_num = tweet_node.xpath(
                './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
            tweet_item['comment_num'] = int(re.search('\d+', comment_num).group())

            images = tweet_node.xpath('.//img[@alt="图片"]/@src')
            if images:
                # tweet_item['image_url'] = images if len(images) > 1 else images[0]
                tweet_item['image_url'] = '=HYPERLINK("{:s}")'.format(images[0])
            else:
                tweet_item['image_url'] = 'Unknown'

            videos = tweet_node.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
            if videos:
                # tweet_item['video_url'] = videos if len(videos) > 1 else videos[0]
                tweet_item['video_url'] = '=HYPERLINK("{:s}")'.format(videos[0])
            else:
                tweet_item['video_url'] = 'Unknown'

            web_urls = tweet_node.xpath('.//a[contains(@href,"https://weibo.cn/sinaurl?f=w&u=")]/@href')
            if web_urls:
                # tweet_item['web_url'] = web_urls if len(web_urls) > 1 else web_urls[0]
                tweet_item['web_url'] = '=HYPERLINK("{:s}")'.format(web_urls[0])
            else:
                tweet_item['web_url'] = 'Unknown'

            # map_node = tweet_node.xpath('.//a[contains(text(),"显示地图")]')
            # if map_node:
            #     map_node = map_node[0]
            #     map_node_url = map_node.xpath('./@href')[0]
            #     map_info = re.search(r'xy=(.*?)&', map_node_url).group(1)
            #     tweet_item['location_map_info'] = map_info
            # else:
            #     tweet_item['location_map_info'] = 'Unknown'

            repost_node = tweet_node.xpath('.//a[contains(text(),"原文评论[")]/@href')
            if repost_node:
                # tweet_item['origin_weibo'] = repost_node[0]
                tweet_item['origin_weibo'] = '=HYPERLINK("{:s}")'.format(repost_node[0])
            else:
                tweet_item['origin_weibo'] = 'Unknown'

            all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
            if all_content_link:
                all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                yield Request(all_content_url, callback=self.parse_all_content, dont_filter=True, meta={'item': tweet_item},
                              priority=1)
            else:
                tweet_html = etree.tostring(tweet_node, encoding='unicode')
                tweet_item['content'] = extract_weibo_content(tweet_html)
                # content_info_node = tweet_node.xpath('.//span[@class="ctt"]')[-1]
                # content_info = content_info_node.xpath('string(.)')
                # tweet_item['content'] = content_info
                yield tweet_item
            # except Exception as e:
            #     self.logger.error(e)

    def parse_all_content(self, response):
        tree_node = etree.HTML(response.body)
        tweet_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        tweet_html = etree.tostring(content_node, encoding='unicode')
        tweet_item['content'] = extract_weibo_content(tweet_html)
        # content_info_node = content_node.xpath('.//span[@class="ctt"]')[-1]
        # content_info = content_info_node.xpath('string(.)')
        # tweet_item['content'] = content_info
        yield tweet_item
