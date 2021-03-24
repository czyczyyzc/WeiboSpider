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
from lxml import etree
from scrapy import Selector, Spider
from scrapy.http import Request
from items import UserItem


class UserSpider(Spider):
    name = "user_spider"
    base_url = "https://weibo.cn"

    def __init__(self, user_ids=[], **kwargs):
        super().__init__(**kwargs)
        self.user_ids = []
        for user_id in user_ids:
            if user_id.endswith('.txt'):
                with open(user_id, 'r', encoding='utf-8-sig', newline='') as f:
                    lines = f.readlines()
                    lines = [line.strip() for line in lines]
                    self.user_ids.extend(lines)
            else:
                self.user_ids.append(user_id)

    def start_requests(self):
        # user_ids = ['1087770692', '1699432410', '1266321801']
        # urls = [f'{self.base_url}/{user_id}/info' for user_id in self.user_ids]
        urls = [f'{self.base_url}{user_id}' for user_id in self.user_ids]
        for url in urls:
            yield Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        user_item = UserItem()
        text = response.text
        tweets_num = re.findall('微博\[(\d+)\]', text)
        if tweets_num:
            user_item['tweets_num'] = int(tweets_num[0])
        else:
            user_item['tweets_num'] = 'Unknown'

        follows_num = re.findall('关注\[(\d+)\]', text)
        if follows_num:
            user_item['follows_num'] = int(follows_num[0])
        else:
            user_item['follows_num'] = 'Unknown'

        fans_num = re.findall('粉丝\[(\d+)\]', text)
        if fans_num:
            user_item['fans_num'] = int(fans_num[0])
        else:
            user_item['fans_num'] = 'Unknown'

        user_item['url'] = response.url.split('weibo.cn')[-1]

        request_meta = response.meta
        request_meta['item'] = user_item
        tree_node = etree.HTML(response.body)
        url = self.base_url + tree_node.xpath('.//a[contains(text(),"资料")]/@href')[0]
        yield Request(url, callback=self.parse_info, dont_filter=True, meta=response.meta)

    def parse_info(self, response):
        # try:
        user_item = UserItem()
        # user_item['crawl_time'] = int(time.time())
        selector = Selector(response)
        user_item['_id'] = re.findall('(\d+)/info', response.url)[0]
        user_info_text = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())
        nick_name = re.findall('昵称;?:?(.*?);', user_info_text)
        gender = re.findall('性别;?:?(.*?);', user_info_text)
        place = re.findall('地区;?:?(.*?);', user_info_text)
        brief_introduction = re.findall('简介;?:?(.*?);', user_info_text)
        birthday = re.findall('生日;?:?(.*?);', user_info_text)
        sex_orientation = re.findall('性取向;?:?(.*?);', user_info_text)
        sentiment = re.findall('感情状况;?:?(.*?);', user_info_text)
        vip_level = re.findall('会员等级;?:?(.*?);', user_info_text)
        authentication = re.findall('认证;?:?(.*?);', user_info_text)
        labels = re.findall('标签;?:?(.*?)更多>>', user_info_text)

        if nick_name and nick_name[0]:
            user_item["nick_name"] = nick_name[0].replace(u"\xa0", "")
        else:
            user_item["nick_name"] = 'Unknown'

        if gender and gender[0]:
            user_item["gender"] = gender[0].replace(u"\xa0", "")
        else:
            user_item["gender"] = 'Unknown'

        if place and place[0]:
            place = place[0].replace(u"\xa0", "").split(" ")
            user_item["province"] = place[0]
            if len(place) > 1:
                user_item["city"] = place[1]
            else:
                user_item["city"] = 'Unknown'
        else:
            user_item["province"] = 'Unknown'
            user_item["city"] = 'Unknown'

        if brief_introduction and brief_introduction[0]:
            user_item["brief_introduction"] = brief_introduction[0].replace(u"\xa0", "")
        else:
            user_item["brief_introduction"] = 'Unknown'

        if birthday and birthday[0]:
            user_item['birthday'] = birthday[0]
        else:
            user_item['birthday'] = 'Unknown'

        if sex_orientation and sex_orientation[0]:
            if sex_orientation[0].replace(u"\xa0", "") == gender[0]:
                user_item["sex_orientation"] = "同性恋"
            else:
                user_item["sex_orientation"] = "异性恋"
        else:
            user_item["sex_orientation"] = 'Unknown'

        if sentiment and sentiment[0]:
            user_item["sentiment"] = sentiment[0].replace(u"\xa0", "")
        else:
            user_item["sentiment"] = 'Unknown'

        if vip_level and vip_level[0]:
            user_item["vip_level"] = vip_level[0].replace(u"\xa0", "")
        else:
            user_item["vip_level"] = 'Unknown'

        if authentication and authentication[0]:
            user_item["authentication"] = authentication[0].replace(u"\xa0", "")
        else:
            user_item["authentication"] = 'Unknown'

        if labels and labels[0]:
            user_item["labels"] = labels[0].replace(u"\xa0", ",").replace(';', '').strip(',')
        else:
            user_item["labels"] = 'Unknown'

        item = response.meta['item']
        user_item['tweets_num'] = item['tweets_num']
        user_item['follows_num'] = item['follows_num']
        user_item['fans_num'] = item['fans_num']
        user_item['url'] = item['url']
        yield user_item
        # request_meta = response.meta
        # request_meta['item'] = user_item
        # yield Request(self.base_url + '/u/{}'.format(user_item['_id']),
        #               callback=self.parse_further_information,
        #               meta=request_meta, dont_filter=True, priority=1)
        # except Exception as e:
        #     self.logger.error(e)

    def parse_further_information(self, response):
        text = response.text
        user_item = response.meta['item']
        tweets_num = re.findall('微博\[(\d+)\]', text)
        if tweets_num:
            user_item['tweets_num'] = int(tweets_num[0])
        else:
            user_item['tweets_num'] = 'Unknown'

        follows_num = re.findall('关注\[(\d+)\]', text)
        if follows_num:
            user_item['follows_num'] = int(follows_num[0])
        else:
            user_item['follows_num'] = 'Unknown'

        fans_num = re.findall('粉丝\[(\d+)\]', text)
        if fans_num:
            user_item['fans_num'] = int(fans_num[0])
        else:
            user_item['fans_num'] = 'Unknown'
        yield user_item
