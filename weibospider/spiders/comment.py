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
from scrapy import Spider
from scrapy.http import Request
from items import CommentItem
from spiders.utils import extract_comment_content, time_fix


class CommentSpider(Spider):
    name = "comment_spider"
    base_url = "https://weibo.cn"

    def __init__(self, tweet_ids=[], **kwargs):
        super().__init__(**kwargs)
        self.tweet_ids = tweet_ids

    def start_requests(self):
        # tweet_ids = ['IDl56i8av', 'IDkNerVCG', 'IDkJ83QaY']
        # tweet_ids = ['FeN9mmEbO']
        # urls = [f"{self.base_url}/comment/hot/{tweet_id}?rl=1&page=1" for tweet_id in self.tweet_ids]
        urls = [f"{self.base_url}/comment/hot/{tweet_id}?page=1" for tweet_id in self.tweet_ids]
        for url in urls:
            yield Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        # try:
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                # all_page = all_page if all_page <= 50 else 50
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, callback=self.parse, dont_filter=True, meta=response.meta)

        tree_node = etree.HTML(response.body)
        comment_nodes = tree_node.xpath('//div[@class="c" and contains(@id,"C_")]')
        for comment_node in comment_nodes:
            comment_item = CommentItem()
            # comment_item['crawl_time'] = int(time.time())
            comment_item['weibo_id'] = response.url.split('/')[-1].split('?')[0]
            # comment_user_url = comment_node.xpath('.//a[contains(@href,"/u/")]/@href')
            # if not comment_user_url:
            #     comment_item['comment_user_id'] = re.search(r'/u/(\d+)', comment_user_url[0]).group(1)
            #     continue
            comment_user_url = comment_node.xpath('.//a[contains(@href,"/")]/@href')[0]
            comment_item['comment_user_id'] = comment_user_url

            comment_item['content'] = extract_comment_content(etree.tostring(comment_node, encoding='unicode'))
            # content_info_node = comment_node.xpath('.//span[@class="ctt"]')[-1]
            # content_info = content_info_node.xpath('string(.)')
            # comment_item['content'] = content_info

            comment_item['_id'] = comment_node.xpath('./@id')[0]
            created_at_info = comment_node.xpath('.//span[@class="ct"]/text()')[0]
            like_num = comment_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
            comment_item['like_num'] = int(re.search('\d+', like_num).group())
            comment_item['created_at'] = time_fix(created_at_info.split('\xa0')[0])
            yield comment_item
        # except Exception as e:
        #     self.logger.error(e)
