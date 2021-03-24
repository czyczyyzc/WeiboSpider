#!/usr/bin/env python
# encoding: utf-8
import os
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.tweet import TweetSpider
from spiders.comment import CommentSpider
from spiders.follower import FollowerSpider
from spiders.user import UserSpider
from spiders.fan import FanSpider
from spiders.repost import RepostSpider


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weibo Spider')
    parser.add_argument('--mode', type=str, default='user', help='the type of the data')
    parser.add_argument('--keywords', type=str, default='用户隐私')
    parser.add_argument('--date-start', type=str, default='')  # '2020-01-01'
    parser.add_argument('--date-end', type=str, default='')
    parser.add_argument('--tweet-ids', type=str, default='JDktUgcsD')
    parser.add_argument('--user-ids', type=str, default='/yht2018')
    args = parser.parse_args()

    os.environ['SCRAPY_SETTINGS_MODULE'] = f'settings'
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    mode_to_spider = {
        'comment': CommentSpider,
        'fan': FanSpider,
        'follow': FollowerSpider,
        'tweet': TweetSpider,
        'user': UserSpider,
        'repost': RepostSpider,
    }
    kwargs = {
        'keywords': args.keywords.split(','),
        'date_start': args.date_start,
        'date_end': args.date_end,
        'tweet_ids': args.tweet_ids.split(','),
        'user_ids': args.user_ids.split(','),
    }
    process.crawl(mode_to_spider[args.mode], **kwargs)
    # the script will block here until the crawling is finished
    process.start()
