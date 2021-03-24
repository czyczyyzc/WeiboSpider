# -*- coding: utf-8 -*-
import os
import random

BOT_NAME = 'spider'

SPIDER_MODULES = ['spiders']
NEWSPIDER_MODULE = 'spiders'

ROBOTSTXT_OBEY = False

cookies_file = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'cookies.txt')
with open(cookies_file, 'r', encoding='utf-8-sig', newline='') as f:
    cookies = f.readlines()
    cookies = [cookie.strip() for cookie in cookies]
COOKIES = dict(zip(range(len(cookies)), cookies))

# change cookie to yours
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'Cookie': COOKIES[int(os.path.basename(os.path.split(os.path.realpath(__file__))[0]).split('_')[-1]) % len(COOKIES.keys())],
    'X-Forwarded-For': '%s.%s.%s.%s' % (random.randrange(1, 200, 20), random.randrange(1, 200, 20), random.randrange(1, 200, 20), random.randrange(1, 200, 20)),
}

CONCURRENT_REQUESTS = 50

DOWNLOAD_DELAY = 3

AUTOTHROTTLE_ENABLED = True

LOG_LEVEL = "INFO"  # 输出级别
LOG_STDOUT = True   # 是否标准输出

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': None,
    'middlewares.IPProxyMiddleware': 100,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 101,
}

# ITEM_PIPELINES = {
#     'pipelines.MongoDBPipeline': 300,
# }

ITEM_PIPELINES = {
    'pipelines.CSVPipeline': 300,
}

SAVE_ROOT = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'temp')

MONGO_HOST = '127.0.0.1'
MONGO_PORT = 27017
