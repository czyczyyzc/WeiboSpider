# -*- coding: utf-8 -*-
import os
import csv
import pymongo
from pymongo.errors import DuplicateKeyError
from settings import MONGO_HOST, MONGO_PORT, SAVE_ROOT


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = client['weibo']
        self.Users = db["Users"]
        self.Tweets = db["Tweets"]
        self.Comments = db["Comments"]
        self.Relationships = db["Relationships"]
        self.Reposts = db["Reposts"]

    def process_item(self, item, spider):
        if spider.name == 'comment_spider':
            self.insert_item(self.Comments, item)
        elif spider.name == 'fan_spider':
            self.insert_item(self.Relationships, item)
        elif spider.name == 'follower_spider':
            self.insert_item(self.Relationships, item)
        elif spider.name == 'user_spider':
            self.insert_item(self.Users, item)
        elif spider.name == 'tweet_spider':
            self.insert_item(self.Tweets, item)
        elif spider.name == 'repost_spider':
            self.insert_item(self.Reposts, item)
        return item

    @staticmethod
    def insert_item(collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            pass


class CSVPipeline(object):

    def __init__(self):
        if not os.path.exists(SAVE_ROOT):
            os.makedirs(SAVE_ROOT)

        users_file = open(os.path.join(SAVE_ROOT, 'users.csv'), 'w', encoding='utf-8-sig', newline='')
        tweets_file = open(os.path.join(SAVE_ROOT, 'tweets.csv'), 'w', encoding='utf-8-sig', newline='')
        comments_file = open(os.path.join(SAVE_ROOT, 'comments.csv'), 'w', encoding='utf-8-sig', newline='')
        relationships_file = open(os.path.join(SAVE_ROOT, 'relationships.csv'), 'w', encoding='utf-8-sig', newline='')
        reposts_file = open(os.path.join(SAVE_ROOT, 'reposts.csv'), 'w', encoding='utf-8-sig', newline='')

        self.users_writer = csv.writer(users_file, dialect='excel')
        self.tweets_writer = csv.writer(tweets_file, dialect='excel')
        self.comments_writer = csv.writer(comments_file, dialect='excel')
        self.relationships_writer = csv.writer(relationships_file, dialect='excel')
        self.reposts_writer = csv.writer(reposts_file, dialect='excel')

        self.users_head = False
        self.tweets_head = False
        self.comments_head = False
        self.relationships_head = False
        self.reposts_head = False

        self.users_ids = []
        self.tweets_ids = []
        self.comments_ids = []
        self.relationships_ids = []
        self.reposts_ids = []

    def process_item(self, item, spider):
        item = dict(item)
        if spider.name == 'comment_spider':
            if not self.comments_head:
                self.comments_writer.writerow(list(item.keys()))
                self.comments_head = True
            # if item['_id'] not in self.comments_ids:
            self.comments_writer.writerow(list(item.values()))
            self.comments_ids.append(item['_id'])

        elif spider.name == 'fan_spider':
            if not self.relationships_head:
                self.relationships_writer.writerow(list(item.keys()))
                self.relationships_head = True
            # if item['_id'] not in self.relationships_ids:
            self.relationships_writer.writerow(list(item.values()))
            self.relationships_ids.append(item['_id'])

        elif spider.name == 'follower_spider':
            if not self.relationships_head:
                self.relationships_writer.writerow(list(item.keys()))
                self.relationships_head = True
            # if item['_id'] not in self.relationships_ids:
            self.relationships_writer.writerow(list(item.values()))
            self.relationships_ids.append(item['_id'])

        elif spider.name == 'user_spider':
            if not self.users_head:
                self.users_writer.writerow(list(item.keys()))
                self.users_head = True
            # if item['_id'] not in self.users_ids:
            self.users_writer.writerow(list(item.values()))
            self.users_ids.append(item['_id'])

        elif spider.name == 'tweet_spider':
            if not self.tweets_head:
                self.tweets_writer.writerow(list(item.keys()))
                self.tweets_head = True
            # if item['_id'] not in self.tweets_ids:
            self.tweets_writer.writerow(list(item.values()))
            self.tweets_ids.append(item['_id'])

        elif spider.name == 'repost_spider':
            if not self.reposts_head:
                self.reposts_writer.writerow(list(item.keys()))
                self.reposts_head = True
            # if item['_id'] not in self.reposts_ids:
            self.reposts_writer.writerow(list(item.values()))
            self.reposts_ids.append(item['_id'])
        return item
