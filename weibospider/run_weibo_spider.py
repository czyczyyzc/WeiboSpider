#!/usr/bin/env python
# encoding: utf-8
import os
import sys
import csv
import shutil
import argparse
import subprocess
from settings import SAVE_ROOT


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weibo Spider')
    parser.add_argument('--keywords', type=str, default='用户隐私')
    parser.add_argument('--date-start', type=str, default='')  # '2020-02-09'
    parser.add_argument('--date-end', type=str, default='')
    parser.add_argument('--min-like-num', type=int, default=1)
    parser.add_argument('--min-repost-num', type=int, default=0)
    parser.add_argument('--min-comment-num', type=int, default=0)
    parser.add_argument('--file-dir', type=str, default='./data')
    args = parser.parse_args()

    if not os.path.exists(args.file_dir):
        os.makedirs(args.file_dir)

    current_dir = os.path.split(os.path.realpath(__file__))[0]

    keywords = args.keywords.split(',')
    user_ids = []
    for keyword in keywords:
        tweets_dir = os.path.join(args.file_dir, keyword)
        if not os.path.exists(tweets_dir):
            os.makedirs(tweets_dir)
        comments_dir = os.path.join(tweets_dir, 'comments')
        if not os.path.exists(comments_dir):
            os.makedirs(comments_dir)

        print("Crawling the tweets for keyword {:s} ...".format(keyword))
        subprocess.run([sys.executable, os.path.join(current_dir, 'run_spider.py'),
                        '--mode', 'tweet', '--keywords', keyword,
                        '--date-start', args.date_start, '--date-end', args.date_end])
        print("Tweets crawling for keyword {:s} is finished.".format(keyword))

        print("Postprocessing the crawled tweets for keyword {:s} ...".format(keyword))
        tweet_ids = []
        tweets = []
        tweets_head = None
        with open(os.path.join(SAVE_ROOT, 'tweets.csv'), 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    tweets_head = row
                    continue
                user_id, tweet_id, like_num, repost_num, comment_num = \
                    row[1], row[2], int(row[5]), int(row[6]), int(row[7])
                if like_num < args.min_like_num or repost_num < args.min_repost_num or \
                        comment_num < args.min_comment_num:
                    continue
                if user_id not in user_ids:
                    user_ids.append(user_id)
                if tweet_id not in tweet_ids:
                    tweet_ids.append(tweet_id)
                    tweets.append(row)
        if len(tweets) > 0:
            tweets.sort(key=lambda x: (int(x[5]), int(x[6]), int(x[7])), reverse=True)
            tweets_file = os.path.join(tweets_dir, 'tweets.csv')
            with open(tweets_file, 'w', encoding='utf-8-sig', newline='') as f:
                tweets_writer = csv.writer(f, dialect='excel')
                tweets_writer.writerow(tweets_head)
                for tweet in tweets:
                    tweets_writer.writerow(tweet)
        print("Tweets postprocessing for keyword {:s} is finished.".format(keyword))

        for tweet_id in tweet_ids:
            print("Crawling the comments for tweet {:s} in keyword {:s} ...".format(tweet_id, keyword))
            subprocess.run([sys.executable, os.path.join(current_dir, 'run_spider.py'),
                            '--mode', 'comment', '--tweet-ids', tweet_id])
            print("Comments crawling for tweet {:s} in keyword {:s} is finished.".format(tweet_id, keyword))

            print("Postprocessing the crawled comments for tweet {:s} of keyword {:s} ...".format(tweet_id, keyword))
            comment_ids = []
            comments = []
            comments_head = None
            with open(os.path.join(SAVE_ROOT, 'comments.csv'), 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if i == 0:
                        comments_head = row
                        continue
                    user_id, comment_id = row[1], row[3]
                    if user_id not in user_ids:
                        user_ids.append(user_id)
                    if comment_id not in comment_ids:
                        comment_ids.append(comment_id)
                        comments.append(row)
            if len(comments) > 0:
                comments.sort(key=lambda x: int(x[4]), reverse=True)
                comments_file = os.path.join(comments_dir, tweet_id + '.csv')
                with open(comments_file, 'w', encoding='utf-8-sig', newline='') as f:
                    comments_writer = csv.writer(f, dialect='excel')
                    comments_writer.writerow(comments_head)
                    for comment in comments:
                        comments_writer.writerow(comment)
            print("Comments postprocessing for tweet {:s} of keyword {:s} is finished.".format(tweet_id, keyword))

    # print("Crawling the users ...")
    # subprocess.run([sys.executable, os.path.join(current_dir, 'run_spider.py'),
    #                 '--mode', 'user', '--user-ids', ','.join(user_ids)])
    # print("Users crawling is finished.")
    #
    # print("Postprocessing the crawled users...")
    # shutil.copy(os.path.join(SAVE_ROOT, 'users.csv'), args.file_dir)
    # print("Users postprocessing is finished.")

    users_file = os.path.join(args.file_dir, 'users.txt')
    with open(users_file, 'w', encoding='utf-8-sig', newline='') as f:
        f.writelines([line+'\n' for line in user_ids])

