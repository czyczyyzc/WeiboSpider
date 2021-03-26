#!/usr/bin/env python
# encoding: utf-8
import os
import sys
import csv
import math
import time
import shutil
import difflib
import argparse
import datetime
import subprocess
from concurrent.futures import ProcessPoolExecutor
from utils import extract_comment_content
from utils.analyzer import DataAnalyzer


class WeiboSpiderRunner(object):
    def __init__(self, keywords, date_start, date_end, min_like_num=0,
                 min_repost_num=0, min_comment_num=0, file_dir='./data', run_dir='./temp',
                 max_workers=4, days_per_worker=10, num_topics=10, max_iter=1000, num_top_words=20):

        self.keywords = keywords.split(',')
        self.date_start = date_start
        self.date_end = date_end
        self.time_spread = datetime.timedelta(days=days_per_worker)
        self.min_like_num = min_like_num
        self.min_repost_num = min_repost_num
        self.min_comment_num = min_comment_num
        self.file_dir = file_dir
        self.run_dir = run_dir
        self.source_dir = os.path.join(self.file_dir, 'source')
        self.result_dir = os.path.join(self.file_dir, 'result')
        self.max_workers = max_workers
        self.num_workers = max_workers
        self.num_topics = num_topics
        self.max_iter = max_iter
        self.num_top_words = num_top_words
        self.task_kwargs = []
        self.data_analyzer = DataAnalyzer(self.result_dir, self.num_topics, self.max_iter, self.num_top_words)

        if not os.path.exists(self.file_dir):
            os.makedirs(self.file_dir)
        if not os.path.exists(self.source_dir):
            os.makedirs(self.source_dir)
        if not os.path.exists(self.result_dir):
            os.makedirs(self.result_dir)

        shutil.copy('settings/ips.txt', 'weibospider/ips.txt')
        shutil.copy('settings/cookies.txt', 'weibospider/cookies.txt')
        shutil.copy('settings/stop_words.txt', 'utils/data/stop_words.txt')
        shutil.copy('settings/my_dict.txt', 'utils/data/my_dict.txt')

    def split_tasks(self):
        task_kwargs = []
        for keyword in self.keywords:
            if self.date_start and self.date_end:
                date_start_all = datetime.datetime.strptime(self.date_start, '%Y-%m-%d')
                date_end_all = datetime.datetime.strptime(self.date_end, '%Y-%m-%d')
                date_start = date_start_all
                while date_start <= date_end_all:
                    date_end = date_start + self.time_spread - datetime.timedelta(days=1)
                    if date_end > date_end_all:
                        date_end = date_end_all
                    date_name = date_start.strftime("%Y-%m-%d")
                    task_name = keyword + '_' + date_name
                    task_kwargs.append([
                        task_name, '--keywords', keyword, '--date-start', date_start.strftime("%Y-%m-%d"),
                        '--date-end', date_end.strftime("%Y-%m-%d"), '--min-like-num', str(self.min_like_num),
                        '--min-repost-num', str(self.min_repost_num), '--min-comment-num', str(self.min_comment_num),
                    ])
                    date_start = date_start + self.time_spread
            else:
                task_name = keyword
                task_kwargs.append([
                    task_name, '--keywords', keyword, '--min-like-num', str(self.min_like_num),
                    '--min-repost-num', str(self.min_repost_num), '--min-comment-num', str(self.min_comment_num),
                ])

        tasks_per_worker = int(math.ceil(len(task_kwargs) / self.max_workers))
        self.num_workers = int(math.ceil(len(task_kwargs) / tasks_per_worker))
        self.task_kwargs = []
        for worker_id in range(self.num_workers):
            run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
            cmds = [sys.executable, os.path.join(run_dir, 'run_weibo_spider.py')]
            args = []
            for kwargs in task_kwargs[worker_id * tasks_per_worker: (worker_id + 1) * tasks_per_worker]:
                args.append(
                    kwargs[1:] + ['--file-dir', os.path.join(run_dir, 'data', kwargs[0])]
                )
            self.task_kwargs.append((cmds, args))

    def split_users(self):
        user_ids = []
        users_file = os.path.join(self.source_dir, 'users.txt')
        with open(users_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
            for i, line in enumerate(lines):
                if i % 1000 == 0:
                    print(i)
                if line not in user_ids:
                    user_ids.append(line)
        users_per_worker = int(math.ceil(len(user_ids) / self.max_workers))
        self.num_workers = int(math.ceil(len(user_ids) / users_per_worker))
        self.task_kwargs = []
        for worker_id in range(self.num_workers):
            run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
            file_dir = os.path.join(run_dir, 'data')
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            user_id = user_ids[worker_id * users_per_worker: (worker_id + 1) * users_per_worker]
            users_file = os.path.join(file_dir, 'users.txt')
            with open(users_file, 'w', encoding='utf-8-sig', newline='') as f:
                f.writelines([line + '\n' for line in user_id])
            self.task_kwargs.append(
                [sys.executable, os.path.join(run_dir, 'run_spider.py'),
                 '--mode', 'user', '--user-ids', users_file]
            )

    def run_weibo_spider_single(self, worker_id):
        time.sleep(worker_id)
        cmds, kwargs = self.task_kwargs[worker_id]
        for args in kwargs:
            task_name = os.path.basename(os.path.realpath(args[-1]))
            print("Runing weibo spider for task {:s} ...".format(task_name))
            subprocess.run(cmds + args)
            print("Weibo spider running for task {:s} is finished!".format(task_name))
        return

    def postprocess_users_single(self, worker_id):
        time.sleep(worker_id)
        print("Crawling users for worker {:d} ...".format(worker_id))
        run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
        file_dir = os.path.join(run_dir, 'data')
        subprocess.run(self.task_kwargs[worker_id])
        shutil.copy(os.path.join(run_dir, 'temp', 'users.csv'), file_dir)
        print("Users crawling for worker {:d} is finished!".format(worker_id))
        return

    def postprocess_tasks(self):
        print("Postprocessing the crawled data from all tasks ...")
        data_dict = {
            'keywords': {
                # '用户隐私': {
                #     'tweets': [],
                #     'comments': [],
                # }
            },
            'users': [],
        }
        for worker_id, (cmds, kwargs) in enumerate(self.task_kwargs):
            run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
            for args in kwargs:
                task_name = os.path.basename(os.path.realpath(args[-1]))
                file_dir = os.path.join(run_dir, 'data', task_name)
                data_dict['users'].append(os.path.join(file_dir, 'users.txt'))

                keyword = task_name.split('_')[0]
                if keyword not in data_dict['keywords']:
                    data_dict['keywords'][keyword] = {'tweets': [], 'comments': []}
                tweets_dir = os.path.join(file_dir, keyword)
                data_dict['keywords'][keyword]['tweets'].append(os.path.join(tweets_dir, 'tweets.csv'))
                comments_dir = os.path.join(tweets_dir, 'comments')
                data_dict['keywords'][keyword]['comments'].extend([os.path.join(comments_dir, file)
                                                                   for file in os.listdir(comments_dir)])

        for keyword, data in data_dict['keywords'].items():
            tweets_dir = os.path.join(self.source_dir, keyword)
            if not os.path.exists(tweets_dir):
                os.makedirs(tweets_dir)
            comments_dir = os.path.join(tweets_dir, 'comments')
            if not os.path.exists(comments_dir):
                os.makedirs(comments_dir)

            print("Postprocessing the crawled tweets from all tasks for keyword {:s} ...".format(keyword))
            tweet_ids = []
            tweets = []
            tweets_head = None
            for i, file in enumerate(data['tweets']):
                if not os.path.exists(file):
                    continue
                with open(file, 'r', encoding='utf-8-sig', newline='') as f:
                    reader = csv.reader(f)
                    for j, row in enumerate(reader):
                        if tweets_head is None and j == 0:
                            tweets_head = row
                        if j == 0:
                            continue
                        tweet_id = row[2]
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
            print("Tweets postprocessing of all tasks for keyword {:s} is finished!".format(keyword))

            print("Postprocessing the crawled comments tweets from all tasks for keyword {:s} ...".format(keyword))
            for i, file in enumerate(data['comments']):
                shutil.copy(file, comments_dir)
            print("Comments postprocessing of all tasks for keyword {:s} is finished!".format(keyword))

        user_ids = []
        for i, file in enumerate(data_dict['users']):
            if not os.path.exists(file):
                continue
            with open(file, 'r', encoding='utf-8-sig', newline='') as f:
                lines = f.readlines()
                lines = [line.strip() for line in lines]
                for line in lines:
                    if line not in user_ids:
                        user_ids.append(line)
        users_file = os.path.join(self.source_dir, 'users.txt')
        with open(users_file, 'w', encoding='utf-8-sig', newline='') as f:
            f.writelines([line + '\n' for line in user_ids])
        print("Data postprocessing for all tasks is finished!")
        return

    def postprocess_users(self):
        print("Postprocessing the crawled users from all tasks...")
        data_users = []
        for worker_id, kwargs in enumerate(self.task_kwargs):
            run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
            file_dir = os.path.join(run_dir, 'data')
            data_users.append(os.path.join(file_dir, 'users.csv'))

        user_ids = []
        users = []
        users_head = None
        for i, file in enumerate(data_users):
            if not os.path.exists(file):
                continue
            with open(file, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f)
                for j, row in enumerate(reader):
                    if users_head is None and j == 0:
                        users_head = row
                    if j == 0:
                        continue
                    user_id = row[15]
                    if user_id not in user_ids:
                        user_ids.append(user_id)
                        users.append(row)
        if len(users) > 0:
            users.sort(key=lambda x: int(x[14]), reverse=True)
            users_file = os.path.join(self.source_dir, 'users.csv')
            with open(users_file, 'w', encoding='utf-8-sig', newline='') as f:
                users_writer = csv.writer(f, dialect='excel')
                users_writer.writerow(users_head)
                for user in users:
                    users_writer.writerow(user)
        print("Users postprocessing of all tasks is finished!")
        return

    def postprocess_final(self):
        print("Postprocessing for all tasks ...")
        users_file = os.path.join(self.source_dir, 'users.csv')
        users_dict = {}
        urls_2_ids = {}
        with open(users_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                users_dict[row[0]] = (i + 1,)
                urls_2_ids[row[15]] = row[0]

        tweets_dict = {}
        for root, dirs, files in os.walk(self.source_dir):
            for file in files:
                if file == 'users.csv':
                    continue
                elif file == 'tweets.csv':
                    tweets_file = os.path.join(root, file)
                    tweets = []
                    tweets_head = None
                    tweets_dict = {}
                    with open(tweets_file, 'r', encoding='utf-8-sig', newline='') as f:
                        reader = csv.reader(f)
                        for i, row in enumerate(reader):
                            if i == 0:
                                tweets_head = row
                                continue
                            try:
                                tweet_id = row[2]
                                row[1] = urls_2_ids[row[1]]
                                row[1] = '=HYPERLINK("../users.csv#users!A{:d}","{:s}")'.format(users_dict[row[1]][0], row[1])
                                row[2] = '=HYPERLINK("./comments/{:s}.csv","{:s}")'.format(row[2], row[2])
                                tweets.append(row)
                                tweets_dict[tweet_id] = len(tweets)
                            except KeyError:
                                print("User {:s} is not found for file {:s}!".format(row[1], tweets_file))
                                continue
                    with open(tweets_file, 'w', encoding='utf-8-sig', newline='') as f:
                        tweets_writer = csv.writer(f, dialect='excel')
                        tweets_writer.writerow(tweets_head)
                        for tweet in tweets:
                            tweets_writer.writerow(tweet)
                elif file.endswith('.csv'):
                    comments_file = os.path.join(root, file)
                    comments = []
                    comments_head = None
                    with open(comments_file, 'r', encoding='utf-8-sig', newline='') as f:
                        reader = csv.reader(f)
                        for i, row in enumerate(reader):
                            if i == 0:
                                comments_head = row
                                continue
                            try:
                                row[1] = urls_2_ids[row[1]]
                                row[0] = '=HYPERLINK("../tweets.csv#tweets!C{:d}","{:s}")'.format(tweets_dict[row[0]][0], row[0])
                                row[1] = '=HYPERLINK("../../users.csv#users!A{:d}","{:s}")'.format(users_dict[row[1]][0], row[1])
                                comments.append(row)
                            except KeyError:
                                print("User {:s} is not found for file {:s}!".format(row[1], comments_file))
                    with open(comments_file, 'w', encoding='utf-8-sig', newline='') as f:
                        comments_writer = csv.writer(f, dialect='excel')
                        comments_writer.writerow(comments_head)
                        for comment in comments:
                            comments_writer.writerow(comment)
        print("Postprocessing for all tasks is finished!")

    def crawl(self):
        if os.path.exists(self.run_dir):
            shutil.rmtree(self.run_dir)
        for worker_id in range(self.max_workers):
            run_dir = os.path.join(self.run_dir, 'weibospider_' + str(worker_id))
            shutil.copytree('weibospider', run_dir)

        self.split_tasks()
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            list(executor.map(self.run_weibo_spider_single, range(self.num_workers)))
        self.postprocess_tasks()

        self.split_users()
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            list(executor.map(self.postprocess_users_single, range(self.num_workers)))
        self.postprocess_users()

        self.postprocess_final()
        shutil.rmtree(self.run_dir)
        return

    def postprocess_topics(self):
        print("Postprocessing for all topics ...")
        with open('settings/remove_users.txt', 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
        users_file = os.path.join(self.source_dir, 'users.csv')
        users_dict = {}
        with open(users_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                users_dict[row[0]] = (row[1], any([(line in row[10] or line in row[1]) for line in lines]))

        topics_dir = os.path.join(self.result_dir, 'topic')
        if not os.path.exists(topics_dir):
            os.makedirs(topics_dir)

        topics_file = None
        topics_head = ['用户名', '时间', '内容', '链接', '赞数', '_id']
        tweets_dict = {}
        for root, dirs, files in os.walk(self.source_dir):
            for file in files:
                if file == 'users.csv':
                    continue
                elif file == 'tweets.csv':
                    tweets_file = os.path.join(root, file)
                    tweets = []
                    with open(tweets_file, 'r', encoding='utf-8-sig', newline='') as f:
                        reader = csv.reader(f)
                        for i, row in enumerate(reader):
                            if i == 0:
                                continue
                            tweets.append(row)
                    topics_file = os.path.join(topics_dir, os.path.basename(os.path.realpath(root)) + '.csv')
                    file_exists = os.path.exists(topics_file)
                    with open(topics_file, 'a', encoding='utf-8-sig', newline='') as f:
                        topics_writer = csv.writer(f, dialect='excel')
                        if not file_exists:
                            topics_writer.writerow(topics_head)
                        for tweet in tweets:
                            user_id = tweet[1].split('","')[-1].split('")')[0]
                            tweet_id = tweet[2].split('","')[-1].split('")')[0]
                            if users_dict[user_id][1]:
                                continue
                            tweets_dict[tweet_id] = (tweet[0],)
                            topics_writer.writerow([users_dict[user_id][0], tweet[3], tweet[12],
                                                   tweet[0], tweet[5], tweet_id])
                elif file.endswith('.csv'):
                    comments_file = os.path.join(root, file)
                    comments = []
                    with open(comments_file, 'r', encoding='utf-8-sig', newline='') as f:
                        reader = csv.reader(f)
                        for i, row in enumerate(reader):
                            if i == 0:
                                continue
                            comments.append(row)
                    with open(topics_file, 'a', encoding='utf-8-sig', newline='') as f:
                        topics_writer = csv.writer(f, dialect='excel')
                        for comment in comments:
                            user_id = comment[1].split('","')[-1].split('")')[0]
                            tweet_id = comment[0].split('","')[-1].split('")')[0]
                            if users_dict[user_id][1] or tweet_id not in tweets_dict:
                                continue
                            topics_writer.writerow([users_dict[user_id][0], comment[5], comment[2],
                                                   tweets_dict[tweet_id][0], comment[4], comment[3]])

        for file in os.listdir(topics_dir):
            file_name = os.path.splitext(file)[0]
            topics_file = os.path.join(topics_dir, file)
            print('Postprocessing for topic {:s}...'.format(file_name))
            if file_name not in self.keywords:
                if os.path.exists(topics_file):
                    os.remove(topics_file)
                continue
            contents = []
            with open(topics_file, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if i == 0:
                        continue
                    row[2] = extract_comment_content(row[2])
                    if len(row[2]) < 5 or any([difflib.SequenceMatcher(None, row[2], content[2]).quick_ratio() > 0.7
                                               for content in contents]):
                        continue
                    contents.append(row)
            contents.sort(key=lambda x: int(x[4]), reverse=True)
            with open(topics_file, 'w', encoding='utf-8-sig', newline='') as f:
                topics_writer = csv.writer(f, dialect='excel')
                topics_writer.writerow(topics_head)
                for content in contents:
                    topics_writer.writerow(content)

        comments = []
        for file in os.listdir(topics_dir):
            topics_file = os.path.join(topics_dir, file)
            with open(topics_file, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if i == 0:
                        continue
                    comments.append(row[2])
        comments_file = os.path.join(self.result_dir, 'comments.txt')
        with open(comments_file, 'w', encoding='utf-8-sig', newline='') as f:
            f.writelines([line + '\n' for line in comments])
        print("Postprocessing for all topics is finished!")
        return

    def analyze(self):
        # self.postprocess_topics()
        self.data_analyzer()
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weibo Spider')
    parser.add_argument('--keywords', type=str, default='')
    parser.add_argument('--date-start', type=str, default='')  # '2018-01-01'
    parser.add_argument('--date-end', type=str, default='')
    parser.add_argument('--min-like-num', type=int, default=0)
    parser.add_argument('--min-repost-num', type=int, default=0)
    parser.add_argument('--min-comment-num', type=int, default=0)
    parser.add_argument('--file-dir', type=str, default='data')
    parser.add_argument('--run-dir', type=str, default='temp')
    parser.add_argument('--max-workers', type=int, default=24)
    parser.add_argument('--days-per-worker', type=int, default=60)
    parser.add_argument('--num-topics', type=int, default=12)
    parser.add_argument('--max-iter', type=int, default=5000)
    parser.add_argument('--num-top-words', type=int, default=20)
    args = parser.parse_args()

    keywords = []
    with open('settings/keywords.txt', 'r', encoding='utf-8-sig', newline='') as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines]
        for i, line in enumerate(lines):
            if line not in keywords:
                keywords.append(line)
    args.keywords = ','.join(keywords)

    weibospider_runner = WeiboSpiderRunner(args.keywords, args.date_start, args.date_end,
                                           args.min_like_num, args.min_repost_num, args.min_comment_num,
                                           args.file_dir, args.run_dir, args.max_workers, args.days_per_worker,
                                           args.num_topics, args.max_iter, args.num_top_words)
    weibospider_runner.crawl()
    weibospider_runner.analyze()

