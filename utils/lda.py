# -*- coding:utf-8 -*-
import os
import csv
import random
import numpy as np
from collections import OrderedDict


class Document(object):
    def __init__(self):
        self.words = []
        self.length = 0


class StatisticalData(object):
    def __init__(self):
        self.docs_count = 0
        self.words_count = 0
        self.docs = []
        self.word2id = OrderedDict()
        self.id2word = OrderedDict()


class LDAModel(object):

    def __init__(self, file_dir, K=10, alpha=0.5, beta=0.1, iter_times=1000, top_words_num=20):
        self.K = K
        self.beta = beta
        self.alpha = alpha
        self.iter_times = iter_times
        self.top_words_num = top_words_num
        self.train_data_file = os.path.join(file_dir, 'keywords.txt')
        self.topic_word_file = os.path.join(file_dir, 'topic_word_file.csv')
        self.topic_docs_file = os.path.join(file_dir, 'topic_docs_file.csv')
        self.topic_docs_word = os.path.join(file_dir, 'topic_docs_word.txt')

        self.data  = self.preprocessing()
        self.p     = np.zeros(self.K)
        self.nw    = np.zeros((self.data.words_count, self.K), dtype='int')
        self.nd    = np.zeros((self.data.docs_count, self.K), dtype='int')
        self.nwsum = np.zeros(self.K, dtype='int')
        self.ndsum = np.zeros(self.data.docs_count, dtype='int')
        self.Z     = np.array([[0 for y in range(self.data.docs[x].length)] for x in range(self.data.docs_count)], dtype=object)
        for x in range(len(self.Z)):
            self.ndsum[x] = self.data.docs[x].length
            for y in range(self.data.docs[x].length):
                topic = random.randint(0, self.K - 1)
                self.Z[x][y] = topic
                self.nw[self.data.docs[x].words[y]][topic] += 1
                self.nd[x][topic] += 1
                self.nwsum[topic] += 1
        self.theta = np.array([[0.0 for y in range(self.K)] for x in range(self.data.docs_count)])
        self.phi = np.array([[0.0 for y in range(self.data.words_count)] for x in range(self.K)])
        self.top_words_num = min(self.top_words_num, self.data.words_count)

    def preprocessing(self):
        print("Loading data for lda analysis ...")
        with open(self.train_data_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip().split() for line in lines]
        print("Data loading is finished!")

        print("Generating the statistical data ...")
        data = StatisticalData()
        items_idx = 0
        for line in lines:
            if len(line) == 0:
                continue
            doc = Document()
            for item in line:
                if item in data.word2id:
                    doc.words.append(data.word2id[item])
                else:
                    data.word2id[item] = items_idx
                    doc.words.append(items_idx)
                    items_idx += 1
            doc.length = len(line)
            data.docs.append(doc)
        print("Data generation is finished!")

        data.id2word = OrderedDict({value: key for key, value in data.word2id.items()})
        data.docs_count = len(data.docs)
        data.words_count = len(data.word2id)
        print("There are total {:d} documents in total.".format(data.docs_count))
        return data

    def sampling(self, i, j):
        topic = self.Z[i][j]
        word = self.data.docs[i].words[j]
        self.nw[word][topic] -= 1
        self.nd[i][topic] -= 1
        self.nwsum[topic] -= 1
        self.ndsum[i] -= 1

        Vbeta = self.data.words_count * self.beta
        Kalpha = self.K * self.alpha
        self.p = (self.nw[word] + self.beta) / (self.nwsum + Vbeta) * \
                 (self.nd[i] + self.alpha) / (self.ndsum[i] + Kalpha)
        p = np.squeeze(np.asarray(self.p / np.sum(self.p)))
        topic = np.argmax(np.random.multinomial(1, p))

        self.nw[word][topic] += 1
        self.nd[i][topic] += 1
        self.nwsum[topic] += 1
        self.ndsum[i] += 1
        return topic

    def __call__(self):
        print("Training for LDA ...")
        for x in range(self.iter_times):
            if x % 1 == 0:
                print("Iteration {:d}".format(x))
            for i in range(self.data.docs_count):
                for j in range(self.data.docs[i].length):
                    topic = self.sampling(i, j)
                    self.Z[i][j] = topic
        print("Training is finished!")

        print("Calculating the distribution of documents and topics ...")
        for i in range(self.data.docs_count):
            self.theta[i] = (self.nd[i] + self.alpha) / (self.ndsum[i] + self.K * self.alpha)
        print("Calculating the distribution of words and topics ...")
        for i in range(self.K):
            self.phi[i] = (self.nw.T[i] + self.beta) / (self.nwsum[i] + self.data.words_count * self.beta)
        print("Calculation is Finished!")
        
        print("The distribution of topics and top {:d} words are saving to {:s}".format(self.K, self.topic_word_file))
        with open(self.topic_word_file, 'w', encoding='utf-8-sig', newline='') as f:
            topic_word_writer = csv.writer(f, dialect='excel')
            for x in range(self.K):
                topic_words = [(n, self.phi[x][n]) for n in range(self.data.words_count)]
                topic_words.sort(key=lambda word: word[1], reverse=True)
                topic_words = [(self.data.id2word[topic_words[y][0]], str(topic_words[y][1]))
                               for y in range(self.top_words_num)]
                topic_words = list(zip(*topic_words))
                topic_word_writer.writerow(['主题{:d}'.format(x)] + list(topic_words[0]))
                topic_word_writer.writerow(['概率'] + list(topic_words[1]))

        print("The distribution of topics and documents are saving to {:s}".format(self.topic_docs_file))
        with open(self.topic_docs_file, 'w', encoding='utf-8-sig', newline='') as f:
            topic_docs_writer = csv.writer(f, dialect='excel')
            topic_docs_writer.writerow([''] + ['主题{:d}'.format(y) for y in range(self.K)])
            for x in range(self.data.docs_count):
                topic_docs = [str(self.theta[x][y]) for y in range(self.K)]
                topic_docs_writer.writerow(['文档{:d}'.format(x)] + topic_docs)

        with open(self.topic_docs_word, 'w', encoding='utf-8-sig', newline='') as f:
            for x in range(self.data.docs_count):
                for y in range(self.data.docs[x].length):
                    f.write(self.data.id2word[self.data.docs[x].words[y]] + ':' +
                            '主题{:d}'.format(self.Z[x][y]) + ' ')
                f.write('\n')
        print("Saving is finished!")
