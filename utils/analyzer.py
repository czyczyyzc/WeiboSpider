# -*- coding: utf-8 -*-
import os
import csv
import math
import jieba
import jieba.analyse
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from PIL import Image
from snownlp import SnowNLP
from wordcloud import WordCloud, ImageColorGenerator
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from concurrent.futures import ProcessPoolExecutor
from .lda import LDAModel

data_root = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'data')
tfidf = jieba.analyse.extract_tags
matplotlib.rcParams['font.sans-serif'] = ['SimSun']  # 用黑体显示中文 # 'SimHei'
matplotlib.rcParams['axes.unicode_minus'] = False    # 正常显示负号


class DataAnalyzer(object):
    def __init__(self, file_dir, num_topics=10, max_iter=1000, n_top_words=20):
        self.file_dir = file_dir
        self.comments_file = os.path.join(file_dir, 'comments.txt')
        self.keywords_file = os.path.join(file_dir, 'keywords.txt')
        self.num_topics = num_topics
        self.max_iter = max_iter
        self.n_top_words = n_top_words
        self.jieba_load = False
        self.cntTf = None

    def load_jieba(self):
        if self.jieba_load:
            return
        jieba.load_userdict(os.path.join(data_root, 'SogouLabDic.txt'))
        jieba.load_userdict(os.path.join(data_root, 'dict_baidu_utf8.txt'))
        jieba.load_userdict(os.path.join(data_root, 'dict_pangu.txt'))
        jieba.load_userdict(os.path.join(data_root, 'dict_sougou_utf8.txt'))
        jieba.load_userdict(os.path.join(data_root, 'dict_tencent_utf8.txt'))
        jieba.load_userdict(os.path.join(data_root, 'my_dict.txt'))
        self.jieba_load = True

    def cut_words(self):
        print("Cutting words ...")
        self.load_jieba()
        stop_words = []
        with open(os.path.join(data_root, 'Stopword.txt'), 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
            stop_words.extend(lines)
        with open(os.path.join(data_root, 'stop_words.txt'), 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
            stop_words.extend(lines)

        comments = []
        with open(self.comments_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
        for line in lines:
            words = jieba.cut(line)
            comment = []
            for word in words:
                if word in stop_words:
                    continue
                comment.append(word)
            comments.append(' '.join(comment))

        key_words = []
        for comment in comments:
            keywords = tfidf(comment,
                             allowPOS=('ns', 'nr', 'nt', 'nz', 'nl', 'n', 'vn', 'vd', 'vg', 'v', 'vf', 'a', 'an', 'i'))
            keywords = [keyword for keyword in keywords]
            if len(keywords) > 0:
                key_words.append(' '.join(keywords))
        with open(self.keywords_file, 'w', encoding='utf-8-sig', newline='') as f:
            f.writelines([line + '\n' for line in key_words])
        print("Words cutting is finished!")
        return

    def wordcloud(self):
        print("Gnerating word cloud ...")
        self.load_jieba()
        with open(self.keywords_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
        lyric = ''
        for line in lines:
            lyric += line
        result = jieba.analyse.textrank(lyric, topK=50, withWeight=True)

        keywords = dict()
        for i in result:
            keywords[i[0]] = i[1]
        # print(keywords)

        image = Image.open(os.path.join(data_root, 'background.png'))
        graph = np.array(image)
        wc = WordCloud(font_path=os.path.join(data_root, 'Songti.ttc'), background_color='White', max_words=50, mask=graph)
        wc.generate_from_frequencies(keywords)
        image_color = ImageColorGenerator(graph)
        plt.imshow(wc)
        plt.imshow(wc.recolor(color_func=image_color))
        plt.axis("off")
        # plt.show()
        wc.to_file(os.path.join(self.file_dir, 'word_cloud.png'))

        bar_width = 0.5
        X = list(keywords.keys())
        Y = list(keywords.values())
        fig = plt.figure(figsize=(25, 10))
        plt.bar(range(len(X)), Y, tick_label=X, width=bar_width)
        # plt.xlabel("X-axis")
        # plt.ylabel("Y-axis")
        plt.xticks(rotation=50, fontsize=20)
        plt.yticks(fontsize=20)
        plt.title("words frequency chart", fontsize=20)
        plt.savefig(os.path.join(self.file_dir, 'word_frequency.png'), dpi=360)
        # plt.show()
        print("Word cloud generating is finished!")
        return

    def sentiment(self):
        print("Analyzing sentiments ...")
        with open(self.keywords_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
        pos_count = 0
        neg_count = 0
        for line in lines:
            s = SnowNLP(line)
            rates = s.sentiments
            if rates >= 0.5:
                pos_count += 1
            elif rates < 0.5:
                neg_count += 1
            else:
                pass
        labels = ('Positive Side', 'Negative Side')
        fracs = [pos_count, neg_count]
        explode = [0.1, 0]  # 0.1 凸出这部分，
        fig = plt.figure(figsize=(10, 5))
        plt.axes(aspect=1)  # set this , Figure is round, otherwise it is an ellipse
        patches, l_text, p_text = plt.pie(x=fracs, labels=labels, explode=explode, autopct='%3.1f %%',
                                          shadow=True, labeldistance=1.1, startangle=90, pctdistance=0.6)
        for t in l_text:
            t.set_size(30)
        for t in p_text:
            t.set_size(20)
        plt.savefig(os.path.join(self.file_dir, 'emotions_pie_chart.png'), dpi=360)
        # plt.show()
        print("Sentiments analyzation is finished!")
        return

    def lda_test(self, num_topic):
        lda = LatentDirichletAllocation(n_components=num_topic, max_iter=50,
                                        learning_method='batch', learning_offset=50., random_state=0)
        lda.fit_transform(self.cntTf)
        return lda.perplexity(self.cntTf)

    def topic_lad(self):
        print("Clustering topics ...")
        # lad_model = LDAModel(self.file_dir)
        # lad_model()
        with open(self.keywords_file, 'r', encoding='utf-8-sig', newline='') as f:
            lines = f.readlines()
            lines = [line.strip() for line in lines]
        corpus = lines
        cntVector = CountVectorizer()
        cntTf = cntVector.fit_transform(corpus)

        # if isinstance(self.num_topics, list) and len(self.num_topics) > 1:
        #     print("The numbers of topics for evaluation are {:s}".format(str(self.num_topics)))
        #     self.cntTf = cntTf
        #     with ProcessPoolExecutor(max_workers=len(self.num_topics)) as executor:
        #         perplexities = list(executor.map(self.lda_test, self.num_topics))
        #     for i in range(len(self.num_topics)):
        #         print("The preplexity for {:d} topics is {}".format(self.num_topics[i], perplexities[i]))
        #     self.num_topics = self.num_topics[perplexities.index(min(perplexities))]

        print("The number of topics is setting to {:d}.".format(self.num_topics))
        vocs = cntVector.get_feature_names()
        lda = LatentDirichletAllocation(n_components=self.num_topics, max_iter=self.max_iter,
                                        learning_method='batch', learning_offset=50., random_state=0)
        docres = lda.fit_transform(cntTf)

        # 文档所属每个类别的概率
        LDA_corpus = np.array(docres)
        LDA_corpus_one = np.argmax(LDA_corpus, axis=1)
        with open(os.path.join(self.file_dir, 'docs_topic_file.csv'), 'w', encoding='utf-8-sig', newline='') as f:
            td_writer = csv.writer(f, dialect='excel')
            td_writer.writerow([''] + ['主题{:d}'.format(y) for y in range(LDA_corpus.shape[1])] + ['属于'])
            for x in range(LDA_corpus.shape[0]):
                topic_docs = [str(LDA_corpus[x][y]) for y in range(LDA_corpus.shape[1])]
                td_writer.writerow(['文档{:d}'.format(x)] + topic_docs + ['主题{:d}'.format(LDA_corpus_one[x])])

        with open(os.path.join(self.file_dir, 'topic_word_file.csv'), 'w', encoding='utf-8-sig', newline='') as f:
            tw_writer = csv.writer(f, dialect='excel')
            tt_matrix = lda.components_
            for i, tt_m in enumerate(tt_matrix):
                tt_dict = [(name, tt) for name, tt in zip(vocs, tt_m)]
                tt_dict = sorted(tt_dict, key=lambda x: x[1], reverse=True)
                tt_dict = tt_dict[:self.n_top_words]
                tt_dict = list(zip(*tt_dict))
                tw_writer.writerow(['主题{:d}'.format(i)] + list(tt_dict[0]))
                tw_writer.writerow(['概率'] + list(np.array(tt_dict[1]) / sum(tt_dict[1])))

        num_cols = int(math.ceil(math.sqrt(self.num_topics) * 4 / 3))
        num_rows = int(math.ceil(self.num_topics / num_cols))
        fig, axes = plt.subplots(num_rows, num_cols, figsize=(num_cols * 5, num_rows * 7), sharex=True)
        axes = axes.flatten()
        for topic_idx, topic in enumerate(lda.components_):
            top_features_ind = topic.argsort()[:-self.n_top_words - 1:-1]
            top_features = [vocs[i] for i in top_features_ind]
            weights = topic[top_features_ind]
            ax = axes[topic_idx]
            ax.barh(top_features, weights, height=0.7)
            ax.set_title('Topic {:d}'.format(topic_idx + 1), fontsize=30)
            ax.invert_yaxis()
            ax.tick_params(axis='both', which='major', labelsize=20)
            for i in 'top right left'.split():
                ax.spines[i].set_visible(False)
            fig.suptitle('Topics in LDA model', fontsize=40)

        plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
        plt.savefig(os.path.join(self.file_dir, 'topic_words.png'), dpi=360)
        # plt.show()
        print("Topics clustering is finished!")
        return

    def __call__(self):
        self.cut_words()
        self.wordcloud()
        self.sentiment()
        self.topic_lad()
