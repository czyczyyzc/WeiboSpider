import os
import re
import jieba
import collections
import numpy as np
import wordcloud
import matplotlib.pyplot as plt
from PIL import Image


def extract_comment_content(s):
    filter_re1 = re.compile("转发理由:(.*)|回复@(.*)|转发微博|转发")
    filter_re2 = re.compile("#(.*?)#|【(.*?)】|\[(.*?)\]|[\U00010000-\U0010ffff]|\\\\")
    filter_re3 = re.compile("(//)?@(.*?)[,，.。:：;； ]")
    filter_re4 = re.compile("(//)?@(.*)")
    filter_re5 = re.compile("(http|https)://[a-zA-Z0-9.?/&=:]*")
    filter_re6 = re.compile("抱歉，作者已设置(.*?)。|该账号因被投诉(.*?)。|查看帮助")
    filter_re7 = re.compile('[^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a\+\-\*/%\(\)（）<=>,，\.。!！:：;；\?？\"\'‘’“”、…《》 ]')

    s = filter_re1.sub('', s)
    s = filter_re2.sub('', s)
    s = filter_re3.sub('', s)
    s = filter_re4.sub('', s)
    s = filter_re5.sub('', s)
    s = filter_re6.sub('', s)
    s = filter_re7.sub('', s)
    for x in ['哇', '哈', '嗯', 'em', 'ok', '\?', '？', '!', '！', '~', ',', '，', '\.', '。', '…']:
        filter_re = re.compile(x + '+')
        s = filter_re.sub(x.replace('\\', ''), s)
    s = s.strip()
    return s


def wordcount(string_data, result_dir):
    # 文本预处理
    pattern = re.compile('[\t\n\\\\\+\-\*/%\(\)（）<=>,，\.。!！:：;；\?？\"\'‘’“”、…《》0-9a-zA-Z]')  # 定义正则表达式匹配模式
    string_data = re.sub(pattern, '', string_data)  # 将符合模式的字符去除

    # 文本分词
    seg_list_exact = jieba.cut(string_data, cut_all=False)  # 精确模式分词
    object_list = []
    remove_chars = ['这', '不', '有', '可', '是', '或', '何', '其', '还', '来', '几', '及', '其',
                    '自', '本', '我', '你', '她', '他', '们', '今', '各', '之', '乎', '者', '也',
                    '前', '后', '左', '右', '上', '下', '东', '西', '南', '北', '中', '已', '出',
                    '的', '得', '时', '过', '很', '好', '在', '必', '相', '直', '用', '如', '无',
                    '多', '少', '为', '因', '所', '以', '对', '进', '行', '应', '要', '须', '需',
                    '到', '始', '每', '天', '于', '而', '且', '虽', '然', '但', '是', '竟', '谢',
                    '了', '才', '只', '最', '早', '就', '随', '便', '能', '那', '些', '哪', '里',
                    '啊', '呵', '哈', '吧', '呀', '嘛', '么',
                    '看', '做',
                    '零', '一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十',
                    '年', '月', '日', '刚',]  # 自定义去除词库

    remove_words = ['问题', '厉害', '关键', '容易', '全部', '支持', '严重', '造成', '分享', '建议',
                    '处理', '事情', '情况', '人员', '解决', '恶心', '提供', '选择', '推荐', '设置',
                    '告诉', '导致', '表示', '确实', '增加', '愿意', '麻烦', '禁止', '避免', '认识',
                    '允许', '结束', '赶紧', '根据', '肯定', '收集', '采集', '活动', '参与', '同样',
                    '影响', '删除', '打开', '方式', '甚至', '继续', '等待', '期间', '非常', '希望',
                    '发现', '特别', '比较', '经常', '完整', '反正', '知道', '喜欢', '注意', '感觉',
                    '获取', '显示', '方面', '理解', '地方', '大家', '别人', '提醒', '重视', '结果',
                    '完全', '明确', '发生', '内容', '包括', '人家', '难道']

    for word in seg_list_exact:  # 循环读出每个分词
        # if word not in remove_words:  # 如果不在去除词库中
        if len(word) < 2 or word in remove_words or any([x in word for x in remove_chars]):
            continue
        object_list.append(word)  # 分词追加到列表

    # 词频统计
    word_counts = collections.Counter(object_list)  # 对分词做词频统计
    word_counts_topk = word_counts.most_common(100)  # 获取前10最高频的词
    print(word_counts_topk)  # 输出检查

    # 词频展示
    # mask = np.array(Image.open(os.path.join(result_dir, 'wordcloud.jpg')))  # 定义词频背景
    mask = None
    wc = wordcloud.WordCloud(
        font_path='C:/Windows/Fonts/simhei.ttf',  # 设置字体格式
        mask=mask,  # 设置背景图
        width=800,
        height=400,
        max_words=200,  # 最多显示词数
        max_font_size=100,  # 字体最大值
        background_color='white',
    )

    wc.generate_from_frequencies(word_counts)  # 从字典生成词云
    # image_colors = wordcloud.ImageColorGenerator(mask)  # 从背景图建立颜色方案
    # wc.recolor(color_func=image_colors)  # 将词云颜色设置为背景图方案

    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0, 0)

    plt.imshow(wc)  # 显示词云
    plt.axis('off')  # 关闭坐标轴
    # plt.show()  # 显示图像

    dpi = 600
    fig = plt.gcf()
    fig.set_size_inches(800 / dpi, 400 / dpi)
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0, 0)
    fig.savefig(os.path.join(result_dir, 'wordcloud.png'), transparent=True, dpi=dpi, pad_inches=0)
    plt.show()
    return
