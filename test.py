#coding=utf-8
import os
import time
import shutil
import requests

ips_file = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'settings', 'ips.txt')
with open(ips_file, 'r', encoding='utf-8-sig', newline='') as f:
    ips = f.readlines()
    ips = [ip.strip() for ip in ips]


def test(proxyaddr, proxyport, proxyusernm="czyczyyzc", proxypasswd='2145786369czy'):
    # proxyurl = "http://" + proxyusernm + ":" + proxypasswd + "@" + proxyaddr + ":" + "%d" % int(proxyport)
    # ips = ['czyczyyzc:2145786369czy@' + ip for ip in ips]
    # url = 'https://search.suning.com/%E8%92%99%E7%89%9B/'
    proxyurl = "http://" + proxyaddr + ":" + "%d" % int(proxyport)
    print(proxyurl)
    url = 'http://www.baidu.com'
    # url = 'https://www.weibo.cn'
    resp = requests.get(url, proxies={'http': proxyurl, 'https': proxyurl}, headers={"User-Agent": "curl/0.7.6"})
    print(resp.status_code)
    print(resp.text)
    return resp.status_code


for ip in ips:
    if ip == 'default':
        continue
    host, port = ip.split(':')
    # if test(host, port) == 200:
    #     print(ip)
    test(host, port)
    time.sleep(2)

# cookies_file = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'cookies.txt')
# with open(cookies_file, 'r', encoding='utf-8-sig', newline='') as f:
#     cookies = f.readlines()
#     cookies = [cookie.strip() for cookie in cookies]
# COOKIES = dict(zip(range(len(cookies)), cookies))
# print(COOKIES)

# shutil.copy('ips.txt', 'weibospider/ips.txt')
# shutil.copy('cookies.txt', 'weibospider/cookies.txt')