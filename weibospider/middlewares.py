# encoding: utf-8
import os

ips_file = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'ips.txt')
with open(ips_file, 'r', encoding='utf-8-sig', newline='') as f:
    ips = f.readlines()
    ips = [ip.strip() for ip in ips]
    ips = [ip if ip != 'default' else None for ip in ips]
IPS = dict(zip(range(len(ips)), ips))


class IPProxyMiddleware(object):

    def fetch_proxy(self):
        # You need to rewrite this function if you want to add proxy pool
        # the function should return a ip in the format of "ip:port" like "12.34.1.4:9090"
        return IPS[int(os.path.basename(os.path.split(os.path.realpath(__file__))[0]).split('_')[-1]) % len(IPS.keys())]

    def process_request(self, request, spider):
        proxy_data = self.fetch_proxy()
        if proxy_data:
            current_proxy = f'http://{proxy_data}'
            spider.logger.debug(f"current proxy:{current_proxy}")
            request.meta['proxy'] = current_proxy
