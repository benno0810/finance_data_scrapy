
# -*- coding: utf-8 -*-

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#sys.path.append(r'C:\Users\benno\OneDrive\Documents\GitHub\xueqiu_data_scrapy\Finance\finance')
import scrapy
import json
import time
import math
import re
import urllib
from scrapy.http import Request
from scrapy.http.cookies import CookieJar 
from urllib import parse
#from  items import IceItem,IcePostingsItem,IceUsersItem,IceResultMapperItem
from ..items import IceItem
from scrapy.loader import ItemLoader
from urllib.parse import quote,unquote
import logging

class XueqiuSpider(scrapy.Spider):
    """
    雪球投资网头条模块页面解析类
    https://xueqiu.com/
    """
    name = 'xueqiu'
    # allowed domains
    # allowed_domains = ['http://xueqiu.com/']
    custom_settings = {
        "COOKIES_ENABLED": False,
        #"DOWNLOAD_TIMEOUT":10
        "DOWNLOAD_DELAY": 1,
        "COOKIES_DEBUG": True,
        'RETRY_TIMES': 14,
        "RETRY_HTTP_CODES":[500, 502, 503, 504, 522, 524, 408, 429,400,501],
        "LOG_FILE" : 'xueqiu.log',
        "LOG_ENABLED":True,
        "LOG_LEVEL": logging.DEBUG,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7,ja;q=0.6',
            'Connection': 'keep-alive',
            'Host':'xueqiu.com',
            'Cookie':'device_id=24700f9f1986800ab4fcc880530dd0ed; s=cg17l9xktw; __utmz=1.1607757839.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); Hm_lvt_1db88642e346389874251b5a1eded6e3=1607495995,1607668233,1607844144,1608110356; bid=7468459e7c81c1a95f34e7ec5608c351_kir7e2mt; __utmc=1; last_account=benno0810%40gmail.com; cookiesu=351608447686949; __utma=1.145531423.1607757839.1608445020.1608447722.6; xq_a_token=ad26f3f7a7733dcd164fe15801383e62b6033003; xqat=ad26f3f7a7733dcd164fe15801383e62b6033003; xq_r_token=15b43888685621c645835bfe2d97242dc20b9005; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYxMTI4MzA4NCwiY3RtIjoxNjA4NzA4NjY4MDcxLCJjaWQiOiJkOWQwbjRBWnVwIn0.Qgamu12xrbTUv1Zed0y9vHUddfmiqPm2HcDN-qzVohogO4Wl4wTGbmgTr3OZNt5GBgBIIbyDUu4leYg5fF_5M2GKgoqQMuogxGxDztRePCQ7q3uaSU6htfkCG4fpDeS6oK6XjbHDsa2BF0kn1XPcH_2gE7YQC5lSIu__N6Yu-axVGbEG9ne5Vq9CdcH7uNdnH7nGMnwR8sifyfW1ygcuHH4j87Ij7bnc4Zu-tF4h2nBjcsuTNG-d0nSF1aJyn0KZ5FPoPf9D5X0e_TwUls0hR8_9Gs4gvsI635t4zPbMhcGWrGfSl4OXwDDTmrqDCpvkUa-cSnOH6T-sfg02WkFtJw; u=761608708685575; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1608709643',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
    }
    start_urls = [
        'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?since_id=-1&max_id=-1&count=10&category=-1']

    def parse(self, response):

        """
        此网站全为异步加载，使用json数据格式
        1. get every announcement's target url for further analysis
        2. get next page url
        :param response:
        :return:
        """
        js = json.loads(response.body.decode('utf-8'))
        print(js)
        next_max_id = js['next_max_id']
        for item in js['list']:
            data = json.loads(item['data'])
            title = data['title']
            create_time = data['created_at']
            yield Request(url=parse.urljoin(response.url, data['target']),
                          meta={'title': title, 'create_time': create_time,'download_timeout':10}, callback=self.parse_detail,
                          dont_filter=True)

        if next_max_id != -1:
            next_url = 'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?since_id=-1&max_id=' + str(
                next_max_id) + '&count=10&category=-1'
            yield Request(url=next_url,meta={'title': title, 'create_time': create_time,'download_timeout':10},callback=self.parse, dont_filter=True)

    def parse_detail(self, response):

        item = ItemLoader(item=IceItem(), response=response)

        title = response.meta.get('title')
        create_time = response.meta.get('create_time')
        item.add_value('title', title)
        item.add_value('create_time', create_time)
        item.add_css('author_name', '.avatar__name a::attr(data-screenname)')
        content = "".join(
            list(response.css('.article__bd__detail p::text,.article__bd__detail img::attr(src)').extract()))
        item.add_value('content', content)
        return item.load_item()
