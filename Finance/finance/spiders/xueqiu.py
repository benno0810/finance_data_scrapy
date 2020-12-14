# -*- coding: utf-8 -*-

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(r"C:\Users\benno\OneDrive\Documents\GitHub\happy-spiders\scrapy_templates\02-finance\finance")

import scrapy
import json
import time
from scrapy.http import Request
from items import IceItem
from scrapy.loader import ItemLoader
from urllib import parse


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
        "DOWNLOAD_DELAY": 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Cookie': 'device_id=24700f9f1986800ab4fcc880530dd0ed; xq_a_token=1132205e8c57eb587b26526804cff9f3b6bf6799; xqat=1132205e8c57eb587b26526804cff9f3b6bf6799; xq_r_token=81b9c911ea3907729d8f8e9f60d9f5251227c551; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYwOTEyMzA1NywiY3RtIjoxNjA3NjY4MTk3MzEwLCJjaWQiOiJkOWQwbjRBWnVwIn0.QCIpBY5aFGNq8Lf7ReIs-SVIQZc98QGVbyapzPTDdJbAYxLVyaZQFXxWgJbOcYWI_hBvBd28Ots5vqT_RjlEu4Q0txqztFOvvyK_BHznRBhZQqTIc6kkZyEruPausPUxc4XtZIlKCscyxjyRKFiIDsMxaLDMD7aoNNLeKowhPPOhea7Q6j3TGhkU1ZKJ_CqQ_-yXZBE6nK_weRYZHDK5f7RZ1rtvQ-1UvFECYRwK6-YR-Mj_jm7Mo4RzdZoogCZpOIE0EqI8t5-zpklR7dwD7rWEz2PMdD-9nUWrbP-nKmqJncB_VciPxfrXqtH_vQIvydfR77zHw8qMkF3Eqj8j9w; u=381607668231386; Hm_lvt_1db88642e346389874251b5a1eded6e3=1607072124,1607317521,1607495995,1607668233; acw_tc=2760824216077551605442227eb983b47da01c5b0e811c7d85494505aa3fca; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1607755285',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
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
        next_max_id = js['next_max_id']
        for item in js['list']:
            data = json.loads(item['data'])
            title = data['title']
            create_time = data['created_at']
            yield Request(url=parse.urljoin(response.url, data['target']),
                          meta={'title': title, 'create_time': create_time}, callback=self.parse_detail,
                          dont_filter=True)

        if next_max_id != -1:
            next_url = 'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?since_id=-1&max_id=' + str(
                next_max_id) + '&count=10&category=-1'
            yield Request(url=next_url, callback=self.parse, dont_filter=True)

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
