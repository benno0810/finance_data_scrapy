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

#from  items import IceItem,IceportfoliosItem,IceUsersItem,IceResultMapperItem
from ..items import IceItem,IcePostingsItem,IceUsersItem,IceResultMapperItem,IcePortfolioItem
from scrapy.loader import ItemLoader
from urllib.parse import quote,unquote
import logging

class XueqiuportfoliosSpider(scrapy.Spider):
    """
    雪球投资网搜索结果解析
    https://xueqiu.com/
    """
    name = 'xueqiu_portfolios'
    # allowed domains
    # allowed_domains = ['http://xueqiu.com/']
    custom_settings = {
        "COOKIES_ENABLED": True,
        "DOWNLOAD_DELAY": 1,
        "COOKIES_DEBUG": True,
        'RETRY_TIMES': 4,
        "RETRY_HTTP_CODES":[500, 502, 503, 504, 522, 524, 408, 429,400,501],
        "LOG_FILE" : 'xueqiuportfolio.log',
        "LOG_ENABLED":True,
        "LOG_LEVEL": logging.DEBUG,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'a"text/html,appliloggingcation/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"',
            'Connection': 'keep-alive',
            "Host": "xueqiu.com",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        }
    }
    #雪球需要手动登录更目录， 存储cookies
    cookie_jar = CookieJar()
    start_urls=[
        "https://xueqiu.com/cube/search.json?q=SP{}&count=20&page=1".format(i) for i in range(1000000,1015000)
    ]
    
    
    def start_requests(self):
        """
        此网站首先访问首页， 获得有效cookies
        """
        main_url="https://xueqiu.com"
        yield Request(url=main_url,callback=self.parse_urls,dont_filter=True,priority=999)

    def parse_urls(self,response):
        for url in self.start_urls:
            yield Request(url=url,callback=self.parse,dont_filter=True)

    def parse(self, response):

        """
        crawl from a json-api 
        1. generate search_result urls for every query_string
        2. get next page url for every query_string
        request exceed maxCount or maxPage will raise error with errorcode 501 and status code 200, which is the same as 
        crowd control mechanism

        :param response:
        :return:
        """
        print('爬取链接',response.url)
        self.logger.info('爬取链接{}'.format(response.url))
        pattern=re.compile('q=([\u4e00-\u9fa5_a-zA-Z0-9]{0,})')
        target_url=unquote(response.url)
        keyword=re.findall(pattern,target_url)
        self.logger.info('组合{}'.format(keyword))
        print('组合{}'.format(keyword))
        js = json.loads(response.body.decode('utf-8'))
        print(js)

        if js.get('code')!=501:
            if js.get('totalCount') and js.get('totalCount') !=0:
                #proceed to next page
                total_count = js['totalCount']
                current_url_id = js['q']

                yield self.parse_detail(response,js)
        else:
            yield Request(url=response.url, callback=self.parse, dont_filter=True)
        

    # it is not economical at all, a very large respons is returned, but it is necessary to return once a portfolio because possible duplicates
    def parse_detail(self,response,portfolios):

        item_portfolio =ItemLoader(item=IcePortfolioItem(),response=response)
        portfolio = portfolios['list'][0]
        name = portfolio['symbol']
        portfolio_screen_name=portfolio['name']

        owner_id=portfolio['owner_id']
        updated_at = portfolio['updated_at'] if portfolio['updated_at'] else portfolio['created_at']
        info = portfolio
        item_portfolio.add_value('name',name)
        item_portfolio.add_value('portfolio_screen_name',portfolio_screen_name)
        item_portfolio.add_value('owner_id',owner_id)
        item_portfolio.add_value('updated_at',updated_at)
        item_portfolio.add_value('info',info)
        return item_portfolio.load_item()

        
        



