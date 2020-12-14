# -*- coding: utf-8 -*-

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import scrapy
import json
import time
import math
import re
import urllib
from scrapy.http import Request
from scrapy.http.cookies import CookieJar 
from ..items import IceItem,IcePostingsItem,IceUsersItem
from scrapy.loader import ItemLoader
from urllib.parse import quote,unquote
import logging
class XueqiupostingsSpider(scrapy.Spider):
    """
    雪球投资网搜索结果解析
    https://xueqiu.com/
    """
    name = 'xueqiupostings'
    # allowed domains
    # allowed_domains = ['http://xueqiu.com/']
    custom_settings = {
        "COOKIES_ENABLED": True,
        "DOWNLOAD_DELAY": 1,
        "COOKIES_DEBUG": True,
        'RETRY_TIMES': 4,
        "RETRY_HTTP_CODES":[500, 502, 503, 504, 522, 524, 408, 429,400,501],
        "LOG_FILE" : 'xueqiuposting.log',
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
    #先爬取， 特定搜索结果， 按时间排序（低频）
    #备选URL：热门文章https://xueqiu.com/statuses/hot/listV2.json?since_id=-1&max_id=141915&size=15
    #备选URL：7*24新闻livenews： https://xueqiu.com/statuses/livenews/list.json?since_id=-1&max_id=1678307&count=15
    cookie_jar = CookieJar()
    #start_urls=["https://xueqiu.com/statuses/search.json?sort=time&source=all&q=123&count=-1&page=-1"]
    #start_urls=["https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=234&count=20&page=1"]
    '''
    start_urls=[
        'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?since_id=-1&max_id=-1&count=10&category=-1'
    ]
    '''
    '''
    start_urls = [
        'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?since_id=-1&max_id=-1&count=10&category=-1']
    '''
    
    start_urls=[
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=苹果合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=棉花合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=棉纱合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=玻璃合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=粳稻谷合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=晚籼稻合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=甲醇合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=普麦合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=菜籽粕合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=油菜籽合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=硅铁合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=锰硅合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=白糖合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=PTA合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=动力煤合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=强麦合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=硬白小麦合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=红枣合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=尿素合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=纯碱合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=豆一合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=豆二合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=胶合板合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=玉米合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=玉米淀粉合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=纤维板合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=铁矿石合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=焦炭合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=鸡蛋合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=焦煤合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=聚乙烯合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=豆粕合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=棕榈油合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=聚丙烯合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=聚氯乙烯合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=豆油合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=乙二醇合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=苯乙烯合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=液化石油气合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=10年期国债合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=20号胶合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=2年期国债合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=5年期国债合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=不锈钢合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=低硫燃料油合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=原油合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=天然橡胶合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=沪深300合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=热轧卷板合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=燃料油合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=白银合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=石油沥青合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=纸浆合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=线材合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=螺纹钢合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=铅合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=铜合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=铝合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=锌合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=锡合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=镍合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=黄金合约期货&count=20&page=1',
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=苹果合约期货&count=20&page=1',
    ]
    
    '''
    start_urls=[
        'https://xueqiu.com/query/v1/search/status?sort=time&source=all&q=白银合约期货&count=20&page=1',
    ]
    '''
    def start_requests(self):
        """
        此网站首先访问首页， 获得有效cookies
        """
        main_url="https://xueqiu.com"
        yield Request(url=main_url,callback=self.parse_main_url,dont_filter=True,priority=999)

    
    def parse_main_url(self,response):
        #print(response.body.decode('utf-8'))
        #print(response.meta['cookiejar'])
        #print(response.headers.getlist('Set-Cookie'))
        self.cookie_jar.extract_cookies(response, response.request)
        access_token=(self.cookie_jar._cookies['.xueqiu.com']['/']['xq_a_token']).value
        print(access_token)
        for start_url in self.start_urls:
            #将access_code拼接进url中,对URL进行编码
            print('爬取链接',start_url)
            self.logger.info('爬取链接{}'.format(start_url))
            start_url=start_url+(str('&access_token='+access_token))
            start_url=quote(start_url,safe=";/?:@&=+$,",encoding='utf-8')
            #print('this url is',start_url)
            yield Request(url=start_url,callback=self.parse,dont_filter=True)


    def parse(self, response):

        """
        此网站全为异步加载，使用json数据格式
        1. generate search_result urls for every query_string
        2. get next page url for every query_string
        :param response:
        :return:
        """
        #current_page=1
        maxCount=10
        maxPage=100

        js = json.loads(response.body.decode('utf-8'))
        request_send_flag=False
        #有时候返回内容会是错误501， 此时无脑重试
        if js.get('count'):
            total_count = js['count']
            current_page=js['page']
            total_page = min(math.ceil(total_count/maxCount),maxPage)
            for items in self.parse_detail(response,js):
                yield items
            next_page= current_page+1
            if next_page<=total_page:
                 request_send_flag=True
            else: 
                request_send_flag=False

        else:
            pattern=re.compile('page=([0-9]{1,})')
            current_page=re.findall(pattern,response.url)[0]
            next_page=current_page
            request_send_flag=True

        if request_send_flag:
            query='page={}'.format(next_page)
            pattern=re.compile('page=[0-9]{1,}')
            next_url=pattern.sub(query,response.url)
            pattern=re.compile('q=([\u4e00-\u9fa5_a-zA-Z0-9]{0,})')
            decoded_url=unquote(response.url)
            keyword=re.findall(pattern,decoded_url)
            self.cookie_jar.extract_cookies(response, response.request)
            #access_token=(self.cookie_jar._cookies['.xueqiu.com']['/']['xq_a_token']).value
            #query='access_token='
            self.logger.info('关键字{}, 第{}页'.format(keyword,current_page))
            print('关键字{}, 第{}页'.format(keyword,current_page))
            yield Request(url=next_url, callback=self.parse, dont_filter=True)
        

    
    def parse_detail(self,response,postings):
        for posting in postings['list']:
            item_posting =ItemLoader(item=IcePostingsItem(),response=response)
            item_user=ItemLoader(item=IceUsersItem(),response=response)
            
            yield self.parse_user_info(posting,item_user)
            
            yield self.parse_posting_info(posting,item_posting)


    def parse_user_info(self,posting,item):
        """方法，获取文章作者信息，被parse_postings()调用

        Args:
            posting (dict): 单条搜索结果，  
                            键值 count:（int）本次搜索最大返回条数 listings：(list of dict) 本页搜索结果列表

        Returns:
            bool: true: 正确更新作者信息 false 未对本作者进行更新
        """
        # global user_id_list
        # 进一步的，进入用户主页获取更详细用户信息
        pattern_stock=re.compile(r'^/S.*')     
        if posting['user']['id']<0:
            search_result=pattern_stock.search(posting['user']['profile'])
            self.logger.info()
            if search_result:
                print('股票,股票名{}'.format(posting['user']['screen_name']))
                self.logger.info('股票,股票名{}'.format(posting['user']['screen_name']))
                item.add_value('auther_type',1)
            else:
                print('被封禁的雪球用户')
                self.logger.info('被封禁的雪球用户')
                item.add_value('auther_type',2)
        else:
            print('活动雪球用户,用户名{}'.format(posting['user']['screen_name']))
            self.logger.info('活动雪球用户,用户名{}'.format(posting['user']['screen_name']))
            item.add_value('auther_type',3)


        auther_info=posting['user']
        auther_name=posting['user']['screen_name']
        auther_id=posting['user']['id']
        item.add_value('auther_info',auther_info)
        item.add_value('auther_name',auther_name)
        item.add_value('auther_id',auther_id)
        return item.load_item()



    def parse_posting_info(self,posting,item):
        """方法，爬取文章信息，被parse_postings()调用

        Args:
            posting (dict): 单条搜索结果，  
                            键值 count:（int）本次搜索最大返回条数 listings：(list of dict) 本页搜索结果列表
            query_string (str): 搜索关键字，默认编码”UTF-8“  

        Returns:
            bool: true: 正确更新文章概略信息及文章全文 false 未对本文章进行任何更新
        """

        posting_id=posting['target'].replace('/',"_")
        item.add_value('posting_id',posting_id)
        item.add_value('reply_count',posting['reply_count'])
        item.add_value('fav_count',posting['fav_count'])
        item.add_value('view_count',posting['view_count'])
        item.add_value('reward_count',posting['reward_count'])
        item.add_value('reward_user_count',posting['reward_user_count'])
        item.add_value('retweet_count',posting['retweet_count'])
        item.add_value('created_at',posting['created_at'])
        item.add_value('text',posting['text'])
        return item.load_item()


'''
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
'''