# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import sys
sys.path.append(r'C:\Users\benno\OneDrive\Documents\GitHub\xueqiu_data_scrapy')
from scrapy import signals
from urllib.parse import urlparse
import logging
import re
import json
import logging

from twisted.internet import defer
from twisted.internet.error import (
    ConnectError,
    ConnectionDone,
    ConnectionLost,
    ConnectionRefusedError,
    DNSLookupError,
    TCPTimedOutError,
    TimeoutError,
)
from twisted.web.client import ResponseFailed

from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.python import global_object_name
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.python import global_object_name
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import IgnoreRequest
from utils.common import is_json

from database_handler import ProxyPool_DB
class FinanceSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FinanceDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class FinanceRetryMiddleware(RetryMiddleware):
    #rewrite retrymiddleware with proxy pools 
    #use default function: process exceptions
    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                    ConnectionRefusedError, ConnectionDone, ConnectError,
                    ConnectionLost, TCPTimedOutError, ResponseFailed,
                    IOError, TunnelError) 
    def __init__(self,settings):
        super().__init__(settings)
        self.DB=ProxyPool_DB()
        self.try_with_host = settings.getbool('PROXY_POOL_TRY_WITH_HOST',True)
        self.logger = logging.getLogger(__name__)
        self.not_ban_status={200,301,302,404,500}
        self.banned_pattern=settings.get('BANNED_PATTERN',None) # banned patter should be a regulator compiler

    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler.settings)

    def process_request(self,request,spider):
        if 'https' in request.url:
            request_type='https'
        elif 'http' in request.url:
            request_type='http'
        else:
            self.logger.warn('UNKNOWN REQUEST TYPE')
            raise IgnoreRequest
        proxy=self._get_proxy()
        if not proxy or request_type not in proxy['type']:
            self.logger.warn('No Proxy Found')
            self.logger.info('TRY TO REQUEST FROM HOST')
            return
        request.meta['proxy_source']=proxy
        request.meta['proxy']='{}://{}'.format(request_type,proxy['address'])
        request.meta['_PROXY_POOL'] = True
        
        self.logger.debug('[ProxyChoosed] {}'.format(request.meta['proxy']))

    def process_response(self,request,response,spider):
        #1: make sure retry is permitted
        if request.meta.get('dont_retry', False):
            return response
        #2: check response status code for instance [500, 502, 503, 504, 522, 524, 408, 429,400] means error 
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        
        #3  check whether this ip is banned from the host
        ban = self.response_is_ban(request,response)
        request.meta['_ban'] =ban
        proxy=request.meta.get('proxy_source',None)
        if not (proxy and request.meta.get('_PROXY_POOL',False)):
            #not using proxy to access
            return response
        ban = request.meta.get('_ban',None)
        if ban is True:
            #TODO there should be a blacklist_proxy for this domain, but not implemented
            request.meta.pop('proxy_source', None)
            request.meta.pop('proxy', None)
            request.meta.pop('download_slot', None)
            request.meta.pop('_PROXY_POOL', None)
            return self._retry(request, reason,spider) or response

        return response
    def process_exception(self, request, exception, spider):
        if (
            isinstance(exception, self.EXCEPTIONS_TO_RETRY)
            and not request.meta.get('dont_retry', False)
        ):
            return self._retry(request, exception, spider)
    
    def _retry(self, request,reason,spider):

        #TODO REFINE RETRY METHOD WHICH IS COPIED FROM THE SCRAPY PACKAGE
        retries = request.meta.get('retry_times', 0) + 1
        retry_times = self.max_retry_times

        if 'max_retry_times' in request.meta:
            retry_times = request.meta['max_retry_times']

        stats = spider.crawler.stats

        if retries <= retry_times:
            #if try with host is true, start to try with host, but will not exceed max retries
            if self.try_with_host:
                self.logger.debug("Try with host ip")
                req = request.copy()
                req.meta.pop('proxy_source', None)
                req.meta.pop('download_slot', None)
                req.meta.pop('_PROXY_POOL', None)
                req.meta['proxy'] = None
                req.dont_filter = True
                return req

            self.logger.debug("Retrying %(request)s with another proxy "
                        "(failed %(retries)d times,  "
                        "max retries: %(retry_times)d):"
                        "%(reason)s",
                        {
                            'request': request, 
                            'retries': retries,
                            'retry_times': retry_times,
                            'reason':reason,

                        },
                        extra={'spider': spider})
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            retryreq.priority=request.priority+self.priority_adjust

            if isinstance(reason, Exception):
                reason = global_object_name(reason.__class__)
            stats.inc_value('retry/count')
            stats.inc_value(f'retry/reason_count/{reason}')
            return retryreq
        else:
            stats.inc_value('retry/max_reached')
            self.logger.debug("Gave up retrying %(request)s (failed %(retries)d "
                        "times with different proxies)"
                        "%(reason)s",
                        {'request': request, 'retries': retries,'reason':reason,},
                        extra={'spider': spider})






    def response_is_ban(self, request, response):
        #only check response status is a valid code,ex. {200, 301, 302, 404}
        #1: handle general banned cases
        if self.banned_pattern.search(response.text):
            return True
        if response.status == 200 and not len(response.body):
            return True
        #handle banned cases for specific domains
        net_loc = urlparse(request.url).netloc
        if 'xueqiu' in net_loc:
            if response.status==200 and response.body:
                #print(response.body.de code('utf-8'))
                text=response.body.decode('utf-8')
                is_js = is_json(text)
                if is_js:
                    js = json.loads(text)
                    if '40016' in js.values() or '501' in js.values():
                        return True
        #so far this ip is not banned
        return False
    def _get_proxy(self):
        sample_one_query=[{'$sample':{'size':1}}]
        proxy=dict()
        proxy['address']=list(self.DB.aggregate(sample_one_query))[0]['ip_address'] # this is too ugly
        proxy['type']=['https','http']
        return proxy

            

'''
class FinanceProxyMiddleware(object):
    #this is deprecated , combined with the default retrymiddleware
    def __init__(self, max_proxies_to_try,try_with_host,retry_http_codes):
        self.DB=ProxyPool_DB()
        self.max_proxies_to_try=max_proxies_to_try
        self.try_with_host=try_with_host
        self.logger = logging.getLogger(__name__)
        self.NOT_BAN_STATUSES = {200, 301, 302, 404, 500}
        self.RETRY_HTTP_CODES =retry_http_codes
        self.NOT_BAN_EXCEPTIONS = (IgnoreRequest,)
        self.BANNED_PATTERN = re.compile(r'(Captive Portal|SESSION EXPIRED)', re.IGNORECASE)


    def exception_is_ban(self, request, exception):
        return not isinstance(exception, self.NOT_BAN_EXCEPTIONS)

    
    @classmethod
    def from_crawler(cls, crawler):
        s=crawler.settings
        #max_proxies_to_try=s.getint('RETRY_TIMES', 5),
        mw = cls(
            max_proxies_to_try=s.getint('RETRY_TIMES', 5),
            try_with_host=s.getbool('PROXY_POOL_TRY_WITH_HOST', True),
            retry_http_codes=s.get('RETRY_HTTP_CODES',[500, 502, 503, 504, 522, 524, 408, 429,400])
        )
        return mw
    def process_request(self,request,spider):
        if 'https' in request.url:
            request_type='https'
        elif 'http' in request.url:
            request_type='http'
        else:
            self.logger.warn('UNKNOWN REQUEST TYPE')
            raise IgnoreRequest
        proxy=self._get_proxy()
        if not proxy or request_type not in proxy['type']:
            self.logger.warn('No Proxy Found')
            self.logger.info('TRY TO REQUEST FROM HOST')
            return
        request.meta['proxy_source']=proxy
        request.meta['proxy']='{}://{}'.format(request_type,proxy['address'])
        request.meta['_PROXY_POOL'] = True
        
        self.logger.debug('[ProxyChoosed] {}'.format(request.meta['proxy']))
        
        
    def process_response(self, request, response, spider):
        ban = self.response_is_ban(request, response)
        request.meta['_ban'] =ban
        
        return self._handle_result(request,spider) or response

    def get_proxy_slot(self, proxy):
        return proxy.host

    def _get_proxy(self):
        sample_one_query=[{'$sample':{'size':1}}]
        proxy=dict()
        proxy['address']=list(self.DB.aggregate(sample_one_query))[0]['ip_address'] # this is too ugly
        proxy['type']=['https','http']
        return proxy
    
    def process_exception(self,request,exception,spider):
        print(exception)
        return self._handle_result(request,spider)
        
    def _handle_result(self,request,spider):
        proxy=request.meta.get('proxy_source',None)
        if not (proxy and request.meta.get('_PROXY_POOL',False)):
            #not using proxy to access
            return
        #enter retry option
        ban = request.meta.get('_ban',None)
        if ban is True:
            #TODO there should be a blacklist_proxy for this domain, but not implemented
            request.meta.pop('proxy_source', None)
            request.meta.pop('proxy', None)
            request.meta.pop('download_slot', None)
            request.meta.pop('_PROXY_POOL', None)
            return self._retry(request, spider)
    def _retry(self, request,spider):
        #TODO REFINE RETRY METHOD WHICH IS COPIED FROM THE SCRAPY PACKAGE
        retries = request.meta.get('proxy_retry_times', 0) + 1
        max_proxies_to_try = request.meta.get('max_proxies_to_try',
                                              self.max_proxies_to_try)

        if retries <= max_proxies_to_try:
            self.logger.debug("Retrying %(request)s with another proxy "
                         "(failed %(retries)d times, "
                         "max retries: %(max_proxies_to_try)d)",
                         {'request': request, 'retries': retries,
                          'max_proxies_to_try': max_proxies_to_try},
                         extra={'spider': spider})
            retryreq = request.copy()
            retryreq.meta['proxy_retry_times'] = retries
            retryreq.dont_filter = True
            return retryreq
        else:
            self.logger.debug("Gave up retrying %(request)s (failed %(retries)d "
                         "times with different proxies)",
                         {'request': request, 'retries': retries},
                         extra={'spider': spider})

            if self.try_with_host:
                self.logger.debug("Try with host ip")
                req = request.copy()
                req.meta.pop('proxy_source', None)
                req.meta.pop('download_slot', None)
                req.meta.pop('_PROXY_POOL', None)
                req.meta['proxy'] = None
                req.dont_filter = True
                return req

    def response_is_ban(self, request, response):
        if self.BANNED_PATTERN.search(response.text):
            return True
        if response.status not in self.NOT_BAN_STATUSES:
            return True
        if response.status == 200 and not len(response.body):
            return True
        if response.status==200 and response.body:
            #print(response.body.de code('utf-8'))
            text=response.body.decode('utf-8')
            is_js = is_json(text)
            if is_js:
                js = json.loads(text)
                if '40016' in js.values() or '501' in js.values():
                    return True
        return False

'''


