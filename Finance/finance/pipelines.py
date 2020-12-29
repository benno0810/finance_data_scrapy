# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#import pymysql.cursors()
import pymysql
import pymongo
from scrapy.pipelines.files import FilesPipeline
from twisted.enterprise import adbapi
from .items import IceItem,IcePostingsItem,IceUsersItem,IceResultMapperItem,IcePortfolioItem
import os
import json
import scrapy
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

BASE_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))


class FinancePipeline(object):
    def process_item(self, item, spider):
        return item

class PDFDownloadPipeline(FilesPipeline):
    def get_media_requests(self, item, info):
        yield scrapy.Request(url=item['files_urls_field'], meta={'item': item})

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        path = '%s/%s_%s_%s.pdf' %(item['site'], item['name'], item['date'], item['title'])
        return path


class DuplicatesPipeline(object):
    '''
    The other thing you need to keep in mind is that the values returned by input
    processors are collected internally (in lists) and then passed to output
    processors to populate the fields.
    i.e itemloader will align a list in default to the item, use "[0]" to avoid unhashable error
    DropItem i is WARNING level log
    '''
    def __init__(self):
        self.posting_id_seen = set()
        self.user_id_seen=set()
        self.query_result_mapper_seen=set()
        self.portfolio_seen=dict()    #{portfolio_name:update_time}

    def process_item(self, item, spider):
        if spider.name=='xueqiu_postings':
            if isinstance(item,IcePostingsItem):
                adapter = ItemAdapter(item)
                if adapter['posting_id'][0] in self.posting_id_seen:
                    raise DropItem(f"Duplicate posting found: {item!r}")
                else:
                    self.posting_id_seen.add(adapter['posting_id'][0])
                    return item
            if isinstance(item,IceUsersItem):
                adapter = ItemAdapter(item)
                if adapter['auther_id'][0] in self.user_id_seen:
                    raise DropItem(f"Duplicate auther found: {item!r}")
                else:
                    self.user_id_seen.add(adapter['auther_id'][0])
                    return item 
            if isinstance(item,IceResultMapperItem):
                #if mapper exist drop item
                adapter = ItemAdapter(item)
                mapper=tuple()
                mapper =(adapter['keyword'],adapter['posting_id'])
                if mapper in self.query_result_mapper_seen:
                    raise DropItem(f"Duplicate mapper found: {item!r}")
                else:
                    self.query_result_mapper_seen.add(mapper)
                    return item
        if spider.name=='xueqiu_portfolios':
            if isinstance(item,IcePortfolioItem):
                adapter = ItemAdapter(item)
                if adapter['name'] in self.portfolio_seen.keys():
                    name=adapter['name']
                    if adapter['updated_at']==self.portfolio_seen[name]:
                        raise DropItem(f"Duplicate portfolio found: {item!r}")
                    else:
                        #archive_file found, update update_time
                        self.portfolio_seen[name]=adapter['name']
                        return item
                else:
                    #find new portfolio, add to the seen_dict
                    name=adapter['name']
                    self.portfolio_seen[name]=adapter['updated_at']
                    return item

class MysqlTwistedPipline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)

        return cls(dbpool)

    def process_item(self, item, spider):
        if spider.name == 'stock_exchange':
            query = self.dbpool.runInteraction(self.do_insert, item)
            query.addErrback(self.handle_error, item, spider)
        return item

    def handle_error(self, failure, item, spider):
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)


class MyJsonPipeline(object):
    def __init__(self, current_dir):
        self.huace_file = open(os.path.join(current_dir, 'json/huace.json'), 'a', encoding='utf-8')
        self.xueqiu_file = open(os.path.join(current_dir, 'json/xueqiu.json'), 'w', encoding='utf-8')
        self.xueqiu_postings_file=open(os.path.join(current_dir, 'json/xueqiu_postings.json'), 'w', encoding='utf-8')
        self.xueqiu_users_file=open(os.path.join(current_dir, 'json/xueqiu_users.json'), 'w', encoding='utf-8')

    @classmethod
    def from_crawler(cls, crawler):
        """
            关于该方法的理解：
            crawler对象应该是scrapy中最核心的对象，它贯穿整个框架，无论任何spider与pipelines
            scrapy启动时，通过该方法返回pipeline的实例
        :param crawler:
        :return:
        """
        return cls(crawler.settings.get('CURRENT_DIR'))

    def process_item(self, item, spider):
        if spider.name == 'guba':
            item_json = json.dumps(dict(item), ensure_ascii=False) + '\n'
            self.huace_file.write(item_json)
            return item
        if spider.name == 'xueqiu' or spider.name == 'xueqiu_postings':
            if isinstance(item,IceItem):
                item_json = json.dumps(dict(item), ensure_ascii=False) + '\n'
                self.xueqiu_file.write(item_json)
            if isinstance(item,IcePostingsItem):
                item_json = json.dumps(dict(item), ensure_ascii=False) + '\n'
                self.xueqiu_postings_file.write(item_json)
            if isinstance(item,IceUsersItem):
                item_json = json.dumps(dict(item), ensure_ascii=False) + '\n'
                self.xueqiu_users_file.write(item_json)
            return item
                

    def close_spider(self, spider):
        self.huace_file.close()
        self.xueqiu_file.close()

import pymongo
from itemadapter import ItemAdapter

class MongoPipeline(object):
    def __init__(self, mongo_url, mongo_db):
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        print(crawler.settings.get('MONGO_DBNAME', 'items'))
        return cls(
            mongo_url=crawler.settings.get('MONGO_HOST'),
            mongo_db=crawler.settings.get('MONGO_DBNAME', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_url)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        #do duplicated_drop job

        self.client.close()

    def process_item(self, item, spider):
        #return none!
        if spider.name=='xueqiu_postings':
            if isinstance(item,IceResultMapperItem):
                collection_name = 'xueqiu_postings'
                #user update insted of insert, warning:init insert needed
                #self.db[collection_name].insert_one(ItemAdapter(item).asdict())
                result=self.db[collection_name].update(ItemAdapter(item).asdict(),ItemAdapter(item).asdict(),upsert=True)
            return item
        if spider.name=='xueqiu_portfolios':
            if isinstance(item,IcePortfolioItem):
                collection_name='xueqiu_portfolios'
                adapter=ItemAdapter(item)
                result=self.db[collection_name].update_many({'name':adapter['name']},{'$set':ItemAdapter(item).asdict()},upsert=True)
            return item
    '''
    def handle_error(self, failure, item, spider):
        print(failure)
        print('an error occurs!')
    '''
