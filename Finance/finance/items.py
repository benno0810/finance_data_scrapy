# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import datetime
import time

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from settings import SQL_DATETIME_FORMAT


class FinanceItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def remove_space(value):
    return value.strip()


class SEAItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


# Unix时间戳(Unix timestamp)转换
def utime_convert(value):
    format_string = "%Y-%m-%d %H:%M:%S"
    time_array = time.localtime(value / 1000)
    str_date = time.strftime(format_string, time_array)
    return str_date


class StockExchangeAnnouncement(scrapy.Item):
    url_object_id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field(
        input_processor=MapCompose(remove_space),
    )
    publish_time = scrapy.Field(
        input_processor=MapCompose(remove_space,
                                   lambda x: datetime.datetime.strptime(x.split('：')[1].strip(), '%Y-%m-%d').date()),
    )
    # number = scrapy.Field()
    content = scrapy.Field(
        input_processor=MapCompose(remove_space),
    )
    # attachment_url = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into stock_exchange(url_object_id, url, title, publish_time, content, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            self['url_object_id'],
            self['url'],
            self['title'],
            self['publish_time'],
            self['content'],
            self['crawl_time'].strftime(SQL_DATETIME_FORMAT),
        )
        return insert_sql, params


class HuaceItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    time = scrapy.Field()
    comment = scrapy.Field(output_processor=Join(separator='###'))


class IceItem(scrapy.Item):
    title = scrapy.Field()
    create_time = scrapy.Field(input_processor=MapCompose(utime_convert))
    content = scrapy.Field()
    author_name = scrapy.Field()
class IcePostingsItem(scrapy.Item):
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
    """
    posting_id=scrapy.Field()
    reply_count=scrapy.Field()
    fav_count=scrapy.Field()
    view_count=scrapy.Field()
    reward_count=scrapy.Field()
    reward_user_count=scrapy.Field()
    retweet_count=scrapy.Field()
    created_at=scrapy.Field(input_processor=MapCompose(utime_convert))
    text=scrapy.Field()

class IceUsersItem(scrapy.Item):
    """
        auther_type:1,2,3
        auther_info=posting['user']
        auther_name=posting['user']['screen_name']
        auther_id=posting['user']['id']
        
    """
    auther_id=scrapy.Field()
    auther_name=scrapy.Field()
    auther_type=scrapy.Field()
    auther_info=scrapy.Field()
    
    
class IceResultMapperItem(scrapy.Item):
    keyword=scrapy.Field(output_processor=TakeFirst())
    posting_id=scrapy.Field(output_processor=TakeFirst())

class IcePortfolioItem(scrapy.Item):
    '''
        name = portfolio['name']
        updated_at= portfolio['updated_at'] if portfolio['updated_at'] else portfolio['created_at']
        market = portfolio['market']
        info=portfolio
    '''
    name=scrapy.Field(output_processor=TakeFirst())
    portfolio_screen_name= scrapy.Field(output_processor=TakeFirst())
    owner_id = scrapy.Field(output_processor=TakeFirst())
    updated_at=scrapy.Field(output_processor=TakeFirst())
    market=scrapy.Field(output_processor=TakeFirst())
    info=scrapy.Field(output_processor=TakeFirst())


class CNInfoItem(scrapy.Item):
    site = scrapy.Field()
    files_urls_field = scrapy.Field()
    name = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()