# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZhiHuItem(scrapy.Item):
    # define the fields for your item here like:
    id = scrapy.Field()
    name = scrapy.Field()
    avatar = scrapy.Field()
    remark = scrapy.Field()
    agree = scrapy.Field()
    thanks = scrapy.Field()
    location = scrapy.Field()
    business = scrapy.Field()
    gender = scrapy.Field()
    employment = scrapy.Field()
    education = scrapy.Field()
    education_extra = scrapy.Field()
    asks = scrapy.Field()
    answers = scrapy.Field()
    posts = scrapy.Field()
    collections = scrapy.Field()
    logs = scrapy.Field()
