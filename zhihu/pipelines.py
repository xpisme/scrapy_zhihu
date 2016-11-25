# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs
from imysql import DB
from settings import MySQL

class ZhihuPipeline(object):

    def open_spider(self, spider):
        self.write_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 

    def close_spider(self, spider):
        self.write_db.__delete__()

    def process_item(self, item, spider):
        sql = """insert ignore into user (u_id, name, avatar, remark, agree, thanks, location, business, gender, employment, education, education_extra, asks, answers, posts, collections, logs) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        self.write_db.execute(sql, (item['id'], item['name'], item['avatar'], item['remark'], item['agree'], item['thanks'], item['location'], item['business'], item['gender'], item['employment'], item['education'], item['education_extra'], item['asks'], item['answers'], item['posts'], item['collections'], item['logs']))
