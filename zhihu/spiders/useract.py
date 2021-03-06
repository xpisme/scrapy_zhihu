# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from zhihu.items import ZhiHuItem
from zhihu.settings import MySQL
from random import randint
from imysql import DB
import scrapy
import time
import json

class useract(scrapy.Spider):
    name = "useract"
    base_url = 'https://www.zhihu.com'
    allowed_domains = ["www.zhihu.com", "xpisme.com"]
    start_urls = [
    ]

    def errback_httpbin(self, failure):
        url = failure.request.url
        print failure
        sql = """update url set status = 1 where url = %s"""
        self.master_db.execute(sql, (url[21:-11]))
        self.master_db.__delete__()

    def start_requests(self):
        #重写了爬虫类的方法, 实现了自定义请求, 运行成功后会调用callback回调函数
        print '--------------------start request------------------'
        request_url = self.get_url()
        if request_url:
            print request_url
            yield scrapy.Request(url=request_url, 
            callback=self.parse_item, 
            dont_filter=True, 
            errback=self.errback_httpbin)

    def get_employment(self, employments):
        if not len(employments):
            return 0
        if employments[0].get('job'):
            return employments[0]['job']['name'].encode('utf-8')
        else:
            if employments[0].get('name'):
                return employments[0]['name'].encode('utf-8')
            else:
                return 0

    def get_url(self):
        self.master_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
        sql = """select * from url where status = 0 limit 1"""
        res = self.master_db.query(sql, ())
        if not res:
            self.master_db.__delete__()
            return False
        url = res[0]['url']
        sql = """update url set status = 2 where url = %s"""
        self.master_db.execute(sql, (url))
        request_url = self.base_url + url + '/activities'
        return request_url
    
    
    def parse_item(self, response):
        print 'parsing response  ', response.url
        self.parse_react(response)

    def parse_react(self, response):
        print 'parsing react'
        body = response.css('#data::attr("data-state")')[0].extract().encode('utf-8')
        print body
        state = json.loads(body)
        people = response.url[29:-11]
        if state['entities']['users'].get(people):
            users = state['entities']['users'][people]
        else:
            users = state['entities']['users']['null']
        zhihu_item = ZhiHuItem()
        zhihu_item['id'] = users['id']
        zhihu_item['name'] = users['name'].encode('utf-8')
        zhihu_item['avatar'] = users['avatarUrl'].replace('_is.', '_xl.')
        zhihu_item['remark'] = users['headline'].encode('utf-8') if users['headline'] else 0
        zhihu_item['agree'] = users['voteupCount']
        zhihu_item['thanks'] = users['thankedCount']
        zhihu_item['location'] = users['locations'][0]['name'].encode('utf-8') if users['locations'] else 0
        zhihu_item['business'] = users['business']['name'].encode('utf-8') if users.get('business', False) else 0
        zhihu_item['gender'] = users['gender']
        zhihu_item['employment'] = self.get_employment(users['employments'])
        zhihu_item['education'] = users['educations'][0]['school']['name'].encode('utf-8') if users.get('educations', False) and users['educations'][0].get('school', False) else 0
        zhihu_item['education_extra'] = users['educations'][0]['major']['name'].encode('utf-8') if users.get('educations', False) and users['educations'][0].get('major', False) else 0
        zhihu_item['asks'] = users['questionCount']
        zhihu_item['answers'] = users['answerCount']
        zhihu_item['posts'] = users['articlesCount']
        zhihu_item['collections'] = users['favoriteCount']
        zhihu_item['logs'] = users['logsCount']
        zhihu_item['url'] = response.url[21:-11]
        zhihu_item['following'] = users['followingCount']
        zhihu_item['followers'] = users['followerCount']
        zhihu_item['lives'] = users['hostedLiveCount']
        zhihu_item['topics'] = users['followingTopicCount']
        zhihu_item['columns'] = users['followingColumnsCount']
        zhihu_item['questions'] = users['followingQuestionCount']
        zhihu_item['weibo'] = users['sinaWeiboUrl'] if users.get('sinaWeiboUrl', False) else 0

        print zhihu_item
        if zhihu_item['id']:
           print '----> db <----'
           sql = """insert ignore into user (
           u_id, name, avatar, remark, 
           agree, thanks, location, business, 
           gender, employment, education, education_extra, 
           asks, answers, posts, collections, 
           logs, url, following, followers, 
           lives, topics, columns, questions, 
           weibo) 
           values (
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s) """
           self.master_db.execute(sql, (zhihu_item['id'], zhihu_item['name'], zhihu_item['avatar'], zhihu_item['remark'], zhihu_item['agree'], zhihu_item['thanks'], zhihu_item['location'], zhihu_item['business'], zhihu_item['gender'], zhihu_item['employment'], zhihu_item['education'], zhihu_item['education_extra'], zhihu_item['asks'], zhihu_item['answers'], zhihu_item['posts'], zhihu_item['collections'], zhihu_item['logs'], zhihu_item['url'], zhihu_item['following'], zhihu_item['followers'], zhihu_item['lives'], zhihu_item['topics'], zhihu_item['columns'], zhihu_item['questions'], zhihu_item['weibo']))
           sql = """update url set status = 1 where url = %s"""
           self.master_db.execute(sql, (zhihu_item['url']))
           self.master_db.__delete__()
