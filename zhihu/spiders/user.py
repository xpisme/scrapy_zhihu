# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from zhihu.items import ZhiHuItem
from zhihu.settings import MySQL
from random import randint
from imysql import DB
import scrapy
import time
import json

class user(scrapy.Spider):
    name = "user"
    base_url = 'https://www.zhihu.com'
    allowed_domains = ["www.zhihu.com", "xpisme.com"]
    start_urls = [
    ]
    headers = {
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding':'gzip, deflate, sdch, br',
        'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
        'Host':'www.zhihu.com',
        'Upgrade-Insecure-Requests':'1',
        'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',
        "Referer": "https://www.zhihu.com",
        "Origin" : "https://www.zhihu.com"
    }

    def start_requests(self):
        #重写了爬虫类的方法, 实现了自定义请求, 运行成功后会调用callback回调函数
        print '--------------------set_refer------------------'
        request_url = self.get_url()
        if request_url:
            print request_url
            yield scrapy.Request(url = request_url, headers=self.headers, callback=self.parse_item)

    def get_employment(self, employments):
        if not len(employments):
            return 0
        if employments[0].get('job'):
            return employments[0]['job']['name'].encode('utf-8')
        else:
            return employments[0]['name'].encode('utf-8')

    def get_url(self):
        master_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
        sql = """select * from url where status = 0 limit 1"""
        res = master_db.query(sql, ())
        if not res:
            master_db.__delete__()
            return False
        url = res[0]['url']
        master_db.__delete__()
        request_url = self.base_url + url
        return request_url

    def parse_item(self, response):
        print 'parsing response  ', response.url
        body = response.css('#data::attr("data-state")')[0].extract().encode('utf-8')
        state = json.loads(body)
        people = response.url[29:]
        users = state['entities']['users'][people]
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
        zhihu_item['education'] = users['educations'][0]['school']['name'].encode('utf-8') if users.get('educations', False) else 0
        zhihu_item['education_extra'] = users['educations'][0]['major']['name'].encode('utf-8') if users.get('educations', False) and users['educations'][0].get('major', False) else 0
        zhihu_item['asks'] = users['questionCount']
        zhihu_item['answers'] = users['answerCount']
        zhihu_item['posts'] = users['articlesCount']
        zhihu_item['collections'] = users['favoriteCount']
        zhihu_item['logs'] = users['logsCount']
        zhihu_item['url'] = response.url[21:]
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
           master_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
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
           resQuery = master_db.execute(sql, (zhihu_item['id'], zhihu_item['name'], zhihu_item['avatar'], zhihu_item['remark'], zhihu_item['agree'], zhihu_item['thanks'], zhihu_item['location'], zhihu_item['business'], zhihu_item['gender'], zhihu_item['employment'], zhihu_item['education'], zhihu_item['education_extra'], zhihu_item['asks'], zhihu_item['answers'], zhihu_item['posts'], zhihu_item['collections'], zhihu_item['logs'], zhihu_item['url'], zhihu_item['following'], zhihu_item['followers'], zhihu_item['lives'], zhihu_item['topics'], zhihu_item['columns'], zhihu_item['questions'], zhihu_item['weibo']))
           sql = """update url set status = 1 where url = %s"""
           master_db.execute(sql, (zhihu_item['url']))
           master_db.__delete__()

