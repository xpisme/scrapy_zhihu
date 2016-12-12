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

    def errback_httpbin(self, failure):
        url = failure.request.url
        print failure
        sql = """update url set status = 1 where url = %s"""
        self.master_db.execute(sql, (url[21:]))
        self.master_db.__delete__()

    def start_requests(self):
        #重写了爬虫类的方法, 实现了自定义请求, 运行成功后会调用callback回调函数
        print '--------------------set_refer------------------'
        request_url = self.get_url()
        if request_url:
            print request_url
            yield scrapy.Request(url=request_url, 
            headers=self.headers, 
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
        request_url = self.base_url + url
        return request_url
    
    
    def parse_item(self, response):
        print 'parsing response  ', response.url
        if len(response.css('#data::attr("data-state")')) < 1:
            self.parse_old(response)
        else:
            self.parse_react(response)

    def parse_old(self, response):
    	zhihu_item = ZhiHuItem()
        zhihu_item['id'] = response.css('.zm-rich-follow-btn::attr("data-id")').extract()
        zhihu_item['name'] = response.css('.title-section  .name::text').extract()
        zhihu_item['avatar'] = response.css('.zm-profile-header-main  .Avatar::attr("src")').extract()
        zhihu_item['remark'] = response.css('.title-section  .bio::text').extract()
        zhihu_item['agree'] = response.css('.zm-profile-header-user-agree  strong::text').extract()
        zhihu_item['thanks'] = response.css('.zm-profile-header-user-thanks  strong::text').extract()
        zhihu_item['location'] = response.css('.zm-profile-header-user-describe .items .info-wrap .location::attr("title")').extract()
        zhihu_item['business'] = response.css('.zm-profile-header-user-describe .items .info-wrap .business::attr("title")').extract()
        zhihu_item['gender'] = response.css('.zm-profile-header-user-describe .items .edit-wrap input[checked=checked]::attr("value")').extract()
        zhihu_item['employment'] = response.css('.zm-profile-header-user-describe .items .info-wrap .employment::attr("title")').extract()
        zhihu_item['education'] = response.css('.zm-profile-header-user-describe .items .education::attr("title")').extract()
        zhihu_item['education_extra'] = response.css('.zm-profile-header-user-describe .items .education-extra::attr("title")').extract()
        zhihu_item['asks'] = response.css('.profile-navbar a[href*=asks] span::text').extract()
        zhihu_item['answers'] = response.css('.profile-navbar a[href*=answers] span::text').extract()
        zhihu_item['posts'] = response.css('.profile-navbar a[href*=posts] span::text').extract()
        zhihu_item['collections'] = response.css('.profile-navbar a[href*=collections] span::text').extract()
        zhihu_item['logs'] = response.css('.profile-navbar a[href*=logs] span::text').extract()
        zhihu_item['url'] = response.url[21:]
        zhihu_item['following'] = response.css('.zm-profile-side-following strong')[0].extract()
        zhihu_item['followers'] = response.css('.zm-profile-side-following strong')[1].extract()
        zhihu_item['topics'] = response.css('.zg-link-litblue')[1].extract()
        zhihu_item['columns'] = response.css('.zg-link-litblue')[0].extract()
        print '=============================================================================================================='
        zhihu_item['id'] = zhihu_item['id'][0].encode('utf-8') if zhihu_item['id'] else 0
        zhihu_item['name'] = zhihu_item['name'][0].encode('utf-8') if zhihu_item['name'] else 0
        zhihu_item['avatar'] = zhihu_item['avatar'][0].encode('utf-8') if zhihu_item['avatar'] else 0
        zhihu_item['remark'] = zhihu_item['remark'][0].encode('utf-8') if zhihu_item['remark'] else 0
        zhihu_item['agree'] = zhihu_item['agree'][0].encode('utf-8') if zhihu_item['agree'] else 0
        zhihu_item['thanks'] = zhihu_item['thanks'][0].encode('utf-8') if zhihu_item['thanks'] else 0
        zhihu_item['location'] = zhihu_item['location'][0].encode('utf-8') if zhihu_item['location'] else 0
        zhihu_item['business'] = zhihu_item['business'][0].encode('utf-8') if zhihu_item['business'] else 0
        zhihu_item['gender'] = zhihu_item['gender'][0].encode('utf-8') if zhihu_item['gender'] else 0
        zhihu_item['employment'] = zhihu_item['employment'][0].encode('utf-8') if zhihu_item['employment'] else 0
        zhihu_item['education'] = zhihu_item['education'][0].encode('utf-8') if zhihu_item['education'] else 0
        zhihu_item['education_extra'] = zhihu_item['education_extra'][0].encode('utf-8') if zhihu_item['education_extra'] else 0
        zhihu_item['asks'] = zhihu_item['asks'][0].encode('utf-8') if zhihu_item['asks'] else 0
        zhihu_item['answers'] = zhihu_item['answers'][0].encode('utf-8') if zhihu_item['answers'] else 0
        zhihu_item['posts'] = zhihu_item['posts'][0].encode('utf-8') if zhihu_item['posts'] else 0
        zhihu_item['collections'] = zhihu_item['collections'][0].encode('utf-8') if zhihu_item['collections'] else 0
        zhihu_item['logs'] = zhihu_item['logs'][0].encode('utf-8') if zhihu_item['logs'] else 0
        zhihu_item['url'] = zhihu_item['url']
        zhihu_item['following'] = filter(str.isdigit, zhihu_item['following'].encode('utf-8'))
        zhihu_item['followers'] = filter(str.isdigit, zhihu_item['followers'].encode('utf-8'))
        zhihu_item['topics'] = filter(str.isdigit, zhihu_item['topics'].encode('utf-8'))
        zhihu_item['columns'] = filter(str.isdigit, zhihu_item['columns'].encode('utf-8'))
        
        if zhihu_item['id']:
           sql = """insert ignore into user (
           u_id, name, avatar, remark, 
           agree, thanks, location, business, 
           gender, employment, education, education_extra, 
           asks, answers, posts, collections, 
           logs, url, followers, topics, 
           columns) 
           values (
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s,
           %s, %s, %s, %s,
           %s)"""
           resQuery = self.master_db.execute(sql, (zhihu_item['id'], zhihu_item['name'], zhihu_item['avatar'], zhihu_item['remark'], zhihu_item['agree'], zhihu_item['thanks'], zhihu_item['location'], zhihu_item['business'], zhihu_item['gender'], zhihu_item['employment'], zhihu_item['education'], zhihu_item['education_extra'], zhihu_item['asks'], zhihu_item['answers'], zhihu_item['posts'], zhihu_item['collections'], zhihu_item['logs'], zhihu_item['url'], zhihu_item['followers'], zhihu_item['topics'], zhihu_item['columns']))
           sql = """update url set status = 1 where url = %s"""
           self.master_db.execute(sql, (zhihu_item['url']))
           self.master_db.__delete__()
           print 'resQuery -------   ', resQuery


    def parse_react(self, response):
        body = response.css('#data::attr("data-state")')[0].extract().encode('utf-8')
        state = json.loads(body)
        people = response.url[29:]
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

