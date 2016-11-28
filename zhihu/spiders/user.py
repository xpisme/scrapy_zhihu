# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from zhihu.items import ZhiHuItem
from zhihu.settings import MySQL
from random import randint
from imysql import DB
import scrapy
import json
import codecs
import time

class user(scrapy.Spider):
    name = "user"
    base_url = 'https://www.zhihu.com'
    allowed_domains = ["www.zhihu.com"]
    start_urls = [
        'https://www.zhihu.com/topic/19552249/followers',
    ]
    password = 'guoxinpeng'
    email = ''
    phone = '18811040172'
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
        yield scrapy.Request(url = "http://www.zhihu.com/#signin", meta = {'cookiejar':1}, headers = self.headers, callback = self.post_login)
        #重写了爬虫类的方法, 实现了自定义请求, 运行成功后会调用callback回调函数

    def post_login(self, response):
        xsrf = response.css('input[name=_xsrf]::attr("value")')[0].extract()
        if self.phone:
            return scrapy.FormRequest(
                'http://www.zhihu.com/login/phone_num',
                meta={'cookiejar': response.meta['cookiejar']},
                formdata={'_xsrf': xsrf, 'password': self.password, 'remember_me' : "True", 'phone_num':self.phone},
                callback=self.set_refer,
                dont_filter=True
            )
        else:
            return scrapy.FormRequest(
                'http://www.zhihu.com/login/email',
                meta={'cookiejar': response.meta['cookiejar']},
                formdata={'_xsrf': xsrf, 'password': self.password, 'captcha_type': 'cn', 'remember_me' : "True", 'email':self.email},
                callback=self.set_refer,
                dont_filter=True
            )

    def set_refer(self, response):
        print '--------------------set_refer------------------'
        yield scrapy.Request(url = "https://www.zhihu.com/topic/19552249/followers", meta = {'cookiejar': 1},  callback=self.parse_follower)

    def parse_follower(self, response):
        scrapy_url = response.url
        print 'parse topic follower'
        urls = response.css('.zm-list-avatar-medium::attr("href")').extract()
        self.parse_urls(urls)
        time.sleep(randint(20, 25))
        print 'start topic json'
        xsrf = response.css('input[name=_xsrf]::attr("value")')[0].extract()
        headers = self.headers
        headers['X-Xsrftoken'] = xsrf
        print 'start  offset  end'
        self.start = response.css('.zm-person-item::attr("id")')[-1].extract().encode('utf-8')[3:]
        self.offset = 40
        self.end = False

        while (not self.end) :
            print 'request json'
            time.sleep(randint(20, 25))
            yield scrapy.FormRequest(
                url = scrapy_url,
                meta = {'cookiejar': response.meta['cookiejar']},
                formdata = {'offset' : str(self.offset), 'start' : self.start},
                callback = self.parse_json,
                headers = headers,
                dont_filter = True
            )

    def parse_json(self, response):
        print 'parsing '
        print response.url
        print self.start
        print self.offset
        jsonresponse = json.loads(response.body_as_unicode())
        body = jsonresponse['msg'][1]
        if jsonresponse['msg'][0] < 20:
            self.end = True
        self.offset = self.offset + 20
        self.start = Selector(text=body).css('.zm-person-item::attr("id")')[-1].extract().encode('utf-8')[3:]
        urls = Selector(text=body).css('.zm-list-avatar-medium::attr("href")').extract()
        yield self.parse_urls(urls)

    def parse_urls(self, urls):
        print urls 
        for i in urls:
            i = self.base_url + i
            print 'parsing ', i
            time.sleep(randint(1,2))
            yield scrapy.Request(url = i, headers = self.headers, callback = self.parse_item)

    def parse_item(self, response):
        print 'parsing response  ', response.url
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
        
        if zhihu_item['id']:
           print '----> db <----'
           write_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
           sql = """insert ignore into user (u_id, name, avatar, remark, agree, thanks, location, business, gender, employment, education, education_extra, asks, answers, posts, collections, logs, url) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
           resQuery = write_db.execute(sql, (zhihu_item['id'], zhihu_item['name'], zhihu_item['avatar'], zhihu_item['remark'], zhihu_item['agree'], zhihu_item['thanks'], zhihu_item['location'], zhihu_item['business'], zhihu_item['gender'], zhihu_item['employment'], zhihu_item['education'], zhihu_item['education_extra'], zhihu_item['asks'], zhihu_item['answers'], zhihu_item['posts'], zhihu_item['collections'], zhihu_item['logs'], response.url))
           write_db.__delete__()
           print 'resQuery -------   ', resQuery
