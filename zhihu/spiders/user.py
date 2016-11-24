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
    'https://www.zhihu.com/people/xiao-wen-75',
    'https://www.zhihu.com/people/grant-51',
    'https://www.zhihu.com/people/memorygarden',
    'https://www.zhihu.com/people/qin-yu-42-94',
    'https://www.zhihu.com/people/zhou-jian-95',
    'https://www.zhihu.com/people/liu-xin-54',
    'https://www.zhihu.com/people/xun-xiao-she',
    'https://www.zhihu.com/people/liu-ha-ha-94',
    'https://www.zhihu.com/people/liu-qian-wen-49',
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
        "Referer": "http://www.zhihu.com/"
    }

    def start_requests(self):
        yield scrapy.Request(url = "http://www.zhihu.com/#signin", meta = {'cookiejar':1}, headers = self.headers, callback = self.post_login)
        #重写了爬虫类的方法, 实现了自定义请求, 运行成功后会调用callback回调函数

    def post_login(self, response):
        print response.css('title::text')[0].extract()
        xsrf = response.css('input[name=_xsrf]::attr("value")')[0].extract()
        if self.phone:
            return scrapy.FormRequest(
                'http://www.zhihu.com/login/phone_num',
                meta={'cookiejar': response.meta['cookiejar']},
                formdata={'_xsrf': xsrf, 'password': self.password, 'remember_me' : "True", 'phone_num':self.phone},
                callback=self.after_login,
                dont_filter=True
            )
        else:
            return scrapy.FormRequest(
                'http://www.zhihu.com/login/email',
                meta={'cookiejar': response.meta['cookiejar']},
                formdata={'_xsrf': xsrf, 'password': self.password, 'captcha_type': 'cn', 'remember_me' : "True", 'email':self.email},
                callback=self.after_login,
                dont_filter=True
            )

    def after_login(self, response):
        for i in self.start_urls:
            yield scrapy.Request(url = i, meta = {'cookiejar': response.meta['cookiejar']}, callback = self.parse_item) 

    def parse_item(self, response):
        time.sleep(randint(3,5))
        print 'parsing ', response.url
        item = ZhiHuItem()
        item['id'] = response.css('.zm-rich-follow-btn::attr("data-id")').extract()
        item['name'] = response.css('.title-section  .name::text').extract()
        item['avatar'] = response.css('.zm-profile-header-main  .Avatar::attr("src")').extract()
        item['remark'] = response.css('.title-section  .bio::text').extract()
        item['agree'] = response.css('.zm-profile-header-user-agree  strong::text').extract()
        item['thanks'] = response.css('.zm-profile-header-user-thanks  strong::text').extract()
        item['location'] = response.css('.zm-profile-header-user-describe .items .info-wrap .location::attr("title")').extract()
        item['business'] = response.css('.zm-profile-header-user-describe .items .info-wrap .business::attr("title")').extract()
        item['gender'] = response.css('.zm-profile-header-user-describe .items .edit-wrap input[checked=checked]::attr("value")').extract()
        item['employment'] = response.css('.zm-profile-header-user-describe .items .info-wrap .employment::attr("title")').extract()
        item['education'] = response.css('.zm-profile-header-user-describe .items .education::attr("title")').extract()
        item['education_extra'] = response.css('.zm-profile-header-user-describe .items .education-extra::attr("title")').extract()
        item['asks'] = response.css('.profile-navbar a[href*=asks] span::text').extract()
        item['answers'] = response.css('.profile-navbar a[href*=answers] span::text').extract()
        item['posts'] = response.css('.profile-navbar a[href*=posts] span::text').extract()
        item['collections'] = response.css('.profile-navbar a[href*=collections] span::text').extract()
        item['logs'] = response.css('.profile-navbar a[href*=logs] span::text').extract()
        print item
        if item['id']:
           print '----> db <----'
           self.db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
           sql = """insert ignore into user 
           (u_id, name, avatar, remark, agree, thanks, location, business, gender, employment, education, education_extra, asks, answers, posts, collections, logs) 
           values 
           (%s, %s, %s, %s, %d, %d, %s, %s, %d, %s, %s, %s, %d, %d, %d, %d, %d)"""
           self.db_write.execute(sql, item)

        return False
        followees_url = response.url + '/followees'
        #yield scrapy.Request(url = followees_url , meta = {'cookiejar': response.meta['cookiejar']}, callback = self.parse_list)

    def parse_list(self, response):
        url_list = response.css('.zm-item-link-avatar::attr("href")').extract()
        for i in url_list:
            people_url = self.base_url + i
            yield scrapy.Request(url = people_url, meta = {'cookiejar': response.meta['cookiejar']}, callback = self.parse_item)
