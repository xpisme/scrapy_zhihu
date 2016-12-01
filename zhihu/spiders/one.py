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
import os

class one(scrapy.Spider):
    name = "one"
    base_url = 'https://www.zhihu.com'
    allowed_domains = ["www.zhihu.com"]
    start_urls = [
    ]
    password = 'mypasswd'
    email = ''
    phone = '手机号'
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
        master_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
        sql = """select * from topic_url where status = 0 limit 1"""
        res = master_db.query(sql, ())
        if res:
            url = res[0]['url']
            sql = """update topic_url set status = 1 where id = %s limit 1"""
            master_db.execute(sql, ( str(res[0]['id'])) )
        master_db.__delete__()

        topic_url = self.base_url + url + """/followers"""
        yield scrapy.Request(url = topic_url, meta = {'cookiejar': 1},  callback=self.parse_follower)

    def parse_follower(self, response):
        scrapy_url = response.url
        print 'parse topic follower'
        urls = response.css('.zm-list-avatar-medium::attr("href")').extract()
        self.parse_urls(urls)
        time.sleep(randint(1, 2))
        print 'start topic json'
        xsrf = response.css('input[name=_xsrf]::attr("value")')[0].extract()
        headers = self.headers
        headers['X-Xsrftoken'] = xsrf
        print 'start  offset  end'
        self.start = response.css('.zm-person-item::attr("id")')[-1].extract().encode('utf-8')[3:]
        self.offset = 40
        self.end = False

        while (True) :
            if self.offset > 12000 :
                print 'xxxxxxxxxxxxx'
                os.system("sh /tmp/scrapy_zhihu.sh")
            if self.end:
                print 'xxxxxxxxxxxxx'
                #/tmp/scrapy_zhihu.sh 内容
                #ps aux | grep crawl | grep one | awk '{print $2}' | xargs -i kill -9 {}
                #cd /home/xpisme/study/scrapy/scrapy_zhihu/zhihu/spiders && scrapy crawl one
                os.system("sh /tmp/scrapy_zhihu.sh")
            print 'request json'
            time.sleep(0.9)
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
        master_db = DB(MySQL['db_host'], MySQL['db_port'], MySQL['db_user'], MySQL['db_password'], MySQL['db_dbname']) 
        for i in urls:
            sql = """insert ignore into url (url) values (%s)"""
            print """insert ignore into url (url) values ('"""+ i  +"""')"""
            resQuery = master_db.execute(sql, (i))
        master_db.__delete__()
