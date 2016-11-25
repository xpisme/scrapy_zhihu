# -*- coding: utf-8 -*-
#encoding=utf-8

import time
import MySQLdb
import pprint
import traceback
class DB:
  def __init__(self, host, port, user, passwd, db):
    self.conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    self.cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
    #self.cur = self.conn.cursor()
    self.cur.execute("SET NAMES utf8")

  def __delete__(self):
    self.conn.close()

  def query(self, sql, params):
    self.cur.execute(sql, params)
    return self.cur.fetchall()

  def execute(self, sql, params):
    try:
      rows = self.cur.execute(sql, params)
      if(rows>0):
        self.conn.commit()
        pass
      return rows
    except Exception, e:
      print sql
      print '--------------'
      print params
      print e.message()
      pass

  def executeMany(self, sql, params):
    try:
      rows = self.cur.executemany(sql, params)
      if(rows>0):
        self.conn.commit()
        pass
      return rows
    except Exception, e:
      print dir(e)
      pass

  def insert(self, sql, params):
    try:
      rows = self.cur.execute(sql, params)
      if(rows>0):
        id = self.cur.lastrowid
        self.conn.commit()
        pass
      return id
    except Exception, e:
      print dir(e)
      pass

