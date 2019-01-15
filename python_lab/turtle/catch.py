#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
抓取交易数据
"""
__author__ = 'lockheed'

import tushare as ts
import pymysql
import pymysql.cursors
import time
import traceback
import json


# import pandas as pd
# from pandas import DataFrame
# import numpy as np

def log(file, content):
    """其他日志"""
    global host
    global log_path
    with open(log_path + "/" + file + ".log", 'a') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')


def mysql_insert(sql):
    """ 执行mysql语句"""
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='test',
                                 port=3306,
                                 charset='utf8')

    with connection.cursor() as cursor:
        cursor.execute(sql)
        connection.commit()
        return cursor.lastrowid


def mysql_fetch(sql, fetchone=True):
    """ 执行mysql语句"""
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='test',
                                 port=3306,
                                 charset='utf8')

    with connection.cursor() as cursor:
        cursor.execute(sql)
        if fetchone is True:
            return cursor.fetchone()
        else:
            return cursor.fetchall()


def catch_stock():
    """ 抓取所有股票代码 """
    global token

    cdate = time.strftime('%Y-%m-%d %H:%M:%S')
    pro = ts.pro_api(token)
    rs = pro.stock_basic(fields='symbol, name, industry, fullname, market, list_status, list_date, delist_date')
    for stock in rs.values:
        try:
            if stock[2] is not None:
                sql = "SELECT * FROM src_industry WHERE name = '" + stock[2] + "'"
                mysql_rs = mysql_fetch(sql)

                if mysql_rs is None:
                    sql = "INSERT INTO src_industry (`name`, `cdate`) VALUES ('" + stock[2] + "', '" + cdate + "')"
                    mysql_rs = mysql_insert(sql)
                    industry_id = str(mysql_rs)
                else:
                    industry_id = str(mysql_rs[0])
            else:
                industry_id = 0

            insert_sql = "INSERT INTO src_stock (`code`, `name`, `fullname`, `industry_id`, `market`, `status`," \
                         " `list_date`, `delist_date`, `cdate`, `udate`) " \
                         "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s')" \
                         % (stock[0], stock[1], stock[3], industry_id, stock[4], stock[5], stock[6], stock[7], cdate,
                            cdate)
            mysql_insert(insert_sql)
        except BaseException as e:
            log('stock_error', str(stock[0]) + '|' + str(stock[1]) + '|' + str(traceback.format_exc()))
            continue


def catch_daily_trade(stock_code, start, end):
    """ 抓取个股 日交易信息 """
    global token

    try:
        pro = ts.pro_api(token)
        data = pro.daily(ts_code=stock_code, start=start, end=end)
        counter = 0
        start_runtime = time.time()

        insert_sql = "INSERT INTO src_base_day (`code`, `date`, `open`, `close`, `high`, `low`, `volume`, `cdate`) VALUES "
        cdate = time.strftime('%Y-%m-%d %H:%M:%S')
        if data.empty is not True:
            for day in data.values:
                insert_sql += "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'),"\
                              % (day[0], day[1], day[2], day[5], day[3], day[4], day[9], cdate)
                counter = counter + 1
            insert_sql = insert_sql.strip(',')
            mysql_insert(insert_sql)

        end_runtime = round(time.time() - start_runtime, 3)
        request_json = json.dumps({
            'code': stock_code,
            'start_date': start,
            'end': end
        })
        result_json = json.dumps({
            'total': counter,
            'runtime': end_runtime
        })
        insert_sql = "INSERT INTO log_catch (`type`, `request`, `result`, `cdate`)" \
                     " VALUES ('daily_trade', '" + str(request_json) + "', '" + result_json + "', '" + time.strftime(
            '%Y-%m-%d %H:%M:%S') + "')"
        mysql_insert(insert_sql)
    except BaseException as e:
        insert_sql = "INSERT INTO log_catch (`type`, `request`, `result`,`exception`, `cdate`)" \
                     " VALUES ('daily_trade', '" + str(request_json) + "', '" + result_json + "','" \
                     + str(traceback.format_exc()) + "', '" + time.strftime('%Y-%m-%d %H:%M:%S') + "')"
        mysql_insert(insert_sql)


def init_all_day_trade():
    """ 初始化所有股票 日交易数据。剔除不追踪的"""
    sql = "SELECT code from src_stock where `is_trace` = 1"
    data = mysql_fetch(sql, False)
    for code in data:
        print(code[0])
        # 1990年有10只股票，而且有些已经不存在了，以此为起点
        catch_daily_trade(code[0], '19900101', '20191022')


log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
token = 'b122fa2788bd599ca9b5ae1b02a51fa0a5e4c7724e54de4955cb1c34'
# init_all_day_trade()

catch_daily_trade('600581.SH', '19900101', '20191022')
