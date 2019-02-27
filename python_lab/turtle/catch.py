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
    rs = pro.stock_basic(
        fields='symbol, ts_code, name, industry, fullname, market, list_status, list_date, delist_date')
    for index, stock in rs.iterrows():
        try:
            if stock['industry'] is not None:
                sql = "SELECT * FROM src_industry WHERE name = '" + stock['industry'] + "'"
                mysql_rs = mysql_fetch(sql)

                if mysql_rs is None:
                    sql = "INSERT INTO src_industry (`name`, `cdate`) VALUES ('" + stock['industry'] \
                          + "', '" + cdate + "')"
                    mysql_rs = mysql_insert(sql)
                    industry_id = str(mysql_rs)
                else:
                    industry_id = str(mysql_rs[0])
            else:
                industry_id = 0

            insert_sql = "INSERT INTO src_stock (`code`, `ts_code`, `name`, `fullname`, `industry_id`, `market`, `status`," \
                         " `list_date`, `delist_date`, `cdate`, `udate`) " \
                         "VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s')" \
                         % (stock['symbol'], stock['ts_code'], stock['name'], stock['fullname'], industry_id,
                            stock['market'], stock['list_status'], stock['list_date'], stock['delist_date'], cdate,
                            cdate)
            mysql_insert(insert_sql)
        except BaseException as e:
            log('stock_error', str(stock['symbol']) + '|' + str(stock['ts_code']) + '|' + str(traceback.format_exc()))
            continue


def catch_daily_trade(stock_code, start, end, adj='hfq'):
    """
    抓取个股 日交易信息
    stock_code  ts代码
    start
    end
    adj         复权方式，默认后
    """
    global token

    try:
        print('正在抓取： ' + stock_code + '  复权: ' + adj)

        api = ts.pro_api(token)
        data = ts.pro_bar(pro_api=api, ts_code=stock_code, adj=adj, start_date=start, end_date=end)
        data = data.sort_index()

        counter = 0
        start_runtime = time.time()

        insert_sql = "INSERT INTO src_base_day (`code`, `date`, `open`, `close`, `high`, `low`, `volume`, `cdate`) VALUES "
        cdate = time.strftime('%Y-%m-%d %H:%M:%S')
        if data.empty is not True:
            for index, day in data.iterrows():
                insert_sql += "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')," \
                              % (day['ts_code'][0:6], day['trade_date'], day['open'], day['close'], day['high'],
                                 day['low'], day['vol'], cdate)
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
    sql = "SELECT ts_code from src_stock where `is_trace` = 1"
    data = mysql_fetch(sql, False)
    for code in data:
        # 1990年有10只股票，而且有些已经不存在了，以此为起点
        catch_daily_trade(code[0], '19900101', '20190227')


log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
token = 'b122fa2788bd599ca9b5ae1b02a51fa0a5e4c7724e54de4955cb1c34'
init_all_day_trade()
