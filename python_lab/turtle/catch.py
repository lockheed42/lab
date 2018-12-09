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
    cdate = time.strftime('%Y-%m-%d %H:%M:%S')
    rs = ts.get_industry_classified()
    for stock in rs.values:
        sql = "SELECT * FROM src_industry WHERE name = '" + stock[2] + "'"
        mysql_rs = mysql_fetch(sql)

        try:
            if mysql_rs is None:
                sql = "INSERT INTO src_industry (`name`, `cdate`) VALUES ('" + stock[2] + "', '" + cdate + "')"
                mysql_rs = mysql_insert(sql)
                industry_id = str(mysql_rs)
            else:
                industry_id = str(mysql_rs[0])

            insert_sql = "INSERT INTO src_stock (`code`, `name`, `industry_id`, `cdate`, `udate`)" \
                         " VALUES ('" + str(stock[0]) + "','" \
                         + str(stock[1]) + "' ," + industry_id + " ,'" + cdate + "' ,'" + cdate + "' )"
            mysql_insert(insert_sql)
        except BaseException as e:
            log('stock_error', str(stock[0]) + '|' + str(stock[1]) + '|' + str(traceback.format_exc()))
            continue


def catch_daily_trade(stock_code, start, end):
    """ 抓取个股 日交易信息 """
    try:
        data = ts.get_k_data(stock_code, start=start, end=end)
        counter = 0
        start_runtime = time.time()

        insert_sql = "INSERT INTO src_base_day (`code`, `date`, `open`, `close`, `high`, `low`, `volume`, `cdate`) VALUES "
        cdate = time.strftime('%Y-%m-%d %H:%M:%S')
        if data.empty is not True:
            for day in data.values:
                insert_sql += "('" + day[6] + "','" + day[0] + "'," + str(day[1]) + "," + str(day[2]) \
                              + "," + str(day[3]) + "," + str(day[4]) + "," + str(day[5]) + ",'" + cdate + "'),"
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
    """ 初始化所有股票 日交易数据"""
    sql = "SELECT code from src_stock where `status` = 1"
    data = mysql_fetch(sql, False)
    for code in data:
        print(code[0])
        # 1990年有10只股票，而且有些已经不存在了，以此为起点
        for year in range(1990, 2019):
            catch_daily_trade(code[0], str(year) + '-01-01', str(year) + '-10-22')


log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
# init_all_day_trade()