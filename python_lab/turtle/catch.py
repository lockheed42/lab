#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
抓取交易数据
"""
__author__ = 'lockheed'

import tushare as ts
import time
import traceback
import json
from base import sim
from base import mysql


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
                mysql_rs = mysql.mysql_fetch(sql)

                if mysql_rs is None:
                    sql = "INSERT INTO src_industry (`name`, `cdate`) VALUES ('" + stock['industry'] \
                          + "', '" + cdate + "')"
                    mysql_rs = mysql.mysql_insert(sql)
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
            mysql.mysql_insert(insert_sql)
        except BaseException as e:
            sim.log('stock_error',
                    str(stock['symbol']) + '|' + str(stock['ts_code']) + '|' + str(traceback.format_exc()))
            continue


def catch_daily_trade(stock_code, start, end, adj='hfq'):
    """
    抓取个股 日交易信息
    :param stock_code: ts代码
    :param start:
    :param end:
    :param adj: 复权方式，默认后
    :return:
    """
    global token

    try:
        request_json = ''
        result_json = ''

        api = ts.pro_api(token)
        data = ts.pro_bar(pro_api=api, ts_code=stock_code, adj=adj, start_date=start, end_date=end)
        if data is None:
            raise BaseException("返回数据为空")
        data = data.sort_index()

        counter = 0
        start_runtime = time.time()

        high_list = []
        insert_sql = "INSERT INTO src_base_day (`code`, `date`, `open`, `close`, `high`, `low`, `volume`, `cdate`) VALUES "
        cdate = time.strftime('%Y-%m-%d %H:%M:%S')
        if data.empty is not True:
            for index, day in data.iterrows():
                high_list.append(day['high'])
                insert_sql += "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')," \
                              % (day['ts_code'][0:6], day['trade_date'], day['open'], day['close'], day['high'],
                                 day['low'], day['vol'], cdate)
                counter = counter + 1

            # 后复权价过高时的检测
            # high_list.sort(reverse=True)
            # print(high_list[0:10])
            # return
            insert_sql = insert_sql.strip(',')
            mysql.mysql_insert(insert_sql)

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
        mysql.mysql_insert(insert_sql)
    except BaseException as e:
        insert_sql = "INSERT INTO log_catch (`type`, `request`, `result`,`exception`, `cdate`)" \
                     " VALUES ('daily_trade', '" + str(request_json) + "', '" + result_json + "','" \
                     + str(traceback.format_exc()) + "', '" + time.strftime('%Y-%m-%d %H:%M:%S') + "')"
        mysql.mysql_insert(insert_sql)


def daily_trad_info(date):
    """
    更新每日数据，每分钟限制200次调用。
    可以通过修改最终调用日期范围，来拉取一个日期范围内的数据
    :param date:
    :return:
    """
    sql = "SELECT ts_code from src_stock where `is_trace` = 1"
    data = mysql.mysql_fetch(sql, False)
    right_time_point = int(time.time()) + 65
    limit_counter = 0
    for code in data:
        # 200次调用对比下时间，次数用完后，休眠满一分钟继续。
        # 卡足60秒经常还会超时，为了稳定延长
        limit_counter += 1
        if limit_counter >= 200:
            now_time = int(time.time())
            sleep_time = right_time_point - now_time
            if sleep_time > 0:
                time.sleep(sleep_time)
            limit_counter = 0
            right_time_point = int(time.time()) + 65
        catch_daily_trade(code[0], date, date)


# TODO 已经在定时任务运行，修改慎重
log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
token = 'b122fa2788bd599ca9b5ae1b02a51fa0a5e4c7724e54de4955cb1c34'

daily_trad_info(time.strftime('%Y%m%d', time.localtime()))
