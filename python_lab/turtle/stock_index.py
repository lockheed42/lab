#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
指数成分股 相关功能代码
"""

__author__ = 'lockheed'

import tushare as ts
from base import mysql
import time
import traceback

"""
      ts_code trade_date  turnover_rate     pe
0   000001.SH   20190531           0.58  13.34
1   000005.SH   20190531           1.09  17.86
2   000006.SH   20190531           0.51   8.07
3   000016.SH   20190531           0.14   9.70	上证50
4   000300.SH   20190531           0.33  12.09	
5   000905.SH   20190531           1.10  26.56	中证500
6   399001.SZ   20190531           1.09  23.53
7   399005.SZ   20190531           1.06  23.57  中小板
8   399006.SZ   20190531           1.28  49.40  创业板
9   399016.SZ   20190531           1.13  26.24
10  399300.SZ   20190531           0.33  12.09  沪深300
11  399905.SZ   20190531           1.10  26.56
"""


def catch():
    """
    抓取指数成分股，并保存
    :return:
    """
    token = 'b122fa2788bd599ca9b5ae1b02a51fa0a5e4c7724e54de4955cb1c34'
    log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'

    # 指数ID
    index_id = 4
    # TS指数代码
    index_code = '399006.SZ'

    print('指数id：', index_id)
    print('TS指数代码：', index_code)

    api = ts.pro_api(token)
    df = api.index_weight(index_code=index_code, start_date='20190131', end_date='20190131')
    # print(count(df))

    cdate = time.strftime('%Y-%m-%d %H:%M:%S')
    for ind_id, info in df.iterrows():
        try:
            sql = "INSERT INTO src_stock_index (`stock_code`, `index_id`, `cdate`) " \
                  "VALUES ('%s', '%s', '%s')" % (info['con_code'][0:6], index_id, cdate)
            mysql.mysql_insert(sql)
        except BaseException as e:
            print('stock_error',
                  str(info['con_code']) + '|' + str(traceback.format_exc()))
            exit()


def set_test(index_id):
    """
    设定指定成分股 回测状态为是
    :param index_id:
    :return:
    """
    sql = "SELECT stock_code FROM src_stock_index WHERE index_id IN (%s)" % index_id
    rs = mysql.mysql_fetch(sql, False)
    code_list_string = ''
    for i in rs:
        code_list_string += i[0] + ','
    code_list_string = code_list_string.rstrip(',')

    mysql.mysql_insert("UPDATE src_stock SET is_test = 0")
    sql = "UPDATE src_stock SET is_test = 1 WHERE code IN (%s)" % code_list_string
    mysql.mysql_insert(sql)


if __name__ == '__main__':
    set_test("3,4")
