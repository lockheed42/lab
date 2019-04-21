#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测
"""

__author__ = 'lockheed'

import pymysql
import pymysql.cursors


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
    """
    查询mysql语句
    :param sql:
    :param fetchone:
    :return:
    """
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
