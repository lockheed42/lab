#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
Redis在海龟内通用模块
"""

__author__ = 'lockheed'

import redis
from base import mysql


class Redis:
    # 需要处理的任务队列
    mission_queue_key = 'turtle_code_queue'
    # 空闲的子进程id
    free_process_key_prefix = 'free_process_turtle'

    handle = None

    def __init__(self):
        """
        设置redis当前池的handle
        :return:
        """
        redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
        self.handle = redis.Redis(connection_pool=redis_pool)

    def init(self, model_code, end_date, start_date):
        """
        初始化redis。
        清空redis，把所有参加回测的code载入队列
        :return:
        """
        self.handle.flushall()
        queue_suffix = '------' + model_code + '------' + end_date + '------' + start_date
        sql = "SELECT id, code FROM src_stock WHERE `status` = 'L' and `is_test` = 1"
        data = mysql.mysql_fetch(sql, False)
        for id, code in data:
            self.handle.lpush(self.mission_queue_key, code + queue_suffix)

