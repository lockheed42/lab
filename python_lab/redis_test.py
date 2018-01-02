#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''test
'''
__author__ = 'lockheed'

import redis

pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool)

r.delete('test')
test = r.setnx('test', 111)
print(test)