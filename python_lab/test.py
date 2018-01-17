#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'test'
__author__ = 'lockheed'

process_num = 4
i = 0
while i < 20:
    pipe_num = i % process_num
    print(pipe_num)
    i = i + 1
