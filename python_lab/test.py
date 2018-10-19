#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'test'
__author__ = 'lockheed'

import os
import fcntl
import time

for i in range(1000):
    with open('test.txt', 'a') as f:
        # fcntl.flock(f, fcntl.LOCK_EX)
        time.sleep(0.003)
        f.write('1\n')
        # fcntl.flock(f, fcntl.LOCK_UN)
