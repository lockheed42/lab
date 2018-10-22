#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""布隆过滤器测试"""

import os
import fcntl
import time


for i in range(1000):
    with open('test.txt', 'a') as f:
        # fcntl.flock(f, fcntl.LOCK_NB)
        time.sleep(0.002)
        f.write('4\n')
        # fcntl.flock(f, fcntl.LOCK_UN)
