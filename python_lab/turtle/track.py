#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
读取测试记录，分析结果
"""

__author__ = 'lockheed'

import time
import numpy as np
import traceback
from decimal import Decimal
from base import sim
from base import mysql
from model_ma import *

sql = "SELECT id, code FROM src_stock WHERE `status` = 'L' and `is_trace` = 1"
data = mysql.mysql_fetch(sql, False)
for ids, code in data:
    # time.strftime('%Y%m%d', time.localtime())
    date = '2019-05-06'
    ModelMa().track_model(code, date, '2018-06-01')
