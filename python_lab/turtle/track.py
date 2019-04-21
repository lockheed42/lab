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


def ma004(code, track_date):
    track_log = mysql.mysql_fetch(
        "select `type` from rpt_ track where code = '" + code + "' and model_code = 'ma-004' order by id desc")
    # 没有买入或者最后一条记录不是卖出，说明为持有状态，跳过
    if track_log is None or track_log[0] != 2:
        return

    end_date = track_date
    # 70日前
    start_date = ''
    sql = "SELECT * FROM src_base_day WHERE code = '%s' and `date` >= '%s' and `date`<='%s'" \
          % (code, start_date, end_date)
    res = mysql.mysql_fetch(sql, False)


sql = "SELECT code FROM src_stock WHERE `status` = 'L' and `is_trace` = 1"
data = mysql.mysql_fetch(sql, False)
for code in data:
    try:
        ma004(code[0], '2019-02-27')
    except BaseException as e:
        sim.log('rpt_track_ma-004', str(code[0]) + '|' + str(traceback.format_exc()))
        continue
