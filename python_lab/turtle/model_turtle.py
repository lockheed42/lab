#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测
"""

__author__ = 'lockheed'

import time
import traceback
from decimal import Decimal
from base import sim
from base import mysql


def turtle(code, model_code, end_date, start_date='1990-01-01'):
    """海龟交易法则"""
    # 测试明细数据最后一个id
    last_test_detail_id = 0
    # 近期最高 和 最低价格列表
    high_list = []
    low_list = []
    # 持有状态，False为未持有
    have_status = False
    # 初始资金
    money = 1000000
    # 持股天数
    have_day = 0
    # 最大回撤度
    max_draw_down = 0
    # 回撤天数
    draw_down_day = 0
    # 最大回撤持续时长
    max_draw_down_day = 0
    # 区间最高值，用于计算最大回撤
    interval_highest = 0
    # 每日波动率列表，存储多少天由程序决定
    tr_list = []
    # 买入价格，用于判断是否触发止损
    buy_price = 0
    # 昨日收盘价，用于计算ATR
    yesterday_close = 0
    # 突破多少日最高价买入
    days_hightest_buy = 55
    # 跌破多少日最低价卖出
    days_lowest_sell = 20

    sql = "SELECT * FROM src_base_day WHERE code = '%s' and `date` >= '%s' and `date`<='%s'" \
          % (code, start_date, end_date)
    print(code)
    res = mysql.mysql_fetch(sql, False)

    # 创建计划
    test_id = mysql.mysql_insert(
        "INSERT INTO rpt_test (`code`, `init_money`, `start_date`, `end_date`, `cdate`, `model_code`) "
        "VALUES ('%s', %s, '%s', '%s', '%s', '%s')"
        % (code, money, start_date, end_date, time.strftime('%Y-%m-%d %H:%M:%S'), model_code))

    # TODO
    '''
    待添加：
    分步加仓
    '''
    for ids, code, date, open_p, close, high, low, vol, c_date in res:
        if have_status is True:
            # 持有天数
            have_day += 1
            # 最大回撤。——这里做了一些变化。考虑到回撤感受时间最长的是收盘之后，所以此处使用 收盘价来做计算
            if close > interval_highest:
                draw_down_day = 0
                interval_highest = close
            else:
                draw_down_day += 1
                if max_draw_down < (1 - close / interval_highest):
                    max_draw_down = 1 - close / interval_highest
                    max_draw_down_day = draw_down_day
        else:
            # 非持有时间，并且回撤数据未清零时，清零回撤数据
            if interval_highest != 0:
                max_draw_down = 0
                draw_down_day = 0
                max_draw_down_day = 0
                interval_highest = 0

        # 是否当日买入，用于屏蔽T+0
        is_just_have = False
        length_high_list = len(high_list)
        # 买入条件
        if length_high_list > days_hightest_buy:
            highest = max(high_list)
            if low <= highest <= high and have_status is False:
                # TODO 均线数据还未添加，先用临时解决方案
                # 过滤器，比较前一日的 55日和300日移动平均线
                # res = mysql_fetch("SELECT avg(`close`) FROM src_base_day WHERE `code`='%s' and `date`<'%s' "
                #                   "and id >= (SELECT id FROM src_base_day WHERE `code`='%s' and `date`<'%s' ORDER BY id DESC limit 55,1)"
                #                   % (code, date, code, date))
                # ma55 = res[0]
                # res = mysql_fetch("SELECT avg(`close`) FROM src_base_day WHERE `code`='%s' and `date`<'%s' "
                #                   "and id >= (SELECT id FROM src_base_day WHERE `code`='%s' and `date`<'%s' ORDER BY id DESC limit 300,1)"
                #                   % (code, date, code, date))
                # ma300 = res[0]
                # # ma300不存在时过滤，不参与新股
                # # TODO 比较数据时，没有ma300也让买入
                # if ma300 is None or ma55 > ma300:
                # TODO 添加过滤器后需要tab
                # 记录买入价
                buy_price = highest
                # TODO
                # 计算止损价，按照海龟法则10%仓位和2%总资金亏损算出 单股 20%止损
                stop_loss_price = buy_price * Decimal(0.8)
                # 获取上次卖出后的现金
                if last_test_detail_id != 0:
                    last_info = mysql.mysql_fetch(
                        "SELECT id, after_money FROM rpt_test_detail WHERE id = %s" % last_test_detail_id)
                    money = last_info[1]

                stock_number = int(money / highest)
                insert_sql = "INSERT INTO rpt_test_detail (`test_id`, `code`,`buy_date`, `sell_date`," \
                             " `stock_number`,`before_money`, `buy_trigger`, `cdate`, `model_code`) " \
                             "VALUES (%s, '%s','%s','%s',%s,%s,%s,'%s','%s')" \
                             % (test_id, code, date, date, stock_number, money, highest,
                                time.strftime('%Y-%m-%d %H:%M:%S'), model_code)
                last_test_detail_id = mysql.mysql_insert(insert_sql)
                have_status = True
                is_just_have = True

        if have_status is True and is_just_have is False:
            # 计算止损价，判断是否需要止损
            # 2ATR
            # stop_loss_price = buy_price - np.mean(tr_list) * 2
            if low < stop_loss_price:
                sell_price = stop_loss_price if open_p >= stop_loss_price else open_p
                sim.sell(sell_price, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id, 2)
                have_status = False
                have_day = 0

        # 正常退出条件，当日不会同时买卖
        if have_status is True and is_just_have is False:
            lowest = min(low_list)
            if low <= lowest <= high:
                sim.sell(lowest, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id)
                # 清空买入价
                buy_price = 0
                have_status = False
                have_day = 0

        # 日线数据完结，需要强制退出 #TODO 此处可能有问题
        if have_status is True and is_just_have is False:
            sim.sell(close, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id)

        # 处理 最高价 和 最低价列表。——采用海龟基本法则的 55天和20天
        if length_high_list > days_hightest_buy:
            high_list.pop(0)
        if len(low_list) > days_lowest_sell:
            low_list.pop(0)
        if len(tr_list) >= days_lowest_sell:
            tr_list.pop(0)

        # 今日波动值，存入波动列表，用于计算ATR
        today_tr = max(high - low, abs(high - yesterday_close), abs(low - yesterday_close))
        tr_list.append(today_tr)
        high_list.append(high)
        low_list.append(low)
        yesterday_close = close

    sim.calc_model_plan(test_id)



# main
log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
model_code = 'turtle-004'

sql = "SELECT code FROM src_stock WHERE `status` = 'L' and `is_trace` = 1"
data = mysql.mysql_fetch(sql, False)
for code in data:
    try:
        turtle(code[0], model_code, '2019-02-27')
    except BaseException as e:
        sim.log('calc_model_detail', str(code[0]) + '|' + str(traceback.format_exc()))
        continue
