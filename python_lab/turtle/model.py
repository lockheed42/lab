#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测
"""

__author__ = 'lockheed'

import pymysql
import pymysql.cursors
import time
import traceback
import math
import numpy as np
from decimal import Decimal


def log(file, content):
    """其他日志"""
    global host
    global log_path
    with open(log_path + "/" + file + ".log", 'a') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')


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
    """ 执行mysql语句"""
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


def sell(sell_price, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id, sell_type=1):
    """实现卖出行为，并做记录

    Args:
        sell_price： 卖出价格
        date： 卖出日期
        have_day： 持仓天数
        max_draw_down： 最大回撤率
        max_draw_down_day： 最大回撤持续天数
        last_test_detail_id： 最近一条买入的测试记录id
        sell_type： 卖出类型。1=正常退出，2=止损
    """
    # 获取买入时的数据
    last_info = mysql_fetch("SELECT * FROM rpt_test_detail WHERE id = %s" % last_test_detail_id)
    stock_number = last_info[15]
    before_money = last_info[11]
    buy_trigger = last_info[13]
    # 未买入现金
    rest_money = before_money - stock_number * buy_trigger
    # 买入部分变化
    after_money = rest_money + sell_price * stock_number
    # 收益率
    profit_rate = (sell_price - buy_trigger) / buy_trigger
    mysql_insert(
        "UPDATE rpt_test_detail SET `have_day`=%s, `profit_rate`=%.2f, `sell_date`='%s',"
        " `after_money`=%.2f, `sell_trigger`= %.3f, `max_retracement`=%.2f, `retracement_day`=%s,`sell_type`='%s', `status`=2 "
        "WHERE id = %s"
        % (have_day, profit_rate * 100, date, after_money, sell_price, max_draw_down * 100,
           max_draw_down_day, sell_type, last_test_detail_id))


def turtle(code, model_code, start_date='1990-01-01', end_date=''):
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

    if end_date == '':
        end_date = time.strftime('%Y-%m-%d')
    sql = "SELECT * FROM src_base_day WHERE code = '%s' and `date` >= '%s' and `date`<='%s'" \
          % (code, start_date, end_date)
    print(sql)
    res = mysql_fetch(sql, False)

    # 创建计划
    test_id = mysql_insert(
        "INSERT INTO rpt_test (`code`, `init_money`, `start_date`, `end_date`, `cdate`, `model_code`) "
        "VALUES ('%s', %s, '%s', '%s', '%s', '%s')"
        % (code, money, start_date, end_date, time.strftime('%Y-%m-%d %H:%M:%S'), model_code))

    # TODO
    '''
    待添加：
    分步加仓
    止损策略
    过滤器
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
            if draw_down_day != 0:
                max_draw_down = 0
                draw_down_day = 0
                max_draw_down_day = 0
                interval_highest = 0

        # 是否当日买入，用于屏蔽T+0
        is_just_have = False
        length_high_list = len(high_list)
        # 买入条件
        if length_high_list > 55:
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
                # 记录买入价 #TODO 添加过滤器后需要tab
                buy_price = highest
                # TODO
                # 计算止损价，按照海龟法则10%仓位和2%总资金亏损算出 单股 20%止损
                stop_loss_price = buy_price * Decimal(0.8)
                # 获取上次卖出后的现金
                if last_test_detail_id != 0:
                    last_info = mysql_fetch(
                        "SELECT id, after_money FROM rpt_test_detail WHERE id = %s" % last_test_detail_id)
                    money = last_info[1]

                stock_number = int(money / highest)
                insert_sql = "INSERT INTO rpt_test_detail (`test_id`, `code`,`buy_date`, `sell_date`," \
                             " `stock_number`,`before_money`, `buy_trigger`, `cdate`, `model_code`) " \
                             "VALUES (%s, '%s','%s','%s',%s,%s,%s,'%s','%s')" \
                             % (test_id, code, date, date, stock_number, money, highest,
                                time.strftime('%Y-%m-%d %H:%M:%S'), model_code)
                last_test_detail_id = mysql_insert(insert_sql)
                have_status = True
                is_just_have = True

        if have_status is True and is_just_have is False:
            # 计算止损价，判断是否需要止损
            # 2ATR
            # stop_loss_price = buy_price - np.mean(tr_list) * 2
            if low < stop_loss_price:
                sell_price = stop_loss_price if open_p >= stop_loss_price else open_p
                sell(sell_price, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id, 2)
                have_status = False
                have_day = 0

        # 正常退出条件，当日不会同时买卖
        if have_status is True and is_just_have is False:
            lowest = min(low_list)
            if low <= lowest <= high:
                sell(lowest, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id)
                # 清空买入价
                buy_price = 0
                have_status = False
                have_day = 0

        # 处理 最高价 和 最低价列表。——采用海龟基本法则的 55天和20天
        if length_high_list > 55:
            high_list.pop(0)
        if len(low_list) > 20:
            low_list.pop(0)
        if len(tr_list) >= 20:
            tr_list.pop(0)

        # 今日波动值，存入波动列表，用于计算ATR
        today_tr = max(high - low, abs(high - yesterday_close), abs(low - yesterday_close))
        tr_list.append(today_tr)
        high_list.append(high)
        low_list.append(low)
        yesterday_close = close

    calc_model_plan(test_id)


def calc_model_plan(test_id=0):
    """统计回测概况数据"""
    plan_info = mysql_fetch("SELECT test_id FROM rpt_test WHERE `status` = 1 AND `test_id`=%s" % test_id)

    try:
        # 最后一条已卖出数据
        last_detail = mysql_fetch(
            "SELECT after_money, sell_date FROM rpt_test_detail WHERE test_id = %s AND `status`=2 ORDER BY id DESC LIMIT 1"
            % plan_info[0])
        # 没有成功的交易记录，设置状态后退出
        if last_detail is None:
            mysql_insert("UPDATE rpt_test SET `status`=9 WHERE `test_id`=%s" % plan_info[0])
            return
        final_money = last_detail[0]
        # 第一条交易数据
        first_detail = mysql_fetch(
            "SELECT before_money, buy_date FROM rpt_test_detail WHERE test_id = %s ORDER BY id LIMIT 1"
            % plan_info[0])
        start_money = first_detail[0]

        # 持有年份，年化收益
        have_year = math.ceil((last_detail[1] - first_detail[1]).days / 365)
        profit_year = (final_money - start_money) / start_money / have_year * 100

        # 回撤数据
        retracement_info = mysql_fetch(
            "SELECT max_retracement, retracement_day FROM rpt_test_detail WHERE test_id = %s ORDER BY max_retracement DESC LIMIT 1"
            % plan_info[0])
        max_retracement = retracement_info[0]
        retracement_day = retracement_info[1]

        trade_times = mysql_fetch(
            "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 " % plan_info[0])
        trade_times = trade_times[0]

        success_times = mysql_fetch(
            "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 AND profit_rate >0"
            % plan_info[0])
        success_times = success_times[0]
        success_rate = success_times / trade_times

        mysql_insert(
            "UPDATE rpt_test SET `final_money`=%.2f, `profit_year`=%.2f, `max_retracement`=%.2f, `retracement_day`=%s,"
            " `trade_times`=%s, `success_rate`=%.2f, `status`=2 WHERE `test_id`=%s"
            % (final_money, profit_year, max_retracement, retracement_day, trade_times, success_rate, plan_info[0]))
    except BaseException as e:
        log('calc_model_plan', str(plan_info[0]) + '|' + str(traceback.format_exc()))


# main
log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
model_code = 'turtle-004'

sql = "SELECT code FROM src_stock WHERE `status` = 1"
data = mysql_fetch(sql, False)
for code in data:
    try:
        turtle(code[0], model_code)
    except BaseException as e:
        log('calc_model_detail', str(code[0]) + '|' + str(traceback.format_exc()))
        continue
