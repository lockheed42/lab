#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测通用模块
"""

__author__ = 'lockheed'

import math
import time
import traceback
from base import mysql
from decimal import Decimal

log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'


def log(file, content):
    """其他日志"""
    global host
    global log_path
    with open(log_path + "/" + file + ".log", 'a') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')


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
    last_info = mysql.mysql_fetch(
        "SELECT `before_money`, `stock_number`, `buy_trigger` FROM rpt_test_detail WHERE id = %s" % last_test_detail_id)
    before_money = last_info[0]
    stock_number = last_info[1]
    buy_trigger = last_info[2]
    # 未买入现金
    rest_money = before_money - stock_number * buy_trigger
    # 买入部分变化
    after_money = rest_money + sell_price * stock_number
    # 收益率
    profit_rate = (sell_price - buy_trigger) / buy_trigger
    mysql.mysql_insert(
        "UPDATE rpt_test_detail SET `have_day`=%s, `profit_rate`=%.2f, `sell_date`='%s',"
        " `after_money`=%.2f, `sell_trigger`= %.3f, `max_retracement`=%.2f, `retracement_day`=%s,`sell_type`='%s', `status`=2 "
        "WHERE id = %s"
        % (have_day, profit_rate * 100, date, after_money, sell_price, max_draw_down * 100,
           max_draw_down_day, sell_type, last_test_detail_id))


def calc_model_plan(test_id=0):
    """统计回测概况数据"""
    plan_info = mysql.mysql_fetch("SELECT test_id FROM rpt_test WHERE `status` = 1 AND `test_id`=%s" % test_id)
    try:
        # 最后一条已卖出数据
        last_detail = mysql.mysql_fetch(
            "SELECT after_money, sell_date FROM rpt_test_detail WHERE test_id = %s AND `status`=2 ORDER BY id DESC LIMIT 1"
            % plan_info[0])
        # 没有成功的交易记录，设置状态后退出
        if last_detail is None:
            mysql.mysql_insert("UPDATE rpt_test SET `status`=9 WHERE `test_id`=%s" % plan_info[0])
            return
        final_money = last_detail[0]
        # 第一条交易数据
        first_detail = mysql.mysql_fetch(
            "SELECT before_money, buy_date FROM rpt_test_detail WHERE test_id = %s ORDER BY id LIMIT 1"
            % plan_info[0])
        start_money = first_detail[0]

        # 持有年份，年化收益
        have_year = math.ceil((last_detail[1] - first_detail[1]).days / 365)
        profit_year = (final_money - start_money) / start_money / have_year * 100
        # 回撤数据
        retracement_info = mysql.mysql_fetch(
            "SELECT max_retracement, retracement_day FROM rpt_test_detail WHERE test_id = %s ORDER BY max_retracement DESC LIMIT 1"
            % plan_info[0])
        max_retracement = retracement_info[0]
        retracement_day = retracement_info[1]

        trade_times = mysql.mysql_fetch(
            "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 " % plan_info[0])
        trade_times = trade_times[0]
        success_times = mysql.mysql_fetch(
            "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 AND profit_rate >0"
            % plan_info[0])
        success_times = success_times[0]
        success_rate = success_times / trade_times
        sql_update = "UPDATE rpt_test SET `final_money`=%.2f, `profit_year`=%.2f, `max_retracement`=%.2f, `retracement_day`=%s, " \
                     "`trade_times`=%s, `success_rate`=%.2f, `start_date`='%s', `end_date`='%s',`status`=2 " \
                     "WHERE `test_id`=%s" \
                     % (final_money, profit_year, max_retracement, retracement_day, trade_times, success_rate,
                        first_detail[1], last_detail[1], plan_info[0])
        mysql.mysql_insert(sql_update)
    except BaseException as e:
        log('calc_model_plan', str(plan_info[0]) + '|' + str(traceback.format_exc()))


class Sim:
    # 测试明细数据最后一个id
    last_test_detail_id = 0
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
    # 是否当日买入，用于屏蔽T+0
    is_just_have = False
    # 止损价，在过程中计算
    stop_loss_price = 0

    # 测试计划ID
    test_id = 0
    # 回测模型代码
    model_code = ''

    def main(self, code, model_code, end_date, start_date='1990-01-01'):
        """
        回测主程序
        :param code:
        :param model_code:
        :param end_date:
        :param start_date:
        :return:
        """

        print(code)
        self.model_code = model_code
        sql = "SELECT * FROM src_base_day WHERE code = '%s' and `date` >= '%s' and `date`<='%s'" \
              % (code, start_date, end_date)
        res = mysql.mysql_fetch(sql, False)

        # 创建计划
        self.test_id = mysql.mysql_insert(
            "INSERT INTO rpt_test (`code`, `init_money`, `start_date`, `end_date`, `cdate`, `model_code`) "
            "VALUES ('%s', %s, '%s', '%s', '%s', '%s')"
            % (code, self.money, start_date, end_date, time.strftime('%Y-%m-%d %H:%M:%S'), model_code))

        for ids, code, date, open_p, close, high, low, vol, c_date in res:
            # 测算并记录【回撤相关数据】
            if self.have_status is True:
                # 持有天数
                self.have_day += 1
                # 最大回撤。——这里做了一些变化。考虑到回撤感受时间最长的是收盘之后，所以此处使用 收盘价来做计算
                if close > self.interval_highest:
                    self.draw_down_day = 0
                    self.interval_highest = close
                else:
                    self.draw_down_day += 1
                    if self.max_draw_down < (1 - close / self.interval_highest):
                        self.max_draw_down = 1 - close / self.interval_highest
                        self.max_draw_down_day = self.draw_down_day
            else:
                # 非持有时间，并且回撤数据未清零时，清零回撤数据
                if self.interval_highest != 0:
                    self.max_draw_down = 0
                    self.draw_down_day = 0
                    self.max_draw_down_day = 0
                    self.interval_highest = 0

            # 是否当日买入，用于屏蔽T+0
            self.is_just_have = False

            # 交易准备模块
            self.main_begin(ids, code, date, open_p, close, high, low, vol, c_date)
            # 交易前模块
            self.main_before(ids, code, date, open_p, close, high, low, vol, c_date)
            # 买入模块
            self.main_buy(ids, code, date, open_p, close, high, low, vol, c_date)

            # 止损策略
            if self.have_status is True and self.is_just_have is False:
                self.lost_control(ids, code, date, open_p, close, high, low, vol, c_date)

            # 卖出策略
            if self.have_status is True and self.is_just_have is False:
                self.main_sell(ids, code, date, open_p, close, high, low, vol, c_date)

            self.main_after(ids, code, date, open_p, close, high, low, vol, c_date)

        # 防止结束时还未卖出。
        if self.have_status is True:
            self.sell(close, date, self.have_day, self.max_draw_down, self.max_draw_down_day, self.last_test_detail_id)

        self.calc_model_plan(self.test_id)

    def calc_model_plan(self, test_id=0):
        """统计回测概况数据"""
        plan_info = mysql.mysql_fetch("SELECT test_id FROM rpt_test WHERE `status` = 1 AND `test_id`=%s" % test_id)
        try:
            # 最后一条已卖出数据
            last_detail = mysql.mysql_fetch(
                "SELECT after_money, sell_date FROM rpt_test_detail WHERE test_id = %s AND `status`=2 ORDER BY id DESC LIMIT 1"
                % plan_info[0])
            # 没有成功的交易记录，设置状态后退出
            if last_detail is None:
                mysql.mysql_insert("UPDATE rpt_test SET `status`=9 WHERE `test_id`=%s" % plan_info[0])
                return
            final_money = last_detail[0]
            # 第一条交易数据
            first_detail = mysql.mysql_fetch(
                "SELECT before_money, buy_date FROM rpt_test_detail WHERE test_id = %s ORDER BY id LIMIT 1"
                % plan_info[0])
            start_money = first_detail[0]

            # 持有年份，年化收益
            have_year = math.ceil((last_detail[1] - first_detail[1]).days / 365)
            profit_year = (final_money - start_money) / start_money / have_year * 100
            # 回撤数据
            retracement_info = mysql.mysql_fetch(
                "SELECT max_retracement, retracement_day FROM rpt_test_detail WHERE test_id = %s ORDER BY max_retracement DESC LIMIT 1"
                % plan_info[0])
            max_retracement = retracement_info[0]
            retracement_day = retracement_info[1]

            trade_times = mysql.mysql_fetch(
                "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 " % plan_info[0])
            trade_times = trade_times[0]
            success_times = mysql.mysql_fetch(
                "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `status`=2 AND profit_rate >0"
                % plan_info[0])
            success_times = success_times[0]
            success_rate = success_times / trade_times
            sql_update = "UPDATE rpt_test SET `final_money`=%.2f, `profit_year`=%.2f, `max_retracement`=%.2f, `retracement_day`=%s, " \
                         "`trade_times`=%s, `success_rate`=%.2f, `start_date`='%s', `end_date`='%s',`status`=2 " \
                         "WHERE `test_id`=%s" \
                         % (final_money, profit_year, max_retracement, retracement_day, trade_times, success_rate,
                            first_detail[1], last_detail[1], plan_info[0])
            mysql.mysql_insert(sql_update)
        except BaseException as e:
            log('calc_model_plan', str(plan_info[0]) + '|' + str(traceback.format_exc()))

    def log(self, file, content):
        """其他日志"""
        global host
        global log_path
        with open(log_path + "/" + file + ".log", 'a') as f:
            f.write(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')

    def sell(self, sell_price, date, have_day, max_draw_down, max_draw_down_day, last_test_detail_id, sell_type=1):
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
        last_info = mysql.mysql_fetch(
            "SELECT `before_money`, `stock_number`, `buy_trigger` FROM rpt_test_detail WHERE id = %s" % last_test_detail_id)
        before_money = last_info[0]
        stock_number = last_info[1]
        buy_trigger = last_info[2]
        # 未买入现金
        rest_money = before_money - stock_number * buy_trigger
        # 买入部分变化
        after_money = rest_money + sell_price * stock_number
        # 收益率
        profit_rate = (sell_price - buy_trigger) / buy_trigger
        mysql.mysql_insert(
            "UPDATE rpt_test_detail SET `have_day`=%s, `profit_rate`=%.2f, `sell_date`='%s',"
            " `after_money`=%.2f, `sell_trigger`= %.3f, `max_retracement`=%.2f, `retracement_day`=%s,`sell_type`='%s', `status`=2 "
            "WHERE id = %s"
            % (have_day, profit_rate * 100, date, after_money, sell_price, max_draw_down * 100,
               max_draw_down_day, sell_type, last_test_detail_id))
