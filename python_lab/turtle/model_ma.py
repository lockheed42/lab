#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测。基于均线系统
"""

__author__ = 'lockheed'

import time
from decimal import Decimal
from base import sim
from base import mysql


class ModelMa(sim.Sim):
    # 5日收盘价集合
    ma_5 = []
    # 10日收盘价集合
    ma_10 = []
    # 20日收盘价集合
    ma_20 = []
    # 30日收盘价集合
    ma_30 = []
    # 60日收盘价集合
    ma_60 = []
    # 5日均线价
    ma_5_p = 0
    # 10日均线价
    ma_10_p = 0
    # 20日均线价
    ma_20_p = 0
    # 30日均线价
    ma_30_p = 0
    # 60日均线价
    ma_60_p = 0
    # 记录K先是否在均线之下
    is_under_ma = False

    def get_ma(self, data):
        """
        求移动平均线当日价
        :param data: 
        :return: 
        """
        return sum(data) / len(data)

    def main_ready(self, code):
        # 每次运行初始化数值，应对多进程复用model的数据问题
        self.ma_5 = []
        self.ma_10 = []
        self.ma_20 = []
        self.ma_30 = []
        self.ma_60 = []
        self.ma_5_p = 0
        self.ma_10_p = 0
        self.ma_20_p = 0
        self.ma_30_p = 0
        self.ma_60_p = 0
        self.is_under_ma = False

    def main_before(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 计算均线价
        # self.ma_5_p = self.get_ma(self.ma_5) if len(self.ma_5) == 5 else 99999
        # self.ma_10_p = self.get_ma(self.ma_10) if len(self.ma_10) == 10 else 99999
        self.ma_20_p = self.get_ma(self.ma_20) if len(self.ma_20) == 20 else 99999
        # self.ma_30_p = self.get_ma(self.ma_30) if len(self.ma_30) == 30 else 99999
        self.ma_60_p = self.get_ma(self.ma_60) if len(self.ma_60) == 60 else 99999
        # if self.ma_20:
        #     self.max_20_p = max(self.ma_20)
        # else:
        #     self.max_20_p = 1
        if self.have_status is False and self.ma_60_p > high:
            self.is_under_ma = True

    def main_buy(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 买入条件
        # ma-001的条件
        # if have_status is False and max_20_p > ma_5_p > ma_10_p > ma_20_p > ma_30_p and low <= max_20_p <= high:
        # 双均线。当K从长均线下方金叉时买入；或者跌破短均线卖出后，未跌破长均线再次金叉短均线买入
        if self.have_status is False and ((low >= self.ma_60_p and self.is_under_ma is True)
                                          or (self.is_under_ma is False and low >= self.ma_20_p)):
            # 记录买入价
            buy_price = low
            # TODO
            # 计算止损价
            self.stop_loss_price = buy_price * Decimal(0.92)
            # 获取上次卖出后的现金
            if len(self.tmp_trade_record) != 0:
                self.money = self.tmp_trade_record[len(self.tmp_trade_record) - 1]['after_money']

            # if self.last_test_detail_id != 0:
            #     last_info = mysql.mysql_fetch(
            #         "SELECT id, after_money FROM rpt_test_detail WHERE id = %s" % self.last_test_detail_id)
            #     self.money = last_info[1]

            stock_number = int(self.money / buy_price)
            self.tmp_trade_record.append({
                'test_id': self.test_id,
                'code': code,
                'buy_date': date,
                'sell_date': date,
                'stock_number': stock_number,
                'before_money': self.money,
                'buy_trigger': buy_price,
                'cdate': time.strftime('%Y-%m-%d %H:%M:%S'),
                'model_code': self.model_code,
            })

            # insert_sql = "INSERT INTO rpt_test_detail (`test_id`, `code`,`buy_date`, `sell_date`," \
            #              " `stock_number`,`before_money`, `buy_trigger`, `cdate`, `model_code`) " \
            #              "VALUES (%s, '%s','%s','%s',%s,%s,%s,'%s','%s')" \
            #              % (self.test_id, code, date, date, stock_number, self.money, buy_price,
            #                 time.strftime('%Y-%m-%d %H:%M:%S'), self.model_code)
            # self.last_test_detail_id = mysql.mysql_insert(insert_sql)
            self.have_status = True
            self.is_just_have = True
            self.is_under_ma = False

    def lost_control(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 计算止损价，判断是否需要止损
        # 2ATR
        # stop_loss_price = buy_price - np.mean(tr_list) * 2
        if close < self.stop_loss_price:
            sell_price = self.stop_loss_price if open_p >= self.stop_loss_price else open_p
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day, 2)
            self.have_status = False
            self.have_day = 0

    def main_sell(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 退出条件 收盘价跌破 日均线，当日不会同时买卖
        # ma-001的条件
        # if close < ma_20_p:
        ma_20_p_97 = self.ma_20_p * Decimal(0.97)
        if low <= ma_20_p_97 or high <= ma_20_p_97:
            sell_price = ma_20_p_97 if high >= ma_20_p_97 else high
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day)
            self.have_status = False
            self.have_day = 0

    def main_after(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 收集收盘价
        if len(self.ma_5) >= 5:
            self.ma_5.pop(0)
        if len(self.ma_10) >= 10:
            self.ma_10.pop(0)
        if len(self.ma_20) >= 20:
            self.ma_20.pop(0)
        if len(self.ma_30) >= 30:
            self.ma_30.pop(0)
        if len(self.ma_60) >= 60:
            self.ma_60.pop(0)

        self.ma_5.append(close)
        self.ma_10.append(close)
        self.ma_20.append(close)
        self.ma_30.append(close)
        self.ma_60.append(close)

    def main_end(self):
        # 防止结束时还未卖出。
        if self.have_status is True:
            self.sell(self.last_close, self.last_date, self.have_day, self.max_draw_down, self.max_draw_down_day)

        # 未交易跳过
        if len(self.tmp_trade_record) == 0:
            return

            # 一次性插入所有交易明细
        sql = "INSERT INTO rpt_test_detail (`model_code`, `test_id`, `code`, `sell_type`, `have_day`, `profit_rate`" \
              ", `max_retracement`, `retracement_day`, `buy_date`, `sell_date`, `before_money`, `after_money`" \
              ", `buy_trigger`, `sell_trigger`, `stock_number`, `status`, `cdate`) VALUES "
        for d in self.tmp_trade_record:
            sql += "('%s', %s, '%s', %s, %s, %.2f, %.2f, %s, '%s', '%s', %.2f, %.2f, %.3f, %.3f, %s, '%s', '%s')," \
                   % (d['model_code'], d['test_id'], d['code'], d['sell_type'], d['have_day'], d['profit_rate'],
                      d['max_retracement'], d['retracement_day'], d['buy_date'], d['sell_date'], d['before_money'],
                      d['after_money'], d['buy_trigger'], d['sell_trigger'], d['stock_number'], d['status'], d['cdate'])

        sql = sql.rstrip(',')
        mysql.mysql_insert(sql)


# main
log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
model_code = 'ma-test'

# 测试单条
# model = ModelMa()
# model.main('002943', model_code, '2019-02-27')
# exit()

# 单进程模拟
# sql = "SELECT code FROM src_stock WHERE `status` = 'L' and `is_test` = 1"
# data = mysql.mysql_fetch(sql, False)
# for code in data:
#     try:
#         model = ModelMa()
#         model.main(code[0], model_code, '2019-02-27')
#     except BaseException as e:
#         sim.log('sim_model_ma', str(code[0]) + '|' + str(traceback.format_exc()))
#         continue

# 多进程
model = ModelMa()
model.multi_main(model_code, '2019-02-27', '2003-01-01')
