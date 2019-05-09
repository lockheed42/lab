#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测。基于均线系统
"""

__author__ = 'lockheed'

import time
from decimal import Decimal
from base import sim


class ModelMa(sim.Sim):
    # 20日收盘价集合
    ma_20 = []
    # 30日收盘价集合
    ma_30 = []
    # 60日收盘价集合
    ma_60 = []
    # 20日均线价
    ma_20_p = 0
    # 30日均线价
    ma_30_p = 0
    # 60日均线价
    ma_60_p = 0
    # 记录K先是否在均线之下
    is_under_ma = False

    # 买入信号。创建时是因为买入点发现后的第二天操作，用于完全穿越K线
    buy_sign = False
    # 卖出信号。
    sell_sign = False

    def get_ma(self, data):
        """
        求移动平均线当日价
        :param data: 
        :return: 
        """
        return sum(data) / len(data)

    def get_sd(self, data):
        """
        计算标准差
        :param data:
        :return:
        """
        avg = self.get_ma(data)
        square_sum = 0
        for ele in data:
            square_sum += pow(ele - avg, 2)
        return pow(square_sum / len(data), 0.5)

    def main_ready(self, code):
        # 每次运行初始化数值，应对多进程复用model的数据问题
        self.ma_20 = []
        self.ma_30 = []
        self.ma_60 = []
        self.ma_20_p = 0
        self.ma_30_p = 0
        self.ma_60_p = 0
        self.is_under_ma = False
        self.buy_sign = False
        self.sell_sign = False

    def main_before(self, ids, code, date, open_p, close, high, low, vol, c_date):
        self.ma_20_p = self.get_ma(self.ma_20) if len(self.ma_20) == 20 else 99999
        # self.ma_30_p = self.get_ma(self.ma_30) if len(self.ma_30) == 30 else 99999
        # self.ma_60_p = self.get_ma(self.ma_60) if len(self.ma_60) == 60 else 99999
        # if self.ma_20:
        #     self.max_20_p = max(self.ma_20)
        # else:
        #     self.max_20_p = 1
        if self.have_status is False and self.ma_30_p > high:
            self.is_under_ma = True

    def main_buy(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 发出买入信号
        # if self.buy_sign is False and self.have_status is False and (
        #         ((self.ma_60_p <= low or self.ma_60_p + self.atr_p * 2 <= high) and self.is_under_ma is True)
        #         or
        #         # 未彻底破ma60前，高于ma20视为趋势延续，但此时必须 ma20高于ma60，防止错误信号
        #         (self.is_under_ma is False and self.ma_60_p < self.ma_20_p <= low)):

        if self.buy_sign is False and self.have_status is False and low >= self.ma_30_p and self.is_under_ma is True:
            self.buy_sign = True
            return

        # 买入条件
        if self.buy_sign is True:
            # 涨停板无法买入
            if high == low:
                return

            self.buy_sign = False

            # TODO 买入价要经常思考是否可以更贴近实际
            # if self.ma_20_p < self.ma_60_p:
            #     self.buy_price = self.ma_60_p if open_p <= self.ma_60_p else open_p
            # else:
            #     self.buy_price = self.ma_20_p if open_p <= self.ma_20_p else open_p

            self.buy_price = self.ma_30_p if open_p <= self.ma_30_p else open_p

            # 计算止损价
            self.stop_loss_price = self.buy_price * Decimal(0.92)
            # 获取上次卖出后的现金
            if len(self.tmp_trade_record) != 0:
                self.money = self.tmp_trade_record[len(self.tmp_trade_record) - 1]['after_money']

            tmp_handling_fee = self.money * self.commission_percent
            self.handling_fee += tmp_handling_fee
            self.money = self.money - tmp_handling_fee
            stock_number = int(self.money / self.buy_price)
            self.tmp_trade_record.append({
                'test_id': self.test_id,
                'code': code,
                'buy_date': date,
                'sell_date': date,
                'stock_number': stock_number,
                'before_money': self.money,
                'buy_trigger': self.buy_price,
                'cdate': time.strftime('%Y-%m-%d %H:%M:%S'),
                'model_code': self.model_code,
            })

            self.have_status = True
            self.is_just_have = True
            self.is_under_ma = False

    def main_sell(self, ids, code, date, open_p, close, high, low, vol, c_date):
        if high <= self.ma_30_p and self.sell_sign is False:
            self.sell_sign = True
            return

        if self.sell_sign is True:
            # 跌停板板无法卖出
            if low == high:
                return

            self.sell_sign = False

            # 当开盘已经时不再卖点，过均线时卖出
            # 当开盘已经在均线下，卖出价取 开盘和收盘平均价
            sell_price = self.ma_30_p if open_p >= self.ma_30_p else open_p

            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day)
            self.have_status = False
            self.have_day = 0

    def lost_control(self, ids, code, date, open_p, close, high, low, vol, c_date):
        return
        # 计算止损价，判断是否需要止损
        # 2ATR
        # stop_loss_price = self.buy_price - np.mean(tr_list) * 2
        if close < self.stop_loss_price:
            sell_price = self.stop_loss_price if open_p >= self.stop_loss_price else open_p
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day, 2)
            self.have_status = False
            self.have_day = 0

    def main_after(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 收集收盘价
        if len(self.ma_20) >= 20:
            self.ma_20.pop(0)
        # if len(self.ma_30) >= 30:
        #     self.ma_30.pop(0)
        # if len(self.ma_60) >= 60:
        #     self.ma_60.pop(0)

        self.ma_20.append(close)
        # self.ma_30.append(close)
        # self.ma_60.append(close)

    def track_model(self, code, end_date, start_date='1990-01-01'):
        self.run_model = 'track'
        self.main(code, 'track', end_date, start_date)


if __name__ == '__main__':
    # main
    model_code = 'ma-30'
    # 测试单条
    model = ModelMa()
    # ModelMa().track_model('000001', '2019-02-27', '2003-01-01')
    model.main('000001', model_code, '2019-02-27', '2003-01-01')
    exit()

    # 多进程
    model = ModelMa()
    model.multi_main(model_code, '2019-02-27', '2003-01-01')
