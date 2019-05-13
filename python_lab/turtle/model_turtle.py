#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测。基于唐齐安
"""

__author__ = 'lockheed'

import time
from decimal import Decimal
from base import sim


class ModelTurtle(sim.Sim):
    # 20日最高价列表
    max_20 = []
    # 20日最高价
    max_20_p = 0
    # 10日最低价
    min_10 = []
    min_10_p = 0

    def main_ready(self, code):
        # 每次运行初始化数值，应对多进程复用model的数据问题
        self.max_20 = []
        self.max_20_p = 0
        self.min_10 = []
        self.min_10_p = 0

    def main_before(self, ids, code, date, open_p, close, high, low, vol, c_date):
        self.max_20_p = max(self.max_20) if len(self.max_20) >= 55 else 999999
        self.min_10_p = min(self.min_10) if len(self.min_10) >= 20 else 1

    def main_buy(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 买入条件
        if high >= self.max_20_p:
            # 涨停板无法买入
            if high == low:
                return

            # TODO 买入价要经常思考是否可以更贴近实际
            self.buy_price = open_p if open_p >= self.max_20_p else self.max_20_p

            # 计算止损价
            self.stop_loss_price = self.buy_price * Decimal(0.98)
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

    def main_sell(self, ids, code, date, open_p, close, high, low, vol, c_date):
        if low <= self.min_10_p:
            # 跌停板板无法卖出
            if low == high:
                return

            # TODO 卖出价要经常思考是否可以更贴近实际
            sell_price = open_p if open_p <= self.min_10_p else self.min_10_p
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day)

    def lost_control(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 计算止损价，判断是否需要止损
        # 2ATR
        self.stop_loss_price = self.buy_price - self.atr_p * 2
        if close < self.stop_loss_price:
            sell_price = self.stop_loss_price if open_p >= self.stop_loss_price else open_p
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day, 2)

    def main_after(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # if len(self.max_20) >= 20:
        #     self.max_20.pop(0)
        # if len(self.min_10) >= 10:
        #     self.min_10.pop(0)
        #
        # self.max_20.append(high)
        # self.min_10.append(low)
        if len(self.max_20) >= 55:
            self.max_20.pop(0)
        if len(self.min_10) >= 20:
            self.min_10.pop(0)

        self.max_20.append(high)
        self.min_10.append(low)

    def track_model(self, code, end_date, start_date='1990-01-01'):
        self.run_model = 'track'
        self.main(code, 'track', end_date, start_date)


if __name__ == '__main__':
    # main
    model_code = 'turtle-5520'
    # 测试单条
    # model = ModelMa()
    # # ModelMa().track_model('000001', '2019-02-27', '2003-01-01')
    # model.main('000001', model_code, '2019-02-27', '2003-01-01')
    # exit()

    # 多进程
    model = ModelTurtle()
    model.multi_main(model_code, '2019-02-27', '2003-01-01')
