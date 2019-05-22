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
    # 收盘价集合
    ma_10 = []
    ma_20 = []
    ma_30 = []
    ma_60 = []
    # 移动平均价
    ma_10_p = 0
    ma_20_p = 0
    ma_30_p = 0
    ma_60_p = 0
    # 记录K先是否在均线之下
    is_under_ma = False

    # 买入信号。创建时是因为买入点发现后的第二天操作，用于完全穿越K线
    buy_sign = False
    # 卖出信号。
    sell_sign = False
    # 是否启用分布加仓
    is_exec_step = False

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
        self.ma_10 = []
        self.ma_20 = []
        self.ma_30 = []
        self.ma_60 = []
        self.ma_10_p = 0
        self.ma_20_p = 0
        self.ma_30_p = 0
        self.ma_60_p = 0
        self.is_under_ma = False
        self.buy_sign = False
        self.sell_sign = False

    def main_before(self, ids, code, date, open_p, close, high, low, vol, c_date):
        self.ma_10_p = self.get_ma(self.ma_10) if len(self.ma_10) == 10 else 99999
        self.ma_20_p = self.get_ma(self.ma_20) if len(self.ma_20) == 20 else 99999
        self.ma_30_p = self.get_ma(self.ma_30) if len(self.ma_30) == 30 else 99999
        self.ma_60_p = self.get_ma(self.ma_60) if len(self.ma_60) == 60 else 99999

        if self.have_status is False and self.ma_60_p > high:
            self.is_under_ma = True

    def main_buy(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 买入条件
        # 买入设限
        # if (self.ma_60_p <= high and self.is_under_ma is True and (self.ma_20_p * Decimal(1.05) <= high)) \
        #         or (self.is_under_ma is False and self.ma_20_p * Decimal(1.05) <= high):
        #     # 涨停板无法买入
        #     if high == low:
        #         return
        #
        #     self.buy_sign = False
        #
        #     # TODO 买入价要经常思考是否可以更贴近实际
        #     if self.ma_60_p <= high and self.is_under_ma is True and (self.ma_20_p * Decimal(1.05) <= high):
        #         if self.ma_20_p * Decimal(1.05) > self.ma_60_p:
        #             self.buy_price = self.ma_20_p * Decimal(1.05)
        #         else:
        #             self.buy_price = self.ma_60_p if open_p <= self.ma_60_p else open_p
        #     else:
        #         self.buy_price = self.ma_20_p * Decimal(1.05) if open_p <= self.ma_20_p * Decimal(1.05) else open_p
        #
        #     self.buy(code, date)
        #     # 有时候价格已经到了均线之上，但其他条件不满足买入，将延迟is_under_ma改为false为买入后
        #     self.is_under_ma = False

        if (self.ma_60_p <= high and self.is_under_ma is True) or (self.is_under_ma is False and self.ma_20_p <= high):
            # 涨停板无法买入
            if high == low:
                return

            # TODO 买入价要经常思考是否可以更贴近实际
            if self.ma_60_p <= high and self.is_under_ma is True:
                self.buy_price = self.ma_60_p if open_p <= self.ma_60_p else open_p
            else:
                self.buy_price = self.ma_20_p if open_p <= self.ma_20_p else open_p

            self.buy(code, date)
            # 有时候价格已经到了均线之上，但其他条件不满足买入，将延迟is_under_ma改为false为买入后
            self.is_under_ma = False

    def main_sell(self, ids, code, date, open_p, close, high, low, vol, c_date):
        if (self.ma_20_p >= high or self.ma_20_p * Decimal(0.95) >= low) \
                or (self.ma_60_p >= high or self.ma_60_p * Decimal(0.95) >= low):
            # 跌停板板无法卖出
            if low == high:
                return

            # 当开盘已经时不再卖点，过均线时卖出
            # 当开盘已经在均线下，直接卖出
            # TODO 卖出价要经常思考是否可以更贴近实际
            if self.ma_20_p >= high or self.ma_20_p * Decimal(0.95) >= low:
                sell_low_limit = self.ma_20_p * Decimal(0.95) if close >= self.ma_20_p * Decimal(0.95) else close
                sell_price = sell_low_limit if open_p >= sell_low_limit else open_p
            else:
                sell_low_limit = self.ma_60_p * Decimal(0.95) if close >= self.ma_60_p * Decimal(0.95) else close
                sell_price = sell_low_limit if open_p >= sell_low_limit else open_p
                self.is_under_ma = True

            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day)

    def lost_control(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 计算止损价，判断是否需要止损
        # 2ATR
        self.stop_loss_price = self.buy_price - self.atr_p * Decimal(1.5)
        if close < self.stop_loss_price:
            sell_price = self.stop_loss_price if open_p >= self.stop_loss_price else open_p
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day, 2)

    def main_after(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 收集收盘价
        if len(self.ma_20) >= 20:
            self.ma_20.pop(0)
        if len(self.ma_30) >= 30:
            self.ma_30.pop(0)
        if len(self.ma_60) >= 60:
            self.ma_60.pop(0)

        self.ma_20.append(close)
        self.ma_30.append(close)
        self.ma_60.append(close)

    def track_model(self, code, end_date, start_date='1990-01-01'):
        self.run_model = 'track'
        self.main(code, 'track', end_date, start_date)


if __name__ == '__main__':
    # main
    model_code = 'ma-6020-filter-ma20-adjust-4'
    # 测试单条
    # model = ModelMa()
    # # ModelMa().track_model('002709', '2019-02-27', '2003-01-01')
    # model.main('000001', model_code, '2019-02-27', '2003-01-01')
    # exit()

    # 多进程
    model = ModelMa()
    model.multi_main(model_code, '2019-02-27', '2003-01-01')
