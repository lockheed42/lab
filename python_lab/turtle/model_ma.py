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

    # MACD相关参数
    diff_list = []
    # DEA或者叫DEM
    dea = 0
    # 12与26平滑移动之差
    diff = 0
    last_ema12 = 0
    last_ema26 = 0
    last_dea = 0

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

        self.diff_list = []
        self.last_ema12 = 0
        self.last_ema26 = 0
        self.dea = 0
        self.last_dea = 0
        self.diff = 0

    def main_before(self, ids, code, date, open_p, close, high, low, vol, c_date):
        self.ma_10_p = self.get_ma(self.ma_10) if len(self.ma_10) == 10 else 99999
        self.ma_20_p = self.get_ma(self.ma_20) if len(self.ma_20) == 20 else 99999
        self.ma_30_p = self.get_ma(self.ma_30) if len(self.ma_30) == 30 else 99999
        self.ma_60_p = self.get_ma(self.ma_60) if len(self.ma_60) == 60 else 99999

        if self.have_status is False and self.ma_60_p > high:
            self.is_under_ma = True

        # MACD相关参数
        if len(self.diff_list) == 0:
            self.diff_list.append(0)
            ema12 = 0
            ema26 = 0
            self.dea = 0
            self.diff = 0
        elif len(self.diff_list) == 1:
            ema12 = self.last_close * 11 / 13 + close * 2 / 13
            ema26 = self.last_close * 25 / 27 + close * 2 / 27
            self.diff = round(ema12 - ema26, 2)
            self.dea = round(0 + self.diff * 2 / 10, 2)
            self.diff_list.append(self.diff)
        else:
            ema12 = self.last_ema12 * 11 / 13 + close * 2 / 13
            ema26 = self.last_ema26 * 25 / 27 + close * 2 / 27
            self.diff = round(ema12 - ema26, 2)
            self.dea = round(self.last_dea * 8 / 10 + self.diff * 2 / 10, 2)

        self.last_dea = Decimal(self.dea)
        self.last_ema12 = Decimal(ema12)
        self.last_ema26 = Decimal(ema26)

    def main_buy(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # TODO 买入价要经常思考是否可以更贴近实际
        # ma-6020-filter-ma20-adjust
        if (self.ma_60_p <= high and self.is_under_ma is True
            and (self.ma_20_p * Decimal(1.05) <= high) and self.diff > self.dea) \
                or (self.is_under_ma is False and self.ma_20_p * Decimal(1.05) <= high and self.diff > self.dea):
            # 涨停板无法买入
            if high == low:
                return

            self.buy_sign = False

            if self.ma_60_p <= high and self.is_under_ma is True and (self.ma_20_p * Decimal(1.05) <= high):
                if self.ma_20_p * Decimal(1.05) > self.ma_60_p:
                    self.buy_price = self.ma_20_p * Decimal(1.05)
                else:
                    self.buy_price = self.ma_60_p if open_p <= self.ma_60_p else open_p
            else:
                self.buy_price = self.ma_20_p * Decimal(1.05) if open_p <= self.ma_20_p * Decimal(1.05) else open_p

            self.buy(code, date)
            # 有时候价格已经到了均线之上，但其他条件不满足买入，将延迟is_under_ma改为false为买入后
            self.is_under_ma = False

    def main_sell(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # TODO 卖出价要经常思考是否可以更贴近实际
        # ma-6020-filter-ma20-adjust
        if (self.ma_20_p >= high) or (self.ma_60_p >= high):
            # 跌停板板无法卖出
            if low == high:
                return

            sell_price = close
            self.sell(sell_price, date, self.have_day, self.max_draw_down, self.max_draw_down_day)

    def lost_control(self, ids, code, date, open_p, close, high, low, vol, c_date):
        # 计算止损价，判断是否需要止损
        # 2ATR
        return
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
    model_code = 'ma-6020-macd-300cyb'
    # 测试单条
    model = ModelMa()
    model.main('000001', model_code, '2019-02-27', '2003-01-01')
    exit()

    # 多进程
    model = ModelMa()
    model.multi_main(model_code, '2019-02-27', '2003-01-01')
