#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'最大回撤计算测试'

__author__ = 'lockheed'

import time
import random
import numpy as np


def max_draw_down(arr):
    """最原始，双层for，仅用作对比基数"""
    drawdown_max = 0
    for i in range(len(arr)):
        for j in range(i, len(arr)):
            drawdown = (arr[i] - arr[j]) / arr[i]
            drawdown_max = max(drawdown_max, drawdown)
    return drawdown_max


def max_draw_down_max(arr):
    """使用max来获取之前最高值，再用max获取 回撤列表内 最大回撤"""
    drawdown_max = 0
    for i in range(len(arr)):
        drawdown = 1 - arr[i] / max(arr[:i + 1])
        drawdown_max = max(drawdown, drawdown_max)
    return drawdown_max


def max_draw_down_np(arr):
    """使用numpy"""
    i = np.argmax((np.maximum.accumulate(arr) - arr) / np.maximum.accumulate(arr))  # end of the period
    j = np.argmax(arr[:i])  # start of period
    return 1 - arr[i] / arr[j]


def get_max_drawdown_fast(array):
    """动态规划"""
    drawdowns = []
    max_so_far = array[0]
    for i in range(len(array)):
        if array[i] > max_so_far:
            max_so_far = array[i]
        else:
            drawdowns.append(1 - array[i] / max_so_far)
    return max(drawdowns)


a = []
for i in range(4000):
    a.append(random.randint(2000, 5000))

time1 = time.time()
res1 = max_draw_down(a)
time2 = time.time()

res2 = max_draw_down_np(a)
time3 = time.time()

res3 = max_draw_down_max(a)
time4 = time.time()

res4 = get_max_drawdown_fast(a)
time5 = time.time()

print(time2 - time1)
print(time3 - time2)
print(time4 - time2)
print(time5 - time4)

"""
四种方式耗时：
2.8647189140319824
0.003380298614501953
0.1840512752532959
0.001161813735961914
"""

print(res1, res2, res3, res4)
