#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""骰宝模拟"""
__author__ = 'lockheed'

import random
import math


def one_roll():
    """
    投一次
    返回点数 或者False，代表豹子通吃
    """
    point = 0
    pool = set([])
    for i in range(0, 3):
        num = random.randint(1, 6)
        point += num
        pool.add(num)

    if len(pool) == 1:
        return False
    else:
        return point


def chance():
    """各点数（包括豹子）的概率模拟"""
    a = {}
    count = 1000000
    for i in range(1, count):
        rs = one_roll()
        if rs in a:
            a[rs] += 1
        else:
            a[rs] = 1

    new_a = sorted(a.items(), key=lambda x: x[0])
    for key, val in new_a:
        print(key, "\t", val / count)


fail_counter = 0
success_counter = 0

n = 7  # n次后覆盖本金，等比求和简化 total = min_bet(2^n - 1)
init_money = 25400  # 初始金额
init_min_bet = init_money / (math.pow(2, n) - 1)  # 最小下注金额
print('总金额：%s' % init_money)
print('最小下注：%s' % init_min_bet)
if init_min_bet - math.floor(init_min_bet) != 0:
    print('最小下注不是整数')
    exit()

for round_c in range(1, 5000):
    win_counter = 0  # 赢的次数
    total = init_money  # 计算用总金额
    min_bet = init_min_bet  # 最小下注金额，计算用
    count = 2000  # 次数
    current_bet = min_bet  # 当前下注金额

    for i in range(1, count):
        # 输赢判断，全堵大
        is_win = False
        point = one_roll()
        if point >= 11:
            is_win = True

        # 输赢金额结算
        if is_win is True:
            total += current_bet
            current_bet = min_bet
            win_counter += 1
        else:
            total -= current_bet
            current_bet *= 2

        # 总金额无法支持下次下注，破产
        if total < current_bet:
            print('输光,第%s轮，余额%s，最小下注金额%s，最后一轮下注%s。盈利次数%s' % (i, total, min_bet, current_bet, win_counter))
            fail_counter += 1
            break

        # 赚回本金，可以收回本金来继续
        if total > init_money * 2:
            print('赚回本金,第%s轮，余额%s，最小下注金额%s，最后一轮下注%s。盈利次数%s' % (i, total, min_bet, current_bet, win_counter))
            success_counter += 1
            break

        # 当总金额允许后，提高最小下注
        if (min_bet + 100) * (math.pow(2, n) - 1) < total and is_win is True:
            min_bet += 100

        if i >= count:
            print("结束，余额%s" % total)

print('成功次数：%s' % success_counter)
print('破产次数：%s' % fail_counter)
