#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
历史回测通用模块
"""

__author__ = 'lockheed'

import math
import time
from decimal import Decimal
import traceback
from multiprocessing import Pool
import multiprocessing
from base import mysql
from base import redis as base_redis


class Sim:
    # 日志目录
    log_path = '/Library/WebServer/Documents/code/lab/python_lab/turtle/log'
    # 运行模式。sim=回测，track=跟踪
    run_model = 'sim'

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
    # 最后一条收盘价，动态变动，用于防止结束时还未卖出。
    last_close = 0
    # 最后一条交易日期，动态变动，用于防止结束时还未卖出。
    last_date = ''
    # 临时记录中途买卖数据
    tmp_trade_record = []
    # 测试计划ID
    test_id = 0
    # 回测模型代码
    model_code = ''
    # 买入价格
    buy_price = 0

    # TR池，记录每日波动值，池子大小取决于ATR需要的天数
    tr_pool = []
    # ATR
    atr_p = 0

    # 是否启用分布加仓
    is_exec_step = True
    # 分布加仓数据
    step_buy = []
    # 持续买入状态。当出发了买入后，按照逐步加仓原则持续买入
    continue_buy = False
    # 加仓时买入次数
    continue_buy_max = 5

    # 券商佣金，万3
    commission_percent = Decimal.from_float(0.0003)
    # 印花税，千1
    tax_percent = Decimal.from_float(0.001)
    # 手续费总和
    handling_fee = 0

    def main(self, code, model_code, end_date, start_date='1990-01-01'):
        """
        回测主程序
        :param code:
        :param model_code:
        :param end_date:
        :param start_date:
        :return:
        """
        try:
            # 每次运行初始化数值，应对多进程复用model的数据问题
            self.last_test_detail_id = 0
            self.have_status = False
            self.money = 1000000
            self.have_day = 0
            self.max_draw_down = 0
            self.draw_down_day = 0
            self.max_draw_down_day = 0
            self.interval_highest = 0
            self.is_just_have = False
            self.stop_loss_price = 0
            self.test_id = 0
            self.tmp_trade_record = []
            self.last_close = 0
            self.last_date = ''
            self.handling_fee = 0
            self.buy_price = 0
            self.tr_pool = []
            self.atr_p = 0

            self.model_code = model_code
            sql = "SELECT * FROM src_base_day WHERE code = '%s' and `date` >= '%s' and `date`<='%s'" \
                  % (code, start_date, end_date)
            res = mysql.mysql_fetch(sql, False)

            # 创建计划
            if self.run_model == 'sim':
                self.test_id = mysql.mysql_insert(
                    "INSERT INTO rpt_test (`code`, `init_money`, `start_date`, `end_date`, `cdate`, `model_code`) "
                    "VALUES ('%s', %s, '%s', '%s', '%s', '%s')"
                    % (code, self.money, start_date, end_date, time.strftime('%Y-%m-%d %H:%M:%S'), model_code))
            elif self.run_model == 'track':
                self.test_id = 0
            else:
                raise BaseException('run_model错误')

            # 交易准备模块
            self.main_ready(code)

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

                # 交易前模块
                self.main_before(ids, code, date, open_p, close, high, low, vol, c_date)
                # 买入模块
                if self.have_status is False:
                    self.main_buy(ids, code, date, open_p, close, high, low, vol, c_date)

                # 逐步加仓
                if self.have_status is True and self.continue_buy is True:
                    self.step_buy_action(open_p, close, high, low, vol, c_date)

                # 卖出策略
                if self.have_status is True and self.is_just_have is False:
                    self.main_sell(ids, code, date, open_p, close, high, low, vol, c_date)

                # 止损策略
                if self.have_status is True and self.is_just_have is False:
                    self.lost_control(ids, code, date, open_p, close, high, low, vol, c_date)

                # 计算atr
                if len(self.tr_pool) >= 20:
                    self.tr_pool.pop(0)
                self.tr_pool.append(max(high - low, abs(self.last_close - low), abs(self.last_close - high)))
                self.atr_p = sum(self.tr_pool) / len(self.tr_pool)

                self.main_after(ids, code, date, open_p, close, high, low, vol, c_date)
                self.last_close = close
                self.last_date = date

            # 防止结束时还未卖出。
            if self.have_status is True:
                self.sell(self.last_close, self.last_date, self.have_day, self.max_draw_down, self.max_draw_down_day)

            # 未交易跳过
            if len(self.tmp_trade_record) == 0:
                return

            if self.run_model == 'sim':
                # 一次性插入所有交易明细
                sql = "INSERT INTO rpt_test_detail (`model_code`, `test_id`, `code`, `sell_type`, `have_day`, `profit_rate`" \
                      ", `max_retracement`, `retracement_day`, `buy_date`, `sell_date`, `before_money`, `after_money`" \
                      ", `buy_trigger`, `sell_trigger`, `stock_number`, `status`, `cdate`, `buy_times`) VALUES "
                for d in self.tmp_trade_record:
                    sql += "('%s', %s, '%s', %s, %s, %.2f, %.2f, %s, '%s', '%s', %.2f, %.2f, %.3f, %.3f, %s, '%s', '%s', %s)," \
                           % (d['model_code'], d['test_id'], d['code'], d['sell_type'], d['have_day'], d['profit_rate'],
                              d['max_retracement'], d['retracement_day'], d['buy_date'], d['sell_date'],
                              d['before_money'],
                              d['after_money'], d['buy_trigger'], d['sell_trigger'], d['stock_number'], d['status'],
                              d['cdate'], str(d['buy_times']))

                sql = sql.rstrip(',')
                mysql.mysql_insert(sql)
                # 统计回测结果
                self.calc_model_plan(self.test_id)
            elif self.run_model == 'track':
                if self.is_just_have is True:
                    sql = "select code, name from src_stock where code = %s" % code
                    res = mysql.mysql_fetch(sql)
                    print(res[0], res[1])
            else:
                raise BaseException('run_model错误')
        except BaseException as e:
            print(traceback.format_exc())
            self.log('sim_model', str(self.test_id) + '|' + str(traceback.format_exc()))

    def buy(self, code, date):
        """
        买入行为
        :param code:
        :param date:
        :return:
        """
        # 获取上次卖出后的现金
        if len(self.tmp_trade_record) != 0:
            self.money = self.tmp_trade_record[len(self.tmp_trade_record) - 1]['after_money']

        if self.is_exec_step is True:
            # 计算分仓买入的价格，每次买入的股数
            self.continue_buy = True
            self.step_buy = []
            start_price = self.buy_price
            end_price = self.buy_price + self.atr_p * 2
            stock_per_buy = int(
                self.money / ((self.buy_price + self.atr_p) * (1 + self.commission_percent)) / self.continue_buy_max)
            step_price = (end_price - start_price) / (self.continue_buy_max - 1)
            for i in range(self.continue_buy_max):
                self.step_buy.append({
                    'price': start_price + step_price * i,
                    'stock': stock_per_buy,
                    'is_done': False if i != 0 else True
                })

            # TODO 下面的参数还没改
            stock_number = stock_per_buy
            tmp_handling_fee = self.buy_price * stock_per_buy * self.commission_percent
            self.handling_fee += tmp_handling_fee
            self.money = self.money - tmp_handling_fee
        else:
            stock_number = int(self.money / (self.buy_price * (1 + self.commission_percent)))
            tmp_handling_fee = self.buy_price * stock_number * self.commission_percent
            self.handling_fee += tmp_handling_fee
            self.money = self.money - tmp_handling_fee

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
            'buy_times': 1,
        })
        self.have_status = True
        self.is_just_have = True

    def step_buy_action(self, open_p, close, high, low, vol, c_date):
        """
        执行分布买入，并修改tmp_trade_record相关数据。
        思路，遍历预先计算好分仓买入数组，根据每条记录的状态和价格判断是否需要买入
        :return:
        """
        # 最后一次交易明细
        last_i = len(self.tmp_trade_record) - 1
        last_record = self.tmp_trade_record[last_i]
        # 分布买入计数，如果次数用完则修改 继续买入的状态
        step_counter = 0
        for i, ele in enumerate(self.step_buy):
            # 已买入计划跳过
            if ele['is_done'] is True:
                step_counter += 1
                continue
            if low <= ele['price'] <= high:
                new_total_money = last_record['buy_trigger'] * last_record['stock_number'] + ele['price'] * ele['stock']
                new_total_stock = last_record['stock_number'] + ele['stock']
                last_record['buy_trigger'] = new_total_money / new_total_stock
                last_record['stock_number'] += ele['stock']
                last_record['buy_times'] += 1
                step_counter += 1
                self.step_buy[i]['is_done'] = True
                # 设置新的全局买入价格
                # self.buy_price = last_record['buy_trigger']

        if step_counter == self.continue_buy_max:
            self.continue_buy = False
        self.tmp_trade_record[last_i] = last_record

    def sell(self, sell_price, date, have_day, max_draw_down, max_draw_down_day, sell_type=1):
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
        tmp = self.tmp_trade_record[len(self.tmp_trade_record) - 1]
        before_money = tmp['before_money']
        stock_number = tmp['stock_number']
        buy_trigger = tmp['buy_trigger']

        # 未买入现金
        rest_money = before_money - stock_number * buy_trigger
        # 买入部分变化
        after_money = rest_money + sell_price * stock_number
        tmp_handling_fee = sell_price * stock_number * (self.commission_percent + self.tax_percent)
        self.handling_fee += tmp_handling_fee
        after_money = after_money - tmp_handling_fee
        # 收益率
        profit_rate = (after_money - before_money) / before_money

        # 修改当次交易数据
        tmp['have_day'] = have_day
        tmp['profit_rate'] = round(profit_rate * 100, 2)
        tmp['sell_date'] = date
        tmp['after_money'] = round(after_money, 2)
        tmp['sell_trigger'] = round(sell_price, 3)
        tmp['max_retracement'] = round(max_draw_down * 100, 2)
        tmp['retracement_day'] = max_draw_down_day
        tmp['sell_type'] = sell_type
        tmp['status'] = 2
        self.tmp_trade_record[len(self.tmp_trade_record) - 1] = tmp

        self.have_status = False
        self.have_day = 0

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
            profit_year = (pow(final_money / start_money,
                               Decimal.from_float(1 / have_year)) - 1) * 100 if have_year > 0 else 0
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

            # 止损占总交易百分比
            lost_control_times = mysql.mysql_fetch(
                "SELECT count(*) FROM rpt_test_detail WHERE test_id = %s AND `sell_type`=2 " % plan_info[0])
            lost_control_times = lost_control_times[0] / trade_times * 100

            # 手续费占比
            hanlding_fee_percent = self.handling_fee / final_money * 100

            sql_update = "UPDATE rpt_test SET `final_money`=%.2f, `profit_year`=%.2f, `max_retracement`=%.2f, `retracement_day`=%s, " \
                         "`trade_times`=%s, `success_rate`=%.2f, `start_date`='%s', `end_date`='%s',`status`=2, `handling_fee`=%s, " \
                         "`lost_percent`=%.2f, `fee_percent`=%.2f WHERE `test_id`=%s" \
                         % (final_money, profit_year, max_retracement, retracement_day, trade_times, success_rate,
                            first_detail[1], last_detail[1], self.handling_fee, lost_control_times,
                            hanlding_fee_percent, plan_info[0])
            mysql.mysql_insert(sql_update)
        except BaseException as e:
            self.log('sim_model', str(plan_info[0]) + '|' + str(traceback.format_exc()))

    def log(self, file, content):
        """其他日志"""
        with open(self.log_path + "/" + file + ".log", 'a') as f:
            f.write(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')

    def sub_process(self, pipe, sub_id):
        """
        子进程
        :param sub_id:
        :return:
        """
        # 为每次子进程提供一个连接池
        redis = base_redis.Redis()

        is_continue = True
        while is_continue:
            message = pipe.recv()
            # self.log('pipe_recv', str(sub_id) + '  ' + message)
            # 收到结束信号中止循环
            if message == '---end---':
                is_continue = False

            message_array = message.split('------')
            code = message_array[0]
            model_code = message_array[1]
            end_date = message_array[2]
            start_date = message_array[3]

            # TODO
            self.main(code, model_code, end_date, start_date)
            redis.handle.lpush(redis.free_process_key_prefix, str(sub_id))

    def multi_main(self, model_code, end_date, start_date='1990-01-01'):
        """
        多进程使用的主程序
        :param model_code:
        :param end_date:
        :param start_date:
        :return:
        """
        # 进程数。考虑CPU核数
        process_num = 4

        # 引入redis，并初始化
        redis = base_redis.Redis()
        redis.init(model_code, end_date, start_date)

        # 多工作进程运行
        process_pool = Pool(process_num)
        pipe_pool = {}
        for i in range(process_num):
            pipe_pool[i] = multiprocessing.Pipe()
            process_pool.apply_async(self.sub_process, args=(pipe_pool[i][1], i,))
            # 设置空闲进程队列
            redis.handle.lpush(redis.free_process_key_prefix, i)
            # 重要，延长每个pipe创建的间隔
            time.sleep(0.5)

        # 主进程逻辑
        while True:
            sub_id = redis.handle.brpop(redis.free_process_key_prefix)[1]
            mission_data = redis.handle.rpop(redis.mission_queue_key)
            if mission_data is None:
                # 任务全部做完，发送信号给子程序，并等待子进程结束
                for j in range(process_num):
                    pipe_pool[j][0].send('---end---')

                process_pool.close()
                process_pool.join()
                print('All done!!!')
                exit()

            # self.log('pipe_send', mission_data)
            pipe_pool[int(sub_id)][0].send(mission_data)
