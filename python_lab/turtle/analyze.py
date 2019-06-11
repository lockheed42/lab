#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
读取测试记录，分析结果
"""

__author__ = 'lockheed'

from base import mysql


def model_report():
    """
    打印模型的统计数据，可复制到excel
    :return:
    """
    data = mysql.mysql_fetch("SELECT model_id, model_code FROM src_model WHERE status = 1", False)
    output = "模型编码 " \
             "盈利个股占比 " \
             "平均年化收益率 " \
             "平均成功率 " \
             "平均交易次数 " \
             "平均止损占比 " \
             "平均手续费占比 " \
             "平均回撤率 " \
             "最大回撤率超过20%的数量 " \
             "最大回撤率超过25%的数量 " \
             "最大回撤率超过30%的数量 " \
             "初始资金总和 " \
             "结束资金总和 " \
             "手续费总和 " \
             "结束与初始资金比例 " \
             "\n"
    for ids, model_code in data:
        output += model_code + " "
        # 获取某一计划 盈利个股 占比
        info = mysql.mysql_fetch(
            "select(select count(*) from rpt_test where model_code='" + model_code + "' and profit_year > 0 and `status` = 2) / (select count(*) from rpt_test where model_code='" + model_code + "' and `status` = 2)")
        output += str(round(info[0] * 100, 2)) + "% "

        # 平均年化收益率
        # 现在为所有测试个股的年华收益求平均
        info = mysql.mysql_fetch(
            "select avg(`profit_year`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "% "

        # 平均成功率
        info = mysql.mysql_fetch(
            "select avg(`success_rate`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0] * 100, 2)) + "% "

        # 平均交易次数
        info = mysql.mysql_fetch(
            "select avg(`trade_times`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + " "

        # 平均止损占比
        info = mysql.mysql_fetch(
            "select avg(`lost_percent`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "% "

        # 平均手续费占比
        info = mysql.mysql_fetch(
            "select avg(`fee_percent`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "% "

        # 平均回撤率
        info = mysql.mysql_fetch(
            "select avg(`max_retracement`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "% "
        # 最大回撤率超过20%的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 20 and `status` = 2")
        output += str(info[0]) + " "
        # 最大回撤率超过n的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 25 and `status` = 2")
        output += str(info[0]) + " "
        # 最大回撤率超过n的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 30 and `status` = 2")
        output += str(info[0]) + " "
        # 总和收益
        info = mysql.mysql_fetch(
            "select sum(init_money), sum(final_money), sum(handling_fee), sum(final_money) / sum(init_money) from rpt_test where model_code = '" + model_code + "' and `status` = 2")
        output += str(format(info[0], ',')) + " " + str(format(info[1], ',')) + " " + str(
            format(info[2], ',')) + " " + str(round(info[3], 2)) + "\n"
    print(output)


model_report()
# generate_echart_data('600118', 'ma-004')
