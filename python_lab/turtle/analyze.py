#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
读取测试记录，分析结果
"""

__author__ = 'lockheed'

from base import mysql


def generate_echart_data(code, model_code):
    """
    已废弃，在view/kchart.php有可视化界面
    生成可视化数据，用于echart展示买点和卖点。
    https://www.echartsjs.com/examples/editor.html?c=candlestick-sh
    :param code:
    :param model_code: 模型代码
    :return:
    """
    info = mysql.mysql_fetch('select * from src_base_day where code = %s' % code, False)
    string = ''
    for logid, code, date, open, close, high, low, volume, cd in info:
        string += "['%s', %s, %s, %s, %s],\n" % (date, open, close, low, high)

    print(string)

    info = mysql.mysql_fetch(
        'select buy_date, sell_date, buy_trigger,sell_trigger  from rpt_test_detail where code = %s and model_code = "%s"'
        % (code, model_code), False)
    string = ''
    for bd, sd, btr, selltr in info:
        string += "{name: 'XX',coord: ['%s', %s],value: %s,itemStyle: {normal: {color: 'rgb(30,144,255)'}}},\n" % (
            bd, btr, btr)
        string += "{name: 'XX',coord: ['%s', %s],value: %s,itemStyle: {normal: {color: 'rgb(255,99,71)'}}},\n" % (
            sd, selltr, selltr)

    print(string)


def model_report():
    """
    打印模型的统计数据，可复制到excel
    :return:
    """
    data = mysql.mysql_fetch("SELECT model_id, model_code FROM src_model WHERE status = 1", False)
    output = "模型编码\t" \
             "盈利个股占比\t" \
             "平均年化收益率\t" \
             "平均成功率\t" \
             "平均交易次数\t" \
             "平均回撤率\t" \
             "最大回撤率超过20%的数量\t" \
             "最大回撤率超过25%的数量\t" \
             "最大回撤率超过30%的数量\t" \
             "初始资金总和\t" \
             "结束资金总和\t" \
             "结束与初始资金比例\t" \
             "\n"
    for ids, model_code in data:
        output += model_code + "\t"
        # 获取某一计划 盈利个股 占比
        info = mysql.mysql_fetch(
            "select(select count(*) from rpt_test where model_code='" + model_code + "' and profit_year > 0 and `status` = 2) / (select count(*) from rpt_test where model_code='" + model_code + "' and `status` = 2)")
        output += str(round(info[0] * 100, 2)) + "%\t"

        # 平均年化收益率
        info = mysql.mysql_fetch(
            "select avg(`profit_year`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "%\t"

        # 平均成功率
        info = mysql.mysql_fetch(
            "select avg(`success_rate`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0] * 100, 2)) + "%\t"

        # 平均交易次数
        info = mysql.mysql_fetch(
            "select avg(`trade_times`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "\t"

        # 平均回撤率
        info = mysql.mysql_fetch(
            "select avg(`max_retracement`) from rpt_test where model_code='" + model_code + "' and `status` = 2")
        output += str(round(info[0], 2)) + "%\t"
        # 最大回撤率超过20%的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 20 and `status` = 2")
        output += str(info[0]) + "\t"
        # 最大回撤率超过n的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 25 and `status` = 2")
        output += str(info[0]) + "\t"
        # 最大回撤率超过n的个数
        info = mysql.mysql_fetch(
            "select count(*) from rpt_test where model_code='" + model_code + "' and max_retracement > 30 and `status` = 2")
        output += str(info[0]) + "\t"

        # 总和收益
        info = mysql.mysql_fetch(
            "select sum(init_money), sum(final_money) - sum(init_money),(sum(final_money) - sum(init_money)) / sum(init_money) from rpt_test where model_code = '" + model_code + "' and `status` = 2")
        output += str(format(info[0], ',')) + "\t" + str(format(info[1], ',')) + "\t" + str(round(info[2], 2)) + "\n"
    print(output)


model_report()
# generate_echart_data('600118', 'ma-004')
