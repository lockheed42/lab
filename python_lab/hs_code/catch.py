#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
   spider
'''

__author__ = 'lockheed'

import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
import os
import redis
import traceback
import time
from multiprocessing import Pool
import multiprocessing
from base import mysql
import json


class HsCatch:
    """
    抓取程序
    """
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
    redis_connect = redis.Redis(connection_pool=redis_pool)

    main_params = ''
    log_path = ''
    error_log_path = ''

    # redis内空闲进程 和 队列的key name
    redis_free_process_key = ''
    redis_queue_key = ''

    def __init__(self, year, month, in_type, currency):
        """
        :param year:
        :param month:
        :param in_type:
        :param currency:
        """
        root = '/Library/WebServer/Documents/code/lab/python_lab/hs_code'
        # 日志文件保存地址
        self.log_path = root + '/log'
        self.error_log_path = root + '/log/error'

        self.main_params = year + '-' + month + '-' + in_type + '-' + currency
        self.redis_free_process_key = 'free_process:' + self.main_params
        self.redis_queue_key = 'url_queue:' + self.main_params
        # 初始化 url队列
        self.init_redis()

    def error_log(self, sub_id, params):
        """
        记录错误日志
        :param sub_id:
        :param params:
        :return:
        """
        with open(self.error_log_path + "/error.log", 'a') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(sub_id) + '  ' + str(params) + '  ' + str(
                traceback.format_exc()) + '\n')

    def log(self, file, content):
        """其他日志"""
        with open(self.log_path + "/" + file + ".log", 'a') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')

    def init_redis(self):
        """初始化一组参数配置下的redis数据"""
        self.redis_connect.flushall()
        self.redis_connect.delete(self.redis_free_process_key)
        self.redis_connect.delete(self.redis_queue_key)
        return

    def url_pop(self):
        """
        从队列获取要抓取的page参数
        :return:
        """
        return self.redis_connect.rpop(self.redis_queue_key)

    def url_push(self, param):
        """
        推入url等待抓取队列
        :param param: 页码，页大小
        :return:
        """
        self.redis_connect.lpush(self.redis_queue_key, param)

    def request_parse(self, url):
        """
        请求程序，模拟真实用户
        还需要优化
        响应程序，处理相应数据以及模拟处理
        :param url:
        :return:
        """
        # TODO header可配置
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
        accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
        accept_language = "zh-CN,zh;q=0.9,en;q=0.8"
        connection = "keep-alive"
        cache_control = "max-age=0"
        headers = {'Accept': accept, 'Accept-Language': accept_language,
                   'Connection': connection, 'User-Agent': user_agent, 'Cache-Control': cache_control}
        req = urllib.request.Request(url, headers=headers)
        response = urllib.request.urlopen(req)

        # 检测是否分块发送
        # content_length = 0
        # transfer_encoding = response.headers.get('Transfer-Encoding')
        # if transfer_encoding != 'chunked':
        #     # 根据length来决定是否抓取，上限20M
        #     content_length = response.headers.get('content-length')
        #     if int(content_length) > 10485760:
        #         log('too_large', url + '  返回大小: ' + str(content_length))
        #         return '', 0

        # 读取内容
        res = response.read()
        # TODO 编码可配置
        return res.decode('utf-8')

    def catch(self, page, size, sub_id):
        """
        抓取程序
        :param page:
        :param size:
        :param sub_id:
        :return:
        """
        try:
            main_p = self.main_params.split('-')
            year = main_p[0]
            month = main_p[1]
            in_type = main_p[2]
            currency = main_p[3]
            currency_code = 'rmb' if currency == '1' else 'usd'
            in_type_code = '1' if in_type == '1' else '0'

            url = 'http://43.248.49.97/queryData/getQueryDataListByWhere?pageNum=' + page + '&pageSize=' + size + \
                  '&iEType=' + in_type_code + '&currencyType=' + currency_code + '&year=' + year + '&startMonth=' + month + \
                  '&endMonth=' + month + '&monthFlag=&codeTsFlag=true&codeLength=8&outerField1=CODE_TS' + \
                  '&outerField2=ORIGIN_COUNTRY&outerField3=TRADE_MODE&outerField4=TRADE_CO_PORT&outerValue1=' + \
                  '&outerValue2=&outerValue3=&outerValue4=&orderType=CODE+ASC&historyTable=true'
            content = self.request_parse(url)
            soup = BeautifulSoup(content, 'html.parser')

            col_counter = 0
            row_counter = 0
            data = []
            row_data = {}
            '''
            c    = code            商品编码
            n    = name            商品名称
            fc   = friend code     贸易伙伴编码
            fn   = friend name     贸易伙伴名称
            tc   = trade type code 贸易方式编码
            tn   = trade type name 贸易方式名称
            ac   = area code       注册地编码
            an   = area name       注册地名称
            fnum = first number    第一数量
            fu   = first unit      第一计量单位
            snum = first number    第二数量
            su   = first unit      第二计量单位
            m    = money           金额
            '''
            # 列抬头
            title_list = ['c', 'n', 'fc', 'fn', 'tc', 'tn', 'ac', 'an', 'fnum', 'fu', 'snum', 'su', 'm']
            div_list = soup("div", class_="th-line")
            div_len = len(div_list)
            for tag in div_list:
                if col_counter == len(title_list) or col_counter == div_len / (row_counter + 1) - 1:
                    data.append(row_data)
                    row_data = {}

                    row_counter += 1
                    col_counter = 0

                # 去除千分位
                if title_list[col_counter] == 'm':
                    tag.string = tag.string.replace(',', '')
                if title_list[col_counter] == 'n':
                    row_data[title_list[col_counter]] = tag.get('title')
                else:
                    row_data[title_list[col_counter]] = tag.string
                col_counter += 1

            new_id = mysql.mysql_insert(
                "INSERT INTO spider_catch "
                "(`year`, `month`, `type`, `currency`, `page`, `size`, `data`, `counter`, `status`, `ctime`) "
                "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                % (
                    year, month, in_type, currency, page, size, json.dumps(data, ensure_ascii=False), str(row_counter),
                    '1',
                    time.time()))

            return True
        except BaseException as e:
            self.error_log(sub_id, self.main_params + '-' + page + '-' + size)
            mysql.mysql_insert(
                "INSERT INTO spider_catch "
                "(`year`, `month`, `type`, `currency`, `page`, `size`, `data`, `counter`, `status`, `ctime`) "
                "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                % (year, month, in_type, currency, page, size, '', '0', '2', time.time()))
            return True

    def sub_process(self, pipe, sub_id):
        """爬取子进程"""
        # 为每次子进程提供一个连接池
        redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
        redis_connect = redis.Redis(connection_pool=redis_pool)

        is_continue = True
        while is_continue:
            message = pipe.recv()
            self.log('pipe_recv', str(sub_id) + '  ' + message)
            # 收到结束信号中止循环
            if message == '---end---':
                is_continue = False
            else:
                page_params = message.split('-')
                page = page_params[0]
                size = page_params[1]

                self.catch(page, size, sub_id)
                redis_connect.lpush(self.redis_free_process_key, str(sub_id))

    def count_and_queue(self, size):
        """
        收集总数据量 并 添加任务队列
        :param size:
        :return:
        """
        main_p = self.main_params.split('-')
        year = main_p[0]
        month = main_p[1]
        in_type = main_p[2]
        currency = main_p[3]
        currency_code = 'rmb' if currency == '1' else 'usd'
        in_type_code = '1' if in_type == '1' else '0'

        url = 'http://43.248.49.97/queryData/getQueryDataListByWhere?pageNum=1&pageSize=10' + \
              '&iEType=' + in_type_code + '&currencyType=' + currency_code + '&year=' + year + '&startMonth=' + month + \
              '&endMonth=' + month + '&monthFlag=&codeTsFlag=true&codeLength=8&outerField1=CODE_TS' + \
              '&outerField2=ORIGIN_COUNTRY&outerField3=TRADE_MODE&outerField4=TRADE_CO_PORT&outerValue1=' + \
              '&outerValue2=&outerValue3=&outerValue4=&orderType=CODE+ASC&historyTable=true'
        content = self.request_parse(url)

        soup = BeautifulSoup(content, 'html.parser')
        total = 0
        for tag in soup("p", class_='c-666'):
            if tag.span is not None:
                total = int(tag.span.string)
        if total == 0:
            raise Exception('无法确定总数据量')

        page = int(total / int(size)) + 1
        for i in range(1, page + 1):
            self.url_push(str(i) + '-' + str(size))

    def main(self):
        """
        main
        :return:
        """
        # 进程数
        process_num = 2

        # 多工作进程运行
        process_pool = Pool(process_num)
        pipe_pool = {}
        for i in range(process_num):
            # 重要，延长每个pipe创建的间隔
            time.sleep(0.5)
            pipe_pool[i] = multiprocessing.Pipe()
            process_pool.apply_async(self.sub_process, args=(pipe_pool[i][1], i,))
            # 设置空闲进程队列
            self.redis_connect.lpush(self.redis_free_process_key, i)

        # 抓取根目录
        page_size = 20000
        self.count_and_queue(page_size)

        # 主进程逻辑
        while True:
            sub_id = self.redis_connect.brpop(self.redis_free_process_key)[1]
            page_params = self.url_pop()
            print(page_params)
            if page_params is None:
                # 任务全部做完，发送信号给子程序，并等待子进程结束
                for j in range(process_num):
                    pipe_pool[j][0].send('---end---')

                process_pool.close()
                process_pool.join()
                print('All url done.')
                exit()

            pipe_pool[int(sub_id)][0].send(page_params)


'''
main
http://43.248.49.97/queryData/getQueryDataListByWhere?pageNum=1&pageSize=10&iEType=1&currencyType=rmb&year=2017&startMonth=1&endMonth=1&monthFlag=&codeTsFlag=true&codeLength=8&outerField1=CODE_TS&outerField2=ORIGIN_COUNTRY&outerField3=TRADE_MODE&outerField4=TRADE_CO_PORT&outerValue1=&outerValue2=&outerValue3=&outerValue4=&orderType=CODE+ASC&historyTable=true
'''

if __name__ == '__main__':
    year = '2017'
    month = '1'
    in_type = '1'
    currency = '1'
    model = HsCatch(year, month, in_type, currency)
    # model.main('2017', '1', '1', '1')
    # model.catch('1', '200', 1)

    # sql = "select data from spider_catch where id =11;"
    # rs = mysql.mysql_fetch(sql)
    # rs = json.loads(rs[0])
    # for i in rs:
    #     print(i)
    #     exit()
        # print(i['n'])
