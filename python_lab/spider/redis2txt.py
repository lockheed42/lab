# !/usr/bin/env python3
# --*-- coding:utf-8 --*--

__author__ = 'lockheed'

"""用于将redis内的【url去重数据(非布隆)】与【爬取队列】的数据转存到txt，或者从txt内恢复数据到redis"""

import os
import redis


def get_domain(url):
    return url.strip('/').strip('https://').strip('http://')


def redis2txt(host):
    global txt_res
    global redis_connect
    # TODO 导出到txt后，显示导出结果与处理条数，确认后才删除redis内的key

    # 保存爬取深度
    _save_deep(host)

    # 已爬取列表
    catch_url_key = 'catch_url:' + host

    # 待爬取列表
    _save_queue(host)

    return

def _save_catch(host):
    """保存过滤器"""
    return

def _save_deep(host):
    """保存当前url的抓取深度"""
    global txt_res
    global redis_connect
    deep_key = 'url_deep:' + host
    deep_val = redis_connect.get(deep_key)
    with open(txt_res + '/' + get_domain(host) + '/' + deep_key.replace('/', '') + '.txt', 'a') as f:
        f.write(deep_val)
    redis_connect.delete(deep_key)
    print('抓取深度已保存！')

def _save_queue(host):
    """把所有待抓取队列保存为txt"""
    global txt_res
    global redis_connect
    # 待爬取列表
    # redis pipe一次获取多少条数据
    limit = 100000
    deep_limit = 10
    for deep in range(1, deep_limit):
        url_queue_key = 'url_queue:' + host + ':' + str(deep)
        # 用pipeline批量pop出队列内的数据，根据limit值计算出分块的数量 与 最后一个块的大小
        # 获取队列的长度，并计算出需要调用几次pipe，以及余数
        count = redis_connect.llen(url_queue_key)
        # 块数量
        loop_num = count // limit
        # 最后一个块的大小
        remainder = count % limit

        print('深度:' + str(deep))
        print('块大小:' + str(loop_num))
        print('剩余部分:' + str(remainder))
        print('\n')

        tmp_list = []
        for loop in range(loop_num + 1):
            pipe = redis_connect.pipeline(transaction=True)
            if loop == loop_num:
                if remainder == 0:
                    continue
                for i in range(remainder):
                    pipe.rpop(url_queue_key)
                rs = pipe.execute()
            else:
                for i in range(limit):
                    pipe.rpop(url_queue_key)
                rs = pipe.execute()

            tmp_list.extend(rs)

            string = '\n'.join(tmp_list)
            if string != '':
                print('loop:' + str(loop))
                with open(txt_res + '/' + get_domain(host) + '/' + url_queue_key.replace('/', '##') + '.txt', 'a') as f:
                    f.write(string)

# TODO
def txt2redis(host):
    """读取txt恢复到redis"""
    return


txt_res = '/Library/WebServer/Documents/code/spider_res/redis2txt'
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
redis_connect = redis.Redis(connection_pool=redis_pool)
host = 'http://www.biqugezw.com'

# 创建目录
if not os.path.exists(txt_res + '/' + get_domain(host)):
    os.mkdir(txt_res + '/' + get_domain(host))

for i in range(50):
    # 设置空闲进程队列
    redis_connect.lpush('free_process:' + host, '1')