#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
   spider
'''

__author__ = 'lockheed'

import urllib.request
from bs4 import BeautifulSoup
import pymysql
import pymysql.cursors
from datetime import datetime
import os
import redis
import traceback

'''
记录错误日志
'''


def error_log(e):
    with open("./log/error/error.log", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(traceback.format_exc()) + '\n')


'''
判断link是否已经抓取过。抓取过返回 False
'''


def url_is_get(url):
    global pool
    r = redis.Redis(connection_pool=pool)

    is_get = r.setnx(url, 1)
    return True if is_get else False


'''
推入url等待抓取队列
key结构：url_queue:{host}:{deep} = url
'''


def url_push(host, url, deep):
    global pool
    r = redis.Redis(connection_pool=pool)
    queue_key = "url_queue:" + host + ":" + deep
    r.lpush(queue_key, url)


'''
从队列获取要抓取的url
辅助key，表达当前哪个深度的队列有数据待处理
    url_deep:{host} = deep
如果当前队列pop为None，辅助key+1，表示下次从更深的队列获取数据
返回值：url, deep
'''


def url_pop(host):
    global pool
    r = redis.Redis(connection_pool=pool)
    url_deep_key = "url_deep:" + host
    deep = r.get(url_deep_key)
    queue_key = "url_queue:" + host + ":" + deep
    pop_url = r.rpop(queue_key)
    # 如果当前深度队列 已取完，从下一级深度读取
    if pop_url is None:
        deep = deep + 1
        r.set(url_deep_key, deep)
        queue_key = "url_queue:" + host + ":" + deep
        pop_url = r.rpop(queue_key)
    return pop_url, deep


'''
获取http前缀的长度值+1，用于截取
'''


def get_http_prefix_len(url):
    if url[:7] == 'https://':
        return 8
    else:
        return 7


'''
获取域名后缀
'''


def get_domain_suffix():
    return ['.com', '.cn', '.net', '.wang', '.tv', '.org']


'''
获取主域名，包括http部分的前缀
'''


def get_host(url):
    index = -1
    suffix_len = 0
    for suffix in get_domain_suffix():
        index = url.find(suffix)
        suffix_len = len(suffix)
        if index != -1:
            break
    # 获取不到后缀返回空
    if index == -1:
        return ''
    else:
        return url[:index + suffix_len]


'''
获取html要写入的文件路径，不存在则会创建
'''


def get_file_name(url):
    index = -1
    suffix_len = 0
    for suffix in get_domain_suffix():
        index = url.find(suffix)
        suffix_len = len(suffix)
        if index != -1:
            break

    # 匹配不到域名后缀，直接保存
    if index == -1:
        return url.replace('/', '_')
    else:
        dir = url[:index + suffix_len]
        dir = dir.replace('/', '_')
        if not os.path.exists("./html/" + dir):
            os.mkdir("./html/" + dir)
        if url[index + suffix_len + 1:] == '':
            file_name = 'index.html'
        else:
            file_name = url[index + suffix_len + 1:].replace('/', '_')

        return dir + '/' + file_name


'''
主程序
'''


def catch(host):
    global deep_limit
    try:
        # 基础信息
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        url, deep = url_pop(host)
        if url is None:
            return

        # TODO 这里有问题，pop是一次一条url，中断之后整个程序就中断了
        # 阻止重复抓取
        if url_is_get(url) == False:
            return

        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='test',
                                     port=3306,
                                     charset='utf8')

        # 抓取数据
        res_data = urllib.request.urlopen(url)
        res = res_data.read()
        content = res.decode('utf-8')

        # 把html文件写入。存入html路径下
        with open("./html/" + get_file_name(url), 'w') as f:
            f.write(content)

        # 链接列表
        href = []
        soup = BeautifulSoup(content, 'html.parser')
        for link in soup.find_all('a'):
            href.append(link.get('href'))

        # 抓取不到链接时中断
        if not href:
            return
        # 去重
        href = set(href)

        if deep == 0:
            # 插入首页html
            pid = 0
            sql_main = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
            sql_main = sql_main + "('" + host[get_http_prefix_len(host):] + "','" + url + "'," + str(pid) + ", '" + str(
                deep) + "','" + time + "')"

            with connection.cursor() as cursor:
                cursor.execute(sql_main)
                connection.commit()
        else:
            # 查询当前url的id
            sql_main = "select * from t_html where `current_url` = '" + url + "'"
            with connection.cursor() as cursor:
                cursor.execute(sql_main)
                data = cursor.fetchone()
                connection.commit()
            if data is not None:
                pid = data[0]
            else:
                pid = -1
                with open("./log/error/skip.log", 'a') as f:
                    f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  在数据库被未插入  ' + str(url) + '\n')

        # 插入当前html下的子链接
        sql = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
        for ele in href:
            if ele is not None and ele[:4] == 'http':
                sql = sql + "('" + host[get_http_prefix_len(host):] + "','" + ele + "','" + str(pid) + "','" + str(
                    deep) + "','" + time + "'),"
        sql = sql.rstrip(',')

        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()

        # 超过挖掘深度后不在继续
        if deep > deep_limit:
            return
        for ele in href:
            # 下一步url 为None时跳过
            if ele is None:
                continue
            # 下一步地址可能省略host。如果检测不到host，把当前host给到url继续抓取。
            next_host = get_host(ele)
            if next_host == '':
                ele = host + ele
            # 继续抓取同一域名下的资源
            if get_host(ele) == host:
                catch(ele, deep + 1)
            else:
                # TODO 非同一域名另外保存根目录
                with open("./log/error/skip.log", 'a') as f:
                    f.write(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  非同一域名  ' + str(host) + '  ' + str(ele) + '\n')
    except BaseException as e:
        error_log(e)
        return


# 抓取深度限制
deep_limit = 2
# 根域名
url = "http://m.yue-me.com/pages/instro/company.html"
# 重置redis缓存
pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool)
r.flushdb()
# 初始化 url队列
r.set('url_deep:' + url, 0)
url_push(url, url, 0)

catch(url)
