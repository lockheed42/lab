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
import time as time_machine

'''
记录错误日志
'''


def error_log(e):
    global host
    with open("./log/error/" + get_domain(host) +"_error.log", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(traceback.format_exc()) + '\n')


'''
其他日志
'''


def log(file, content):
    with open("./log/" + get_domain(host) +"_" + file + ".log", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')


'''
html源文件保存
获取html要写入的文件路径，不存在则会创建
'''


def save_html(url):
    index = -1
    suffix_len = 0
    for suffix in get_domain_suffix():
        index = url.find(suffix)
        suffix_len = len(suffix)
        if index != -1:
            break

    # 匹配不到域名后缀，直接保存
    if index == -1:
        content = url.replace('/', '_')
    else:
        dir = url[:index + suffix_len]
        dir = dir.replace('/', '_')
        if not os.path.exists("./html/" + dir):
            os.mkdir("./html/" + dir)
        if url[index + suffix_len + 1:] == '':
            file_name = 'index.html'
        else:
            file_name = url[index + suffix_len + 1:].replace('/', '_')

        content = dir + '/' + file_name

    # 把html文件写入。存入html路径下
    with open("./html/" + content, 'w') as f:
        f.write(content)


'''
判断link是否已经抓取过。抓取过返回 False
'''


def url_is_get(url):
    global pool
    r = redis.Redis(connection_pool=pool)

    is_get = r.setnx("catch_url:" + url, 1)
    return True if is_get else False


'''
推入url等待抓取队列
key结构：url_queue:{host}:{deep} = url
'''


def url_push(host, url, deep):
    global pool
    r = redis.Redis(connection_pool=pool)
    queue_key = "url_queue:" + host + ":" + str(deep)
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
    deep = deep.decode('utf-8')
    queue_key = "url_queue:" + host + ":" + deep
    pop_url = r.rpop(queue_key)
    # 如果当前深度队列 已取完，从下一级深度读取
    if pop_url is None:
        deep = int(deep) + 1
        r.set(url_deep_key, deep)
        queue_key = "url_queue:" + host + ":" + str(deep)
        pop_url = r.rpop(queue_key)

    if pop_url is not None:
        pop_url = pop_url.decode('utf-8')

    return pop_url, int(deep)


'''
获取 url 的 domain部分
'''


def get_domain(url):
    return url.strip('/').strip('https://').strip('http://')


'''
获取域名后缀
'''


def get_domain_suffix():
    return ['.com', '.cn', '.net', '.wang', '.tv', '.org', '.cc']


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
请求程序，模拟真实用户
还需要优化
'''


def request_parse(url):
    # TODO header可配置
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
    accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
    accept_encoding = "gzip, deflate"
    accept_language = "zh-CN,zh;q=0.9,en;q=0.8"
    connection = "keep-alive"
    cache_control = "max-age=0"
    headers = {'Accept': accept, 'Accept-Encoding': accept_encoding, 'Accept-Language': accept_language,
               'Connection': connection, 'User-Agent': user_agent, 'Cache-Control': cache_control}
    req = urllib.request.Request(url, headers=headers)
    return urllib.request.urlopen(req)

'''
响应程序，处理相应数据以及模拟处理
'''

def response_parse(response):
    # 检测是否分块发送
    transfer_encoding = response.headers.get('Transfer-Encoding')
    if transfer_encoding != 'chunked':
        # 根据length来决定是否抓取，上限20M
        content_length = response.headers.get('content-length')
        if int(content_length) > 10485760:
            log('too_large', url + '  返回大小: ' + str(content_length))
            return True

    # 有 Set-Cookie响应时保存
    set_cookie = response.headers.get('Set-Cookie')
    if set_cookie is not None:
        with open("./log/" + get_domain(host) +"_cookie.log", 'a') as f:
            f.write(set_cookie)


    # 读取内容
    res = response.read()
    print(res)
    # TODO 编码可配置
    return res.decode('utf-8')


'''
主程序
'''


def catch(host):
    global deep_limit
    try:
        # 基础信息
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        url, deep = url_pop(host)

        # 降低抓取速度，减少访问压力 TODO 多进程正式抓取时开启
        # time_machine.sleep(0.5)

        if url is None:
            return False

        # 阻止重复抓取
        if url_is_get(url) == False:
            return True

        # 抓取数据
        response = request_parse(url)
        content = response_parse(response)
        real_length = len(content)

        # 记录当前正在抓取的url
        log('run', url + '  ' + str(content_length) + 'byte  ' + str(real_length) + 'byte  ' + str(deep))
        # 把html文件写入。存入html路径下
        save_html(url)

        # 链接列表
        href = []
        soup = BeautifulSoup(content, 'html.parser')
        for link in soup.find_all('a'):
            href.append(link.get('href'))

        # 抓取不到链接时中断
        if not href:
            return True
        # 去重
        href = set(href)

        # 插入新任务
        # 超过挖掘深度后不在存入子url
        if deep > deep_limit - 1:
            return True
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
                url_push(host, ele, deep + 1)
            else:
                # TODO 非同一域名另外保存根目录
                log('skip', '  非同一域名  ' + str(host) + '  ' + str(ele))

        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='test',
                                     port=3306,
                                     charset='utf8')

        if deep == 0:
            # 插入首页html
            pid = 0
            sql_main = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
            sql_main = sql_main + "('" + get_domain(host) + "','" + url + "'," + str(pid) + ", '" + str(
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
                log('skip', '  在数据库被未插入  ' + str(url))

        # 插入当前html下的子链接
        sql = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
        for ele in href:
            if ele is not None and ele[:4] == 'http':
                sql = sql + "('" + get_domain(host) + "','" + ele + "','" + str(pid) + "','" + str(
                    deep + 1) + "','" + time + "'),"
        sql = sql.rstrip(',')

        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
        return True
    except BaseException as e:
        error_log(e)
        return False


# 抓取深度限制
deep_limit = 0
# 根域名
host = "http://www.heiyan.com/"
# 重置redis缓存
pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool)

# 初始化 url队列
# r.flushdb()
# r.set('url_deep:' + host, 0)
# url_push(host, host, 0)

is_continue = True
while is_continue:
    is_continue = catch(host)
