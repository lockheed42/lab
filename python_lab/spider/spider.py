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
import time
from multiprocessing import Pool
import multiprocessing


def error_log(sub_id):
    """记录错误日志"""
    global host
    global error_log_path
    # TODO 日志与html保存路径可配置
    with open(error_log_path + "/" + get_domain(host) + "_error.log", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(sub_id) + '  ' + str(
            traceback.format_exc()) + '\n')


def log(file, content):
    """其他日志"""
    global host
    global log_path
    with open(log_path + "/" + get_domain(host) + "_" + file + ".log", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(content) + '\n')


def save_html(url, content):
    """
    html源文件保存
    获取html要写入的文件路径，不存在则会创建
    :param url:
    :param content:
    :return:
    """
    global html_res_path
    url = get_domain(url)
    url = url.strip('/')
    index = -1
    suffix_len = 0
    for suffix in get_domain_suffix():
        index = url.find(suffix)
        suffix_len = len(suffix)
        if index != -1:
            break

    # 匹配不到域名后缀，直接保存
    if index == -1:
        file = url.replace('/', '_')
    else:
        root_path = url[:index + suffix_len]
        if not os.path.exists(html_res_path + "/" + root_path):
            os.mkdir(html_res_path + "/" + root_path)
        dir_path = url[index + suffix_len + 1:]
        # 根目录
        if dir_path == '':
            dir_path = 'index.html'

        # 迭代创建目录
        dir_path_array = dir_path.split('/')
        tmp_dir = html_res_path + "/" + root_path
        for dir_block in dir_path_array:
            if dir_block == dir_path_array[-1]:
                # 添加文件后缀，防止文件与目录重名
                if dir_block.find('.') == -1:
                    dir_path = dir_path + '.html'
                continue

            if not os.path.isdir(tmp_dir + '/' + dir_block):
                os.mkdir(tmp_dir + '/' + dir_block)

            tmp_dir = tmp_dir + '/' + dir_block

        file = root_path + '/' + dir_path

    # 把html文件写入。存入html路径下
    with open(html_res_path + "/" + file, 'w') as f:
        f.write(content)


def set_url_catch(url):
    """判断link是否已经抓取过。抓取过返回 False"""
    global redis_connect
    is_get = redis_connect.set("catch_url:" + url, 1)
    return True if is_get else False


def is_url_catch(url):
    """判断url是否被抓去过。抓取过返回 False"""
    global redis_connect
    rs = redis_connect.get("catch_url:" + url)
    return False if rs else True


def url_push(host, url, deep):
    """
    推入url等待抓取队列
    key结构：url_queue:{host}:{deep} = url:param host:
    :param url:
    :param deep: 
    :return:
    """
    global redis_connect
    queue_key = "url_queue:" + host + ":" + str(deep)
    redis_connect.lpush(queue_key, url)


def url_pop(host):
    """
    从队列获取要抓取的url
    辅助key，表达当前哪个深度的队列有数据待处理
    url_deep:{host} = deep
    如果当前队列pop为None，辅助key+1，表示下次从更深的队列获取数据
    返回值：url, deep

    :param host:
    :return:
    """
    global redis_connect
    url_deep_key = "url_deep:" + host
    deep = redis_connect.get(url_deep_key)
    queue_key = "url_queue:" + host + ":" + deep
    pop_url = redis_connect.rpop(queue_key)
    # 如果当前深度队列 已取完，从下一级深度读取
    if pop_url is None:
        deep = int(deep) + 1
        redis_connect.set(url_deep_key, deep)
        queue_key = "url_queue:" + host + ":" + str(deep)
        pop_url = redis_connect.rpop(queue_key)

    return pop_url, int(deep)


def get_domain(url):
    """获取 url 的 domain部分"""
    return url.strip('/').lstrip('https://')


def get_domain_suffix():
    """获取域名后缀"""
    return ['.com', '.cn', '.net', '.wang', '.tv', '.org', '.cc']


def get_host(url):
    """获取主域名，包括http部分的前缀"""
    url = url.strip(" ").strip("\n")
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


def request_parse(url):
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
    content_length = 0
    transfer_encoding = response.headers.get('Transfer-Encoding')
    if transfer_encoding != 'chunked':
        # 根据length来决定是否抓取，上限20M
        content_length = response.headers.get('content-length')
        if int(content_length) > 10485760:
            log('too_large', url + '  返回大小: ' + str(content_length))
            return '', 0

    # 有 Set-Cookie响应时保存 TODO 暂时关闭
    # set_cookie = response.headers.get('Set-Cookie')
    # if set_cookie is not None:
    #     with open("./log/" + get_domain(host) + "_cookie.log", 'a') as f:
    #         f.write(set_cookie)

    # 读取内容
    res = response.read()
    # TODO 编码可配置
    return res.decode('utf-8'), content_length


'''
插入数据库，暂时搁置
'''


# def insert():
# connection = pymysql.connect(host='localhost',
#                              user='root',
#                              password='',
#                              db='test',
#                              port=3306,
#                              charset='utf8')
#
# if deep == 0:
#     # 插入首页html
#     pid = 0
#     sql_main = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
#     sql_main = sql_main + "('" + get_domain(host) + "','" + url + "'," + str(pid) + ", '" + str(
#         deep) + "','" + ctime + "')"
#
#     with connection.cursor() as cursor:
#         cursor.execute(sql_main)
#         connection.commit()
# else:
#     # 查询当前url的id
#     sql_main = "select * from t_html where `current_url` = '" + url + "'"
#     with connection.cursor() as cursor:
#         cursor.execute(sql_main)
#         data = cursor.fetchone()
#         connection.commit()
#     if data is not None:
#         pid = data[0]
#     else:
#         pid = -1
#         log('skip', '  在数据库被未插入  ' + str(url))
#
# # 插入当前html下的子链接
# sql = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
# for ele in href:
#     if ele is not None and ele[:4] == 'http':
#         sql = sql + "('" + get_domain(host) + "','" + ele + "','" + str(pid) + "','" + str(
#             deep + 1) + "','" + ctime + "'),"
# sql = sql.rstrip(',')
#
# with connection.cursor() as cursor:
#     cursor.execute(sql)
#     connection.commit()


def catch(url, deep, sub_id):
    """抓取程序"""
    global deep_limit
    try:
        # 降低抓取速度，减少访问压力 TODO 多进程正式抓取时开启
        # time.sleep(0.5)

        # 阻止重复抓取
        if is_url_catch(url) == False:
            return True

        # 抓取数据
        content, content_length = request_parse(url)
        real_length = len(content)

        # 把html文件写入。存入html路径下
        save_html(url, content)
        set_url_catch(url)
        # 记录当前正在抓取的url
        log('run',
            str(sub_id) + '  ' + url + '  ' + str(content_length) + 'byte  ' + str(real_length) + 'byte  ' + str(deep))

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
                # 阻止已抓取的页面丢入队列
                if is_url_catch(ele) is False:
                    continue
                url_push(host, ele, deep + 1)
            else:
                # TODO 非同一域名另外保存根目录
                log('skip', str(sub_id) + '  ' + '  非同一域名  ' + str(host) + '  ' + str(ele))
        return True
    except BaseException as e:
        error_log(sub_id)
        return True


def sub_process(pipe, sub_id):
    """爬取子进程"""
    # 为每次子进程提供一个连接池
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
    redis_connect = redis.Redis(connection_pool=redis_pool)

    is_continue = True
    while is_continue:
        message = pipe.recv()
        log('pipe_recv', str(sub_id) + '  ' + message)
        # 收到结束信号中止循环
        if message == '---end---':
            is_continue = False

        message_array = message.split('------')
        url = message_array[0]
        deep = int(message_array[1])

        catch(url, deep, sub_id)
        redis_connect.lpush('free_process:' + host, str(sub_id))


def do_cmd(cmd):
    """执行接收的命令"""
    print(cmd)
    switcher = {
        "stop": end_program,
    }
    return switcher.get(cmd, '')() if callable(switcher.get(cmd, '')) else print('命令错误')


def end_program():
    """结束程序，发送信号给子程序，并等待子进程结束"""
    global process_num
    global pipe_pool
    global process_pool
    for j in range(process_num):
        pipe_pool[j][0].send('---end---')

    process_pool.close()
    process_pool.join()
    print('All url done.')
    exit()


def init_redis(host):
    """初始化一个域名下的redis数据"""
    global redis_connect
    redis_connect.flushall()
    redis_connect.delete('free_process:' + host)
    for deep in range(20):
        redis_connect.delete("url_queue:" + host + ":" + str(deep))
    redis_connect.set('url_deep:' + host, 0)
    return


'''
main
'''
# 抓取深度限制
deep_limit = 3
# 进程数
process_num = 10
# 下载的html资源文件保存地址
html_res_path = '/Library/WebServer/Documents/code/spider_res/html'
# 日志文件保存地址
log_path = '/Library/WebServer/Documents/code/spider_res/log'
error_log_path = '/Library/WebServer/Documents/code/spider_res/log/error'
host = 'https://www.elegomall.com'
cmd_file = './cmd.txt'

# 重置redis缓存
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
redis_connect = redis.Redis(connection_pool=redis_pool)

# 初始化 url队列
init_redis(host)
url_push(host, host, '0')

# 多工作进程运行
process_pool = Pool(process_num)
pipe_pool = {}
for i in range(process_num):
    # 重要，延长每个pipe创建的间隔
    time.sleep(0.5)
    pipe_pool[i] = multiprocessing.Pipe()
    process_pool.apply_async(sub_process, args=(pipe_pool[i][1], i,))
    # 设置空闲进程队列
    redis_connect.lpush('free_process:' + host, i)

# 抓取根目录
url, deep = url_pop(host)
pipe_pool[0][0].send(url + '------' + str(deep))
redis_connect.brpop('free_process:' + host)
time.sleep(10)

# 主进程逻辑
while True:
    if os.path.exists(cmd_file):
        with open(cmd_file, 'r') as f:
            cmd = f.readline().strip()
            do_cmd(cmd)

        os.remove(cmd_file)

    sub_id = redis_connect.brpop('free_process:' + host)[1]
    url, deep = url_pop(host)
    print(url, deep)
    if url is None:
        # 任务全部做完，发送信号给子程序，并等待子进程结束
        for j in range(process_num):
            pipe_pool[j][0].send('---end---')

        process_pool.close()
        process_pool.join()
        print('All url done.')
        exit()

    log('pipe_send', url + '------' + str(deep))
    pipe_pool[int(sub_id)][0].send(url.strip("\n") + '------' + str(deep))

'''
临时方案，异常中止之后的继续采集
'''
# # 抓取深度限制
# deep_limit = 10
# # 进程数
# process_num = 10
# # 下载的html资源文件保存地址
# html_res_path = '/Library/WebServer/Documents/code/spider_res/html'
# # 日志文件保存地址
# log_path = '/Library/WebServer/Documents/code/spider_res/log'
# error_log_path = '/Library/WebServer/Documents/code/spider_res/log/error'
#
# host = "https://www.elegomall.com"
# cmd_file = './cmd.txt'
#
# # 重置redis缓存
# redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
# redis_connect = redis.Redis(connection_pool=redis_pool)
# redis_connect.delete('free_process:' + host)
#
# # 多工作进程运行
# process_pool = Pool(process_num)
# pipe_pool = {}
# for i in range(process_num):
#     # 重要，延长每个pipe创建的间隔
#     time.sleep(0.5)
#     pipe_pool[i] = multiprocessing.Pipe()
#     process_pool.apply_async(sub_process, args=(pipe_pool[i][1], i,))
#     # 设置空闲进程队列
#     redis_connect.lpush('free_process:' + host, i)
#
# # 主进程逻辑
# while True:
#     # 接收新命令执行
#     if os.path.exists(cmd_file):
#         with open(cmd_file, 'r') as f:
#             cmd = f.readline().strip()
#             do_cmd(cmd)
#         os.remove(cmd_file)
#
#     sub_id = redis_connect.brpop('free_process:' + host)[1]
#     url, deep = url_pop(host)
#     if url is None:
#         end_program()
#
#     log('pipe_send', url + '------' + str(deep))
#     pipe_pool[int(sub_id)][0].send(url.strip("\n") + '------' + str(deep))
