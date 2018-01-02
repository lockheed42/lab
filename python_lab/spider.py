#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
   spider
'''

__author__ = 'lockheed'

import pymysql
import pymysql.cursors
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
import os
import redis

'''
记录错误日志
'''


def error_log(e):
    with open("./log/error/error.txt", 'a') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(e) + '\n')


'''判断link是否已经抓取过。抓取过返回 False
'''


def url_is_get(pool, url):
    r = redis.Redis(connection_pool=pool)

    is_get = r.setnx(url, 1)
    return True if is_get == True else False


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
        return url[7:].replace('/', '_')
    else:
        dir = url[7:index + suffix_len]
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


def catch(url, deep=0):
    try:
        global pool
        # 阻止重复抓取
        if url_is_get(pool, url) == False:
            return

        # 基础信息
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        host = get_host(url)

        # 抓取数据
        res_data = urllib.request.urlopen(url)
        res = res_data.read()
        content = res.decode('utf-8')

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

        #TODO 需要优化 ----->
        #插入首页html
        sql_main = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
        sql_main = sql_main + "('" + host[7:] + "','" + url + "', 0, '" + str(deep)  + "','" + time + "')"

        #插入当前html下的子链接
        sql = "INSERT INTO t_html (`domain`,`current_url`, `pid`, `deep`, `cdate`) VALUES "
        for ele in href:
            if ele is not None and ele[:4] == 'http':
                sql = sql + "('" + host[7:] + "','" + url + "','" + str(deep) + "','" + ele + "','" + time + "'),"
        sql = sql.rstrip(',')
        #TODO <------ 优化结束


        # 把html文件写入。存入html路径下
        with open("./html/" + get_file_name(url), 'w') as f:
            f.write(content)

        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='test',
                                     port=3306,
                                     charset='utf8')

        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()

        # 超过挖掘深度后不在继续
        if deep > 2:
            return
        for ele in href:
            # 下一步地址可能省略host。如果检测不到host，把当前host给到url继续抓取。
            next_host = get_host(ele)
            if next_host == '':
                ele = host + ele
            # 继续抓取同一域名下的资源
            if get_host(ele) == host:
                catch(ele, deep + 1)
            else:
                # TODO 非同一域名另外保存根目录
                with open("./log/error/skip.txt", 'a') as f:
                    f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(host) + '  ' + str(ele) + '\n')
    except BaseException as e:
        error_log(e)
        return


url = "http://www.dilidili.wang"

pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool)
r.flushdb()

catch(url)
