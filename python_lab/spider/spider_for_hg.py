#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
   spider
'''

__author__ = 'lockheed'

import requests
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
import os
import redis
import traceback
import time
from multiprocessing import Pool
import multiprocessing
import re


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
    global session
    """
    请求程序，模拟真实用户
    还需要优化
    响应程序，处理相应数据以及模拟处理
    :param url:
    :return:
    """
    response = session.get(url)

    # 有 Set-Cookie响应时保存 TODO 暂时关闭
    # set_cookie = response.headers.get('Set-Cookie')
    # if set_cookie is not None:
    #     with open("./log/" + get_domain(host) + "_cookie.log", 'a') as f:
    #         f.write(set_cookie)

    # 读取内容
    res = response.content
    # TODO 编码可配置
    return res.decode('utf-8'), 0


def catch(url, deep, sub_id):
    """抓取程序"""
    global deep_limit
    global key_path
    try:
        # 降低抓取速度，减少访问压力 TODO 多进程正式抓取时开启
        # time.sleep(0.5)

        # 阻止重复抓取
        if is_url_catch(url) is False:
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
            next_link = link.get('href')
            for url_keyword in key_path:
                # 过滤除了产品以及分类 以外的所有url
                if next_link is not None and next_link.find(url_keyword) != -1:
                    href.append(next_link)

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


def init_redis(host):
    """初始化一个域名下的redis数据"""
    global redis_connect
    redis_connect.flushall()
    redis_connect.delete('free_process:' + host)
    for deep in range(20):
        redis_connect.delete("url_queue:" + host + ":" + str(deep))
    redis_connect.set('url_deep:' + host, 0)
    return


def get_file_info(root, file_url):
    """获取商品快照关键数据"""
    global log_path
    net_url = 'https://www.elegomall.com/product/'
    output = ''
    with open(root + '/' + file_url, 'r') as f:
        content = f.read()
        soup = BeautifulSoup(content, "lxml")
        url = net_url + file_url
        product_name = soup.find(class_='goods_detail_title_name').string

        # 抓取sku数据并整理，同时抓取阶梯价格
        sku_origin = re.search("<script>var skus.*; var minBuyQty", content)
        sku_origin = sku_origin.group().lstrip('<script>var skus = {').rstrip('}; var minBuyQty')
        sku_origin = re.sub('"', '', sku_origin).split(',')
        sku_data = {}
        soup_table = BeautifulSoup(re.sub('<table', '</table><table', content), "lxml")
        step_price = {}
        for i in sku_origin:
            i = i.split(':')
            sku_data[i[0]] = i[1]
            # 抓取阶梯价
            id_key = "group_table_" + i[0]
            table_html = soup_table.find('table', id=id_key)

            tmp_step_price = ''
            for idx, tr in enumerate(table_html.find_all('tr')):
                if idx == 0:
                    continue

                tds = tr.find_all('td')
                tmp_step_price += tds[0].contents[0] + ' -> ' + tds[1].contents[0] + '&&&'

            step_price[i[0]] = tmp_step_price

        # 解析规格、单价、库存
        rule_html = soup.find('table', class_='shoppingcar-table-wrap')
        for idx, tr in enumerate(rule_html.find_all('tr')):
            if idx == 0:
                continue

            rule = tr.find('td', class_='td_first').get_text().strip(', ')
            stock_status = tr.find('td', class_='stock_status').get_text()
            price = tr.find_all('span')[1].get_text().strip('$')
            add_status = tr.find('td', class_='shoppingcar-add-status').get_text()
            sku_id = tr.find('input')['value']

            # 组装数据
            output += product_name + ';' + rule + ';' + sku_data[sku_id] \
                      + ';' + price + ';' + stock_status + ';' + add_status + ';' + step_price[sku_id].strip('&&&') \
                      + ';' + url + '\n'

    with open(log_path + '/result.txt', 'a') as f:
        f.write(output)


def get_single_product_file_info(root, file_url):
    """获取商品只含有一个sku 快照关键数据"""
    global log_path
    net_url = 'https://www.elegomall.com/product/'
    output = ''
    with open(root + '/' + file_url, 'r') as f:
        content = f.read()
        soup = BeautifulSoup(content, "lxml")
        url = net_url + file_url
        product_name = soup.find(class_='goods_detail_title_name').string

        # 抓取sku数据并整理，同时抓取阶梯价格
        soup_table = BeautifulSoup(re.sub('<table', '</table><table', content), "lxml")

        sku = soup.find('span', class_='sku_number').get_text().strip("\n").strip(" ")
        sku_id = soup.find('div', class_='add_cart')['product_id']

        # 阶梯价格
        id_key = "group_table_" + sku_id
        table_html = soup_table.find('table', id=id_key)

        tmp_step_price = ''
        for idx, tr in enumerate(table_html.find_all('tr')):
            if idx == 0:
                continue

            tds = tr.find_all('td')
            tmp_step_price += tds[0].contents[0] + ' -> ' + tds[1].contents[0] + '&&&'

        step_price = tmp_step_price.strip('&&&')

        # 解析规格、单价、库存
        # TODO
        stock_status = soup.find('span', class_='avail_static').get_text()

        price = soup.find('i', class_='new_price').get_text().strip("\n").strip(" ").strip("$")
        # TODO
        add_status = ''

        # 组装数据
        output += product_name + '; ;' + str(sku) \
                  + ';' + str(price) + ';' + str(stock_status) + ';' + str(add_status) + ';' + str(step_price) \
                  + ';' + str(url) + '\n'

    with open(log_path + '/result_single.txt', 'a') as f:
        f.write(output)


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
key_path = ['brands.html', 'starter-kits.html', 'apv-mods.html', 'atomizers.html', 'batteries.html', 'e-liquids.html',
            'accessories.html', 'product']

# # 重置redis缓存
# redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
# redis_connect = redis.Redis(connection_pool=redis_pool)
#
# init_redis(host)
# url_push(host, host, '0')
#
# # 模拟登陆
# headers = {
#     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
#     "X-CSRF-Token": "RnVkc1pkMjEXOCkhNRFwUnc5PAsALAt8IzpQJ3cwVGsgEiVEEAFIdQ==",
#     "Cookie": "age=qualified;_ga=GA1.2.834221224.1517461768;__lc.visitor_id.8854324=S1517461769.107aed3dda;__cfduid=d7e816258e17995ad55310e1b663972dd1529888335;_csrf=c8c098b0a73a3dfa09177fac167febb8b7586274721e8388973315d53f171f38s%3A32%3A%22QMMRouBc1LXxZH9MeO4T-TfZfgA7JezD%22%3B;_gid=GA1.2.1190735462.1543281423;autoinvite_callback=true;autoinvite_callback=true;currencyCode=USD;currency=dd37fdf39f2b0330dfed5e02c5e2c1c80f060e8b1c20b31e2c011150ae0726d6s%3A3%3A%22USD%22%3B;age=qualified;lc_invitation_opened=opened;lc_sso8854324=1543369475826;productHistory=5f137add19678183251c5806b326e1777ac666ff3fe744fa053a26bb943b4010s%3A29%3A%2218803%2C18731%2C17932%2C18550%2C16825%22%3B;lc_window_state=minimized;PHPSESSID=k244seasj5t9s99u8rifveagv3",
#     "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
#     "Referer": "https://www.elegomall.com/"
# }
# data = {
#     "FrontendLoginFrom[email]": "christina@calistondistro.com",
#     "FrontendLoginFrom[password]": "1sexymanhalko",
# }
#
# session = requests.session()
# response = session.post(host + "/site/login.html", data=data, headers=headers)
# # 把html文件写入。存入html路径下
# with open(html_res_path + "/" + get_domain(host) + "/a_login_response.txt", 'w') as f:
#     f.write(response.content.decode("utf-8"))
#
# # 轮询抓取
# while True:
#     time.sleep(0.5)
#     url, deep = url_pop(host)
#     if url is None:
#         exit()
#     catch(url, deep, 99)


# 解析数据
# if os.path.exists(log_path + '/result.txt'):
#     os.remove(log_path + '/result.txt')
#
# file_url = html_res_path + '/' + get_domain(host) + '/product'
# for root, dirs, files in os.walk(file_url):
#     for file_obj in files:
#         try:
#             print(file_obj)
#             get_file_info(root, file_obj)
#         except BaseException as e:
#             with open(error_log_path + "/result_error.log", 'a') as f:
#                 f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(file_obj) + '  ' + str(
#                     traceback.format_exc()) + '\n')
#             continue


# 读取error_logs获取单个sku，并重新抓取
filter_list = []
with open(error_log_path + "/result_error.log", 'r') as f:
    content = f.read()
    rs = re.findall(" \s.*\.html", content)
    counter = 0
    for i in rs:
        counter += 1
        filter_list.append(i.lstrip(' '))

if os.path.exists(log_path + '/result_single.txt'):
    os.remove(log_path + '/result_single.txt')

file_url = html_res_path + '/' + get_domain(host) + '/product'
for root, dirs, files in os.walk(file_url):
    for file_obj in files:
            try:
                print(file_obj)
                get_single_product_file_info(root, file_obj)
            except BaseException as e:
                with open(error_log_path + "/result_single_error.log", 'a') as f:
                    f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(file_obj) + '  ' + str(
                        traceback.format_exc()) + '\n')
                continue
