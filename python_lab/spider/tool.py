# !/usr/bin/env python3
# --*-- coding:utf-8 --*--

__author__ = 'lockheed'

"""爬虫业务相关工具"""

import os
import redis


def _get_domain(url):
    return url.strip('/').lstrip('https://')


def redis2txt():
    """把内存的内容迁移到磁盘上"""
    global host
    global txt_res
    global redis_connect
    # TODO 导出到txt后，显示导出结果与处理条数，确认后才删除redis内的key
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
    redis_connect = redis.Redis(connection_pool=redis_pool)

    if not os.path.exists(txt_res + '/' + _get_domain(host)):
        os.mkdir(txt_res + '/' + _get_domain(host))

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
    with open(txt_res + '/' + _get_domain(host) + '/' + deep_key.replace('/', '') + '.txt', 'a') as f:
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
                with open(txt_res + '/' + _get_domain(host) + '/' + url_queue_key.replace('/', '##') + '.txt',
                          'a') as f:
                    f.write(string)


def txt2redis(host):
    """读取txt恢复到redis"""
    return


def analysis_subprocess():
    """分析子进程的使用率"""
    return


def count_html(current_path=None):
    """计算一个host下的所有html文件数量"""
    global html_res_path
    global host
    global count

    print(current_path)
    # 初始化
    if current_path is None:
        count = 0
        current_path = html_res_path + '/' + _get_domain(host)

    file_list = os.listdir(current_path)
    for ele in file_list:
        if os.path.isdir(current_path + '/' + ele) is True:
            count_html(current_path + '/' + ele)
        else:
            count = count + 1

    return


def skip_log_set():
    """不同域名日志去重处理"""
    global host
    global log_path

    count_dict = {}
    count = 0
    path = log_path + '/' + _get_domain(host) + '_skip.log'
    with open(path, 'r') as f:
        for content in f:
            print(count)
            count = count + 1
            url = content[content.find('  ', content.find('http') + 4) + 2:].strip('\n')
            if url in count_dict.keys():
                count_dict[url] = count_dict[url] + 1
            else:
                count_dict[url] = 1

    with open(log_path + '/' + _get_domain(host) + '_skip_set.log', 'a') as f:
        for key, val in count_dict.items():
            f.write(key + ':' + str(val) + '\n')


def parse_error_log():
    """统计异常出现频次"""
    global host
    global error_log_path

    temp_line = ''
    error_dict = {}
    with open(error_log_path + '/' + _get_domain(host) + '_error.log') as f:
        for line in f:
            line = line.strip()
            if line == '':
                if temp_line in error_dict.keys():
                    error_dict[temp_line] = error_dict[temp_line] + 1
                else:
                    error_dict[temp_line] = 1
            temp_line = line

    with open(error_log_path + '/' + _get_domain(host) + '_error_set.log', 'a') as f:
        for key, val in error_dict.items():
            f.write(key + ':' + str(val) + '\n')


def send_cmd():
    """向采集主程序发送命令"""
    global cmd_file
    cmd = input('plz input cmd:')
    with open(cmd_file, 'w') as f:
        f.write(cmd)


def main():
    """主程序"""
    global host
    host = input('请输入域名:')
    cmd = input("redis2txt: 将redis内存的数据写入磁盘\n"
                "count_html: 计算一个host下的所有html文件数量\n"
                "skip_log_set: 不同域名日志去重处理\n"
                "send_cmd: 向采集主程序发送命令\n"
                "parse_error_log: 统计异常出现频次\n"
                "plz choice:")
    switcher = {
        'redis2txt': redis2txt,
        'count_html': count_html,
        'skip_log_set': skip_log_set,
        'send_cmd': send_cmd,
        'parse_error_log': parse_error_log,
    }

    switcher.get(cmd, '')() if callable(switcher.get(cmd, '')) else print('命令错误')
    exit('done')


txt_res = '/Library/WebServer/Documents/code/spider_res/redis2txt'
html_res_path = '/Library/WebServer/Documents/code/spider_res/html'
log_path = '/Library/WebServer/Documents/code/spider_res/log'
error_log_path = '/Library/WebServer/Documents/code/spider_res/log/error'
cmd_file = './cmd.txt'
# http://www.elegomall.com

if __name__ == '__main__':
    main()
