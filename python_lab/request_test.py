#!usr/bin/env/ python3
# -*- coding:utf-8 -*-

'''
请求测试
'''

__author__ = 'lockheed'

import urllib.request

url = 'http://www.biqugezw.com'

user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
headers = {'User-Agent': user_agent}
req = urllib.request.Request(url=url,headers=headers)
f = urllib.request.urlopen(req)


header = f.headers
print(header)


print(f.read().decode('utf-8'))

exit()


user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
accept_language = "zh-CN,zh;q=0.9,en;q=0.8"
connection = "keep-alive"
cache_control = "max-age=0"
headers = {'Accept': accept, 'Accept-Language': accept_language,
           'Connection': connection, 'User-Agent': user_agent, 'Cache-Control': cache_control}

req = urllib.request.Request(url, headers=headers)
response = urllib.request.urlopen(req)

res = response.read()
# TODO 编码可配置
print(res.decode('gbk'))
