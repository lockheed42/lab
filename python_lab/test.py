#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'test'
__author__ = 'lockheed'


'''
获取域名后缀
'''
def get_domain_suffix():
    return ['.com', '.cn', '.net','.wang']

'''
获取主域名
'''
def get_host(url):
    index = -1
    suffix_len = 0
    for suffix in get_domain_suffix():
        index = url.find(suffix)
        suffix_len = len(suffix)
        if index != -1:
            break
    return url[7:index + suffix_len]



a = '/anime/phi-brain3/'
print(get_host(a))
