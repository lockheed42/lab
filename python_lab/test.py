#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'test'
__author__ = 'lockheed'


def trim(string):
    left = 0
    right = -1
    length = len(string)
    for i in range(length):
        if string[i:i + 1] == ' ':
            print(left)
            left += 1
        else:
            break
    for i in range(length):
        if string[-1:] == ' ':
            right -= -1
    return string[left:right]


if trim('hello  ') != 'hello':
    print('测试失败1!')
elif trim('  hello') != 'hello':
    print('测试失败2!')
elif trim('  hello  ') != 'hello':
    print('测试失败3!')
elif trim('  hello  world  ') != 'hello  world':
    print('测试失败4!')
elif trim('') != '':
    print('测试失败5!')
elif trim('    ') != '':
    print('测试失败6!')
else:
    print('测试成功!')
