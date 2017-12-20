#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'threading homework'
__author__ = 'lockheed'

count = 0
L = []


def move(n, a='A', b='B', c='C'):
    global count
    global L
    if n == 1:
        # print(a, '->', c)
        L.append(a + '->' + c)
        count += 1
    if n > 1:
        move(n - 1, a, c, b)
        move(1, a, b, c)
        move(n - 1, b, a, c)


n = int(input('n:'))
move(n, 'A', 'B', 'C')
print(L)
print(count)
