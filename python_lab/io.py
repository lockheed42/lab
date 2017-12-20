#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'python io test'
__author__ = 'lockheed'

try:
    f = open('/path/to/file', 'r')
    print(f.read())
finally:
    if f:
        f.close()
    print('end')