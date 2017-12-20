#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'python test'
__author__ = 'lockheed'

import pdb

def foo(s):
    n = int(s)
    pdb.set_trace()
    assert n != 0, 'n is zero!'
    return 10 / n

def main():
    return foo(0)

print(main())