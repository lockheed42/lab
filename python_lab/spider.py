#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'spider'
__author__ = 'lockheed'

import urllib
import urllib.request

url = "https://shanghai.anjuke.com/"

res_data = urllib.request.urlopen(url)
res = res_data.read()
print(res)