#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'os test'
__author__ = 'lockheed'

import os

print(os.name)

py = [x for x in os.listdir('.') if os.path.isfile(x) and os.path.splitext(x)[1]=='.py']
print(py)