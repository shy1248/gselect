#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author: yushuibo
@Copyright (c) 2018 yushuibo. All rights reserved.
@Licence: GPL-2
@Email: hengchen2005@gmail.com
@Create: gselect.py.py
@Last Modified: 2018/5/25 下午 05:28
@Desc: --
"""


import yaml

with open('./test.yml', 'r', encoding='UTF-8') as f:
    conf = yaml.load(f)

print(conf)