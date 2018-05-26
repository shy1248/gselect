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

import re
import sys
import yaml

from server import *


result_list = []
has_based = False

with open('./test.yml', 'r', encoding='UTF-8') as f:
    conf = yaml.load(f)

print(conf)

for select in conf['selects']:
    print('==> server: {}'.format(select['server']))
    ser = globals().get(select['server'])()
    for statment in select['statments']:
        print('==> id: {}'.format(statment['id']))
        print('==> based: {}'.format(statment['based']))
        print('==> names: {}'.format(statment['names']))
        print('==> sql: {}'.format(statment['sql']))
        id_ = statment['id']
        based = statment['based']
        names = statment['names']
        sql = statment['sql']

        if id_ == 0 and based is not None:
            raise RuntimeError('The fisrt select statment cannot be based another statment!')
            sys.exit(1)
        if id_ == 0 and re.match(r'.*\{.*\}', sql):
            raise RuntimeError('The SQL in the fisrt statment cannot used templates!')
            sys.exit(1)


def rs_format(name, rs):
    rows = []
    data = rs[0][name]
    for row in data:
        rows.append(list(row))
    return rows
