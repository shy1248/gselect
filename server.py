#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author: yushuibo
@Copyright (c) 2018 yushuibo. All rights reserved.
@Licence: GPL-2
@Email: hengchen2005@gmail.com
@Create: sql_oper.py
@Last Modified: 2018/5/25 下午 03:24
@Desc: --
"""


import pymysql
from sshtunnel import SSHTunnelForwarder

import const


def rs2list(rs, names):
    rows = []
    rows.append(names)
    for r in rs:
        rows.append(list(r))
    return rows

class Server(object):
    """
    Base class of a database server
    """

    def __init__(self, ip, port, username, passwd, remote_ip, remote_port, db,
                 db_user, db_pass):
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd
        self.r_ip = remote_ip
        self.r_port = remote_port
        self.db = db
        self.db_user = db_user
        self.db_pass = db_pass

    def select(self, sql):
        with SSHTunnelForwarder(
                (self.ip, self.port),
                ssh_username=self.username,
                ssh_password=self.passwd,
                remote_bind_address=(self.r_ip, self.r_port),
                local_bind_address=('0.0.0.0', 22222)
        ) as server:
            server.start()
            db = pymysql.connect(host='127.0.0.1', port=22222,
                                 user=self.db_user, passwd=self.db_pass,
                                 db=self.db, charset='utf8')
            cursor = db.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def selects(self, statments):
        with SSHTunnelForwarder(
                (self.ip, self.port),
                ssh_username=self.username,
                ssh_password=self.passwd,
                remote_bind_address=(self.r_ip, self.r_port),
                local_bind_address=('0.0.0.0', 22222)
        ) as server:
            server.start()
            db = pymysql.connect(host='127.0.0.1', port=22222,
                                 user=self.db_user, passwd=self.db_pass,
                                 db=self.db, charset='utf8')
            cursor = db.cursor()
            for stmt in statments:
                id_ = stmt['id']
                sql = stmt['sql']
                names = stmt['names']
                cursor.execute(sql)
                result = cursor.fetchall()
                yield {id_: rs2list(result, names)}


class LymjGame(Server):
    """
    lymj game database server
    """

    def __init__(self):
        super(LymjGame, self).__init__(
            ip=const.BJ_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.LY_G_IP,
            remote_port=const.DB_PORT,
            db=const.DB_LY_NG,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class LymjLog(Server):
    """
    lymj log database server
    """

    def __init__(self):
        super(LymjLog, self).__init__(
            ip=const.BJ_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.LY_L_IP,
            remote_port=const.DB_PORT,
            db=const.DB_LY_NL,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class LymjPlayback(Server):
    """
    lymj playback database server
    """

    def __init__(self):
        super(LymjPlayback, self).__init__(
            ip=const.BJ_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.LY_P_IP,
            remote_port=const.DB_PORT,
            db=const.DB_LY_NP,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class LymjAgent(Server):
    """
    lymj agent database server
    """

    def __init__(self):
        super(LymjAgent, self).__init__(
            ip=const.BJ_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.LY_L_IP,
            remote_port=const.DB_PORT,
            db=const.DB_LY_NA,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class ZgmjGame(Server):
    """
    zgmj game database server
    """

    def __init__(self):
        super(ZgmjGame, self).__init__(
            ip=const.SH_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.ZG_G_IP,
            remote_port=const.DB_PORT,
            db=const.DB_ZG_NG,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class ZgmjLog(Server):
    """
    zgmj log database server
    """

    def __init__(self):
        super(ZgmjLog, self).__init__(
            ip=const.SH_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.ZG_L_IP,
            remote_port=const.DB_PORT,
            db=const.DB_ZG_NL,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class ZgmjPlayback(Server):
    """
    zgmj playback database server
    """

    def __init__(self):
        super(ZgmjPlayback, self).__init__(
            ip=const.SH_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.ZG_P_IP,
            remote_port=const.DB_PORT,
            db=const.DB_ZG_NP,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )


class ZgmjAgent(Server):
    """
    zgmj agent databses server
    """

    def __init__(self):
        super(ZgmjAgent, self).__init__(
            ip=const.SH_SSH,
            port=const.SSH_PORT,
            username=const.SSH_USER,
            passwd=const.SSH_PASS,
            remote_ip=const.ZG_L_IP,
            remote_port=const.DB_PORT,
            db=const.DB_ZG_NA,
            db_user=const.DB_USER,
            db_pass=const.DB_PASS
        )
