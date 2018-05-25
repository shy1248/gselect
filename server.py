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

from abc import ABCMeta

import pymysql
from sshtunnel import SSHTunnelForwarder

import const


class Server(object, metaclass=ABCMeta):
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

    def conn(self):
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
            self.select(db)

            # for test
            # cursor = db.cursor()
            # sql = 'show databases;'
            # cursor.execute(sql)
            # result = cursor.fetchall()
            # print(result)

    def select(self, db):
        '''Abstract method, implements the logic of yourself'''
        pass


class LymjGame(Server, metaclass=ABCMeta):
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


class LymjLog(Server, metaclass=ABCMeta):
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


class LymjPlayback(Server, metaclass=ABCMeta):
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


class LymjAgent(Server, metaclass=ABCMeta):
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


class ZgmjGame(Server, metaclass=ABCMeta):
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


class ZgmjLog(Server, metaclass=ABCMeta):
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


class ZgmjPlayback(Server, metaclass=ABCMeta):
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


class ZgmjAgent(Server, metaclass=ABCMeta):
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


if __name__ == '__main__':
    ssh_ip = '58.87.67.124'
    ssh_user = 'rdev'
    ssh_pass = '59e6a7c0'
    db_ip = '10.0.1.16'
    db_user = 'readuser'
    db_pass = 'jmdb_read'
    db = 'ly_mj_log'
    s = Server(ssh_ip, 22, ssh_user, ssh_pass, db_ip, 3306, db, db_user,
               db_pass)
    s.conn()
