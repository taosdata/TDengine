# -*- coding: utf-8 -*-

import sys
import string
import random
import subprocess
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table TestSensitiveT(ts timestamp, i int)')
        tdSql.execute('create table TestSensitiveT(ts timestamp, i int)')
        tdLog.info('create table TestSensitiveSt(ts timestamp,i int) tags(j int)')
        tdSql.execute('create table TestSensitiveSt(ts timestamp,i int) tags(j int)')
        tdLog.info('create table Abcde using TestSensitiveSt tags(1)')
        tdSql.execute('create table AbcdeFgh using TestSensitiveSt tags(1)')
        tdLog.info('=============== step2')
        tdLog.info('test normal table ')
        tdSql.error('create table testsensitivet(ts timestamp, i int)')
        tdSql.error('create table testsensitivet(ts timestamp, j int)')
        tdSql.error('create table testsensItivet(ts timestamp, j int)')
        tdSql.error('create table TESTSENSITIVET(ts timestamp, i int)')
        tdLog.info('=============== step3')
        tdLog.info('test super table ')
        tdSql.error('create table testsensitivest(ts timestamp,i int) tags(j int)')
        tdSql.error('create table testsensitivest(ts timestamp,i int) tags(k int)')
        tdSql.error('create table TESTSENSITIVEST(ts timestamp,i int) tags(j int)')
        tdSql.error('create table Testsensitivest(ts timestamp,i int) tags(j int)')
        tdLog.info('=============== step4')
        tdLog.info('test subtable ')
        tdSql.error('create table abcdefgh using TestSensitiveSt tags(1)')
        tdSql.error('create table ABCDEFGH using TestSensitiveSt tags(1)')
        tdSql.error('create table Abcdefgh using TestSensitiveSt tags(1)')
        tdSql.error('create table abcdeFgh using TestSensitiveSt tags(1)')
        tdSql.error('insert into table abcdefgh using TestSensitiveSt tags(1) values(now,1)')
        tdSql.error('insert into table ABCDEFGH using TestSensitiveSt tags(1) values(now,1)')
        tdSql.error('insert into table Abcdefgh using TestSensitiveSt tags(1) values(now,1)')
        tdSql.error('insert into table abcdeFgH using TestSensitiveSt tags(1) values(now,1)')
        tdSql.query('show tables')
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(2)
       
       
        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
