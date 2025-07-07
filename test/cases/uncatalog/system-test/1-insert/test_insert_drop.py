# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import threading


class TestInsertDrop:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), logSql)

    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=tdSql.execute, args=(insert_sql,))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def test_insert_drop(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        tdSql.prepare()
        tdSql.execute('create database if not exists test;')
        tdSql.execute('create table test.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int );')
        tdSql.execute('create table test.tb using test.stb TAGS (1, 1);')        

        # double comma insert check error
        tdSql.error("insert into test.tb(ts, c11) values(now,,100)")

        sql_list = list()
        for i in range(5):
            sql = f'insert into test.tb values (now-{i}m, {i}, {i});'
            sql_list.append(sql)
        sql_list.append(f'drop database test;')
        tlist = self.genMultiThreadSeq(sql_list)
        self.multiThreadRun(tlist)
        tdSql.query(f'select * from information_schema.ins_databases')
        tdLog.success(f"{__file__} successfully executed")
