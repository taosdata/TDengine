import datetime
from new_test_framework.utils import tdLog, tdSql, tdDnodes
from math import inf

class TestPkError:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-] 
        ''' 
        return
    
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # tdSql.init(conn.cursor(), True)
        # self.conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use pk_error")

    def test_pk_error(self):
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

        print("running {}".format(__file__))
        tdSql.execute("drop database if exists pk_error")
        tdSql.execute("create database if not exists pk_error")
        tdSql.execute('use pk_error')
        tdSql.execute('drop database IF EXISTS d1;')

        tdSql.execute('drop database IF EXISTS d2;')

        tdSql.execute('create database d1 vgroups 1')

        tdSql.execute('use d1;')

        tdSql.execute('create table st(ts timestamp, pk int primary key, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 2, 2);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', 4, 4);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 1, 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', 4, 4);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 3, 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2, 2);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 6, 6);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', 5, 5);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 8, 8);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', 7, 7);")

        tdSql.query('select first(*) from d1.st partition by t order by t;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by t order by t;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by t order by t;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f),t from d1.st partition by t order by 3,1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by t order by t;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0),t from d1.st partition by t order by 3,1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by t order by t;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by t order by t,ts,pk;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 6)
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(5, 1, 3)
        tdSql.checkData(5, 2, 3)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(6, 1, 4)
        tdSql.checkData(6, 2, 4)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, 2)
        tdSql.checkData(7, 2, 2)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, 7)
        tdSql.checkData(8, 2, 7)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, 8)
        tdSql.checkData(9, 2, 8)

        tdSql.execute('create table nt(ts timestamp, pk int primary key, f int);')

        tdSql.execute("insert into nt values('2021-04-19 00:00:00', 1, 1);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:00', 2, 2);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:00', 3, 3);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:00', 4, 4);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:01', 1, 1);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:01', 4, 4);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:01', 3, 3);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:01', 2, 2);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:02', 6, 6);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:02', 5, 5);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:02', 8, 8);")

        tdSql.execute("insert into nt values('2021-04-19 00:00:02', 7, 7);")

        tdSql.query('select first(*) from d1.nt;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query('select last(*) from d1.nt;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last_row(*) from d1.nt;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select ts,diff(f) from d1.nt;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)

        tdSql.query('select irate(f) from d1.nt;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.nt;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)

        tdSql.query('select twa(f) from d1.nt;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.0)

        tdSql.query('select ts,pk,unique(f) from d1.nt order by ts,pk;')
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(4, 2, 5)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(5, 2, 6)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(6, 1, 7)
        tdSql.checkData(6, 2, 7)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(7, 1, 8)
        tdSql.checkData(7, 2, 8)

        tdSql.execute('create table st2(ts timestamp, vcpk varchar(64) primary key, f int);')

        tdSql.error('alter table st drop column pk;')

        tdSql.error('alter table nt drop column pk;')

        tdSql.error('alter table st modify column pk bigint;')

        tdSql.error('alter table nt modify column pk bigint;')

        tdSql.error('alter table st2 modify column vcpk varchar(32)')

        tdSql.execute('drop database pk_error')
        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
