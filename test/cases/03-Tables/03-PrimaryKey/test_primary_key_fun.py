import datetime
from new_test_framework.utils import tdLog, tdSql, tdDnodes
from math import inf

class TestPkError:    
    def setup_class(cls):
        pass
        
    #
    # ------------------- pk error ----------------
    # 
    def do_pk_error(self):
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

        print("do pk error ........................... [passed]")

    #
    # ------------------- fun groups ----------------
    # 
    def do_pk_func_group(self):   
        tdSql.execute("drop database if exists pk_func_group")
        tdSql.execute("create database if not exists pk_func_group")
        tdSql.execute('use pk_func_group')
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

        tdSql.query('select first(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by tbname order by tbname,ts,pk;')
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

        tdSql.execute('create database d2 vgroups 2')

        tdSql.execute('use d2;')

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

        tdSql.query('select first(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 8)
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by tbname order by tbname,ts,pk;')
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

        tdSql.execute('drop database pk_func_group')    
        print("do fun groups ......................... [passed]")

    #
    # ------------------- fun ----------------
    # 
    def do_pk_func(self):
        tdSql.execute("drop database if exists pk_func")
        tdSql.execute("create database if not exists pk_func")
        tdSql.execute('use pk_func')
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

        tdSql.query('select * from d1.ct1 order by ts,pk;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(4, 2, 5)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(5, 2, 6)

        tdSql.query('select * from d1.ct2 order by ts,pk;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 7)
        tdSql.checkData(4, 2, 7)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(5, 1, 8)
        tdSql.checkData(5, 2, 8)

        tdSql.query('select * from d1.st order by ts,pk;')
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(3, 3, 2)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 3, 1)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(5, 2, 2)
        tdSql.checkData(5, 3, 2)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(6, 2, 3)
        tdSql.checkData(6, 3, 2)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, 4)
        tdSql.checkData(7, 2, 4)
        tdSql.checkData(7, 3, 1)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, 5)
        tdSql.checkData(8, 2, 5)
        tdSql.checkData(8, 3, 1)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, 6)
        tdSql.checkData(9, 2, 6)
        tdSql.checkData(9, 3, 1)
        tdSql.checkData(10, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(10, 1, 7)
        tdSql.checkData(10, 2, 7)
        tdSql.checkData(10, 3, 2)
        tdSql.checkData(11, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(11, 1, 8)
        tdSql.checkData(11, 2, 8)
        tdSql.checkData(11, 3, 2)

        tdSql.query('select first(*) from d1.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query('select first(*) from d1.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)

        tdSql.query('select first(*) from d1.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query('select last(*) from d1.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)

        tdSql.query('select last(*) from d1.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last(*) from d1.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last_row(*) from d1.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)

        tdSql.query('select last_row(*) from d1.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last_row(*) from d1.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select ts,diff(f) from d1.ct1;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)

        tdSql.query('select ts,diff(f) from d1.ct2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 5)

        tdSql.query('select ts,diff(f) from d1.st;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)

        tdSql.query('select irate(f) from d1.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.0)

        tdSql.query('select irate(f) from d1.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.0)

        tdSql.query('select irate(f) from d1.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.ct1;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.ct2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, -1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)

        tdSql.query('select twa(f) from d1.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.0)

        tdSql.query('select twa(f) from d1.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3.5)

        tdSql.query('select twa(f) from d1.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.0)

        tdSql.query('select ts,pk,unique(f) from d1.ct1 order by ts,pk;')
        tdSql.checkRows(5)
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

        tdSql.query('select ts,pk,unique(f) from d1.ct2 order by ts,pk;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 7)
        tdSql.checkData(3, 2, 7)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 8)
        tdSql.checkData(4, 2, 8)

        tdSql.query('select ts,pk,unique(f) from d1.st order by ts,pk;')
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

        tdSql.execute('create database d2 vgroups 2')

        tdSql.execute('use d2;')

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

        tdSql.query('select * from d2.ct1 order by ts,pk;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(4, 2, 5)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(5, 1, 6)
        tdSql.checkData(5, 2, 6)

        tdSql.query('select * from d2.ct2 order by ts,pk;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(3, 1, 3)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 7)
        tdSql.checkData(4, 2, 7)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(5, 1, 8)
        tdSql.checkData(5, 2, 8)

        tdSql.query('select * from d2.st order by ts,pk;')
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)
        tdSql.checkData(3, 3, 2)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(4, 3, 1)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(5, 1, 2)
        tdSql.checkData(5, 2, 2)
        tdSql.checkData(5, 3, 2)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(6, 1, 3)
        tdSql.checkData(6, 2, 3)
        tdSql.checkData(6, 3, 2)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, 4)
        tdSql.checkData(7, 2, 4)
        tdSql.checkData(7, 3, 1)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, 5)
        tdSql.checkData(8, 2, 5)
        tdSql.checkData(8, 3, 1)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, 6)
        tdSql.checkData(9, 2, 6)
        tdSql.checkData(9, 3, 1)
        tdSql.checkData(10, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(10, 1, 7)
        tdSql.checkData(10, 2, 7)
        tdSql.checkData(10, 3, 2)
        tdSql.checkData(11, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(11, 1, 8)
        tdSql.checkData(11, 2, 8)
        tdSql.checkData(11, 3, 2)

        tdSql.query('select first(*) from d2.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query('select first(*) from d2.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)

        tdSql.query('select first(*) from d2.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query('select last(*) from d2.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)

        tdSql.query('select last(*) from d2.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last(*) from d2.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last_row(*) from d2.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, 6)

        tdSql.query('select last_row(*) from d2.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select last_row(*) from d2.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)

        tdSql.query('select ts,diff(f) from d2.ct1;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)

        tdSql.query('select ts,diff(f) from d2.ct2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 5)

        tdSql.query('select ts,diff(f) from d2.st;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)

        tdSql.query('select irate(f) from d2.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.0)

        tdSql.query('select irate(f) from d2.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.0)

        tdSql.query('select irate(f) from d2.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d2.ct1;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d2.ct2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, -1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d2.st;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)

        tdSql.query('select twa(f) from d2.ct1;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.0)

        tdSql.query('select twa(f) from d2.ct2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3.5)

        tdSql.query('select twa(f) from d2.st;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.0)

        tdSql.query('select ts,pk,unique(f) from d2.ct1 order by ts,pk;')
        tdSql.checkRows(5)
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

        tdSql.query('select ts,pk,unique(f) from d2.ct2 order by ts,pk;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 7)
        tdSql.checkData(3, 2, 7)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, 8)
        tdSql.checkData(4, 2, 8)

        tdSql.query('select ts,pk,unique(f) from d2.st order by ts,pk;')
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
        
        tdSql.query('select ts, last(pk) from d1.st order by pk')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 8)
        
        tdSql.execute('drop database pk_func')    
        print("do fun ................................ [passed]")

    #
    # ------------------- varchar ----------------
    # 
    def do_pk_varchar(self):
        tdSql.execute("drop database if exists pk_varchar")
        tdSql.execute("create database if not exists pk_varchar")
        tdSql.execute('use pk_varchar')
        tdSql.execute('drop database IF EXISTS d1;')

        tdSql.execute('drop database IF EXISTS d2;')

        tdSql.execute('create database d1 vgroups 1')

        tdSql.execute('use d1;')

        tdSql.execute('create table st(ts timestamp, pk varchar(256) primary key, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', '1', 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', '2', 2);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', '3', 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', '4', 4);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', '1', 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', '4', 4);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', '3', 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', '2', 2);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', '6', 6);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', '5', 5);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', '8', 8);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', '7', 7);")

        tdSql.query('select first(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '3')
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, '6')
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, '8')
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, '6')
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, '8')
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d1.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d1.st partition by tbname order by tbname,ts,pk;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, '4')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, '5')
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, '6')
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(5, 1, '3')
        tdSql.checkData(5, 2, 3)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(6, 1, '4')
        tdSql.checkData(6, 2, 4)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, '2')
        tdSql.checkData(7, 2, 2)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, '7')
        tdSql.checkData(8, 2, 7)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, '8')
        tdSql.checkData(9, 2, 8)

        tdSql.query('select * from d1.st order by ts,pk limit 2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1)

        tdSql.execute('create database d2 vgroups 2')

        tdSql.execute('use d2;')

        tdSql.execute('create table st(ts timestamp, pk varchar(256) primary key, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', '1', 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', '2', 2);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', '3', 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:00', '4', 4);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', '1', 1);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:01', '4', 4);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', '3', 3);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', '2', 2);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', '6', 6);")

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:02', '5', 5);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', '8', 8);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:02', '7', 7);")

        tdSql.query('select first(*) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '3')
        tdSql.checkData(1, 2, 3)

        tdSql.query('select last(*) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, '6')
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, '8')
        tdSql.checkData(1, 2, 8)

        tdSql.query('select last_row(*) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, '6')
        tdSql.checkData(0, 2, 6)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, '8')
        tdSql.checkData(1, 2, 8)

        tdSql.query('select ts,diff(f) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5)

        tdSql.query('select irate(f) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.0)
        tdSql.checkData(1, 0, 5.0)

        tdSql.query('select ts,derivative(f, 1s, 0) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 0.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 4.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, 5.0)

        tdSql.query('select twa(f) from d2.st partition by tbname order by tbname;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2.0)
        tdSql.checkData(1, 0, 3.5)

        tdSql.query('select ts,pk,unique(f) from d2.st partition by tbname order by tbname,ts,pk;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(2, 1, '4')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(3, 1, '5')
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(4, 1, '6')
        tdSql.checkData(4, 2, 6)
        tdSql.checkData(5, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(5, 1, '3')
        tdSql.checkData(5, 2, 3)
        tdSql.checkData(6, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(6, 1, '4')
        tdSql.checkData(6, 2, 4)
        tdSql.checkData(7, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(7, 1, '2')
        tdSql.checkData(7, 2, 2)
        tdSql.checkData(8, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(8, 1, '7')
        tdSql.checkData(8, 2, 7)
        tdSql.checkData(9, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(9, 1, '8')
        tdSql.checkData(9, 2, 8)

        tdSql.query('select * from d2.st order by ts,pk limit 2;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0))
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 1)

        tdSql.execute('drop database pk_varchar')    
        print("do pk varchar ......................... [passed]")


    #
    # ------------------- main ----------------
    # 
    def test_primary_key_basic(self):
        """Composite Primary Key Functions

        1. Create primary key on super/normal table
        2. Create primary key on int/varchar column
        3. Use primary key column in aggregate functions:
            - first, last, last_row, unique
            - diff, irate, derivative, twa 
        4. Use primary key column in order by clause
        5. Use primary key column in limit clause
        6. Use primary key column in partition by clause
        7. Check except for invalid use of primary key column
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_pk_error.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_pk_func.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_pk_varchar.py
            - 2025-12-08 Alex Duan Migrated from uncatalog/system-test/2-query/test_pk_func_group.py

        """
        print("test_primary_key_fun ...")
        self.do_pk_error()
        self.do_pk_func_group()
        self.do_pk_func()
        self.do_pk_varchar()
        