from new_test_framework.utils import tdLog, tdSql, tdDnodes
import sys 
from math import inf

class TestTagScan:
    def runSingleVgroup(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tagscan2")
        tdSql.execute("create database if not exists tagscan2 vgroups 1")
        tdSql.execute('use tagscan2')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double)')

        sql = "create table "
        sql += " tb1 using stb1 tags(1,'1',1.0)"
        sql += " tb2 using stb1 tags(2,'2',2.0)"
        sql += " tb3 using stb1 tags(3,'3',3.0)"
        sql += " tb4 using stb1 tags(4,'4',4.0)"
        sql += " tb5 using stb1 tags(5,'5',5.0)"
        sql += " tb6 using stb1 tags(5,'5',5.0)"
        tdSql.execute(sql)

        sql = "insert into "
        sql += ' tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb3 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb4 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb5 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb6 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        tdSql.execute(sql)

        tdSql.query('select tags t1,t2 from stb1 order by t1,t2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, '3')
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, '4')
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, '5')
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(5, 1, '5')

        tdSql.query('select * from (select tags t1,t2 from stb1 group by t1,t2 slimit 2,3) order by t1,t2')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn from stb1 group by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname order by tbname limit 2,3) order by tn')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'tb3')

        tdSql.query('select * from (select distinct tbname tn from stb1 limit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select distinct tbname tn, t1,t2 from stb1 limit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags t1,t2 from stb1 order by t1, t2 limit 2,3) order by t1, t2')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, '3')
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, '4')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, '5')

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 partition by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.execute('drop database tagscan2')

    def runMultiVgroups(self):

        tdSql.execute("create database if not exists tagscan")
        tdSql.execute('use tagscan')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double)')

        sql = "create table "
        sql += " tb1 using stb1 tags(1,'1',1.0)"
        sql += " tb2 using stb1 tags(2,'2',2.0)"
        sql += " tb3 using stb1 tags(3,'3',3.0)"
        sql += " tb4 using stb1 tags(4,'4',4.0)"
        sql += " tb5 using stb1 tags(5,'5',5.0)"
        sql += " tb6 using stb1 tags(5,'5',5.0)"
        tdSql.execute(sql)

        sql = "insert into "
        sql += ' tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb3 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb4 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb5 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        sql += ' tb6 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1)'
        tdSql.execute(sql)

        tdSql.query('select tags t1,t2 from stb1 order by t1,t2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, '3')
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, '4')
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, '5')
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(5, 1, '5')

        tdSql.query('select * from (select tags t1,t2 from stb1 group by t1,t2 slimit 2,3) order by t1,t2')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn from stb1 group by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname order by tbname limit 2,3) order by tn')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'tb3')

        tdSql.query('select * from (select distinct tbname tn from stb1 limit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select distinct tbname tn, t1,t2 from stb1 limit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags t1,t2 from stb1 order by t1, t2 limit 2,3) order by t1, t2')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, '3')
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, '4')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, '5')

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 partition by tbname slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn')
        tdSql.checkRows(3)


        tdSql.execute('drop database tagscan')

    def test_tag_scan(self):
        """Query: tag scan

        1. Create super table with tags
        2. Create multiple child tables with different tag values
        3. Insert data into child tables
        4. Query tag with order by, limit, slimit, group by, distinct, partition by
        5. Query tag with sub-queries
        6. Query tag with single vgroup and multi vgroups
        7. Query tag with different data types
        8. Query tag with various combinations of clauses

        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_tag_scan.py

        """
        self.runMultiVgroups()
        self.runSingleVgroup()


