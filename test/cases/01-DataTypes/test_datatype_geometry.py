import os
import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, etool

class TestDatatypeGeometry:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    #
    # ------------------- sim ----------------
    #
    def do_sim_geometry(self):
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()
        print("\n")
        print("do sim geometry ....................... [passed]")

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_geometry (ts timestamp, c geometry(128)) tags(tagname geometry(128))"
        )

        tdLog.info(f"case 0: static create table for test tag values")
        tdSql.execute(f"create table st_geometry_0 using mt_geometry tags(NULL)")
        tdSql.query(f"show tags from st_geometry_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_geometry_1 using mt_geometry tags(NULL)")
        tdSql.query(f"show tags from st_geometry_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_geometry_2 using mt_geometry tags('NULL')")
        tdSql.query(f"show tags from st_geometry_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_geometry_3 using mt_geometry tags('NULL')")
        tdSql.query(f"show tags from st_geometry_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_geometry_4 using mt_geometry tags("NULL")')
        tdSql.query(f"show tags from st_geometry_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_geometry_5 using mt_geometry tags("NULL")')
        tdSql.query(f"show tags from st_geometry_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(
            f'create table st_geometry_6 using mt_geometry tags("POINT(1.0 1.0)")'
        )
        tdSql.query(f"show tags from st_geometry_6")
        tdSql.checkData(0, 5, "POINT (1.000000 1.000000)")

        tdSql.execute(
            f'create table st_geometry_7 using mt_geometry tags(" LINESTRING(1.0 1.0, 2.0 2.0)")'
        )
        tdSql.query(f"show tags from st_geometry_7")
        tdSql.checkData(0, 5, "LINESTRING (1.000000 1.000000, 2.000000 2.000000)")

        tdSql.execute(
            f'create table st_geometry_8 using mt_geometry tags("POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")'
        )
        tdSql.query(f"show tags from st_geometry_8")
        tdSql.checkData(
            0, 5, "POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))"
        )

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_geometry_0 values(now, NULL)")
        tdSql.query(f"select * from st_geometry_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_geometry_1 values(now, NULL)")
        tdSql.query(f"select * from st_geometry_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_geometry_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_geometry_2")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_geometry_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_geometry_3")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_geometry_4 values(now, "NULL")')
        tdSql.query(f"select * from st_geometry_4")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_geometry_5 values(now, "NULL")')
        tdSql.query(f"select * from st_geometry_5")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_geometry_6 values(now, "POINT(1.0 1.0)")')
        tdSql.query(f"select * from st_geometry_6")
        # tdSql.checkData(0, 1, "POINT (1.000000 1.000000)")

        tdSql.execute(
            f'insert into st_geometry_7 values(now, " LINESTRING(1.0 1.0, 2.0 2.0)")'
        )
        tdSql.query(f"select * from st_geometry_7")
        # tdSql.checkData(0, 1, "LINESTRING (1.000000 1.000000, 2.000000 2.000000)")

        tdSql.execute(
            f'insert into st_geometry_8 values(now, "POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")'
        )
        tdSql.query(f"select * from st_geometry_8")
        # tdSql.checkData(
        #     0, 1, "POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))"
        # )

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_geometry_100 using mt_geometry tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_geometry_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_geometry_101 using mt_geometry tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_geometry_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_geometry_102 using mt_geometry tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_geometry_102")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_geometry_103 using mt_geometry tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_geometry_103")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_geometry_104 using mt_geometry tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_geometry_104")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_geometry_105 using mt_geometry tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_geometry_105")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_geometry_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_geometry_106 using mt_geometry tags("POINT(1.0 1.0)") values(now, "POINT(1.0 1.0)")'
        )
        tdSql.query(f"show tags from st_geometry_106")
        tdSql.checkData(0, 5, "POINT (1.000000 1.000000)")

        tdSql.query(f"select * from st_geometry_106")
        # tdSql.checkData(0, 1, "POINT (1.000000 1.000000)")

        tdSql.execute(
            f'insert into st_geometry_107 using mt_geometry tags(" LINESTRING(1.0 1.0, 2.0 2.0)") values(now, "LINESTRING(1.0 1.0, 2.0 2.0)")'
        )
        tdSql.query(f"show tags from st_geometry_107")
        # tdSql.checkData(0, 5, "LINESTRING (1.000000 1.000000, 2.000000 2.000000)")

        tdSql.query(f"select * from st_geometry_107")
        # tdSql.checkData(0, 1, "LINESTRING (1.000000 1.000000, 2.000000 2.000000)")

        tdSql.execute(
            f'insert into st_geometry_108 using mt_geometry tags("POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))") values(now, "POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))")'
        )
        tdSql.query(f"show tags from st_geometry_108")
        tdSql.checkData(
            0, 5, "POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))"
        )

        tdSql.query(f"select * from st_geometry_108")
        # tdSql.checkData(
        #     0, 1, "POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))"
        # )

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_geometry_0 set tag tagname=NULL")
        tdSql.query(f"show tags from st_geometry_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_geometry_1 set tag tagname=NULL")
        tdSql.query(f"show tags from st_geometry_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_geometry_2 set tag tagname='NULL'")
        tdSql.query(f"show tags from st_geometry_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_geometry_3 set tag tagname='NULL'")
        tdSql.query(f"show tags from st_geometry_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_geometry_4 set tag tagname="NULL"')
        tdSql.query(f"show tags from st_geometry_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_geometry_5 set tag tagname="NULL"')
        tdSql.query(f"show tags from st_geometry_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'alter table st_geometry_6 set tag tagname="POINT(1.0 1.0)"')
        tdSql.query(f"show tags from st_geometry_6")
        tdSql.checkData(0, 5, "POINT (1.000000 1.000000)")

        tdSql.execute(
            f'alter table st_geometry_7 set tag tagname=" LINESTRING(1.0 1.0, 2.0 2.0)"'
        )
        tdSql.query(f"show tags from st_geometry_7")
        tdSql.checkData(0, 5, "LINESTRING (1.000000 1.000000, 2.000000 2.000000)")

        tdSql.execute(
            f'alter table st_geometry_8 set tag tagname="POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))"'
        )
        tdSql.query(f"show tags from st_geometry_8")
        tdSql.checkData(
            0, 5, "POLYGON ((1.000000 1.000000, -2.000000 2.000000, 1.000000 1.000000))"
        )

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_geometry_206 using mt_geometry tags(+0123)")
        tdSql.error(f"create table st_geometry_207 using mt_geometry tags(-01.23)")
        tdSql.error(f"create table st_geometry_208 using mt_geometry tags(+0x01)")
        tdSql.error(f"create table st_geometry_209 using mt_geometry tags(-0b01)")
        tdSql.error(f"create table st_geometry_2010 using mt_geometry tags(-0.1e-10)")
        tdSql.error(f"create table st_geometry_2011 using mt_geometry tags(+0.1E+2)")
        tdSql.error(f"create table st_geometry_2012 using mt_geometry tags(tRue)")
        tdSql.error(f"create table st_geometry_2013 using mt_geometry tags(FalsE)")
        tdSql.error(f"create table st_geometry_2014 using mt_geometry tags(noW)")
        tdSql.error(f"create table st_geometry_2015 using mt_geometry tags(toDay)")
        tdSql.error(
            f"insert into st_geometry_206 using mt_geometry tags(+0123) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_207 using mt_geometry tags(-01.23) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_208 using mt_geometry tags(+0x01) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_209 using mt_geometry tags(-0b01) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2010 using mt_geometry tags(-0.1e-10) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2011 using mt_geometry tags(+0.1E+2) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2012 using mt_geometry tags(tRue) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2013 using mt_geometry tags(FalsE) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2014 using mt_geometry tags(noW) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_2015 using mt_geometry tags(toDay) values(now, NULL)"
        )
        tdSql.error(
            f"insert into st_geometry_106 using mt_varbinary tags(NULL) values(now(), +0123)"
        )
        tdSql.error(
            f"insert into st_geometry_107 using mt_varbinary tags(NULL) values(now(), -01.23)"
        )
        tdSql.error(
            f"insert into st_geometry_108 using mt_varbinary tags(NULL) values(now(), +0x01)"
        )
        tdSql.error(
            f"insert into st_geometry_109 using mt_varbinary tags(NULL) values(now(), -0b01)"
        )
        tdSql.error(
            f"insert into st_geometry_1010 using mt_varbinary tags(NULL) values(now(), -0.1e-10)"
        )
        tdSql.error(
            f"insert into st_geometry_1011 using mt_varbinary tags(NULL) values(now(), +0.1E+2)"
        )
        tdSql.error(
            f"insert into st_geometry_1012 using mt_varbinary tags(NULL) values(now(), tRue)"
        )
        tdSql.error(
            f"insert into st_geometry_1013 using mt_varbinary tags(NULL) values(now(), FalsE)"
        )
        tdSql.error(
            f"insert into st_geometry_1014 using mt_varbinary tags(NULL) values(now(), noW)"
        )
        tdSql.error(
            f"insert into st_geometry_1015 using mt_varbinary tags(NULL) values(now(), toDay)"
        )

    #
    # ------------------- army ----------------
    #   
    def checkGeometry(self):
        tdLog.info(f"check geometry")

        tdSql.execute("create database db_geometry;")
        tdSql.execute("use db_geometry;")
        tdSql.execute("create table t_ge (ts timestamp, id int, c1 GEOMETRY(512));")
        tdSql.execute("insert into t_ge values(1717122943000, 1, 'MULTIPOINT ((0 0), (1 1))');")
        tdSql.execute("insert into t_ge values(1717122944000, 1, 'MULTIPOINT (0 0, 1 1)');")
        tdSql.execute("insert into t_ge values(1717122945000, 2, 'POINT (0 0)');")
        tdSql.execute("insert into t_ge values(1717122946000, 2, 'POINT EMPTY');")
        tdSql.execute("insert into t_ge values(1717122947000, 3, 'LINESTRING (0 0, 0 1, 1 2)');")
        tdSql.execute("insert into t_ge values(1717122948000, 3, 'LINESTRING EMPTY');")
        tdSql.execute("insert into t_ge values(1717122949000, 4, 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))');")
        tdSql.execute("insert into t_ge values(1717122950000, 4, 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))');")
        tdSql.execute("insert into t_ge values(1717122951000, 4, 'POLYGON EMPTY');")
        tdSql.execute("insert into t_ge values(1717122952000, 5, 'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))');")
        tdSql.execute("insert into t_ge values(1717122953000, 6, 'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((4 3, 6 3, 6 1, 4 1, 4 3)))');")
        tdSql.execute("insert into t_ge values(1717122954000, 7, 'GEOMETRYCOLLECTION (MULTIPOINT((0 0), (1 1)), POINT(3 4), LINESTRING(2 3, 3 4))');")
        tdSql.query("select * from t_ge;")
        tdSql.checkRows(12)
        tdSql.query("select * from t_ge where id=1;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=2;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=3;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=4;")
        tdSql.checkRows(3)
        tdSql.query("select * from t_ge where id=5;")
        tdSql.checkRows(1)
        tdSql.query("select * from t_ge where id=6;")
        tdSql.checkRows(1)
        tdSql.query("select * from t_ge where id=7;")
        tdSql.checkRows(1)

 
    def do_army_geometry(self):
        self.checkGeometry()
        print("do army geometry ...................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_datatype_geometry(self):
        """DataTypes: geometry

        1. Create stable with geometry datatype on column and tag
        2. Insert and query data with geometry datatype
        3. Automatically create child table with geometry datatype on tag
        4. Alter tag value with geometry datatype
        5. Test illegal input for geometry datatype
        6. Create normal table with geometry datatype on column
        7. Insert and query data with geometry datatype
        8. Insert geometry column/tag with NULL value
        9. Insert geometry format: POINT/LINESTRING/POLYGON,/MULTIPOINT/MULTILINESTRING/MULTIPOLYGON/GEOMETRYCOLLECTION

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_geometry.sim
            - 2025-10-24 Alex Duan Migrated from test/cases/uncatalog/army/insert/test_insert_basic.py

        """
        self.do_sim_geometry()
        self.do_army_geometry()