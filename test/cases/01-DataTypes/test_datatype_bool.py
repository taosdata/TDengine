from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeBool:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    def test_datatype_bool(self):
        """DataTypes: bool

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_bool.sim

        """
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()

    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(f"create table mt_bool (ts timestamp, c bool) tags(tagname bool)")

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_bool_0 using mt_bool tags(NULL)")
        tdSql.query(f"show create table st_bool_0")
        tdSql.query(f"show tags from st_bool_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bool_1 using mt_bool tags(NULL)")
        tdSql.query(f"show tags from st_bool_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bool_2 using mt_bool tags('NULL')")
        tdSql.query(f"show tags from st_bool_2")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_bool_3 using mt_bool tags('NULL')")
        tdSql.query(f"show tags from st_bool_3")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_bool_4 using mt_bool tags("NULL")')
        tdSql.query(f"show tags from st_bool_4")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_bool_5 using mt_bool tags("NULL")')
        tdSql.query(f"show tags from st_bool_5")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f'create table st_bool_6 using mt_bool tags("true")')
        tdSql.query(f"show tags from st_bool_6")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_7 using mt_bool tags('true')")
        tdSql.query(f"show tags from st_bool_7")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_8 using mt_bool tags(true)")
        tdSql.query(f"show tags from st_bool_8")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_9 using mt_bool tags("false")')
        tdSql.query(f"show tags from st_bool_9")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_bool_10 using mt_bool tags('false')")
        tdSql.query(f"show tags from st_bool_10")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_bool_11 using mt_bool tags(false)")
        tdSql.query(f"show tags from st_bool_11")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_bool_12 using mt_bool tags(0)")
        tdSql.query(f"show tags from st_bool_12")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f"create table st_bool_13 using mt_bool tags(1)")
        tdSql.query(f"show tags from st_bool_13")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_14 using mt_bool tags(6.9)")
        tdSql.query(f"show tags from st_bool_14")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_15 using mt_bool tags(-3)")
        tdSql.query(f"show tags from st_bool_15")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_16 using mt_bool tags(+300)")
        tdSql.query(f"show tags from st_bool_16")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_17 using mt_bool tags(-8.03)")
        tdSql.query(f"show tags from st_bool_17")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_18 using mt_bool tags("-8.03")')
        tdSql.query(f"show tags from st_bool_18")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_19 using mt_bool tags("+300")')
        tdSql.query(f"show tags from st_bool_19")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_20 using mt_bool tags("-8e+2")')
        tdSql.query(f"show tags from st_bool_20")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_21 using mt_bool tags("0x01")')
        tdSql.query(f"show tags from st_bool_21")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_22 using mt_bool tags("0b01")')
        tdSql.query(f"show tags from st_bool_22")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_23 using mt_bool tags("+0x01")')
        tdSql.query(f"show tags from st_bool_23")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_24 using mt_bool tags("-0b00")')
        tdSql.query(f"show tags from st_bool_24")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f'create table st_bool_26 using mt_bool tags("-0.11e-30")')
        tdSql.query(f"show tags from st_bool_26")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'create table st_bool_27 using mt_bool tags("-1.0e-307")')
        tdSql.query(f"show tags from st_bool_27")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_28 using mt_bool tags( -1e-1 )")
        tdSql.query(f"show tags from st_bool_28")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_29 using mt_bool tags(-0.11e-30)")
        tdSql.query(f"show tags from st_bool_29")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_30 using mt_bool tags(-1.1e-307)")
        tdSql.query(f"show tags from st_bool_30")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_31 using mt_bool tags( 0x01)")
        tdSql.query(f"show tags from st_bool_31")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_32 using mt_bool tags(0b01 )")
        tdSql.query(f"show tags from st_bool_32")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_33 using mt_bool tags(+0x01)")
        tdSql.query(f"show tags from st_bool_33")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"create table st_bool_34 using mt_bool tags( -0b00 )")
        tdSql.query(f"show tags from st_bool_34")
        tdSql.checkData(0, 5, "false")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_bool_0 values(now, NULL)")
        tdSql.query(f"select * from st_bool_0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bool_1 values(now, NULL)")
        tdSql.query(f"select * from st_bool_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bool_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_bool_2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_bool_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_bool_3")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_bool_4 values(now, "NULL")')
        tdSql.query(f"select * from st_bool_4")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_bool_5 values(now, "NULL")')
        tdSql.query(f"select * from st_bool_5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.execute(f'insert into st_bool_6 values(now, "true")')
        tdSql.query(f"select * from st_bool_6")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_7 values(now, 'true')")
        tdSql.query(f"select * from st_bool_7")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_8 values(now, true)")
        tdSql.query(f"select * from st_bool_8")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_9 values(now, "false")')
        tdSql.query(f"select * from st_bool_9")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_10 values(now, 'false')")
        tdSql.query(f"select * from st_bool_10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_11 values(now, false)")
        tdSql.query(f"select * from st_bool_11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_12 values(now, 0)")
        tdSql.query(f"select * from st_bool_12")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_13 values(now, 1)")
        tdSql.query(f"select * from st_bool_13")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_14 values(now, 6.9)")
        tdSql.query(f"select * from st_bool_14")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_15 values(now, -3)")
        tdSql.query(f"select * from st_bool_15")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_16 values(now, +300)")
        tdSql.query(f"select * from st_bool_16")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_17 values(now, -3.15)")
        tdSql.query(f"select * from st_bool_17")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_18 values(now,"-8.03")')
        tdSql.query(f"select * from st_bool_18")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_19 values(now,"+300")')
        tdSql.query(f"select * from st_bool_19")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_20 values(now,"-8e+2")')
        tdSql.query(f"select * from st_bool_20")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_21 values(now,"0x01")')
        tdSql.query(f"select * from st_bool_21")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_22 values(now,"0b01")')
        tdSql.query(f"select * from st_bool_22")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_23 values(now,"+0x01")')
        tdSql.query(f"select * from st_bool_23")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_24 values(now,"-0b00")')
        tdSql.query(f"select * from st_bool_24")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f'insert into st_bool_26 values(now,"-0.11e-30")')
        tdSql.query(f"select * from st_bool_26")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f'insert into st_bool_27 values(now,"-1.0e-307")')
        tdSql.query(f"select * from st_bool_27")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_28 values(now, -1e-1 )")
        tdSql.query(f"select * from st_bool_28")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_29 values(now,-0.11e-30)")
        tdSql.query(f"select * from st_bool_29")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_30 values(now,-1.1e-307)")
        tdSql.query(f"select * from st_bool_30")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_31 values(now, 0x01)")
        tdSql.query(f"select * from st_bool_31")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_32 values(now,0b01 )")
        tdSql.query(f"select * from st_bool_32")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_33 values(now,+0x01)")
        tdSql.query(f"select * from st_bool_33")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_34 values(now, -0b00 )")
        tdSql.query(f"select * from st_bool_34")
        tdSql.checkData(0, 1, 0)

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_bool_116 using mt_bool tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show create table st_bool_116")
        tdSql.query(f"show tags from st_bool_116")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_116")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bool_117 using mt_bool tags(NULL) values(now, NULL)"
        )
        tdSql.query(f"show tags from st_bool_117")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_117")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bool_118 using mt_bool tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_bool_118")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_118")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_bool_119 using mt_bool tags('NULL') values(now, 'NULL')"
        )
        tdSql.query(f"show tags from st_bool_119")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_119")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_bool_120 using mt_bool tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_bool_120")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_120")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_bool_121 using mt_bool tags("NULL") values(now, "NULL")'
        )
        tdSql.query(f"show tags from st_bool_121")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_bool_121")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f'insert into st_bool_122 using mt_bool tags("true") values(now, "true")'
        )
        tdSql.query(f"show tags from st_bool_122")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_122")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_123 using mt_bool tags('true') values(now, 'true')"
        )
        tdSql.query(f"show tags from st_bool_123")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_123")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_124 using mt_bool tags(true) values(now, true)"
        )
        tdSql.query(f"show tags from st_bool_124")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_124")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_125 using mt_bool tags("false") values(now, "false")'
        )
        tdSql.query(f"show tags from st_bool_125")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_125")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_bool_126 using mt_bool tags('false') values(now, 'false')"
        )
        tdSql.query(f"show tags from st_bool_126")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_126")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f"insert into st_bool_127 using mt_bool tags(false) values(now, false)"
        )
        tdSql.query(f"show tags from st_bool_127")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_127")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_128 using mt_bool tags(0) values(now, 0)")
        tdSql.query(f"show tags from st_bool_128")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_128")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"insert into st_bool_129 using mt_bool tags(1) values(now, 1)")
        tdSql.query(f"show tags from st_bool_129")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_129")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_130 using mt_bool tags(6.9) values(now, 6.9)"
        )
        tdSql.query(f"show tags from st_bool_130")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_130")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(f"insert into st_bool_131 using mt_bool tags(-3) values(now, -3)")
        tdSql.query(f"show tags from st_bool_131")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_131")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_132 using mt_bool tags(+300) values(now, +300)"
        )
        tdSql.query(f"show tags from st_bool_132")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_132")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_133 using mt_bool tags(+30.890) values(now, +30.890)"
        )
        tdSql.query(f"show tags from st_bool_133")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_133")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_218 using mt_bool tags("-8.03") values(now,"-8.03")'
        )
        tdSql.query(f"show tags from st_bool_218")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_218")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_219 using mt_bool tags("+300") values(now,"+300")'
        )
        tdSql.query(f"show tags from st_bool_219")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_219")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_220 using mt_bool tags("-8e+2") values(now,"-8e+2")'
        )
        tdSql.query(f"show tags from st_bool_220")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_220")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_221 using mt_bool tags("0x01") values(now,"0x01")'
        )
        tdSql.query(f"show tags from st_bool_221")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_221")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_222 using mt_bool tags("0b01") values(now,"0b01")'
        )
        tdSql.query(f"show tags from st_bool_222")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_222")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_223 using mt_bool tags("+0x01") values(now,"+0x01")'
        )
        tdSql.query(f"show tags from st_bool_223")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_223")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_224 using mt_bool tags("-0b00") values(now,"-0b00")'
        )
        tdSql.query(f"show tags from st_bool_224")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_224")
        tdSql.checkData(0, 1, 0)

        tdSql.execute(
            f'insert into st_bool_226 using mt_bool tags("-0.11e-30") values(now,"-0.11e-30")'
        )
        tdSql.query(f"show tags from st_bool_226")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_226")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f'insert into st_bool_227 using mt_bool tags("-1.0e-307") values(now,"-1.0e-307")'
        )
        tdSql.query(f"show tags from st_bool_227")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_227")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_228 using mt_bool tags( -1e-1 ) values(now, -1e-1 )"
        )
        tdSql.query(f"show tags from st_bool_228")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_228")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_229 using mt_bool tags(-0.11e-30) values(now,-0.11e-30)"
        )
        tdSql.query(f"show tags from st_bool_229")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_229")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_230 using mt_bool tags(-1.1e-307) values(now,-1.1e-307)"
        )
        tdSql.query(f"show tags from st_bool_230")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_230")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_231 using mt_bool tags( 0x01) values(now, 0x01)"
        )
        tdSql.query(f"show tags from st_bool_231")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_231")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_232 using mt_bool tags(0b01 ) values(now, 0b01)"
        )
        tdSql.query(f"show tags from st_bool_232")
        tdSql.checkData(0, 5, "true")

        tdSql.query(f"select * from st_bool_232")
        tdSql.checkData(0, 1, 1)

        tdSql.execute(
            f"insert into st_bool_233 using mt_bool tags(+0x01) values(now,+0x01)"
        )
        tdSql.query(f"show tags from st_bool_233")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(
            f"insert into st_bool_234 using mt_bool tags( -0b00 ) values(now, -0b00)"
        )
        tdSql.query(f"show tags from st_bool_234")
        tdSql.checkData(0, 5, "false")

        tdSql.query(f"select * from st_bool_234")
        tdSql.checkData(0, 1, 0)

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_bool_16 set tag tagname=+300")
        tdSql.query(f"show tags from st_bool_16")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_17 set tag tagname=-8.03")
        tdSql.query(f"show tags from st_bool_17")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_18 set tag tagname="-8.03"')
        tdSql.query(f"show tags from st_bool_18")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_19 set tag tagname="+300"')
        tdSql.query(f"show tags from st_bool_19")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_20 set tag tagname="-8e+2"')
        tdSql.query(f"show tags from st_bool_20")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_21 set tag tagname="0x01"')
        tdSql.query(f"show tags from st_bool_21")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_22 set tag tagname="0b01"')
        tdSql.query(f"show tags from st_bool_22")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_23 set tag tagname="+0x01"')
        tdSql.query(f"show tags from st_bool_23")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_24 set tag tagname="-0b00"')
        tdSql.query(f"show tags from st_bool_24")
        tdSql.checkData(0, 5, "false")

        tdSql.execute(f'alter table st_bool_26 set tag tagname="-0.11e-30"')
        tdSql.query(f"show tags from st_bool_26")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f'alter table st_bool_27 set tag tagname="-1.0e-307"')
        tdSql.query(f"show tags from st_bool_27")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_28 set tag tagname= -1e-1 ")
        tdSql.query(f"show tags from st_bool_28")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_29 set tag tagname=-0.11e-30")
        tdSql.query(f"show tags from st_bool_29")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_30 set tag tagname=-1.1e-307")
        tdSql.query(f"show tags from st_bool_30")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_31 set tag tagname= 0x01")
        tdSql.query(f"show tags from st_bool_31")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_32 set tag tagname=0b01 ")
        tdSql.query(f"show tags from st_bool_32")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_33 set tag tagname=+0x01")
        tdSql.query(f"show tags from st_bool_33")
        tdSql.checkData(0, 5, "true")

        tdSql.execute(f"alter table st_bool_34 set tag tagname= -0b00 ")
        tdSql.query(f"show tags from st_bool_34")
        tdSql.checkData(0, 5, "false")

    def illegal_input(self):
        """illegal input

        1. 使用 bool 作为超级表的标签列
        2. 使用非法标签值创建子表

        Catalog:
            - DataTypes:Bool

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework

        """

        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_bool_e0 using mt_bool tags(123abc)")
        tdSql.error(f'create table st_bool_e1 using mt_bool tags("123abc")')
        tdSql.execute(f'create table st_bool_e2 using mt_bool tags("123")')
        tdSql.error(f"create table st_bool_e3 using mt_bool tags(abc)")
        tdSql.error(f'create table st_bool_e4 using mt_bool tags("abc")')
        tdSql.error(f'create table st_bool_e5 using mt_bool tags(" ")')
        tdSql.error(f"create table st_bool_e6 using mt_bool tags('')")

        tdSql.execute(f"create table st_bool_f0 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f1 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f2 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f3 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f4 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f5 using mt_bool tags(true)")
        tdSql.execute(f"create table st_bool_f6 using mt_bool tags(true)")

        tdSql.error(f"insert into st_bool_g0 values(now, 123abc)")
        tdSql.error(f'insert into st_bool_g1 values(now, "123abc")')
        tdSql.execute(f'insert into st_bool_f2 values(now, "123")')
        tdSql.error(f"insert into st_bool_g3 values(now, abc)")
        tdSql.error(f'insert into st_bool_g4 values(now, "abc")')
        tdSql.error(f'insert into st_bool_g5 values(now, " ")')
        tdSql.error(f"insert into st_bool_g6 values(now, '')")

        tdSql.error(f"insert into st_bool_h0 using mt_bool tags(123abc) values(now, 1)")
        tdSql.error(
            f'insert into st_bool_h1 using mt_bool tags("123abc") values(now, 1)'
        )
        tdSql.execute(
            f'insert into st_bool_h2 using mt_bool tags("123") values(now, 1)'
        )
        tdSql.error(f"insert into st_bool_h3 using mt_bool tags(abc) values(now, 1)")
        tdSql.error(f'insert into st_bool_h4 using mt_bool tags("abc") values(now, 1)')
        tdSql.error(f'insert into st_bool_h5 using mt_bool tags(" ") values(now, 1)')
        tdSql.error(f"insert into st_bool_h6 using mt_bool tags(" ") values(now, 1)")

        tdSql.error(f"insert into st_bool_h0 using mt_bool tags(1) values(now, 123abc)")
        tdSql.error(
            f'insert into st_bool_h1 using mt_bool tags(1) values(now, "123abc")'
        )
        tdSql.execute(
            f'insert into st_bool_h2 using mt_bool tags(1) values(now, "123")'
        )
        tdSql.error(f"insert into st_bool_h3 using mt_bool tags(1) values(now, abc)")
        tdSql.error(f'insert into st_bool_h4 using mt_bool tags(1) values(now, "abc")')
        tdSql.error(f'insert into st_bool_h5 using mt_bool tags(1) values(now, " ")')
        tdSql.error(f"insert into st_bool_h6 using mt_bool tags(1) values(now, '')")

        tdSql.execute(f"insert into st_bool_i0 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i1 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i2 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i3 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i4 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i5 using mt_bool tags(1) values(now, 1)")
        tdSql.execute(f"insert into st_bool_i6 using mt_bool tags(1) values(now, 1)")

        tdSql.error(f"alter table st_bool_i0 set tag tagname=123abc")
        tdSql.error(f'alter table st_bool_i1 set tag tagname="123abc"')
        tdSql.execute(f'alter table st_bool_i2 set tag tagname="123"')
        tdSql.error(f"alter table st_bool_i3 set tag tagname=abc")
        tdSql.error(f'alter table st_bool_i4 set tag tagname="abc"')
        tdSql.error(f"alter table st_bool_i4 set tag tagname=now")
        tdSql.error(f"alter table st_bool_i4 set tag tagname=now()+1d")
        tdSql.error(f"alter table st_bool_i4 set tag tagname=1+1d")
        tdSql.error(f'alter table st_bool_i5 set tag tagname=" "')
        tdSql.error(f"alter table st_bool_i6 set tag tagname=''")
