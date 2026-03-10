from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestBetweenAnd:
    #
    # ------------------- 1 ----------------
    #
    def do_sim_between_and(self):
        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 float, f3 double, f4 bigint, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10)) tags (id1 int, id2 float, id3 nchar(10), id4 double, id5 smallint, id6 bigint, id7 binary(10))"
        )
        tdSql.execute(f'create table tb1 using st2 tags (1,1.0,"1",1.0,1,1,"1");')
        tdSql.execute(f'create table tb2 using st2 tags (2,2.0,"2",2.0,2,2,"2");')
        tdSql.execute(f'create table tb3 using st2 tags (3,3.0,"3",3.0,3,3,"3");')
        tdSql.execute(f'create table tb4 using st2 tags (4,4.0,"4",4.0,4,4,"4");')

        tdSql.execute(f'insert into tb1 values (now-200s,1,1.0,1.0,1,1,1,true,"1","1")')
        tdSql.execute(f'insert into tb1 values (now-100s,2,2.0,2.0,2,2,2,true,"2","2")')
        tdSql.execute(f'insert into tb1 values (now,3,3.0,3.0,3,3,3,true,"3","3")')
        tdSql.execute(f'insert into tb1 values (now+100s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb1 values (now+200s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb2 values (now+300s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb3 values (now+400s,4,4.0,4.0,4,4,4,true,"4","4")')
        tdSql.execute(f'insert into tb4 values (now+500s,4,4.0,4.0,4,4,4,true,"4","4")')

        tdSql.query(f"select distinct(tbname), id1 from st2;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from st2;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from st2 where ts between now-50s and now+450s")
        tdSql.checkRows(5)

        tdSql.query(f"select tbname, id1 from st2 where id1 between 2 and 3;")
        tdSql.checkRows(2)

        tdSql.query(f"select tbname, id2 from st2 where id2 between 0.0 and 3.0;")
        tdSql.checkRows(7)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2.00000)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3.00000)

        tdSql.query(f"select tbname, id4 from st2 where id2 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2.00000)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3.00000)

        tdSql.query(f"select tbname, id5 from st2 where id5 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3)

        tdSql.query(f"select tbname,id6 from st2 where id6 between 2.0 and 3.0;")
        tdSql.checkRows(2)
        tdSql.checkKeyData("tb2", 0, "tb2")
        tdSql.checkKeyData("tb2", 1, 2)
        tdSql.checkKeyData("tb3", 0, "tb3")
        tdSql.checkKeyData("tb3", 1, 3)

        tdSql.query(
            f"select * from st2 where f1 between 2 and 3 and f2 between 2.0 and 3.0 and f3 between 2.0 and 3.0 and f4 between 2.0 and 3.0 and f5 between 2.0 and 3.0 and f6 between 2.0 and 3.0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)

        tdSql.query(f"select * from st2 where f7 between 2.0 and 3.0;")
        tdSql.query(f"select * from st2 where f8 between 2.0 and 3.0;")
        tdSql.query(f"select * from st2 where f9 between 2.0 and 3.0;")

        print("\ndo sim case ........................... [passed]")

    #
    # ------------------- 2 ----------------
    #
    def do_query_between(self):

        dbname = "db"
        stb = f"{dbname}.stb1"
        rows = 10

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute(
            f'''create table if not exists {stb}
            (ts timestamp, c1 int, c2 float, c3 bigint, c4 double, c5 smallint, c6 tinyint)
            tags(location binary(64), type int, isused bool , family nchar(64))'''
        )
        tdSql.execute(f"create table {dbname}.t1 using {stb} tags('beijing', 1, 1, 'nchar1')")
        tdSql.execute(f"create table {dbname}.t2 using {stb} tags('shanghai', 2, 0, 'nchar2')")

        tdLog.printNoPrefix("==========step2:insert data")
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.t1 values (now()+{i}m, {32767+i}, {20.0+i/10}, {2**31+i}, {3.4*10**38+i/10}, {127+i}, {i})"
            )
            tdSql.execute(
                f"insert into {dbname}.t2 values (now()-{i}m, {-32767-i}, {20.0-i/10}, {-i-2**31}, {-i/10-3.4*10**38}, {-127-i}, {-i})"
            )
        tdSql.execute(
            f"insert into {dbname}.t1 values (now()+11m, {2**31-1}, {pow(10,37)*34}, {pow(2,63)-1}, {1.7*10**308}, 32767, 127)"
        )
        tdSql.execute(
            f"insert into {dbname}.t2 values (now()-11m, {1-2**31}, {-3.4*10**38}, {1-2**63}, {-1.7*10**308}, -32767, -127)"
        )
        tdSql.execute(
            f"insert into {dbname}.t2 values (now()-12m, null , {-3.4*10**38}, null , {-1.7*10**308}, null , null)"
        )

        tdLog.printNoPrefix("==========step3:query timestamp type")

        tdSql.query(f"select * from {dbname}.t1 where ts between now()-1m and now()+10m")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where ts between '2021-01-01 00:00:00.000' and '2121-01-01 00:00:00.000'")
        # tdSql.checkRows(11)
        tdSql.query(f"select * from {dbname}.t1 where ts between '1969-01-01 00:00:00.000' and '1969-12-31 23:59:59.999'")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where ts between -2793600 and 31507199")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where ts between 1609430400000 and 4765104000000")
        tdSql.checkRows(rows+1)

        tdLog.printNoPrefix("==========step4:query int type")

        tdSql.query(f"select * from {dbname}.t1 where c1 between 32767 and 32776")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 32766.9 and 32776.1")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 32776 and 32767")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c1 between 'a' and 'e'")
        tdSql.checkRows(0)
        # tdSql.query("select * from {dbname}.t1 where c1 between 0x64 and 0x69")
        # tdSql.checkRows(6)
        tdSql.query(f"select * from {dbname}.t1 where c1 not between 100 and 106")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c1 between {2**31-2} and {2**31+1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c1 between null and {1-2**31}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c1 between {-2**31} and {1-2**31}")
        tdSql.checkRows(1)

        tdLog.printNoPrefix("==========step5:query float type")

        tdSql.query(f"select * from {dbname}.t1 where c2 between 20.0 and 21.0")
        tdSql.checkRows(10)
        tdSql.query(f"select * from {dbname}.t1 where c2 between {-3.4*10**38-1} and {3.4*10**38+1}")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c2 between 21.0 and 20.0")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c2 between 'DC3' and 'SYN'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c2 not between 0.1 and 0.2")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c2 between {pow(10,38)*3.4} and {pow(10,38)*3.4+1}")
        # tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c2 between {-3.4*10**38-1} and {-3.4*10**38}")
        # tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.t2 where c2 between null and {-3.4*10**38}")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step6:query bigint type")

        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**31} and {2**31+10}")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {-2**63} and {2**63}")
        tdSql.checkRows(rows+1)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**31+10} and {2**31}")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 between 'a' and 'z'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c3 between {2**63-2} and {2**63-1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c3 between {-2**63} and {1-2**63}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c3 between null and {1-2**63}")
        # tdSql.checkRows(2)

        tdLog.printNoPrefix("==========step7:query double type")

        tdSql.query(f"select * from {dbname}.t1 where c4 between {3.4*10**38} and {3.4*10**38+10}")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {1.7*10**308+1} and {1.7*10**308+2}")
        # 因为精度原因，在超出bigint边界后，数值不能进行准确的判断
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {3.4*10**38+10} and {3.4*10**38}")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between 'a' and 'z'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c4 between {1.7*10**308} and {1.7*10**308+1}")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c4 between {-1.7*10**308-1} and {-1.7*10**308}")
        # tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.t2 where c4 between null and {-1.7*10**308}")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step8:query smallint type")

        tdSql.query(f"select * from {dbname}.t1 where c5 between 127 and 136")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 126.9 and 135.9")
        tdSql.checkRows(rows-1)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 136 and 127")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 between '~' and '^'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 not between 1 and 2")
        # tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c5 between 32767 and 32768")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c5 between -32768 and -32767")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c5 between null and -32767")
        # tdSql.checkRows(1)

        tdLog.printNoPrefix("==========step9:query tinyint type")

        tdSql.query(f"select * from {dbname}.t1 where c6 between 0 and 9")
        tdSql.checkRows(rows)
        tdSql.query(f"select * from {dbname}.t1 where c6 between -1.1 and 8.9")
        tdSql.checkRows(rows-1)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 9 and 0")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 'NUL' and 'HT'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t1 where c6 not between 1 and 2")
        # tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t1 where c6 between 127 and 128")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c6 between -128 and -127")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.t2 where c6 between null and -127")
        # tdSql.checkRows(3)

        tdLog.printNoPrefix("==========step10:invalid query type")

        # TODO tag is not finished
        tdSql.query(f"select * from {stb} where location between 'beijing' and 'shanghai'")
        tdSql.checkRows(rows * 2 + 3)
        # 非0值均解析为1，因此"between 负值 and o"解析为"between 1 and 0"
        tdSql.query(f"select * from {stb} where isused between 0 and 1")
        tdSql.checkRows(rows * 2 + 3)
        tdSql.query(f"select * from {stb} where isused between -1 and 0")
        tdSql.checkRows(rows + 2)
        tdSql.query(f"select * from {stb} where isused between false and true")
        tdSql.checkRows(rows * 2 + 3)
        tdSql.query(f"select * from {stb} where family between '拖拉机' and '自行车'")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("==========step11:query HEX/OCT/BIN type")

        tdSql.error(f"select * from {dbname}.t1 where c6 between 0x7f and 0x80")      # check filter HEX
        tdSql.error(f"select * from {dbname}.t1 where c6 between 0b1 and 0b11111")    # check filter BIN
        tdSql.error(f"select * from {dbname}.t1 where c6 between 0b1 and 0x80")
        tdSql.error(f"select * from {dbname}.t1 where c6=0b1")
        tdSql.error(f"select * from {dbname}.t1 where c6=0x1")
        # 八进制数据会按照十进制数据进行判定
        tdSql.query(f"select * from {dbname}.t1 where c6 between 01 and 0200")        # check filter OCT
        tdSql.checkRows(rows)

        print("do query case ......................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_and_or(self):
        """Operator between and

        1. Comparison of numeric types
        2. Comparison of timestamp types
        3. Multiple between and operators together
        4. Boundary value for numeric types
        5. Between and for tag columns
        6. Invalid between and usage
        7. Null value comparison
        8. Mixed data types comparison

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/between_and.sim
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_between.py

        """
        self.do_sim_between_and()
        self.do_query_between()