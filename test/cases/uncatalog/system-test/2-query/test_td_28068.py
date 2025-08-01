from new_test_framework.utils import tdLog, tdSql

class TestTd28068:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def test_td_28068(self):
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
        tdSql.execute("drop database if exists td_28068;")
        tdSql.execute("create database td_28068;")
        tdSql.execute("create database if not exists td_28068;")
        tdSql.execute("create stable td_28068.st (ts timestamp, test_case nchar(10), time_cost float, num float) tags (branch nchar(10), scenario nchar(10));")
        tdSql.execute("insert into td_28068.ct1 using td_28068.st (branch, scenario) tags ('3.0', 'scenario1') values (1717122943000, 'query1', 1,2);")
        tdSql.execute("insert into td_28068.ct1 using td_28068.st (branch, scenario) tags ('3.0', 'scenario1') values (1717122944000, 'query1', 2,3);")
        tdSql.execute("insert into td_28068.ct2 using td_28068.st (branch, scenario) tags ('3.0', 'scenario2') values (1717122945000, 'query1', 10,1);")
        tdSql.execute("insert into td_28068.ct2 using td_28068.st (branch, scenario) tags ('3.0', 'scenario2') values (1717122946000, 'query1', 11,5);")
        tdSql.execute("insert into td_28068.ct3 using td_28068.st (branch, scenario) tags ('3.1', 'scenario1') values (1717122947000, 'query1', 20,4);")
        tdSql.execute("insert into td_28068.ct3 using td_28068.st (branch, scenario) tags ('3.1', 'scenario1') values (1717122948000, 'query1', 30,1);")
        tdSql.execute("insert into td_28068.ct4 using td_28068.st (branch, scenario) tags ('3.1', 'scenario2') values (1717122949000, 'query1', 8,8);")
        tdSql.execute("insert into td_28068.ct4 using td_28068.st (branch, scenario) tags ('3.1', 'scenario2') values (1717122950000, 'query1', 9,10);")


        tdSql.query('select last(ts) as ts, last(branch) as branch, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch);')
        tdSql.checkRows(4)
        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch), last(scenario); ')
        tdSql.checkRows(4)
        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch); ')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case)  from td_28068.st group by st.branch, st.scenario order by last(branch), last(test_case);')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch), last(scenario);')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by branch1, scenario1;')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by tbname; ')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by test_case;')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case1  from td_28068.st group by st.branch, st.scenario order by last(test_case);')
        tdSql.checkRows(4)

        tdSql.query('select time_cost, num, time_cost + num as final_cost  from td_28068.st partition by st.branch; ')
        tdSql.checkRows(8)

        tdSql.query('select count(*) from td_28068.st partition by branch order by branch; ')
        tdSql.checkRows(2)

        tdSql.query('select time_cost, num, time_cost + num as final_cost from td_28068.st order by time_cost;')
        tdSql.checkRows(8)

        tdSql.query('select time_cost, num, time_cost + num as final_cost from td_28068.st order by final_cost;')
        tdSql.checkRows(8)

        tdSql.execute("drop database if exists td_28068;")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
