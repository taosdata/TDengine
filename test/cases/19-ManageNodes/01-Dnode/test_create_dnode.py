import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestCreateDnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_create_dnode(self):
        """create dnode

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/create_dnode.sim

        """

        clusterComCheck.checkDnodes(2)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
        tdSql.connect('root')

        tdLog.info(f'=============== select * from information_schema.ins_dnodes')
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}')
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

sleep 2000
        tdSql.query(f"select * from information_schema.ins_mnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)}')
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 2, "leader")

        tdLog.info(f'=============== create dnodes')
        tdSql.execute(f"create dnode {hostname} port 7200")
sleep 2000

        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 2)

# check 'vnodes' feild ?
#if $tdSql.getData(0,2) != 0 then
#  return -1
#endi

        tdSql.checkData(1, 2, 0)

        tdSql.checkData(0, 4, "ready")

        tdSql.checkData(1, 4, "ready")

        tdSql.query(f"select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 2, "leader")

        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database d1 vgroups 4;")
        tdSql.execute(f"create database d2;")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.execute(f"use d1")
        tdSql.query(f"show vgroups;")

        tdSql.checkRows(4)

        tdLog.info(f'=============== create table')
        tdSql.execute(f"use d1")

        tdSql.execute(f"create table st (ts timestamp, i int) tags (j int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}')

        tdSql.execute(f"create table c1 using st tags(1)")
        tdSql.execute(f"create table c2 using st tags(2)")
        tdSql.execute(f"create table c3 using st tags(2)")
        tdSql.execute(f"create table c4 using st tags(2)")
        tdSql.execute(f"create table c5 using st tags(2)")

        tdSql.query(f"show tables")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}')

        tdSql.checkRows(5)

        tdLog.info(f'=============== insert data')
        tdSql.execute(f"insert into c1 values(now+1s, 1)")
        tdSql.execute(f"insert into c1 values(now+2s, 2)")
        tdSql.execute(f"insert into c1 values(now+3s, 3)")
        tdSql.execute(f"insert into c2 values(now+1s, 1)")
        tdSql.execute(f"insert into c2 values(now+2s, 2)")
        tdSql.execute(f"insert into c2 values(now+3s, 3)")
        tdSql.execute(f"insert into c3 values(now+1s, 1)")
        tdSql.execute(f"insert into c3 values(now+2s, 2)")
        tdSql.execute(f"insert into c3 values(now+3s, 3)")
        tdSql.execute(f"insert into c4 values(now+1s, 1)")
        tdSql.execute(f"insert into c4 values(now+2s, 2)")
        tdSql.execute(f"insert into c4 values(now+3s, 3)")
        tdSql.execute(f"insert into c5 values(now+1s, 1)")
        tdSql.execute(f"insert into c5 values(now+2s, 2)")
        tdSql.execute(f"insert into c5 values(now+3s, 3)")

        tdLog.info(f'=============== query data')
        tdSql.query(f"select * from c1")
        tdSql.checkRows(3)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(1,1)}')

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)

        tdSql.query(f"select * from c2")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c4")
        tdSql.checkRows(3)

        tdSql.query(f"select * from c5")
        tdSql.checkRows(3)

        tdSql.query(f"select ts, i from st")
        tdSql.checkRows(15)

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
