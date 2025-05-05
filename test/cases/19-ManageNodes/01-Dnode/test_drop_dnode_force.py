import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDropDnodeForce:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_drop_dnode_force(self):
        """drop dnode force

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/drop_dnode_force.sim

        """

        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2
#system sh/deploy.sh -n dnode3 -i 3
#system sh/deploy.sh -n dnode4 -i 4
#system sh/deploy.sh -n dnode5 -i 5
#system sh/cfg.sh -n dnode1 -c supportVnodes -v 0
#system sh/exec.sh -n dnode1 -s start
#system sh/exec.sh -n dnode2 -s start
#system sh/exec.sh -n dnode3 -s start
#system sh/exec.sh -n dnode4 -s start
        tdSql.connect('root')

        tdLog.info(f'=============== step1 create dnode2 dnode3 dnode4 dnode 5')
        tdSql.execute(f"create dnode {hostname} port 7200")
        tdSql.execute(f"create dnode {hostname} port 7300")
        tdSql.execute(f"create dnode {hostname} port 7400")
        tdSql.execute(f"create dnode {hostname} port 7500")

        x = 0
step1:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdSql.checkRows(5)

        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "offline")
        tdLog.info(f'=============== step2 create database')
        tdSql.execute(f"create database d1 vgroups 1 replica 3")
        tdSql.execute(f"use d1")

        wt = 0
stepwt1:
 	wt = wt + 1
 	sleep 1000
 	if wt == 200 "":
 	          tdLog.info(f'====> dnode not ready!')
        #return -1

        tdSql.query(f"show transactions")
        tdSql.checkRows(0)
          tdLog.info(f'wait 1 seconds to alter')
  goto stepwt1

        tdSql.execute(f"create table d1.st0 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c0 using st0 tags(0)")
        tdSql.execute(f"create table d1.c1 using st0 tags(1)")
        tdSql.execute(f"create table d1.c2 using st0 tags(2)")
        tdSql.execute(f"create table d1.c3 using st0 tags(3)")
        tdSql.execute(f"create table d1.c4 using st0 tags(4)")
        tdSql.execute(f"create table d1.c5 using st0 tags(5)")
        tdSql.execute(f"create table d1.c6 using st0 tags(6)")
        tdSql.execute(f"create table d1.c7 using st0 tags(7)")
        tdSql.execute(f"create table d1.c8 using st0 tags(8)")
        tdSql.execute(f"create table d1.c9 using st0 tags(9)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(10)

        tdLog.info(f'd1.rows ===> {tdSql.getRows()})')
        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st0' and db_name = 'd1'")
        tdSql.checkRows(10)

        tdSql.execute(f"create database d2 vgroups 3 replica 1")
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table d2.st1 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c10 using st1 tags(0)")
        tdSql.execute(f"create table d2.c11 using st1 tags(1)")
        tdSql.execute(f"create table d2.c12 using st1 tags(2)")
        tdSql.execute(f"create table d2.c13 using st1 tags(3)")
        tdSql.execute(f"create table d2.c14 using st1 tags(4)")
        tdSql.execute(f"create table d2.c15 using st1 tags(5)")
        tdSql.execute(f"create table d2.c16 using st1 tags(6)")
        tdSql.execute(f"create table d2.c17 using st1 tags(7)")
        tdSql.execute(f"create table d2.c18 using st1 tags(8)")
        tdSql.execute(f"create table d2.c19 using st1 tags(9)")
        tdSql.execute(f"create table d2.c190 using st1 tags(9)")
        tdSql.query(f"show d2.tables")
        tdSql.checkRows(11)

        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st1' and db_name = 'd2'")
        tdLog.info(f'd2.st1.tables ===> {tdSql.getRows()})')
        tdSql.checkRows(11)

        tdSql.execute(f"create table d2.st2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c20 using st2 tags(0)")
        tdSql.execute(f"create table d2.c21 using st2 tags(1)")
        tdSql.execute(f"create table d2.c22 using st2 tags(2)")
        tdSql.execute(f"create table d2.c23 using st2 tags(3)")
        tdSql.execute(f"create table d2.c24 using st2 tags(4)")
        tdSql.execute(f"create table d2.c25 using st2 tags(5)")
        tdSql.execute(f"create table d2.c26 using st2 tags(6)")
        tdSql.execute(f"create table d2.c27 using st2 tags(7)")
        tdSql.execute(f"create table d2.c28 using st2 tags(8)")
        tdSql.execute(f"create table d2.c29 using st2 tags(9)")
        tdSql.execute(f"create table d2.c290 using st2 tags(9)")
        tdSql.execute(f"create table d2.c291 using st2 tags(9)")
        tdSql.query(f"show d2.tables")
        tdSql.checkRows(23)

        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'")
        tdLog.info(f'd2.st2.tables ===> {tdSql.getRows()})')
        tdSql.checkRows(12)

        tdLog.info(f'=============== step3: create qnode snode on dnode 3')
        tdSql.execute(f"create qnode on dnode 3")
        tdSql.execute(f"create snode on dnode 3")
        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(1)

        tdSql.query(f"show snodes")
        tdSql.checkRows(1)

        tdLog.info(f'=============== step4: create mnode on dnode 2')
        tdSql.execute(f"create mnode on dnode 3")
        tdSql.execute(f"create mnode on dnode 2")
        x = 0
step4:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_mnodes -x step4")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(1,4)}')
#if $data(1)[2] != leader then
#  goto step4
#endi
        if data(2)[2] != "follower" "":
  goto step4

#if $data(3)[2] != follower then
#  goto step4
#endi

        tdLog.info(f'=============== step5: create dnode 5')
#system sh/exec.sh -n dnode3 -s stop -x SIGINT
#system sh/exec.sh -n dnode5 -s start
        x = 0
step5:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdLog.info(f'===> {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}')
        tdLog.info(f'===> {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}')
        tdLog.info(f'===> {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}')
        tdSql.checkRows(5)

        tdSql.checkKeyData(1, 4, "ready")
  goto step5

        tdSql.checkKeyData(2, 4, "ready")
  goto step5

        tdSql.checkKeyData(3, 4, "offline")
  goto step5

        tdSql.checkKeyData(4, 4, "ready")
  goto step5

        tdSql.checkKeyData(5, 4, "ready")
  goto step5

        tdLog.info(f'=============== step5a: drop dnode 3')
        tdSql.error(f"drop dnode 3")
        tdSql.error(f"drop dnode 3 force")
        tdSql.execute(f"drop dnode 3 unsafe")

        tdLog.info(f'select * from information_schema.ins_dnodes;')
        tdSql.query(f"select * from information_schema.ins_dnodes;")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdSql.checkRows(4)

        x = 0
step5a:
	 = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not online!')
        #return -1

        tdLog.info(f'select * from information_schema.ins_mnodes;')
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdSql.checkRows(2)
  goto step5a

        if data(1)[2] != "leader" "":
  goto step5a

        tdSql.query(f"select * from information_schema.ins_qnodes")
        tdSql.checkRows(0)

        tdSql.query(f"show snodes")
        tdSql.checkRows(0)

        tdLog.info(f'=============== step6: check d1')
        tdSql.execute(f"reset query cache")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(10)

        tdLog.info(f'=============== step7: check d2')
        tdSql.query(f"show d2.tables")
        tdLog.info(f'===> d2.tables: {tdSql.getRows()}) remained')
        if rows > 23 "":

        if rows <= 0 "":

        tdLog.info(f'=============== step8: drop stable and recreate it')
        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'")
        tdLog.info(f'd2.st2.tables ==> {tdSql.getRows()})')

        tdSql.execute(f"drop table d2.st2;")
        tdSql.execute(f"create table d2.st2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d2.c20 using st2 tags(0)")
        tdSql.execute(f"create table d2.c21 using st2 tags(1)")
        tdSql.execute(f"create table d2.c22 using st2 tags(2)")
        tdSql.execute(f"create table d2.c23 using st2 tags(3)")
        tdSql.execute(f"create table d2.c24 using st2 tags(4)")
        tdSql.execute(f"create table d2.c25 using st2 tags(5)")
        tdSql.execute(f"create table d2.c26 using st2 tags(6)")
        tdSql.execute(f"create table d2.c27 using st2 tags(7)")
        tdSql.execute(f"create table d2.c28 using st2 tags(8)")
        tdSql.execute(f"create table d2.c29 using st2 tags(9)")
        tdSql.execute(f"create table d2.c30 using st2 tags(9)")
        tdSql.execute(f"create table d2.c31 using st2 tags(9)")
        tdSql.execute(f"create table d2.c32 using st2 tags(9)")

        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st2' and db_name = 'd2'")
        tdLog.info(f'd2.st2.tables ==> {tdSql.getRows()})')
        tdSql.checkRows(13)

        tdLog.info(f'=============== step9: alter stable')
return
        tdLog.info(f'By modifying the stable, the missing stable information can be reconstructed in the vnode.')
        tdLog.info(f'However, currently, getting the stable meta from the vnode, and return the table not exist')
        tdLog.info(f'To handle this, we need to modify the way stable-meta is fetched')

        tdSql.query(f"select * from information_schema.ins_tables where stable_name = 'st1' and db_name = 'd2'")
        tdLog.info(f'd2.st1.tables ==> {tdSql.getRows()})')
        remains = rows

        tdSql.execute(f"alter table d2.st1 add column b smallint")
return
        tdSql.execute(f"create table d2.c30 using st tags(0)")
        tdSql.execute(f"create table d2.c31 using st tags(1)")
        tdSql.execute(f"create table d2.c32 using st tags(2)")
        tdSql.execute(f"create table d2.c33 using st tags(3)")
        tdSql.execute(f"create table d2.c34 using st tags(4)")
        tdSql.execute(f"create table d2.c35 using st tags(5)")
        tdSql.execute(f"create table d2.c36 using st tags(6)")
        tdSql.execute(f"create table d2.c37 using st tags(7)")
        tdSql.execute(f"create table d2.c38 using st tags(8)")
        tdSql.execute(f"create table d2.c39 using st tags(9)")
        tdSql.query(f"show d2.tables")
        tdLog.info(f'd2.st1.tables ==> {tdSql.getRows()})')

        total = remains + 10
        tdSql.checkRows(total)

#system sh/exec.sh -n dnode1 -s stop -x SIGINT
#system sh/exec.sh -n dnode2 -s stop -x SIGINT
