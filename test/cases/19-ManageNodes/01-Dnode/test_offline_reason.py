import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestOfflineReason:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_offline_reason(self):
        """dnode offline reason

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated to new test framework, from tsim/dnode/offline_reason.sim

        """

        clusterComCheck.checkDnodes(2)
        
#system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/deploy.sh -n dnode2 -i 2

        tdLog.info(f'========== step1')
#system sh/exec.sh -n dnode1 -s start
        tdSql.connect('root')
        tdSql.execute(f"create dnode {hostname} port 7200")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode2 off: {data}(2)[7]')

        if data(2)[7] != @"status" "not" "received"@ "":

        tdLog.info(f'========== step2')
#system sh/exec.sh -n dnode2 -s start

        x = 0
step2:
	x = x + 1
	sleep 1000
	if x == 10 "":
	          tdLog.info(f'====> dnode not ready!')
        #return -1

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}')
        tdLog.info(f'===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}')
        tdSql.checkRows(2)

        tdSql.checkKeyData(1, 4, "ready")
  goto step2

        tdSql.checkKeyData(2, 4, "ready")
  goto step2

        tdLog.info(f'========== step3')
#system sh/exec.sh -n dnode2 -s stop

        x = 0
step3:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode2 off: {data}(2)[7]')
        if data(2)[7] != @"status" "msg" "timeout"@ "":
	goto step3

        tdLog.info(f'========== step4')
        tdSql.execute(f"drop dnode 2 force")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(1)

        tdLog.info(f'========== step5')
        tdSql.execute(f"create dnode {hostname} port 7200")
#system sh/exec.sh -n dnode2 -s start

return
        x = 0
step5:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode2 off: {data}(3)[7]')
        if data(3)[7] != @"dnodeId" "not" "match"@ "":
	goto step5

        tdLog.info(f'========== step6')
#system sh/deploy.sh -n dnode4 -i 4
#system sh/cfg.sh -n dnode4 -c statusInterval -v 4
#system sh/exec.sh -n dnode4 -s start
        tdSql.execute(f"create dnode {hostname} port 7400")

        x = 0
step6:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode2 off: {data}(3)[7]')
        tdLog.info(f'dnode3 off: {data}(4)[67')
        if data(4)[7] != @"interval" "not" "match"@ "":
	goto step6

        tdLog.info(f'========== step7')
#system sh/deploy.sh -n dnode5 -i 5
#system sh/cfg.sh -n dnode5 -c locale -v zn_CH.UTF-8
#system sh/exec.sh -n dnode5 -s start
        tdSql.execute(f"create dnode {hostname} port 7500")

        x = 0
step7:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode3 off: {data}(3)[7]')
        tdLog.info(f'dnode4 off: {data}(4)[7]')
        tdLog.info(f'dnode5 off: {data}(5)[7]')
        if data(5)[7] != @"locale" "not" "match"@ "":
	goto step7

        tdLog.info(f'========== step8')
#system sh/deploy.sh -n dnode6 -i 6
#system sh/cfg.sh -n dnode6 -c charset -v UTF-16
#system sh/exec.sh -n dnode6 -s start
        tdSql.execute(f"create dnode {hostname} port 7600")

        x = 0
step8:
	x = x + 1
	sleep 1000
	if x == 10 "":

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(f'dnode1 off: {data}(1)[7]')
        tdLog.info(f'dnode3 off: {data}(3)[7]')
        tdLog.info(f'dnode4 off: {data}(4)[7]')
        tdLog.info(f'dnode5 off: {data}(5)[7]')
        tdLog.info(f'dnode6 off: {data}(6)[7]')
        if data(6)[7] != @"charset" "not" "match"@ "":
	goto step8

#system sh/exec.sh -n dnode1 -s stop  -x SIGINT
#system sh/exec.sh -n dnode2 -s stop  -x SIGINT
#system sh/exec.sh -n dnode3 -s stop  -x SIGINT
#system sh/exec.sh -n dnode4 -s stop  -x SIGINT
#system sh/exec.sh -n dnode5 -s stop  -x SIGINT
#system sh/exec.sh -n dnode6 -s stop  -x SIGINT
#system sh/exec.sh -n dnode7 -s stop  -x SIGINT
#system sh/exec.sh -n dnode8 -s stop  -x SIGINT
