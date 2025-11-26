import time
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck,tdCom

class TestUserPrivilegeSysinfo:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------- sim----------------
    #
    def do_user_privilege_sysinfo(self):
        tdLog.info(f"=============== create user and login")
        tdSql.execute(f"create user sysinfo0 pass 'taosdata'")
        tdSql.execute(f"create user sysinfo1 pass 'taosdata'")
        tdSql.execute(f"alter user sysinfo0 sysinfo 0")
        tdSql.execute(f"alter user sysinfo1 sysinfo 1")

        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table db.stb (ts timestamp, i int) tags (t int)")
        tdSql.execute(f"create table db.ctb using db.stb tags (1)")
        tdSql.execute(f"create table db.ntb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.ctb values (now, 1);")
        tdSql.execute(f"insert into db.ntb values (now, 1);")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.ctb")
        tdSql.query(f"select * from db.ntb")

        tdSql.execute(f"create database d2")
        tdSql.execute(f"GRANT all ON d2.* to sysinfo0;")

        tdLog.info(f"user sysinfo0 login")
        tdSql.connect("sysinfo0")

        tdLog.info(f"=============== check oper")
        tdSql.error(f"create user u1 pass 'u1'")
        if platform.system() != "Windows":
            tdSql.query(f"show anodes;")
        tdSql.query(f"show d2.disk_info;")
        tdSql.query(
            f"select * from INFORMATION_SCHEMA.INS_DISK_USAGE where db_name = 'db'"
        )
        tdSql.error(f"drop user sysinfo1")
        tdSql.error(f"alter user sysinfo0 pass '1'")
        tdSql.error(f"alter user sysinfo0 enable 0")
        tdSql.error(f"alter user sysinfo0 enable 1")
        tdSql.error(f"alter user sysinfo1 pass '1'")
        tdSql.error(f"alter user sysinfo1 enable 1")
        tdSql.error(f"alter user sysinfo1 enable 1")
        tdSql.error(f"GRANT read ON db.* to sysinfo0;")
        tdSql.error(f"GRANT read ON *.* to sysinfo0;")
        tdSql.error(f"REVOKE read ON db.* from sysinfo0;")
        tdSql.error(f"REVOKE read ON *.* from sysinfo0;")
        tdSql.error(f"GRANT write ON db.* to sysinfo0;")
        tdSql.error(f"GRANT write ON *.* to sysinfo0;")
        tdSql.error(f"REVOKE write ON db.* from sysinfo0;")
        tdSql.error(f"REVOKE write ON *.* from sysinfo0;")
        tdSql.error(f"REVOKE write ON *.* from sysinfo0;")

        tdSql.error(f"create dnode 'localhost' port 7200")
        tdSql.error(f"drop dnode 1")
        tdSql.error(f"alter dnode 1 'debugFlag 135'")
        tdSql.error(f"alter dnode 1 'dDebugFlag 131'")
        tdSql.error(f"alter dnode 1 'resetlog'")
        tdSql.error(f"alter dnode 1 'monitor' '1'")
        tdSql.error(f"alter dnode 1 'monitor' '0'")
        tdSql.error(f"alter dnode 1 'monitor 1'")
        tdSql.error(f"alter dnode 1 'monitor 0'")

        tdSql.error(f"create qnode on dnode 1")
        tdSql.error(f"drop qnode on dnode 1")

        tdSql.error(f"create mnode on dnode 1")
        tdSql.error(f"drop mnode on dnode 1")

        tdSql.error(f"create snode on dnode 1")
        tdSql.error(f"drop snode on dnode 1")

        tdSql.error(f"redistribute vgroup 2 dnode 1 dnode 2")
        tdSql.error(f"balance vgroup")
        tdSql.error(f"balance vgroup leader")

        tdSql.error(f"kill transaction 1")
        tdSql.error(f"kill connection 1")
        tdSql.error(f"kill query 1")

        tdLog.info(f"=============== check db")
        tdSql.error(f"create database d1")
        tdSql.error(f"drop database db")
        tdSql.error(f"use db")
        tdSql.error(f"alter database db replica 1;")
        tdSql.error(f"alter database db keep 21")
        tdSql.error(f"show db.vgroups")

        tdSql.error(f"create table db.stb1 (ts timestamp, i int) tags (t int)")
        tdSql.error(f"create table db.ctb1 using db.stb1 tags (1)")
        tdSql.error(f"create table db.ntb1 (ts timestamp, i int)")
        tdSql.error(f"insert into db.ctb values (now, 1);")
        tdSql.error(f"insert into db.ntb values (now, 1);")
        tdSql.error(f"select * from db.stb")
        tdSql.error(f"select * from db.ctb")
        tdSql.error(f"select * from db.ntb")

        tdSql.execute(f"use d2")
        tdSql.execute(f"create table d2.stb2 (ts timestamp, i int) tags (t int)")
        tdSql.execute(f"create table d2.ctb2 using d2.stb2 tags (1)")
        tdSql.execute(f"create table d2.ntb2 (ts timestamp, i int)")
        tdSql.execute(f"insert into d2.ctb2 values (now, 1);")
        tdSql.execute(f"insert into d2.ntb2 values (now, 1);")
        tdSql.query(f"select * from d2.stb2")
        tdSql.query(f"select * from d2.ctb2")
        tdSql.query(f"select * from d2.ntb2")

        loop_cnt = 5
        loop_idx = 0
        loop_flag = 0
        print("do sim case ....................... [passed]")

        def loop_check_sysinfo_0():
            tdLog.info(f"=============== check show of sysinfo 0")
            tdSql.error(f"show users")
            tdSql.error(f"show user privileges")
            tdSql.error(f"show cluster")
            tdSql.query(f"show cluster alive")
            tdSql.error(f"select * from information_schema.ins_dnodes")
            tdSql.error(f"select * from information_schema.ins_mnodes")
            tdSql.error(f"show snodes")
            tdSql.error(f"select * from information_schema.ins_qnodes")
            tdSql.error(f"show dnodes")
            tdSql.error(f"show snodes")
            tdSql.error(f"show qnodes")
            tdSql.error(f"show mnodes")
            tdSql.error(f"show db.vgroups")
            tdSql.error(f"show db.stables")
            tdSql.error(f"show db.tables")
            tdSql.error(f"show indexes from stb from db")
            tdSql.query(f"      show databases")
            tdSql.error(f"show d2.vgroups")
            tdSql.query(f"      show d2.stables")
            tdSql.query(f"      show d2.tables")
            tdSql.query(f"      show indexes from stb2 from d2")
            # sql_error show create database db
            tdSql.error(f"show create table db.stb;")
            tdSql.error(f"show create table db.ctb;")
            tdSql.error(f"show create table db.ntb;")
            tdSql.query(f"      show d2.streams")
            tdSql.query(f"      show consumers")
            tdSql.query(f"      show topics")
            tdSql.query(f"      show subscriptions")
            tdSql.query(f"      show functions")
            tdSql.error(f"show grants")
            tdSql.error(f"show grants full;")
            tdSql.error(f"show grants logs;")
            tdSql.error(f"show cluster machines;")
            tdSql.query(f"      show queries")
            tdSql.query(f"      show connections")
            tdSql.query(f"      show apps")
            tdSql.query(f"      show transactions")
            tdSql.error(f"show create database d2")
            tdSql.query(f"      show create table d2.stb2;")
            tdSql.query(f"      show create table d2.ctb2;")
            tdSql.query(f"      show create table d2.ntb2;")
            tdSql.query(f"      show local variables;")
            tdSql.error(f"show dnode 1 variables;")
            tdSql.query(f"      show variables;")

            tdLog.info(f"=============== check information_schema of sysinfo 0")
            tdSql.query(f"show databases")
            tdSql.checkRows(3)

            tdSql.execute(f"use information_schema;")
            tdSql.error(f"select * from information_schema.ins_dnodes")
            tdSql.error(f"select * from information_schema.ins_mnodes")
            tdSql.error(f"select * from information_schema.ins_modules")
            tdSql.error(f"select * from information_schema.ins_qnodes")
            tdSql.error(f"select * from information_schema.ins_cluster")
            tdSql.error(f"select * from information_schema.ins_users")
            tdSql.error(f"select * from information_schema.ins_user_privileges")
            tdSql.query(f"select * from information_schema.ins_databases")
            tdSql.query(f"select * from information_schema.ins_functions")
            tdSql.query(f"select * from information_schema.ins_indexes")
            tdSql.query(f"select * from information_schema.ins_stables")
            tdSql.query(f"select * from information_schema.ins_tables")
            tdSql.query(f"select * from information_schema.ins_tags")
            tdSql.query(f"select * from information_schema.ins_topics")
            tdSql.query(f"select * from information_schema.ins_subscriptions")
            tdSql.query(f"select * from information_schema.ins_streams")
            tdSql.error(f"select * from information_schema.ins_grants")
            tdSql.error(f"select * from information_schema.ins_grants_full")
            tdSql.error(f"select * from information_schema.ins_grants_logs")
            tdSql.error(f"select * from information_schema.ins_machines")
            tdSql.error(f"select * from information_schema.ins_vgroups")
            tdSql.query(f"select * from information_schema.ins_configs")
            tdSql.error(f"select * from information_schema.ins_dnode_variables")

            tdLog.info(f"=============== check performance_schema of sysinfo 0")
            tdSql.execute(f"use performance_schema;")
            tdSql.query(f"select * from performance_schema.perf_connections")
            tdSql.query(f"select * from performance_schema.perf_queries")
            tdSql.query(f"select * from performance_schema.perf_consumers")
            tdSql.query(f"select * from performance_schema.perf_trans")
            tdSql.query(f"select * from performance_schema.perf_apps")

        def loop_check_sysinfo_1():
            tdLog.info(f"=============== check show of sysinfo 1")
            tdSql.query(f"      show users")
            tdSql.query(f"      show user privileges")
            tdSql.query(f"      show cluster")
            tdSql.query(f"      select * from information_schema.ins_dnodes")
            tdSql.query(f"      select * from information_schema.ins_mnodes")
            tdSql.query(f"      show snodes")
            tdSql.query(f"      select * from information_schema.ins_qnodes")
            tdSql.query(f"      show dnodes")
            tdSql.query(f"      show snodes")
            tdSql.query(f"      show qnodes")
            tdSql.query(f"      show mnodes")
            tdSql.query(f"      show db.vgroups")
            tdSql.error(f"show db.stables")
            tdSql.error(f"show db.tables")
            tdSql.query(f"      show indexes from stb from db")
            tdSql.query(f"      show databases")
            tdSql.query(f"      show d2.vgroups")
            tdSql.query(f"      show d2.stables")
            tdSql.query(f"      show d2.tables")
            tdSql.query(f"      show indexes from stb2 from d2")
            # sql_error show create database db
            tdSql.error(f"show create table db.stb;")
            tdSql.error(f"show create table db.ctb;")
            tdSql.error(f"show create table db.ntb;")
            tdSql.query(f"      show d2.streams")
            tdSql.query(f"      show consumers")
            tdSql.query(f"      show topics")
            tdSql.query(f"      show subscriptions")
            tdSql.query(f"      show functions")
            tdSql.query(f"      show grants")
            tdSql.query(f"      show grants full;")
            tdSql.query(f"      show grants logs;")
            tdSql.query(f"      show cluster machines;")
            tdSql.query(f"      show queries")
            tdSql.query(f"      show connections")
            tdSql.query(f"      show apps")
            tdSql.query(f"      show transactions")
            tdSql.query(f"      show create database d2")
            tdSql.query(f"      show create table d2.stb2;")
            tdSql.query(f"      show create table d2.ctb2;")
            tdSql.query(f"      show create table d2.ntb2;")
            tdSql.query(f"      show local variables;")
            tdSql.query(f"      show dnode 1 variables;")
            tdSql.query(f"      show variables;")

            tdLog.info(f"=============== check information_schema of sysinfo 1")
            tdSql.query(f"show databases")
            tdSql.checkRows(3)

            tdSql.execute(f"use information_schema;")
            tdSql.query(f"select * from information_schema.ins_dnodes")
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.error(f"select * from information_schema.ins_modules")
            tdSql.query(f"select * from information_schema.ins_qnodes")
            tdSql.query(f"select * from information_schema.ins_cluster")
            tdSql.query(f"select * from information_schema.ins_users")
            tdSql.query(f"select * from information_schema.ins_user_privileges")
            tdSql.query(f"select * from information_schema.ins_databases")
            tdSql.query(f"select * from information_schema.ins_functions")
            tdSql.query(f"select * from information_schema.ins_indexes")
            tdSql.query(f"select * from information_schema.ins_stables")
            tdSql.query(f"select * from information_schema.ins_tables")
            tdSql.query(f"select * from information_schema.ins_tags")
            tdSql.query(f"select * from information_schema.ins_topics")
            tdSql.query(f"select * from information_schema.ins_subscriptions")
            tdSql.query(f"select * from information_schema.ins_streams")
            tdSql.query(f"select * from information_schema.ins_grants")
            tdSql.query(f"select * from information_schema.ins_grants_full")
            tdSql.query(f"select * from information_schema.ins_grants_logs")
            tdSql.query(f"select * from information_schema.ins_machines")
            tdSql.query(f"select * from information_schema.ins_vgroups")
            tdSql.query(f"select * from information_schema.ins_configs")
            tdSql.query(f"select * from information_schema.ins_dnode_variables")

            tdLog.info(f"=============== check performance_schema of sysinfo 1")
            tdSql.execute(f"use performance_schema;")
            tdSql.query(f"select * from performance_schema.perf_connections")
            tdSql.query(f"select * from performance_schema.perf_queries")
            tdSql.query(f"select * from performance_schema.perf_consumers")
            tdSql.query(f"select * from performance_schema.perf_trans")
            tdSql.query(f"select * from performance_schema.perf_apps")

        loop_check_sysinfo_0()

        while loop_idx <= loop_cnt:
            loop_idx = loop_idx + 1
            tdSql.connect("root")

            if loop_flag == 0:
                loop_flag = 1
                tdSql.execute("alter user sysinfo0 sysinfo 1")
                tdSql.connect("sysinfo0")
                loop_check_sysinfo_1()
            else:
                loop_flag = 0
                tdSql.execute("alter user sysinfo0 sysinfo 0")
                tdSql.connect("sysinfo0")
                loop_check_sysinfo_0()

    #
    # ------------------- test_TS_3581.py ----------------
    #
    def prepare_user(self):
        tdSql.execute(f"create user test pass 'test123@#$' sysinfo 1")

    def check_connect_user(self, uname):
        try:
            for db in ['information_schema', 'performance_schema']:
                new_tdsql = tdCom.newTdSql(user=uname, password=self.passwd[uname], database=db)
                new_tdsql.query('show databases')
                new_tdsql.checkData(0, 0, 'information_schema')
                new_tdsql.checkData(1, 0, 'performance_schema')
                tdLog.success(f"Test User {uname} for {db} .......[OK]")
        except:
            tdLog.exit(f'{__file__} failed')
    
    def do_ts_5130(self):
        tdSql.connect('root')
        self.passwd = {'root':'taosdata',
                       'test':'test123@#$'}

        self.prepare_user()
        self.check_connect_user('root')
        self.check_connect_user('test')

        print("do TS-5130 ............................ [passed]")
    
    #
    # ------------------- main ----------------
    #
    def test_user_privilege_sysinfo(self):
        """Privilege: sysinfo

        1. Verify user privileges related to sysinfo operation, including grant, revoke, and query privileges.
        2. Verify bug TS-5130 (normal user with sysinfo privilege cannot access information_schema)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_sysinfo.sim
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TS_5130.py

        """
        self.do_user_privilege_sysinfo()
        self.do_ts_5130()