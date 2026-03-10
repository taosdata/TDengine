import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck, cluster, TDSetSql

class TestBalanceReplica3:
    updatecfgDict = {
        "supportVnodes":"1000",
    }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------- main ----------------
    #
    def do_balance_replica_3(self):
        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        sc.dnodeStop(5)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"=============== step1 create dnode2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 4 replica 3")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step32 wait vgroup2")
        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step33 wait vgroup3")

        tdLog.info(f"=============== step34 wait vgroup4")

        tdLog.info(f"=============== step35 wait vgroup5")

        tdLog.info(f"=============== step36: create table")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1.c1 using st tags(1)")
        tdSql.execute(f"create table d1.c2 using st tags(1)")
        tdSql.execute(f"create table d1.c3 using st tags(1)")
        tdSql.execute(f"create table d1.c4 using st tags(1)")
        tdSql.execute(f"create table d1.c5 using st tags(1)")
        tdSql.execute(f"create table d1.c6 using st tags(1)")
        tdSql.query(f"show d1.tables")
        tdSql.checkRows(6)

        tdLog.info(f"=============== step4: start dnode5")
        sc.dnodeStart(5)
        clusterComCheck.checkDnodes(5)

        tdLog.info(f"=============== step5: balance")
        tdSql.execute(f"balance vgroup")

        tdLog.info(f"=============== step62 wait vgroup2")
        clusterComCheck.checkDbReady("d1")

        tdLog.info(f"=============== step63 wait vgroup3")

        tdLog.info(f"=============== step64 wait vgroup4")

        tdLog.info(f"=============== step65 wait vgroup5")

        tdLog.info(f"=============== step7: select data")
        tdSql.query(f"show d1.tables")
        tdLog.info(f"rows {tdSql.getRows()})")
        tdSql.checkRows(6)
        tdSql.execute(f"drop database d1")
        
        print("do sim balance replica 3 .............. [passed]")

    #
    # ------------------- test_balance_vgroups_r1.py ----------------
    #
    def init_class(self):
        tdLog.debug("start to execute %s" % __file__)
        self.dnode_num=len(cluster.dnodes)
        self.dbname = 'db_test'
        self.setsql = TDSetSql()
        self.stbname = f'{self.dbname}.stb'
        self.rowNum = 5
        self.tbnum = 10
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }
        self.replica = [1,3]

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def prepare_data(self,dbname,stbname,column_dict,tbnum,rowNum,replica):
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {dbname} vgroups 1 replica {replica} ")
        tdSql.execute(f'use {dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,column_dict,tag_dict))
        for i in range(tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',rowNum)
    def redistribute_vgroups(self,replica,stbname,tbnum,rownum):
        tdSql.query('show vgroups')
        vnode_id = tdSql.queryResult[0][0]
        if replica == 1:
            for dnode_id in range(1,self.dnode_num+1) :
                tdSql.execute(f'redistribute vgroup {vnode_id} dnode {dnode_id}')
                tdSql.query(f'select count(*) from {stbname}')
                tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        elif replica == 3:
            for dnode_id in range(1,self.dnode_num-1):
                tdSql.execute(f'redistribute vgroup {vnode_id} dnode {dnode_id} dnode {dnode_id+1} dnode {dnode_id+2}')
                tdSql.query(f'select count(*) from {stbname}')
                tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        
    def do_balance_vgroups_r1(self):
        self.init_class()
        for replica in self.replica:
            self.prepare_data(self.dbname,self.stbname,self.column_dict,self.tbnum,self.rowNum,replica)
            self.redistribute_vgroups(replica,self.stbname,self.tbnum,self.rowNum)
            tdSql.execute(f'drop database {self.dbname}')

        print("do balance vgroup r1 .................. [passed]")

    #
    # ------------------- main ----------------
    #
    def test_balance_replica_3(self):
        """Balance: replica-3

        1. Create a 3-replica database with 4 vgroups and insert data
        2. Start a new dnode and add it to the cluster
        3. Execute BALANCE VGROUP
        4. Verify vnode distribution and data integrity

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/balance_replica3.sim
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_balance_vgroups_r1.py

        """
        self.do_balance_vgroups_r1()
        self.do_balance_replica_3()