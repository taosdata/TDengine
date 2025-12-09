# author : wenzhouwww
from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import time
import os
import threading
sys.path.append(os.path.dirname(__file__))

class Test4dnode1mnodeBasicReplica3InsertdatasQuerys:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.mnode_list = {}
        cls.dnode_list = {}
        cls.ts = 1483200000000
        cls.db_name ='testdb'
        cls.replica = 3 
        cls.vgroups = 1
        cls.tb_nums = 10 
        cls.row_nums = 100
        cls.query_times = 10


    def check_setup_cluster_status(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        for mnode in tdSql.queryResult:
            name = mnode[1]
            info = mnode
            self.mnode_list[name] = info

        tdSql.query("select * from information_schema.ins_dnodes")
        for dnode in tdSql.queryResult:
            name = dnode[1]
            info = dnode
            self.dnode_list[name] = info

        count = 0
        is_leader = False
        mnode_name = ''
        for k,v in self.mnode_list.items():
            count +=1
            # only for 1 mnode
            mnode_name = k

            if v[2] =='leader':
                is_leader=True

        if count==1 and is_leader:
            tdLog.notice("===== depoly cluster success with 1 mnode as leader =====")
        else:
            tdLog.notice("===== depoly cluster fail with 1 mnode as leader =====")

        for k ,v in self.dnode_list.items():
            if k == mnode_name:
                if v[3]==0:
                    tdLog.notice("===== depoly cluster mnode only success at {} , support_vnodes is {} ".format(mnode_name,v[3]))
                else:
                    tdLog.notice("===== depoly cluster mnode only fail at {} , support_vnodes is {} ".format(mnode_name,v[3]))
            else:
                continue

    def create_db_check_vgroups(self):

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database if not exists test replica 1 duration 100")
        tdSql.execute("use test")
        tdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(5):
            tdSql.execute("create table sub_tb_{} using stb1 tags({})".format(i,i))
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("show test.vgroups;")
        vgroups_infos = {}  # key is id: value is info list
        for vgroup_info in tdSql.queryResult:
            vgroup_id = vgroup_info[0]
            tmp_list = []
            for role in vgroup_info[3:-4]:
                if role in ['leader', 'leader*', 'leader**', 'follower']:
                    tmp_list.append(role)
            vgroups_infos[vgroup_id]=tmp_list

        for k , v in vgroups_infos.items():
            if len(v) == 1 and v[0] in ['leader', 'leader*', 'leader**']:
                tdLog.notice(" === create database replica only 1 role leader  check success of vgroup_id {} ======".format(k))
            else:
                tdLog.notice(" === create database replica only 1 role leader  check fail of vgroup_id {} ======".format(k))

    def create_db_replica_3_insertdatas(self, dbname, replica_num ,vgroup_nums ,tb_nums , row_nums ):
        newTdSql=tdCom.newTdSql()
        drop_db_sql = "drop database if exists {}".format(dbname)
        create_db_sql = "create database {} replica {} vgroups {}".format(dbname,replica_num,vgroup_nums)

        tdLog.notice(" ==== create database {} and insert rows begin =====".format(dbname))
        newTdSql.execute(drop_db_sql)
        newTdSql.execute(create_db_sql)
        newTdSql.execute("use {}".format(dbname))
        newTdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        newTdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(tb_nums):
            sub_tbname = "sub_tb_{}".format(i)
            newTdSql.execute("create table {} using stb1 tags({})".format(sub_tbname,i))
            # insert datas about new database

            for row_num in range(row_nums):
                if row_num %100 ==0:
                    tdLog.info(" === writing is going now === ")
                ts = self.ts + 1000*row_num
                newTdSql.execute(f"insert into {sub_tbname} values ({ts}, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")

        tdLog.notice(" ==== create database {} and insert rows execute end =====".format(dbname))

    

    def check_insert_status(self, newTdSql ,dbname, tb_nums , row_nums):
        
        newTdSql.execute("use {}".format(dbname))
        newTdSql.query("select count(*) from {}.{}".format(dbname,'stb1'))
        # tdSql.checkData(0 , 0 , tb_nums*row_nums)
        newTdSql.query("select distinct tbname from {}.{}".format(dbname,'stb1'))
        # tdSql.checkRows(tb_nums)

    def loop_query_constantly(self, times ,  db_name, tb_nums ,row_nums):
        newTdSql=tdCom.newTdSql()
        for loop_time in range(times):
            tdLog.debug(" === query is going ,this is {}_th query === ".format(loop_time))
            self.check_insert_status( newTdSql ,db_name, tb_nums , row_nums)

    def test_4dnode1mnode_basic_replica3_insertdatas_querys(self):
        """Cluster 4 dnodes 1 mnode replica 3 insert-query
        
        1. Create 4 node and 1 mnode cluster
        2. Ensure above cluster setup success
        3. Check mnode is leader and only 1 mnode
        4. Create database with replica 3
        5. Create 1 super table and 1 normal table
        6. Create 5 subtables using super table
        7. Ensure above tables created success
        8. Insert each subtable 100 rows data
        9. Ensure above insert success by query
        10. Check vgroups info , ensure each vgroup only has 1 leader role

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-30 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_4dnode1mnode_basic_replica3_insertdatas_querys.py

        """
        self.check_setup_cluster_status()
        self.create_db_check_vgroups()
        
        # start writing constantly 
        writing = threading.Thread(target = self.create_db_replica_3_insertdatas, args=(self.db_name , self.replica , self.vgroups , self.tb_nums , self.row_nums))
        writing.start()
        tdSql.query(" show {}.stables ".format(self.db_name))
        while not tdSql.queryResult:
            print(tdSql.queryResult)
            time.sleep(0.1)
            tdSql.query(" show {}.stables ".format(self.db_name))

        reading = threading.Thread(target = self.loop_query_constantly, args=(self.query_times,self.db_name , self.tb_nums , self.row_nums))
        reading.start()
        
        writing.join()
        reading.join()

        tdLog.success(f"{__file__} successfully executed")

