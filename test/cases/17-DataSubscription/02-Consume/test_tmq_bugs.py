import taos
import sys
import time
import socket
import os
import threading
import subprocess
from  datetime import datetime

from new_test_framework.utils import tdLog, tdSql, etool, tdCom, tdDnodes
from taos.tmq import *
from taos import *


class TestTmqBugs:
    updatecfgDict = {
        'debugFlag': 135, 
        'asynclog': 0
    }

    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    #
    # ------------------- 1 ----------------
    #
    def do_td31283(self):
        tdSql.execute(f'create database d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)
        
        tdSql.error(f'create topic t1 with meta as database d2', expectErrInfo="Database not exist")
        tdSql.error(f'create topic t1 as database d2', expectErrInfo="Database not exist")
        tdSql.error(f'create topic t2 as select * from st2', expectErrInfo="Table does not exist")
        tdSql.error(f'create topic t3 as stable st2', expectErrInfo="STable not exist")
        tdSql.error(f'create topic t3 with meta as stable st2', expectErrInfo="STable not exist")

        tdSql.execute(f'create topic t1 with meta as database d1')
        
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            # "msg.enable.batchmeta": "true",
            "experimental.snapshot.enable": "true",
        }
        consumer1 = Consumer(consumer_dict)

        try:
            consumer1.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer1.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer1.close()


        tdSql.query(f'show consumers')
        tdSql.checkRows(0)

        tdSql.execute(f'drop topic t1')
        tdSql.execute(f'drop database d1')
        print("bug TD-31283 ................ [passed]")

    #
    # ------------------- 2 ----------------
    #
    def do_td30270(self):
        tdSql.execute(f'create database if not exists d1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')

        tdSql.execute(f'create topic topic_all as select * from st')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.unsubscribe()
            consumer.unsubscribe()
            consumer.subscribe(["topic_all"])
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(2)
                if not res:
                    break
                val = res.value()
                if val is None:
                    print(f"null val")
                    continue
                for block in val:
                    cnt += len(block.fetchall())

                print(f"block {cnt} rows")

        finally:
            consumer.unsubscribe()
            consumer.close()
        
        print("bug TD-30270 ................ [passed]")

    #
    # ------------------- 3 ----------------
    #
    def do_td32187(self):
        tdSql.execute(f'create database if not exists db_32187')
        tdSql.execute(f'use db_32187')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 0, 1)')
        tdSql.execute(f'insert into t1(ts, c1, c2) values(1669092069067, 0, 1)')

        tdSql.execute("create topic topic_test with meta as database db_32187")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32187'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        
        print("bug TD-32187 ................ [passed]")

    #
    # ------------------- 4 ----------------
    #
    def do_td33225(self):
        tdSql.execute(f'create database if not exists db_33225')
        tdSql.execute(f'use db_33225')
        tdSql.execute(f'create stable if not exists s33225 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 int)')
        tdSql.execute(f'insert into t1 using s33225 tags("__devicid__", 1) values(1669092069068, 0, 1)')
        tdSql.execute("create topic db_33225_topic as select ts,c1,t2 from s33225")
        tdSql.execute(f'alter table s33225 modify column c2 COMPRESS "zlib"')
        tdSql.execute(f'create index dex1 on s33225(t2)')
        
        print("bug TD-33225 ................ [passed]")

    #
    # ------------------- 5 ----------------
    #
    def do_td32471(self):
        tdSql.execute(f'create database if not exists db_32471')
        tdSql.execute(f'use db_32471')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32471'%(buildPath)
        # tdLog.info(cmdStr)
        # os.system(cmdStr)
        #
        # tdSql.execute("drop topic db_32471_topic")
        tdSql.execute(f'alter stable meters add column  item_tags nchar(500)')
        tdSql.execute(f'alter stable meters add column  new_col nchar(100)')
        tdSql.execute("create topic db_32471_topic as select * from db_32471.meters")

        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-06 14:38:05.000',10.30000,219,0.31000, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '1')")

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)
        
        print("bug TD-32471 ................ [passed]")

    #
    # ------------------- 6 ----------------
    #
    def do_td32526(self):
        tdSql.execute(f'create database if not exists td32526')
        tdSql.execute(f'use td32526')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32526'%(buildPath)
        tdSql.execute(f'alter stable meters add column  item_tags nchar(500)')
        tdSql.execute(f'alter stable meters add column  new_col nchar(100)')
        tdSql.execute("create topic td32526_topic as select * from td32526.meters")

        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-06 14:38:05.000',10.30000,219,0.31000, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '1')")

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)
        
        print("bug TD-32526 ................ [passed]")

    #
    # ------------------- 7 ----------------
    #
    def do_td33504(self):
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database db')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1002 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1003 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1004 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        tdSql.execute(f'create topic t0 as select * from meters')
        tdSql.execute(f'create topic t1 as select * from meters')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            res = consumer.poll(1)
            print(res)

            consumer.unsubscribe()

            try:
                consumer.subscribe(["t1"])
            except TmqError:
                tdLog.exit(f"subscribe error")


            res = consumer.poll(1)
            print(res)
            if res == None and taos_errno(None) != 0:
                tdLog.exit(f"poll error %d" % taos_errno(None))

        except TmqError:
            tdLog.exit(f"poll error")
        finally:
            consumer.close()
        
        tdSql.execute(f'drop topic t0')
        tdSql.execute(f'drop topic t1')
        
        print("bug TD-33504 ................ [passed]")       

    #
    # ------------------- 8 ----------------
    #
    def do_td35698(self):
        tdSql.execute(f'create database if not exists db_taosx')
        tdSql.execute(f'create database if not exists td35698')
        tdSql.execute(f'use td35698')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 decimal(8,2)) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 99.98)')
        tdSql.execute(f'flush database td35698')

        tdSql.execute("create topic td35698_topic with meta as database td35698")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td35698'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        
        print("bug TD-35698 ................ [passed]")

    #
    # ------------------- 9 ----------------
    #
    def do_td37436(self):
        tdSql.execute(f'create snode on dnode 1')
        tdSql.execute(f'drop database if exists test')
        tdSql.execute(f'create database test vgroups 1')
        tdSql.execute(f'use test')
        tdSql.execute(f'create table stream_query (ts timestamp, id int)')
        tdSql.execute(f'create stream s1 session (ts, 1s) from stream_query stream_options(fill_history) into stream_out as select _twstart, avg(id) from stream_query')
        while 1:
            tdSql.query("show test.streams")
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getData(0, 1) == "Running":
                break
            else:
                time.sleep(0.5)

        tdSql.execute(f"insert into test.stream_query values ('2025-01-01 00:00:01', 0), ('2025-01-01 00:00:04', 1), ('2025-01-01 00:00:05', 2)")

        tdSql.execute("create topic tt with meta as database test")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td37436'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            raise Exception("run failed")
        tdSql.execute("drop topic tt")
        
        print("bug TD-37436 ................ [passed]")

    #
    # ------------------- 10 ----------------
    #
    def do_td38404(self):
        tdSql.execute(f'create database if not exists db_td38404 vgroups 1')
        tdSql.execute(f'use db_td38404')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 nchar(4))')
        tdSql.execute(f'insert into t1 using s5466 tags("","") values(1669092069068, 0, 1)')

        tdSql.execute("create topic db_38404_topic with meta as database db_td38404")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td38404'%(buildPath)
        # cmdStr = '/Users/mingming/code/TDengine2/debug/build/bin/tmq_td38404'
        tdLog.info(cmdStr)
        os.system(cmdStr)
        
        print("bug TD-38404 ................ [passed]")

    #
    # ------------------- 11 ----------------
    #
    def consumeTest_TS_4563(self):
        tdSql.execute(f'use db_stmt')

        tdSql.query("select ts,k from st")
        tdSql.checkRows(self.expected_affected_rows)

        tdSql.execute(f'create topic t_unorder_data as select ts,k from st')
        tdSql.execute(f'create topic t_unorder_data_none as select i,k from st')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t_unorder_data", "t_unorder_data_none"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(1)
                print(res)
                if not res:
                    if cnt == 0 or  cnt != 2*self.expected_affected_rows:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    print(block.fetchall(),len(block.fetchall()))
                    cnt += len(block.fetchall())
        finally:
            consumer.close()


    def newcon(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        print(con)
        return con

    def check_stmt_insert_multi(self,conn):
        # type: (TaosConnection) -> None

        dbname = "db_stmt"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table st(ts timestamp, i int, j int, k int)",
            )
            # conn.load_table_info("log")
            tdLog.debug("statement start")
            start = datetime.now()
            stmt = conn.statement("insert into st(ts,j) values(?, ?)")

            params = new_multi_binds(2)
            params[0].timestamp((1626861392589, 1626861392590))
            params[1].int([3, None])
      
            # print(type(stmt))
            tdLog.debug("bind_param_batch start")
            stmt.bind_param_batch(params)

            tdLog.debug("bind_param_batch end")
            stmt.execute()
            tdLog.debug("execute end")
            conn.execute("flush  database %s" % dbname)

            params1 = new_multi_binds(2)
            params1[0].timestamp((1626861392587,1626861392586))
            params1[1].int([None,3])     
            stmt.bind_param_batch(params1)
            stmt.execute()

            end = datetime.now()
            print("elapsed time: ", end - start)
            print(stmt.affected_rows)
            self.expected_affected_rows = 4
            if stmt.affected_rows != self.expected_affected_rows :
                tdLog.exit("affected_rows error")
            tdLog.debug("close start")

            stmt.close()
            
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def do_ts4563(self):
        buildPath = tdCom.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.check_stmt_insert_multi(connectstmt)
        self.consumeTest_TS_4563()
        
        print("bug TS-4563 ................ [passed]")
    
    #
    # ------------------- 12 ----------------
    #
    def do_ts5466(self):
        tdSql.execute(f'create database if not exists db_taosx')
        tdSql.execute(f'create database if not exists db_5466')
        tdSql.execute(f'use db_5466')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 0, 1)')
        for i in range(80):
            if i < 3:
                continue
            tdSql.execute(f'alter stable s5466 add column c{i} int')
        tdSql.execute(f'insert into t1(ts, c1, c2) values(1669092069067, 0, 1)')
        tdSql.execute(f'flush database db_5466')

        tdSql.execute("create topic db_5466_topic with meta as database db_5466")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts5466'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        
        print("bug TS-5466 ................ [passed]")
    
    #
    # ------------------- 13 ----------------
    #
    def do_ts5906(self):
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database db vgroups 1')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco1', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        tdSql.execute(f'create topic t0 as select * from meters')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                if index == 2:
                    break
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(f"data len: {len(data)}")
                        print(element)
                    if index == 0 and data[0][-1] != 2:
                        tdLog.exit(f"error: {data[0][-1]}")
                    if index == 1 and data[0][-1] != 100:
                        tdLog.exit(f"error: {data[0][-1]}")

                tdSql.execute("alter table d1001 set tag groupId = 100")
                tdSql.execute("INSERT INTO d1001 VALUES('2018-10-05 14:38:06.000',10.30000,219,0.31000)")
                index += 1
        finally:
            consumer.close()
            
        tdSql.execute(f'drop topic t0')
        
        print("bug TS-5906 ................ [passed]")
    
    #
    # ------------------- 14 ----------------
    #
    def do_ts6115(self):
        insertJson = '''{
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "localhost",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "connection_pool_size": 10,
            "thread_count": 10,
            "create_table_thread_count": 10,
            "result_file": "./insert-2-2-1.txt",
            "confirm_parameter_prompt": "no",
            "num_of_records_per_req": 3600,
            "prepared_rand": 3600,
            "chinese": "no",
            "escape_character": "yes",
            "continue_if_fail": "no",
            "databases": [
                {
                    "dbinfo": {
                        "name": "ts6115",
                        "drop": "yes",
                        "vgroups": 10,
                        "precision": "ms",
                "buffer": 512,
                "cachemodel":"'both'",
                "stt_trigger": 1
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "no",
                            "childtable_count": 10000,
                            "childtable_prefix": "d_",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 10,
                            "data_source": "csv",
                            "insert_mode": "stmt",
                            "non_stop_mode": "no",
                            "line_protocol": "line",
                            "insert_rows": 1000,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "partial_col_num": 0,
                            "timestamp_step": 1000,
                            "start_timestamp": "2024-11-01 00:00:00.000",
                            "sample_format": "csv",
                            "sample_file": "./td_double10000_juchi.csv",
                            "use_sample_ts": "no",
                            "tags_file": "",
                            "columns": [
                                {"type": "DOUBLE", "name": "val"},
                                { "type": "INT", "name": "quality"}
                            ],
                            "tags": [
                                {"type": "INT", "name": "id", "max": 100, "min": 1}
                            ]
                        }
                    ]
                }
            ]
        }'''

        with open('ts-6115.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f ts-6115.json")
        if os.system("taosBenchmark -f ts-6115.json") != 0:
            tdLog.exit("taosBenchmark -f ts-6115.json")

        tdLog.info("test ts-6115 ......")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts6115'%(buildPath)

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)
        
        print("bug TS-6115 ................ [passed]")

    #
    # ------------------- 15 ----------------
    #
    def do_ts6392(self):
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1 wal_retention_period 10')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1002 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1003 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1004 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        tdSql.execute(f'create topic t0 as select * from meters')

        consumer_dict = {
            "group.id": "g0",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "session.timeout.ms": "100000",
        }

        consumer_dict1 = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "session.timeout.ms": "6000",
        }

        consumer = Consumer(consumer_dict)
        consumer1 = Consumer(consumer_dict1)

        try:
            consumer.subscribe(["t0"])
            consumer1.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        res = consumer.poll(1)
        res1 = consumer1.poll(1)
        print(res)
        print(res1)

        time.sleep(10)
        tdSql.execute(f'flush database db')

        tdSql.execute("INSERT INTO d1003 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:48:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1004 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:48:15.000',10.30000,219,0.31000)")

        time.sleep(5)
        tdSql.execute(f'flush database db')

        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)

        tdSql.execute(f'use db')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:58:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:58:06.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:58:07.000',10.30000,219,0.31000)")
        time.sleep(5)
        tdSql.execute(f'flush database db')

        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)

        consumer1.unsubscribe()
        try:
            consumer1.subscribe(["t0"])
            res1 = consumer1.poll(1)
            print(res1)
            if res1 == None:
                tdLog.exit(f"poll g1 error %d" % taos_errno(None))
        except TmqError:
            tdLog.exit(f"poll g1 error")
        finally:
            consumer1.close()


        try:
            res = consumer.poll(1)
            print(res)
            if res == None:
                tdLog.exit(f"poll g0 error %d" % taos_errno(None))

        except TmqError:
            tdLog.exit(f"poll g0 error")
        finally:
            consumer.close()
            
        tdSql.execute(f'drop topic t0')
        tdSql.execute(f'drop database db')
        
        print("bug TS-6392 ................ [passed]")

    #
    # ------------------- 16 ----------------
    #
    def do_ts7402(self):
        tdSql.execute(f'drop topic if exists topic1')
        tdSql.execute(f'drop topic if exists topic2')
        tdSql.execute(f'drop database if exists test')
        tdSql.execute(f'create database test vgroups 1')
        tdSql.execute(f'use test')
        tdSql.execute(f'create table st (ts timestamp, id int) tags (t1 int)')

        tdSql.execute("create topic topic1 with meta as stable st where t1 = 2")
        tdSql.execute("create topic topic2 with meta as database test")

        tdSql.execute(f'create table nt (ts timestamp, id int)')
        tdSql.execute(f'create table t1 using st tags(1)')
        tdSql.execute(f'create table t2 using st tags(2)')
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:01', 0)")
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:02', 0)")
        tdSql.execute(f"insert into t3 using st tags(2) values ('2025-01-02 00:00:03', 0)")
        tdSql.execute(f"insert into t4 using st tags(3) values ('2025-01-02 00:00:04', 0)")
        tdSql.execute(f'alter table t1 set tag t1=2')
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:05', 0)")
        tdSql.execute(f'alter table t2 set tag t1=23')
        tdSql.execute(f"insert into t2 values ('2025-01-01 00:00:06', 0)")
        tdLog.info("write data done, wait tmq process exit")
        
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts7402'%(buildPath)
        # cmdStr = '/Users/mingming/code/TDengine2/debug/build/bin/tmq_ts7402'
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            raise Exception("run failed")
        
        print("bug TS-7402 ................ [passed]")

    #
    # ------------------- 17 ----------------
    #
    def do_ts7662(self):
        tdSql.execute(f'create database if not exists db_7662 vgroups 1')
        tdSql.execute(f'use db_7662')
        tdSql.execute(f'create stable if not exists ts7662 (ts timestamp, c1 decimal(8,2)) tags (t binary(32))')
        tdSql.execute(f'create table t1 using ts7662 tags("t1") t2 using ts7662 tags("t2")')

        tdSql.execute("create topic topic_ts7662 with meta as stable ts7662 where t='t1'")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts7662'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("varbinary_test ret != 0")
        
        print("bug TS-7662 ................ [passed]")        

    #
    # ------------------- 18 ----------------
    #
    def get_leader(self):
        tdLog.debug("get leader")
        tdSql.query("show vnodes")
        for result in tdSql.queryResult:
            if result[3] == 'leader':
                tdLog.debug("leader is %d"%(result[0]))
                return result[0]
        return -1

    def balance_vnode(self):
        leader_before = self.get_leader()
        
        while True:
            leader_after = -1
            tdLog.debug("balancing vgroup leader")
            tdSql.execute("balance vgroup leader")
            while True:
                tdLog.debug("get new vgroup leader")
                leader_after = self.get_leader()
                if leader_after != -1 :
                    break
                else:
                    time.sleep(1)
            if leader_after != leader_before:
                tdLog.debug("leader changed")
                break
            else :
                time.sleep(1)
                tdLog.debug("leader not changed")

    def do_ts4674(self):
        tdSql.execute(f'create database d1 replica 3 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')

        tdSql.execute(f'create topic topic_all as select * from st')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        balance = False
        try:
            while True:
                res = consumer.poll(2)
                if not res:
                    print(f"null res")
                    if balance == False and cnt != 6 :
                        tdLog.exit(f"subscribe num != 6")
                    if balance == True :
                        if cnt != 8 :
                            tdLog.exit(f"subscribe num != 8")
                            # tdLog.debug(f"subscribe num != 8")
                            # continue
                        else :
                            break
                    self.balance_vnode()
                    balance = True
                    tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')
                    continue
                val = res.value()
                if val is None:
                    print(f"null val")
                    continue
                for block in val:
                    cnt += len(block.fetchall())

                print(f"block {cnt} rows")

        finally:
            consumer.close()

        tdSql.execute(f'drop topic topic_all')
        tdSql.execute(f'drop database d1')        
        print("bug TS-4674 ................ [passed]") 

    #
    # ------------------- 19 ----------------
    #
    def do_td_tmq_token(self):
        insertJson = '''{
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "localhost",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "connection_pool_size": 10,
            "thread_count": 10,
            "create_table_thread_count": 10,
            "result_file": "./insert-2-2-1.txt",
            "confirm_parameter_prompt": "no",
            "num_of_records_per_req": 3600,
            "prepared_rand": 3600,
            "chinese": "no",
            "escape_character": "yes",
            "continue_if_fail": "no",
            "databases": [
                {
                    "dbinfo": {
                        "name": "tmq_token",
                        "drop": "yes",
                        "vgroups": 10,
                        "precision": "ms",
                "buffer": 512,
                "cachemodel":"'both'",
                "stt_trigger": 1
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "no",
                            "childtable_count": 10000,
                            "childtable_prefix": "d_",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 10,
                            "data_source": "csv",
                            "insert_mode": "stmt",
                            "non_stop_mode": "no",
                            "line_protocol": "line",
                            "insert_rows": 100,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "partial_col_num": 0,
                            "timestamp_step": 1000,
                            "start_timestamp": "2024-11-01 00:00:00.000",
                            "sample_format": "csv",
                            "sample_file": "./td_double10000_juchi.csv",
                            "use_sample_ts": "no",
                            "tags_file": "",
                            "columns": [
                                {"type": "DOUBLE", "name": "val"},
                                { "type": "INT", "name": "quality"}
                            ],
                            "tags": [
                                {"type": "INT", "name": "id", "max": 100, "min": 1}
                            ]
                        }
                    ]
                }
            ]
        }'''

        with open('insert.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f insert.json")
        if os.system("taosBenchmark -f insert.json") != 0:
            tdLog.exit("taosBenchmark -f insert.json")

        tdLog.info("test tmq_token insert done ......")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_token'%(buildPath)

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)

        print("test tmq_token ................ [passed]")       

    #
    # ------------------- main ----------------
    #
    def test_tmq_bugs(self):
        """Consumer bugs

        1. Jira TD-31283:
         - Tmq consumer fails to consume data from topic created on stable with tags after altering stable to add columns.
        2. Jira TD-30270:
         - Test tmq consumer subscribe/unsubscribe operations can be called multiple times.
        3. Jira TD-32187:
         - Test tmq consumption with meta topic on database containing stable with special tag names.
        4. Jira TD-33225:
         - Test tmq consumption after altering stable column compression and creating index.
        5. Jira TD-32471:
         - Test tmq consumption after altering stable to add new columns.
        6. Jira TD-32526:
         - Test tmq consumption with native C client after altering stable to add columns.
        7. Jira TD-33504:
         - Test tmq consumer can switch topics after unsubscribe.
        8. Jira TD-35698:
         - Test tmq consumption with meta topic containing decimal columns.
        9. Jira TD-37436:
         - Test tmq consumption with meta topic on database containing stream.
        10. Jira TD-38404:
         - Tmq_get_json_meta behaves unexpectedly when the tags of subscribed meta messages contain empty strings.
        11. Jira TS-4563:
         - Test tmq consumption of unordered data inserted by stmt.
        12. Jira TS-5466:
         - Test tmq consumption with meta topic after altering stable to add many columns.
        13. Jira TS-5906:
         - Test tmq consumption after altering child table tags and inserting new data.
        14. Jira TS-6115:
         - Test tmq consumption with large amount of data inserted by taosBenchmark.
        15. Jira TS-6392:
         - Test tmq consumer group rebalance and recovery after dnode restart with WAL retention.
        16. Jira TS-7402:
         - Test tmq consumption with meta topic created on stable with filter conditions.
        17. Jira TS-7662:
         - Test tmq consumption with meta topic created on stable with where clause.
        19. Jira TS-4674:
         - Test tmq consumption behavior during vgroup leader rebalance in multi-replica environment.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-24 Alex Duan Migrated from uncatalog/army/tmq/test_tmq_bugs.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_td_30270.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_td_32187.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_td_33225.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td32471.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td32526.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td33504.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td35698.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td37436.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_td38404.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts4563.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts5466.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts5906.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts6115.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts6392.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts7402.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmq_ts7662.py
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_ts_4674.py

        """

        self.do_ts4674()
        self.do_ts7662()
        self.do_td31283()
        self.do_td30270()
        self.do_td32187()
        self.do_td33225()
        self.do_td32471()
        self.do_td32526()
        self.do_td33504()
        self.do_td35698()
        self.do_td37436()
        self.do_td38404()
        self.do_ts4563()
        self.do_ts5466()
        self.do_ts5906()
        self.do_ts6115()
        self.do_ts6392()
        self.do_ts7402()
        self.do_td_tmq_token()