
import taos
import sys
import time
import socket
import os
import threading

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from taos.tmq import *
from frame import etool
from datetime import datetime
from taos.tmq import Consumer

class TDTestCase:
    # updatecfgDict = {'debugFlag': 135, 'asynclog': 0}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def caseDescription(self):
        '''
        drop_lost_consmuers<hrchen>: 
        1. verifying that the boundary and valid values of session_timeout_ms are in effect
        2. verifying that the boundary and valid values of max_poll_interval_ms are in effect
        3. verifying that consumer will be closed when the session_timeout_ms and max_poll_interval_ms is expired
        '''
        return
    
    def sub_consumer(self,consumer,group_id):
        group_id = int(group_id)
        if group_id < 100 :
            try:
                consumer.subscribe(["select_d1"])
            except TmqError:
                tdLog.exit(f"subscribe error")         

        nrows = 0
        while True:
            start = datetime.now()
            print(f"time:{start},consumer:{group_id}, start to consume")
            message = consumer.poll(timeout=10.0)
            
            if message:
                id = message.offset()
                topic = message.topic()
                database = message.database()
                
                for block in message:
                    addrows = block.nrows()
                    nrows += block.nrows()
                    ncols = block.ncols()
                    values = block.fetchall
                end = datetime.now()
                elapsed_time = end -start
                print(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
            consumer.commit()
            print(f"consumer:{group_id},consumer_nrows:{nrows}")
                # consumer.unsubscribe()
                # consumer.close()
                # break
            # if nrows >= 1000000:
            #     break
    def sub_consumer_once(self,consumer,group_id):
        group_id = int(group_id)
        if group_id < 100 :
            consumer.subscribe(["select_d1"])
        nrows = 0
        consumer_nrows = 0
        while True:
            start = datetime.now()
            tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
            #start = datetime.now()
            #print(f"time:{start},consumer:{group_id}, start to consume")
            tdLog.info(f"consumer_nrows:{consumer_nrows}")
            if consumer_nrows < 1000000:
                message = consumer.poll(timeout=10.0)
            else:
                tdLog.info(" stop consumer when consumer all rows")
                break
            if message:
                id = message.offset()
                topic = message.topic()
                database = message.database()
                
                for block in message:
                    addrows = block.nrows()
                    nrows += block.nrows()
                    ncols = block.ncols()
                    values = block.fetchall
                end = datetime.now()
                elapsed_time = end -start
                # tdLog.info(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
            consumer.commit()
            # tdLog.info(f"consumer:{group_id},consumer_nrows:{nrows}")
            consumer_nrows = nrows
                # consumer.unsubscribe()
                # consumer.close()
                # break

    def set_conf(self,td_connect_ip="localhost",group_id=1,client_id="test_consumer_py",enable_auto_commit="false",auto_commit_interval_ms="1000",auto_offset_reset="earliest",msg_with_table_name="true",session_timeout_ms=10000,max_poll_interval_ms=20000,experimental_snapshot_enable="false"):
        conf = {
            # auth options
            # consume options
            "td.connect.ip": f"{td_connect_ip}",
            "group.id": f"{group_id}", 
            "client.id": f"{client_id}",
            "enable.auto.commit": f"{enable_auto_commit}",
            "auto.commit.interval.ms": f"{auto_commit_interval_ms}",
            "auto.offset.reset": f"{auto_offset_reset}",
            "msg.with.table.name": f"{msg_with_table_name}",
            "session.timeout.ms": f"{session_timeout_ms}",
            "max.poll.interval.ms": f"{max_poll_interval_ms}",
            "experimental.snapshot.enable" :f"{experimental_snapshot_enable}",
        }
        return conf

    def taosc_consumer(self,conf):
        consumer = Consumer(conf)
        group_id = int(conf["group.id"])
        tdLog.info(f"{consumer},{group_id}")
        #counsmer sub:
        # while True:
        #     try:
        #         self.sub_consumer_once(consumer,group_id)
        #     except Exception as e:
        #         print(str(e))
        #         time.sleep(1)
        #         break
        # only consumer once
        try:
            self.sub_consumer_once(consumer,group_id)
        except Exception as e:
            print(str(e))
                
        #consumer.close()
    def check_consumer(self,count,rows):
        while True:
            time.sleep(count)
            try:
                for ct in range(3):
                    tdSql.query(f'show consumers')
                    anser_rows=tdSql.getRows()
                    if tdSql.checkRows(rows):
                        break
                    else:
                        time.sleep(1)
                        tdLog.info(f"wait for {count} seconds to check that consumers number is {rows}")
                if anser_rows != rows:
                    tdLog.exit(f"consumer number is not {rows}")
            except Exception as e:
                tdLog.exit(f"{e},check consumer error")
        

    def drop_session_timeout_consmuers(self,consumer_groups_num,conf,timeout):
        tdSql.execute(f'drop topic if exists select_d1;')
        # insert data 
        
        tdSql.execute(f'use db_sub')
        tdSql.execute(f'create topic select_d1 as  select * from db_sub.meters;')
        threads = []
        
        for id in range(consumer_groups_num):
            # conf = self.set_conf(group_id=id,)
            threads.append(threading.Thread(target=self.taosc_consumer, args=(conf,)))
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        tr.join()

        self.check_consumer(timeout,0)
        time.sleep(2)
        tdSql.execute(f'drop topic if exists select_d1;')
        
    
    def run(self):
        etool.benchMark(command="-d db_sub -t 1000 -n 1000 -v 4 -y")
        #test session_timeout_ms=20s
        session_timeout_ms=200000

        conf = self.set_conf(group_id=1, session_timeout_ms=session_timeout_ms)
        print(conf)
        self.drop_session_timeout_consmuers(consumer_groups_num=1,conf=conf,timeout=int(session_timeout_ms/1000))
        

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
