
import taos
import sys
import time
import socket
import os
import threading
import multiprocessing
from multiprocessing import Process, Queue

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from taos.tmq import *
from frame import etool
from datetime import datetime
from taos.tmq import Consumer
from frame.common import *

class TaosConsumer:
    def __init__(self):
        pass
        
    def sub_consumer(self ,consumer ,group_id ,topic_name ):
        group_id = int(group_id)
        if group_id < 100 :
            try:
                consumer.subscribe([topic_name])
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

    def set_conf(self,td_connect_ip="localhost",group_id=1,client_id="test_consumer_py",enable_auto_commit="false",auto_commit_interval_ms="1000",auto_offset_reset="earliest",msg_with_table_name="true",session_timeout_ms=10000,max_poll_interval_ms=180000,experimental_snapshot_enable="false"):
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
    
    def sub_consumer_once(self,consumer, group_id, topic_name, counter, stop_event):
        group_id = int(group_id)
        if group_id < 100 :
            consumer.subscribe([topic_name])
        nrows = 0
        consumer_nrows = 0
        
        while not stop_event.is_set():
            start = datetime.now()
            tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
            #start = datetime.now()
            #print(f"time:{start},consumer:{group_id}, start to consume")
            tdLog.info(f"consumer_nrows:{consumer_nrows}")
            message = consumer.poll(timeout=10.0)

            if message:
                id = message.offset()
                topic = message.topic()
                database = message.database()
                for block in message:
                    addrows = block.nrows()
                    nrows += block.nrows()
                    counter.rows(block.nrows())
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

        print("Consumer subscription thread is stopping.")
    def taosc_consumer(self, conf, topic_name, counter,stop_event):
        try:
            print(conf)
            from taos.tmq import Consumer
            print("3333")
            consumer = Consumer(conf)
            print("456")
            group_id = int(conf["group.id"])
            tdLog.info(f"{consumer},{group_id}")
        except Exception as e:
            tdLog.exit(f"{e}")
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
            self.sub_consumer_once(consumer, group_id, topic_name, counter, stop_event)
            
        except Exception as e:
            tdLog.exit(f"{e}")
                
        #consumer.close()

            
class ThreadSafeCounter:
    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    def rows(self, rows):
        with self.lock:
            self.counter += rows

    def get(self):
        with self.lock:
            return self.counter


class TDTestCase:
    # updatecfgDict = {'debugFlag': 135, 'asynclog': 0}


    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.consumer_instance = TaosConsumer()
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file
    def caseDescription(self):
        '''
        drop_lost_consmuers<hrchen>: 
        1. verifying that the boundary and valid values of session_timeout_ms are in effect
        2. verifying that the boundary and valid values of max_poll_interval_ms are in effect
        3. verifying that consumer will be closed when the session_timeout_ms and max_poll_interval_ms is expired
        '''
        return
    
    def check_consumer(self,count,rows):
        time.sleep(count)
        print(count)
        try:
            for ct in range(5):
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

    def drop_session_timeout_consmuers(self, consumer_groups_num, session_timeout_ms, max_poll_interval_ms, topic_name, timeout):
        tdSql.execute(f'drop topic if exists {topic_name};')
        tdSql.execute(f'use db_sub')
        tdSql.execute(f'create topic {topic_name} as  select * from db_sub.meters;')

        # start consumer and config some parameters
        os.system(f"nohup python3 ./tmq/per_consumer.py  -c {consumer_groups_num} -s {session_timeout_ms} -p {max_poll_interval_ms} > consumer.log &")
        # wait 4s for consuming data
        time.sleep(4)
        # kill consumer to simulate session_timeout_ms
        tdLog.info("kill per_consumer.py")
        tdCom.kill_signal_process(signal=9,processor_name="python3\s*./tmq/per_consumer.py")
        self.check_consumer(timeout,0)
        tdSql.execute(f'drop topic if exists {topic_name};')
        os.system("rm -rf consumer.log")
 
   
    def drop_max_poll_timeout_consmuers(self, consumer_groups_num, consumer_rows, topic_name, timeout):
        tdSql.execute(f'drop topic if exists {topic_name};')
        tdSql.execute(f'use db_sub')
        tdSql.execute(f'create topic {topic_name} as  select * from db_sub.meters;')

        threads = []
        counter = ThreadSafeCounter()
        stop_event = threading.Event()
        for id in range(consumer_groups_num):
            conf = self.consumer_instance.set_conf(group_id=id, session_timeout_ms=self.session_timeout_ms, max_poll_interval_ms=self.max_poll_interval_ms)
            threads.append(threading.Thread(target=self.consumer_instance.taosc_consumer, args=(conf, topic_name, counter, stop_event)))
        for tr in threads:
            tr.start()
        
        consumer_all_rows = consumer_rows * consumer_groups_num
        while True:
            if counter.get() < consumer_all_rows:
                time.sleep(5)
                print(f"consumer_all_rows:{consumer_all_rows},counter.get():{counter.get()}")
            elif counter.get() >= consumer_all_rows:
                self.check_consumer(timeout+20, 0)
                stop_event.set()
                tr.join()
                break
        time.sleep(2)
        tdSql.execute(f'drop topic if exists {topic_name};')
            
    def case_session_12s(self):
        #test session_timeout_ms=12s
        session_timeout_ms=12000
        max_poll_interval_ms=180000
        topic_name = "select_d1"
        self.drop_session_timeout_consmuers(consumer_groups_num=1, session_timeout_ms=session_timeout_ms, max_poll_interval_ms=max_poll_interval_ms, topic_name=topic_name , timeout=int(session_timeout_ms/1000))
       

    def case_max_poll_12s(self,consumer_rows):
        #test max_poll_interval_ms=12s
        self.session_timeout_ms=180000
        self.max_poll_interval_ms=12000
        topic_name = "select_d1"
        self.drop_max_poll_timeout_consmuers(consumer_groups_num=1, topic_name=topic_name, consumer_rows=consumer_rows, timeout=int(self.max_poll_interval_ms/1000))
        

    def run(self):
        table_number = 1000
        rows_per_table = 1000
        vgroups = 4
        etool.benchMark(command=f"-d db_sub -t {table_number} -n {rows_per_table} -v {vgroups} -y")
        consumer_rows = table_number *  rows_per_table # 消费的目标行数
        # self.case_session_12s()
        self.case_max_poll_12s(consumer_rows)
        remaining_threads = threading.Lock()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
