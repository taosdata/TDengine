
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
    #TODo: add to tq.py and remove from here
    def __init__(self):
        self.sub_once = True
        self.once_consumer_rows = 0
        self.sub_log = False
        self.safe_counter = ThreadSafeCounter()

    def log_info(self, message):
        if self.sub_log:
            tdLog.info(message)

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
            tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
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
                tdLog.info(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
            consumer.commit()
            tdLog.info(f"consumer:{group_id},consumer_nrows:{nrows}")
                # consumer.unsubscribe()
                # consumer.close()

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
    
    def sub_consumer_once(self, consumer, group_id, topic_name, stop_event):
        group_id = int(group_id)
        if group_id < 100 :
            consumer.subscribe([topic_name])
        nrows = 0
        consumer_nrows = 0
        
        while not stop_event.is_set():
            start = datetime.now()
            self.log_info(f"time:{start},consumer:{group_id}, start to consume,consumer_nrows:{consumer_nrows}")
            if consumer_nrows < self.once_consumer_rows:
                    message = consumer.poll(timeout=1.0)
            elif consumer_nrows >= self.once_consumer_rows:
                pass
                # tdLog.info("stop consumer when consumer all rows") 

            if message:
                id = message.offset()
                topic = message.topic()
                database = message.database()
                for block in message:
                    addrows = block.nrows()
                    nrows += block.nrows()
                    self.safe_counter.rows(block.nrows())
                    ncols = block.ncols()
                    values = block.fetchall
                end = datetime.now()
                elapsed_time = end -start

                self.log_info(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}") 
            self.log_info(f"consumer:{group_id},consumer_nrows:{nrows},counter.counter:{self.safe_counter.counter}")
            # consumer.commit()
            consumer_nrows = nrows
            # consumer.unsubscribe()
            # consumer.close()
            # break 
        tdLog.info("Consumer subscription thread is stopping.")

    def taosc_consumer(self, conf: list, topic_name: str, stop_event):
        try:
            tdLog.info(conf)
            from taos.tmq import Consumer
            tdLog.info("start to config consumer")
            consumer = Consumer(conf)
            tdLog.info("start to subscribe")
            group_id = int(conf["group.id"])
            tdLog.info(f"{consumer},{group_id}")
            if self.sub_once:
                    self.sub_consumer_once(consumer, group_id, topic_name, stop_event)
            else:
                self.sub_consumer(consumer, group_id, topic_name)
            # only consumer once
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
        #db parameter
        self.table_number = 1000
        self.rows_per_table = 1000
        #consumer parameter
        self.consumer_groups_num = 2
        self.session_timeout_ms= 180000
        self.max_poll_interval_ms= 180000
        #case consumer parameter
        self.consumer_rows_per_thread = self.table_number * self.rows_per_table
        self.consumer_all_rows = self.consumer_rows_per_thread * self.consumer_groups_num

        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file
    def caseDescription(self):
        '''
        drop_lost_consmuers<hrchen>: 
        1. verifying that the boundary and valid values of session_timeout_ms are in effect
        2. verifying that the boundary and valid values of max_poll_interval_ms are in effect
        3. verifying that consumer will be closed when the session_timeout_ms and max_poll_interval_ms is expired
        '''
        return
    
    def check_consumer(self, count, rows, stop_event=None):
        time.sleep(count)
        try:
            tdLog.info(f"wait timeout count:{count} and check consumer status whether is closed")
            for ct in range(5):
                tdSql.query(f'show consumers')
                anser_rows=tdSql.getRows()
                if anser_rows == rows:
                    break
                else:
                    time.sleep(1)
                    tdLog.info(f"wait for {count} seconds to check that consumers number is {anser_rows}")
            if anser_rows != rows:
                if stop_event:
                    stop_event.set()
                tdLog.exit(f"consumer number is {anser_rows } but not  expected {rows}")
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
 
   
    def drop_max_poll_timeout_consmuers(self, topic_name, timeout):
        tdSql.execute(f'drop topic if exists {topic_name};')
        tdSql.execute(f'use db_sub')
        tdSql.execute(f'create topic {topic_name} as  select * from db_sub.meters;')

        threads = []
        self.safe_counter = ThreadSafeCounter()
        self.consumer_instance.safe_counter = self.safe_counter
        stop_event = threading.Event()
        self.consumer_instance.once_consumer_rows = self.consumer_rows_per_thread
        tdLog.info(f"consumer_rows:{self.consumer_instance.once_consumer_rows}")
        self.consumer_instance.sub_once = True
        for id in range(self.consumer_groups_num):
            conf = self.consumer_instance.set_conf(group_id=id, session_timeout_ms=self.session_timeout_ms, max_poll_interval_ms=self.max_poll_interval_ms)
            threads.append(threading.Thread(target=self.consumer_instance.taosc_consumer, args=(conf, topic_name, stop_event)))
        for tr in threads:
            tr.start()
        
        while True:
            if self.safe_counter.get() < self.consumer_all_rows:
                time.sleep(5)
                tdLog.info(f"consumer_all_rows:{self.consumer_all_rows},counter.get():{self.safe_counter.get()}")
            elif self.safe_counter.get() >= self.consumer_all_rows:
                # adding 5s is for heartbeat check  
                self.check_consumer(timeout+5, 0, stop_event)
                stop_event.set()
                tr.join()
                break
        time.sleep(1)
        tdSql.execute(f'drop topic if exists {topic_name};')
            
    def case_session_timeout(self):
        tdLog.info("start to test session_timeout_ms=12s")
        #test session_timeout_ms=12s
        self.session_timeout_ms=12000
        self.max_poll_interval_ms=180000
        topic_name = "select_d1"
        self.drop_session_timeout_consmuers(consumer_groups_num=1, session_timeout_ms=self.session_timeout_ms, max_poll_interval_ms=self.max_poll_interval_ms, topic_name=topic_name , timeout=int(self.session_timeout_ms/1000))
        tdLog.info("stop to test session_timeout_ms=12s and done ")

    def case_max_poll_timeout(self):
        tdLog.info("start to test max_poll_interval_ms=20s")
        #test max_poll_interval_ms=20s
        self.session_timeout_ms=180000
        self.max_poll_interval_ms=20000
        topic_name = "select_d1"
        self.drop_max_poll_timeout_consmuers(topic_name=topic_name, timeout=int(self.max_poll_interval_ms/1000))
        tdLog.info("stop to test max_poll_interval_ms=20s and done ")
        

    def run(self):
        vgroups = 4
        etool.benchMark(command=f"-d db_sub -t {self.table_number} -n {self.rows_per_table} -v {vgroups} -a {self.replicaVar} -y")
        # test case start here
        
        self.case_session_timeout()
        self.case_max_poll_timeout()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
