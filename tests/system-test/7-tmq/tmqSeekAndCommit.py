import sys
import re
import time
import threading
from taos.tmq import *
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        
        self.db_name = "tmq_db"
        self.topic_name = "tmq_topic"
        self.stable_name = "tmqst"
        self.prepareData()
        

    def prepareData(self):
        # create database
        tdSql.execute("create database if not exists %s;"%(self.db_name))
        tdSql.execute("use %s;"%(self.db_name))
        # create stable
        tdSql.execute("create table %s.tmqst (ts timestamp, col0 int) tags(groupid int);"%(self.db_name))
        # create child tables
        tdSql.execute("create table tmqct_1 using %s.%s tags(1);"%(self.db_name, self.stable_name))
        tdSql.execute("create table tmqct_2 using %s.%s tags(2);"%(self.db_name, self.stable_name))
        tdSql.execute("create table tmqct_3 using %s.%s tags(3);"%(self.db_name, self.stable_name))
        tdSql.execute("create table tmqct_4 using %s.%s tags(4);"%(self.db_name, self.stable_name))
        tdSql.execute("create table tmqct_5 using %s.%s tags(5);"%(self.db_name, self.stable_name))
        # insert into data
        ctb_list = ["tmqct_1", "tmqct_2", "tmqct_3", "tmqct_4", "tmqct_5"]
        for i in range(5):
            sql = "insert into %s "%(ctb_list[i])
            sql_values = "values"
            for j in range(1000 * i, 1000 * (i+1)):
                sql_values += "(%s, %s)"%("now" if j == 0 else "now+%s"%(str(j) + "s"), str(j))
            sql += sql_values + ";"
            tdLog.info(sql)
            tdSql.execute(sql)
        tdLog.info("Insert data into child tables successfully")
        # create topic
        tdSql.execute("create topic %s as select * from %s;"%(self.topic_name, self.stable_name))

    def tmqSubscribe(self, inputDict):
        consumer_dict = {
            "group.id": inputDict['group_id'],
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": inputDict['auto_commit'],
            "auto.offset.reset": inputDict['offset_reset'],
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }
        
        consumer = Consumer(consumer_dict)
        try:
            consumer.subscribe([inputDict['topic_name']])
        except Exception as e:
            tdLog.info("consumer.subscribe() fail ")
            tdLog.info("%s"%(e))        

        tdLog.info("create consumer success!")
        return consumer

    def test_seek_and_committed_position_with_autocommit(self):
        """Check the position and committed offset of the topic for autocommit scenario
        """
        try:
            inputDict = {
                "topic_name": self.topic_name,
                "group_id": "1",
                "auto_commit": "true",
                "offset_reset": "earliest"
            }
            consumer = self.tmqSubscribe(inputDict)
            while(True):
                res = consumer.poll(1)
                if not res:
                    break
                err = res.error()
                if err is not None:
                    raise err
                val = res.value()
                for block in val:
                    tdLog.info("block.fetchall() number: %s"%(len(block.fetchall())))

            partitions = consumer.assignment()
            position_partitions = consumer.position(partitions)
            for i in range(len(position_partitions)):
                tdLog.info("position_partitions[%s].offset: %s"%(i, position_partitions[i].offset))
            committed_partitions = consumer.committed(partitions)
            origin_committed_position = []
            for i in range(len(committed_partitions)):
                tdLog.info("committed_partitions[%s].offset: %s"%(i, committed_partitions[i].offset))
                origin_committed_position.append(committed_partitions[i].offset)
            assert(len(position_partitions) == len(committed_partitions))
            for i in range(len(position_partitions)):
                assert(position_partitions[i].offset == committed_partitions[i].offset)
            # seek to the specified offset of the topic, then check position and committed offset
            for partition in partitions:
                partition.offset = 5
                consumer.seek(partition)
            position_partitions = consumer.position(partitions)
            for i in range(len(position_partitions)):
                assert(position_partitions[i].offset == 5)
            committed_partitions = consumer.committed(partitions)
            for i in range(len(committed_partitions)):
                assert(committed_partitions[i].offset != 5 and committed_partitions[i].offset == origin_committed_position[i])
        except Exception as ex:
            raise Exception("Failed to test seek and committed position with autocommit with error: {}".format(str(ex)))
        finally:
            consumer.unsubscribe()
            consumer.close()

    def test_commit_by_offset(self):
        """Check the position and committed offset of the topic for commit by offset scenario
        """
        try:
            inputDict = {
                "topic_name": self.topic_name,
                "group_id": "1",
                "auto_commit": "false",
                "offset_reset": "earliest"
            }
            consumer = self.tmqSubscribe(inputDict)
            origin_committed_position = []
            while(True):
                res = consumer.poll(1)
                if not res:
                    break
                err = res.error()
                if err is not None:
                    raise err
                partitions = consumer.assignment()
                consumer.commit(offsets=partitions)
                val = res.value()
                for block in val:
                    tdLog.info("block.fetchall() number: %s"%(len(block.fetchall())))
                position_partitions = consumer.position(partitions)
                committed_partitions = consumer.committed(partitions)
                for i in range(len(position_partitions)):
                    assert(position_partitions[i].offset == committed_partitions[i].offset)
            committed_partitions = consumer.committed(partitions)
            for i in range(len(committed_partitions)):
                origin_committed_position.append(committed_partitions[i].offset)
                tdLog.info("original committed_partitions[%s].offset: %s"%(i, committed_partitions[i].offset))
            # seek to the specified offset of the topic, then check position and committed offset
            for partition in partitions:
                partition.offset = 2
                consumer.seek(partition)
            position_partitions = consumer.position(partitions)
            for i in range(len(position_partitions)):
                assert(position_partitions[i].offset == 2)
            committed_partitions = consumer.committed(partitions)
            for i in range(len(committed_partitions)):
                tdLog.info("after seek committed_partitions[%s].offset: %s"%(i, committed_partitions[i].offset))
                assert(committed_partitions[i].offset != 2 and committed_partitions[i].offset == origin_committed_position[i])
            # continue to consume data from seek offset
            while(True):
                res = consumer.poll(1)
                if not res:
                    break
                err = res.error()
                if err is not None:
                    raise err
                partitions = consumer.assignment()
                # commit by offset
                consumer.commit(offsets=partitions)
                val = res.value()
                for block in val:
                    tdLog.info("block.fetchall() number: %s"%(len(block.fetchall())))
            partitions = consumer.assignment()
            position_partitions = consumer.position(partitions)
            committed_partitions = consumer.committed(partitions)
            assert(len(position_partitions) == len(committed_partitions))
            for i in range(len(position_partitions)):
                assert(position_partitions[i].offset == committed_partitions[i].offset)
        except Exception as ex:
            raise Exception("Failed to test commit by offset with error: {}".format(str(ex)))
        finally:
            consumer.unsubscribe()
            consumer.close()

    def run(self):
        self.test_seek_and_committed_position_with_autocommit()
        self.test_commit_by_offset()

    def stop(self):
        tdSql.execute("drop topic %s" % self.topic_name)
        tdSql.execute("drop database %s"%(self.db_name))
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
