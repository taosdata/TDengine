###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
from taostest import TDCase
from taostest.util.common import TDCom
from taostest.util.remote import Remote
from ..utils.step_mnode import StepMnode
from ..utils.step_dnode import StepDnode
from ..utils.thread_pool import ThreadPool, db_write_data_thread, db_query_data_thread, db_topic_subscribe_thread
from pathlib import Path
import os
import random
import time


class TestMnodeAdd(TDCase):
    def init(self):
        super(TestMnodeAdd, self).init()
        self.db_name = "stability_db"
        # case running round number
        self.run_round = 2
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.mnode_utils = StepMnode(self.tdSql, self.logger)
        self.dnode_utils = StepDnode(self.tdSql, self.logger)

    def run(self):
        try:
            self.tp = ThreadPool(4, self.logger, 3)
            taos_benchmark_write_json_file = os.sep.join([str(Path(__file__).resolve().parent.parent), 'db_write.json'])
            taos_benchmark_write_json_output_file = os.sep.join([self.run_log_dir, "db_write_res.txt"])
            self.tp.put(db_write_data_thread, args=(self.lcmd, taos_benchmark_write_json_file, taos_benchmark_write_json_output_file, self.logger,))
            self.logger.info("Wait 10 seconds for the db query data thread")
            time.sleep(10)

            # start the thread for db query data
            taos_benchmark_query_json_file = os.sep.join([str(Path(__file__).resolve().parent.parent), 'db_query.json'])
            taos_benchmark_query_json_output_file = os.sep.join([self.run_log_dir, "db_query_res.txt"])
            self.tp.put(db_query_data_thread, args=(self.lcmd, taos_benchmark_query_json_file, taos_benchmark_query_json_output_file, self.logger,))

            # start the thread for db streaming
            self.tdSql.execute("use {}".format(self.db_name))
            self.stream_param_list = [
                {
                    "stream_name": "cft_stream1",
                    "des_table": "cft_stream1_des_table",
                    "source_sql": "select _wstart, count(*), avg(fc) from {}.st_common partition by tbname interval(1m);".format(self.db_name)
                }
            ]
            for stream_obj in self.stream_param_list:
                stream_name = stream_obj["stream_name"]
                des_table = stream_obj["des_table"]
                source_sql = stream_obj["source_sql"]
                self.tdCom.create_stream(stream_name=stream_name, des_table=des_table, source_sql=source_sql)
                self.logger.info("Create the stream {} successfully".format(stream_name))

            # start the thread for db topic subscription
            taos_benchmark_topic_subscription_json_file = os.sep.join([str(Path(__file__).resolve().parent.parent), 'db_topic_subscription.json'])
            taos_benchmark_topic_subscription_json_output_file = os.sep.join([self.run_log_dir, "db_topic_subscription_res.txt"])
            self.tp.put(db_topic_subscribe_thread, args=(self.lcmd, taos_benchmark_topic_subscription_json_file, taos_benchmark_topic_subscription_json_output_file, self.logger,))

            # add and delete mnode
            for i in range(self.run_round):
                self.logger.info("Start the round {} test".format(str(i + 1)))
                # check mnode number in cluster
                mndoes_leader_list = self.mnode_utils.get_mnodes()
                self.logger.info("mnode list: {}".format(mndoes_leader_list))
                mndoes_follower_list = self.mnode_utils.get_mnodes("follower")
                self.logger.info("mnode follower list: {}".format(mndoes_follower_list))
                mndoes_candidate_list = self.mnode_utils.get_mnodes("candidate")
                self.logger.info("mnode candidate list: {}".format(mndoes_candidate_list))

                # delete the follower mnode
                dnode_list = self.dnode_utils.get_dnodes()
                if len(mndoes_follower_list) > 0:
                    for follower in mndoes_follower_list:
                        self.logger.info("delete follower mnode: {}".format(follower[0]))
                        dnode_id = 0
                        # find the dnode id for the follower mnode
                        for dnode in dnode_list:
                            if follower[1] == dnode[1]:
                                dnode_id = dnode[0]
                                break
                        if dnode_id != 0:
                            self.mnode_utils.drop_mnode(dnode_id)
                self.logger.info("All the follower mnode is deleted")

                # check dnode number in cluster and make sure the number is 2 or more
                dnode_list = self.dnode_utils.get_dnodes(include_mnodes=False)
                if len(dnode_list) < 2:
                    raise Exception("Case failed due to no enough dnodes in cluster")
                else:
                    self.logger.info("dnode list: {}".format(dnode_list))

                # add mnode until the specifiec number
                dnode_id_for_mnode_list = [dnode[0] for dnode in random.sample(dnode_list, 2)]
                self.logger.info("The dnode id for mnode: {}".format(dnode_id_for_mnode_list))
                for dnode_id in dnode_id_for_mnode_list:
                    self.mnode_utils.add_mnode(dnode_id)
                    self.logger.info("The mnode is created on dnode: {}".format(str(dnode_id)))
                mndoes_leader_list = self.mnode_utils.get_mnodes()
                self.logger.info("mnode list: {}".format(mndoes_leader_list))
        except Exception as ex:
            # raise Exception("Case failed due to {}".format(str(ex)))
            self.logger.error("Case failed due to {}".format(str(ex)))
            return False
        finally:
            self.logger.info("Cleanup the environment......")
            # stop the db write, query, streaming, topic threads
            self.tp.terminal = True
            # stop and drop the streaming
            if self.stream_param_list:
                for stream_obj in self.stream_param_list:
                    self.tdCom.pause_stream(stream_name=stream_obj["stream_name"])
                    self.tdCom.drop_stream(stream_name=stream_obj["stream_name"])
                    self.logger.info("Pause and drop the stream {} successfully".format(stream_obj["stream_name"]))

            # stop taosBenchmark process
            msg, err, code = self.lcmd.run_local_command("ps aux | grep 'taosBenchmark' | grep -v grep | awk '{print $2}' | xargs kill -9")
            self.logger.info("Kill taosBenchmark process return code: {}".format(code))

            # check zombie process
            # proc = subprocess.Popen("ps aux | grep 'taosBenchmark' | grep -v grep", shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid)
            # self.logger.info("result2:" + str(proc.stdout))
            # os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            # if res.stdout:
            #     for line in res.stdout.readlines():
            #         self.logger.info("Get taosbenchmark process: {}".format(line))
            #         if "[taosBenchmark] <defunct>" in line:
            #             subprocess.Popen("ps aux | grep 'taosBenchmark' | grep -v grep | awk '{print $3}' | xargs kill -9", shell=True)
            # os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            self.logger.info("All the taosbenchmark processes are killed successfully")
            # wait 15 seconds for the heartbeat of the mnode, then drop the topic subscription
            time.sleep(15)

            # drop topic subscription
            drop_all_flag = False
            for i in range(10):
                try:
                    self.tdSql.query("show topics;")
                    topics_data = self.tdSql.query_data
                    if len(topics_data) == 0:
                        drop_all_flag = True
                        break
                    for topic in topics_data:
                        self.tdSql.execute("drop topic if exists {};".format(topic[0]))
                        self.logger.info("Drop the topic {} successfully".format(topic[0]))
                except Exception as ex:
                    if "Topic subscribed cannot be dropped" in str(ex) and i < 9:
                        time.sleep(1)
                        self.logger.info("Wait 2 seconds and try to drop the topic subscription again")
                        continue
            if drop_all_flag:
                self.logger.info("Drop all the topics successfully")
            else:
                self.logger.error("Failed to drop all the topics in 10 times")

    def cleanup(self):
        pass
        # # delete the database
        # self.tdSql.execute("drop database if exists {};".format(self.db_name))

        # # stop taosd process
        # subprocess.Popen("ps aux | grep 'taosd' | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)

    def desc(self) -> str:
        case_description = """This test case is used to verify the system high availability when mnode is added."""
        return case_description

    def author(self) -> str:
        return "Charles"

    def tags(self) -> str:
        pass
