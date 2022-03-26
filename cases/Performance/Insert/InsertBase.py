# utf-8

import threading
import time
from datetime import datetime
from typing import List, Dict

# from matplotlib import pyplot as plt
from prettytable import PrettyTable
from taostest import TDCase, logger
from taostest.components import PrometheusServer
from taostest.performance.perfor_basic import InsertFile
from taostest.util import remote
from taostest.util.benchmark import DEFAULT_INSERT_CONFIG
from taostest.util.remote import Remote
import json


class InsertTest(TDCase):

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def put_file(self, iplist: list, json_data: list):
        """
        description: This method is used to put zhe file to target machine.

        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        return: result_files
                """
        remote = Remote(self.logger)

        # create test_log and put task json files on target machine
        for i in range(len(iplist)):
            remote.cmd(iplist[i], [f'mkdir {json_data[i]["test_log"]}'])
            remote.put(iplist[i], self.run_log_dir + "/insert" +
                       str(i) + ".json", json_data[i]["test_log"])

    def threads_run_taosBenchmark(self, iplist, json_data, file_name: list):
        """
        description: This method is used to start several threads to run taosBenchmark ,and then
                    get the result file to local machine

        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        return: result_files,use
        """
        remote = Remote(self.logger)
        t = []
        for i in range(len(iplist)):
            t.append(threading.Thread(target=remote.cmd,
                                      args=(
                                          iplist[i],
                                          [
                                              f'taosBenchmark -f {json_data[i]["test_log"]}{file_name[i]} 2>&1 | tee /tmp/{i}.log '])))

            t[i].start()
            time.sleep(1)

        for i in t:
            i.join()

        result_files = []

        for i in range(len(t)):
            # rename result file
            filename = self.run_log_dir + "/" + str(i) + "-" + iplist[i]
            result_files.append(filename)
            # get result_files and remove test_log
            remote.get(iplist[i], f"/tmp/{i}.log", filename)
            remote.cmd(iplist[i], [f'rm -rf {json_data[i]["test_log"]}'])

        return result_files

    # def draw_linechart(self, dict: dict):
    #     """
    #     description: This method is used to draw line chart(折线图) for final result.
    #     param dict: result of every time after test case running.
    #     return: none
    #     """
    #     x = []
    #     y = []
    #     for name, value in dict.items():
    #         x.append(name)
    #         y.append(value)

    #     plt.plot(x, y)
    #     plt.show()

    def draw_table(self, tablelist: list, datalist: list):
        """
        description:This method is used to draw table(表格) for final result.
        :param tablelist -> dict : header list of table
        :param datalist -> dict : data
        :return:
        """
        global x
        table_list = []
        value_list = []
        table_list.append(tablelist)

        # 制表
        for i in table_list:
            x = PrettyTable(i)
        # 构建数据
        for i in datalist:
            value_list.append(i)
        # 填表
        for i in value_list:
            x.add_row(i)

        # print(x)

        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')

        f.write(str(x) + '\n')

        # pass

    def full_create_tb_result(self, result_files: list):
        """
        description: This method is used to get the result of create tables
        :param result_files:result files in run_log.dir
        :return:
        """

        j_file = InsertFile()

        create_tb_summary = j_file.get_dt_create_tb_summary(result_files)

        taosBenchmark_list = []
        data_list = []

        # get taosBenchmark_id and result data
        for k, v in create_tb_summary.items():
            taosBenchmark_list.append(k)
            data_list.append(v)

        table_list = ["taosBenchmark_id",
                      "times(s)", "tables", "threads", "actual_create"]
        data_list1 = []

        # insert the result data
        for i in range(len(data_list)):
            data_list1.append(list(data_list[i].values()))
            data_list1[i].insert(0, taosBenchmark_list[i])

        # draw table

        file_name = self.run_log_dir + '/perf_report.txt'
        total_table_num = 0
        actual_create_num = 0
        for i in range(len(data_list1)):
            total_table_num += data_list1[i][2]
            actual_create_num += data_list1[i][4]
        f = open(file_name, 'a')
        f.write(
            "\n*********************** Create Table Result **********************\n")
        f.close()

        self.draw_table(table_list, data_list1)

        f = open(file_name, 'a')
        f.write("Total number of creating tables :" +
                str(total_table_num) + "\n")
        f.write("Actual number of creating tables :" +
                str(actual_create_num) + "\n")
        f.write("\n")
        f.close()

    def taosBenchmark_id_insert_result(self, result_files: list = None):
        """
        description: This method is used to get the result of insert rows for every taosBenchmark_id
        :param result_files:result files in run_log.dir
        :return:
        """
        j_file = InsertFile()
        thread_list = []
        data_list = []
        table_list = ["thread_num", "actual_insert(rows)", "rate(rec/s)"]
        sum_dict = j_file.get_dt_thread_times(result_files)

        file_name = self.run_log_dir + '/perf_report.txt'
        for k, v in sum_dict.items():
            thread_list.append(k)
            data_list.append(v)

        for i in range(len(data_list)):

            thread_id_list = []
            data_list1 = []

            f = open(file_name, 'a')
            f.write("\n************** " +
                    thread_list[i] + " Insert Result *************\n")
            f.close()

            for k, v in data_list[i].items():
                thread_id_list.append(k)
                data_list1.append(list(v))

            for j in range(len(data_list1)):
                data_list1[j].insert(0, thread_id_list[j])

            self.draw_table(table_list, data_list1)

            actual_insert_num = 0

            for m in range(len(data_list1)):
                actual_insert_num += data_list1[m][1]

            f = open(file_name, 'a')
            f.write("Actual insert rows of " +
                    thread_list[i] + ":\t" + str(actual_insert_num) + "\trows\n")
            f.write("\n")
            f.close()

    def taosBenchmark_insert_summary_result(self, result_files: list = None):
        """
        description: This method is used to get the result of statistical information for every taosBenchmark_id
        :param result_files:result files in run_log.dir
        :return:
        """
        j_file = InsertFile()

        thread_proc = j_file.get_dt_total_summary(result_files)
        thread_list = []
        data_list = []
        for k, v in thread_proc.items():
            thread_list.append(k)
            data_list.append(v)

        data_list1 = []
        table_list = ["taosBenchmark_id", "min(ms)", "avg(ms)", "p90(ms)", "p95(ms)", "p99(ms)", "max(ms)",
                      "total_times(s)", "rate(rec/s)"]

        rate_sum = 0
        for i in range(len(data_list)):
            data_list1.append(list(data_list[i].values()))
            data_list1[i].insert(0, thread_list[i])

        for i in range(len(data_list1)):
            rate_sum += data_list1[i][-1]

        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write(
            "\n****************************** Total insert result ******************************\n")
        f.close()

        self.draw_table(table_list, data_list1)
        f = open(file_name, 'a')
        f.write("The insert rate sum is :\t" + str(rate_sum) + "\t rec/s\n\n")
        f.close()

    def get_process_exporter_info(self, env_setting: list = None, interval: int = None, timestamp_start: str = None, timestamp_end: str = None):
        """
        :description: This method is used to get the result of process_thread_info
        :param result_files:env_setting in .yaml file
        :return:
        """
        env_setting_dict = env_setting[0]["spec"]["process_exporter"]["config"]["custom_process"]
        remote = Remote(self.logger)
        p_result = PrometheusServer(remote)

        data_dict, dataframe_dict = p_result.get_custom_query_range_datas(env_setting[0],
                                                                          ["cpu_utilization", "mem_usage", "disk_write",
                                                                           "disk_read"],
                                                                          timestamp_start, timestamp_end,
                                                                          interval, env_setting_dict
        )
        # get process_exporter data
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write(
            "\n******************************\tprocess_info\t******************************\n")

        for fqdn_key in data_dict["process_exporter"].keys():

            for thread_name in data_dict["process_exporter"][fqdn_key].keys():
                table_list = ["index_name", "max_value",
                              "min_value", "avg_value", "p90", "p95", "p99"]
                statistics_data = []

                # print(data_dict["process_exporter"][fqdn_key][thread_name].keys())
                if data_dict["process_exporter"][fqdn_key][thread_name]["cpu_utilization"]:
                    for index_name in data_dict["process_exporter"][fqdn_key][thread_name].keys():

                        statistics_index = []

                        if index_name == "cpu_utilization" or index_name == "mem_usage":
                            for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                                statistics_index.append(
                                    float(data_dict["process_exporter"][fqdn_key][thread_name][index_name][0][i]))
                        elif index_name == "disk_write" or index_name == "disk_read":
                            for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                                statistics_index.append(round(float(
                                    data_dict["process_exporter"][fqdn_key][thread_name][index_name][0][i]) / 1024, 2))

                        if index_name == "cpu_utilization":
                            statistics_index.insert(0, "cpu_utilization(%)")
                        elif index_name == "mem_usage":
                            statistics_index.insert(0, "mem_usage(M)")
                        elif index_name == "disk_write":
                            statistics_index.insert(0, "disk_write(Byte/s)")
                        elif index_name == "disk_read":
                            statistics_index.insert(0, "disk_read(Byte/s)")

                    statistics_data.append(statistics_index)

                    f.write(
                        "\n*******************\t" + fqdn_key + ":\t" + thread_name + "\t****************************\n\n")
                    self.draw_table(table_list, statistics_data)
                else:
                    f = open(file_name, 'a')
                    f.write(
                        "\n*******************\tno responce data of process\t" + fqdn_key + ":\t" + thread_name + "\t****************************\n")
                    f.close()

    def get_node_exporter_info(self, env_setting: list = None, interval: int = None, timestamp_start: str = None, timestamp_end: str = None):
        """
        :description: This method is used to get the result of node_info
        :param result_files:env_setting in .yaml file
        :return:
        """
        env_setting_dict = env_setting[0]["spec"]["process_exporter"]["config"]["custom_process"]
        remote = Remote(self.logger)
        p_result = PrometheusServer(remote)

        data_dict, dataframe_dict = p_result.get_custom_query_range_datas(env_setting[0],
                                                                          ["net_read",
                                                                              "net_write"],
                                                                          timestamp_start, timestamp_end,
                                                                          interval, env_setting_dict
                                                                          )
        print(data_dict)
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write(
            "\n******************************\tnode_info\t******************************\n")

        for fqdn_key in data_dict["node_exporter"].keys():

            statistics_data = []
            table_list = ["index_name", "max_value",
                          "min_value", "avg_value", "p90", "p95", "p99"]

            if data_dict["node_exporter"][fqdn_key]["net_read"]:
                for index_name in data_dict["node_exporter"][fqdn_key].keys():

                    statistics_index = []
                    if data_dict["node_exporter"][fqdn_key][index_name][0]:
                        for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                            statistics_index.append(
                                round(float(data_dict["node_exporter"][fqdn_key][index_name][0][i]) / 1024, 2))

                        if index_name == "net_write":
                            statistics_index.insert(0, "net_write(Byte/s)")
                        elif index_name == "net_read":
                            statistics_index.insert(0, "net_read(Byte/s)")
                        statistics_data.append(statistics_index)

                f.write("\n")
                f.write("\n*******************\t" + fqdn_key +
                        "\t****************************\n")

                self.draw_table(table_list, statistics_data)
            else:
                f = open(file_name, 'a')
                f.write("\n*******************\tno responce data of nodes:\t" +
                        fqdn_key + "\t****************************\n")
                f.close()

    def run(self):

        iplist: List = self.get_fqdn("taosBenchmark")

        json_data: List = []
        file_name = []
        jfile = InsertFile()
        col = jfile.schemacfg(intcount=4, binarycount=(
            2, 16), doublecount=4, tscount=1)
        tag = jfile.schemacfg(intcount=1, binarycount=(1, 16))

        # print(col)
        # print(tag)
        for i in range(len(iplist)):
            if i == 0:
                db = jfile.setDBinfo(name="db1", drop="yes")
            else:
                db = jfile.setDBinfo(name="db1", drop="no")
            stb = jfile.setStbinfo(name="stb", childtable_prefix="stb_" + str(i), childtable_count=100,
                                   insert_rows=10000, columns=col, tags=tag)

            database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
            json_info = jfile.setJsoninfo(host="vm85", databases=[database1])
            json_info.update({"test_log": "/root/testlog/"})
            json_data.append({})
            json_data[i] = json_info
            file_name.append("insert" + str(i) + ".json")
            jfile.genBenchmarkJson(
                self.run_log_dir, file_name[i], json_info)

        # put the file to target
        self.put_file(iplist, json_data)
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # run taosBenchmark
        result_filename = self.threads_run_taosBenchmark(
            iplist, json_data, file_name)

        timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # get insert result
        self.full_create_tb_result(result_filename)
        self.taosBenchmark_id_insert_result(result_filename)
        self.taosBenchmark_insert_summary_result(result_filename)

        # get node_info and process_info
        env_setting = self.get_component_by_name("prometheus")
        # self.get_result(env_setting)
        self.get_process_exporter_info(
            env_setting, 1, timestamp_start, timestamp_end)
        self.get_node_exporter_info(
            env_setting, 1, timestamp_start, timestamp_end)
        # ………………
