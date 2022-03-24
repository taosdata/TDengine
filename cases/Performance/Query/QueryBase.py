import threading
from datetime import datetime

from prettytable import PrettyTable
from taostest import TDCase
from taostest.components import PrometheusServer
from taostest.performance.perfor_basic import QueryFile

from taostest.util.remote import Remote
import json


class QueryTest(TDCase):
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def get_json(self, filename) -> dict:
        """
            description: This method is used to get the contents of a JSON file
            param filename: the JSON file in the current directory
            return: json_data
                """
        file = open(filename, 'r', encoding='utf-8')
        json_data = json.load(file)
        return json_data

    def put_file(self, iplist: list, json_data: list):
        """
        description: This method is used to get the contents of a JSON file.

        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        return: none
        """

        remote = Remote(self.logger)
        for i in range(len(iplist)):
            filename = "/Query-data" + str(i) + ".json"
            json_name = "Query-test" + str(i) + ".json"
            remote.cmd(iplist[i], [f'mkdir {json_data[i][json_name]["test_log"]}'])
            remote.put(iplist[i], self.run_log_dir + filename, json_data[i][json_name]["test_log"])

    def draw_table(self, tablelist, datalist):
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

        file_name = self.run_log_dir + '/report_file.txt'
        f = open(file_name, 'a')

        f.write(str(x) + '\n')
        f.close()

    def threads_run_taosBenchmark(self, iplist, json_data):
        """
        description: This method is used to start several threads to run taosBenchmark ,and then
                    get the result file to local machine

        param iplist: the list of fqdn from env.yaml
        param json_data: the data from jsonfile
        return: result_files list
        """
        remote = Remote(self.logger)
        t = []
        # start threads
        for i in range(len(iplist)):
            json_name = "Query-test" + str(i) + ".json"
            t.append(threading.Thread(target=remote.cmd,
                                      args=(
                                          iplist[i],
                                          [
                                              f'taosBenchmark -f {json_data[i][json_name]["test_log"]}Query-data{i}.json 2>&1 | tee /tmp/{i}.log '])))
            t[i].start()

        # waiting for threads run done
        for i in t:
            i.join()

        # get result_file and remove test_log
        result_files = []
        for i in range(len(t)):
            json_name = "Query-test" + str(i) + ".json"

            # rename result_file
            filename = self.run_log_dir + "/" + str(i) + "-" + iplist[i]
            result_files.append(filename)

            remote.get(iplist[i], f"/tmp/{i}.log", filename)
            remote.cmd(iplist[i], [f'rm -rf {json_data[i][json_name]["test_log"]}'])

        return result_files

    def get_summary_result_table(self, result_filename):
        q_file = QueryFile()

        query_full_result = q_file.get_dt_total_summary(result_filename)
        thread_list = []
        data_list = []
        for k, v in query_full_result.items():
            thread_list.append(k)
            data_list.append(v)

        # print(thread_list)
        # print(data_list)
        data_list1 = []
        table_list = []
        # print(data_list[0])
        table_list = ["taosBenchmark_id", "total times", "QPS /s"]

        for i in range(len(data_list)):
            data_list1.append(list(data_list[i].values()))
            data_list1[i].insert(0, thread_list[i])

        # print(data_list1)
        file_name = self.run_log_dir + '/report_file.txt'
        f = open(file_name, 'a')
        f.write("\n*****************\tQuery result\t*****************\n")
        f.close()
        self.draw_table(table_list, data_list1)

    def get_taosBenchmark_process_info(self, result_filename):
        q_file = QueryFile()
        proc_dict = q_file.get_dt_thread_summary(result_filename)
        file_name = self.run_log_dir + '/report_file.txt'
        f = open(file_name, 'a')
        f.write("\n*****************\ttaosBenchmark info\t*****************\n")
        f.close()
        for taosBenchmark_id in proc_dict.keys():
            # print(taosBenchmark_id)
            statistics_data = []
            table_list = ["thread_num", "min (us)", "avg (us)", "p90(us)", "p95(us)", "p99(us)", "max(us)"]
            for thread_id in proc_dict[taosBenchmark_id].keys():
                # print(thread_id)

                statistics_index = []
                statistics_index.append(thread_id)
                for i in "min", "avg", "p90", "p95", "p99", "max":
                    statistics_index.append(proc_dict[taosBenchmark_id][thread_id][i])

                statistics_data.append(statistics_index)
            f = open(file_name, 'a')
            f.write("\n-----------------\t" + taosBenchmark_id + "\t-----------------\n")
            f.close()
            self.draw_table(table_list, statistics_data)
    def get_process_exporter_info(self, env_setting: list = None, interval: int = None):
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
                                                                          self.timestamp_start, self.timestamp_end,
                                                                          interval, env_setting_dict
                                                                          )
        # get process_exporter data
        file_name = self.run_log_dir + '/report_file.txt'
        f = open(file_name, 'a')
        f.write("\n*****************\tprocess_info\t*****************\n")
        f.close()
        for fqdn_key in data_dict["process_exporter"].keys():

            for thread_name in data_dict["process_exporter"][fqdn_key].keys():
                table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
                statistics_data = []

                for index_name in data_dict["process_exporter"][fqdn_key][thread_name].keys():

                    statistics_index = []
                    for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                        statistics_index.append(data_dict["process_exporter"][fqdn_key][thread_name][index_name][0][i])
                    if index_name == "cpu_utilization":
                        statistics_index.insert(0, "cpu_utilization(%)")
                    elif index_name == "mem_usage":
                        statistics_index.insert(0, "mem_usage(M)")
                    elif index_name == "disk_write":
                        statistics_index.insert(0, "disk_write(Byte/s)")
                    elif index_name == "disk_read":
                        statistics_index.insert(0, "disk_read(Byte/s)")

                    statistics_data.append(statistics_index)

                f = open(file_name, 'a')
                f.write("\n-----------------\t" + fqdn_key + ":\t" + thread_name + "\t-----------------\n")
                f.close()
                self.draw_table(table_list, statistics_data)

    def get_node_exporter_info(self, env_setting: list = None, interval: int = None):
        """
        :description: This method is used to get the result of node_info
        :param result_files:env_setting in .yaml file
        :return:
        """
        env_setting_dict = env_setting[0]["spec"]["process_exporter"]["config"]["custom_process"]
        remote = Remote(self.logger)
        p_result = PrometheusServer(remote)

        data_dict, dataframe_dict = p_result.get_custom_query_range_datas(env_setting[0],
                                                                          ["net_read", "net_write"],
                                                                          self.timestamp_start, self.timestamp_end,
                                                                          interval, env_setting_dict
                                                                          )
        file_name = self.run_log_dir + '/report_file.txt'
        f = open(file_name, 'a')
        f.write("\n-----------------\tnode_info\t-----------------\n")
        f.close()
        for fqdn_key in data_dict["node_exporter"].keys():
            statistics_data = []
            table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
            for index_name in data_dict["node_exporter"][fqdn_key].keys():

                statistics_index = []

                for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                    statistics_index.append(data_dict["node_exporter"][fqdn_key][index_name][0][i])

                if index_name == "net_write":
                    statistics_index.insert(0, "net_write(Byte/s)")
                elif index_name == "net_read":
                    statistics_index.insert(0, "net_read(Byte/s)")
                statistics_data.append(statistics_index)

            f = open(file_name, 'a')
            f.write("\n*****************\t" + fqdn_key + "\t*****************\n")
            f.close()
            self.draw_table(table_list, statistics_data)

    def get_result(self, result_filename):
        j_file = QueryFile()

        print(j_file.get_dt_thread_summary(result_filename))
        # print(j_file.get_thread_process_info(result_filename))

    def run(self):
        # create json_file
        iplist = self.get_fqdn("taosBenchmark")
        json_data = []
        for i in range(len(iplist)):
            filename = "Query-test" + str(i) + ".json"
            outfile_name = "Query-data" + str(i) + ".json"
            json_data.append({})
            json_data[i][filename] = self.get_json(filename)
            self.genBenchmarkJson(outfile_name, "./query-data.json", json_data[i][filename])

        # put the file to target
        self.put_file(iplist, json_data)
        # run taosBenchmark and get result file
        self.timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        result_filename = self.threads_run_taosBenchmark(iplist, json_data)
        self.timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        # 提取结果用于测试报告
        self.get_summary_result_table(result_filename)
        self.get_taosBenchmark_process_info(result_filename)
        # self.get_result(result_filename)


        env_setting = self.get_component_by_name("prometheus")
        self.get_process_exporter_info(env_setting, 1)
        self.get_node_exporter_info(env_setting, 1)