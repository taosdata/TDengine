import threading
from datetime import datetime

from prettytable import PrettyTable
from taostest import TDCase
from taostest.components import PrometheusServer
from taostest.performance.perfor_basic import QueryFile

from taostest.util.remote import Remote



class QueryTest(TDCase):
    def desc(self):
        pass


    def author(self):
        pass

    def tags(self):
        pass

    def put_file(self, iplist: list, json_data: list, file_name: list):
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
            remote.put(iplist[i], self.run_log_dir + "/" + str(file_name[i]), json_data[i]["test_log"])

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

        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')

        f.write(str(x) + '\n')
        f.close()

    def threads_run_taosBenchmark(self, iplist, json_data,file_name:list):
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

            t.append(threading.Thread(target=remote.cmd,
                                      args=(
                                          iplist[i],
                                          [
                                              f'taosBenchmark -f {json_data[i]["test_log"]}{file_name[i]} 2>&1 | tee /tmp/{i}.log '])))
            t[i].start()

        for i in t:
            i.join()

        result_files = []
        for i in range(len(t)):
            filename = self.run_log_dir + "/" + str(i) + "-" + iplist[i]
            result_files.append(filename)

            remote.get(iplist[i], f"/tmp/{i}.log", filename)
            remote.cmd(iplist[i], [f'rm -rf {json_data[i]["test_log"]}'])

        return result_files

    def get_summary_result_table(self, result_filename):
        q_file = QueryFile()

        query_full_result = q_file.get_dt_total_summary(result_filename)
        thread_list = []
        data_list = []
        for k, v in query_full_result.items():
            thread_list.append(k)
            data_list.append(v)


        data_list1 = []

        table_list = ["taosBenchmark_id", "total times", "QPS /s"]

        for i in range(len(data_list)):
            data_list1.append(list(data_list[i].values()))
            data_list1[i].insert(0, thread_list[i])


        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write("\n*****************\tQuery result\t*****************\n")
        f.close()
        self.draw_table(table_list, data_list1)

    def get_taosBenchmark_process_info(self, result_filename):
        q_file = QueryFile()
        proc_dict = q_file.get_dt_thread_summary(result_filename)
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write("\n*****************\ttaosBenchmark info\t*****************\n")
        f.close()
        for taosBenchmark_id in proc_dict.keys():

            statistics_data = []
            table_list = ["thread_num", "min (ms)", "avg (ms)", "p90(ms)", "p95(ms)", "p99(ms)", "max(ms)"]
            for thread_id in proc_dict[taosBenchmark_id].keys():


                statistics_index = []
                statistics_index.append(thread_id)
                for i in "min", "avg", "p90", "p95", "p99", "max":
                    statistics_index.append(round(float(proc_dict[taosBenchmark_id][thread_id][i])/1000,2))

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
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write("\n******************************\tprocess_info\t******************************\n")
        f.close()
        for fqdn_key in data_dict["process_exporter"].keys():

            for thread_name in data_dict["process_exporter"][fqdn_key].keys():
                table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
                statistics_data = []
                if (len(data_dict["process_exporter"][fqdn_key][thread_name]["cpu_utilization"]) > 1) and \
                    (len(data_dict["process_exporter"][fqdn_key][thread_name]["mem_usage"]) > 1) and \
                        (len(data_dict["process_exporter"][fqdn_key][thread_name]["disk_write"]) > 1) and \
                            (len(data_dict["process_exporter"][fqdn_key][thread_name]["disk_read"]) > 1):
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
                            statistics_index.insert(0, "mem_usage(MB)")
                        elif index_name == "disk_write":
                            statistics_index.insert(0, "disk_write(KB/s)")
                        elif index_name == "disk_read":
                            statistics_index.insert(0, "disk_read(KB/s)")

                        statistics_data.append(statistics_index)

                    f = open(file_name, 'a')
                    f.write(
                        "\n*******************\t" + fqdn_key + ":\t" + thread_name + "\t****************************\n\n")
                    f.close()
                    self.draw_table(table_list, statistics_data)
                else:
                    f = open(file_name, 'a')
                    f.write(
                        "\n*******************\tno responce data of process\t" + fqdn_key + ":\t" + thread_name + "\t****************************\n")
                    f.close()

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
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write("\n******************************\tnode_info\t******************************\n")
        f.close()
        for fqdn_key in data_dict["node_exporter"].keys():
            statistics_data = []
            table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
            if len(data_dict["node_exporter"][fqdn_key]["net_write"])>1 and len(data_dict["node_exporter"][fqdn_key]["net_read"])>1:
                for index_name in data_dict["node_exporter"][fqdn_key].keys():
                    statistics_index = []
                    for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                        statistics_index.append(
                            round(float(data_dict["node_exporter"][fqdn_key][index_name][0][i]) / 1024, 2))

                    if index_name == "net_write":
                        statistics_index.insert(0, "net_write(Kb/s)")
                    elif index_name == "net_read":
                        statistics_index.insert(0, "net_read(Kb/s)")
                    statistics_data.append(statistics_index)

                f = open(file_name, 'a')
                f.write("\n")
                f.write("\n*******************\t" + fqdn_key + "\t****************************\n")
                f.close()
                self.draw_table(table_list, statistics_data)
            else:
                f = open(file_name, 'a')
                f.write(
                        "\n*******************\tno responce data of process\t" + fqdn_key + "\t****************************\n")
                f.close()
    def run(self):
        # create json_file
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosd_iplist = self.get_fqdn("taosd")
        jfile = QueryFile()
        file_name = []
        json_data = []

        for i in range(len(taosBenchmark_iplist)):
            json_data.append({})
            sql_info = jfile.setSqlInfo(sql="select last(*) from stb")
            specify_query = jfile.setSpecifyQuery(concurrent=1, sqls=[sql_info])
            json_info = jfile.setJsoninfo(host=taosd_iplist[0], query_times=100, confirm_parameter_prompt="no",
                                          specified_table_query=specify_query, databases="db1")
            json_info.update({"query_mode": "taosc","test_log": "/root/testlog/"})
            json_data[i] = json_info

            file_name.append("query" + str(i) + ".json")
            jfile.genBenchmarkJson(self.run_log_dir, file_name[i], json_info)


        # put the file to target
        self.put_file(taosBenchmark_iplist, json_data,file_name)

        # run taosBenchmark and get result file
        self.timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        result_filename = self.threads_run_taosBenchmark(taosBenchmark_iplist, json_data,file_name)
        self.timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


        # 提取结果用于测试报告
        self.get_summary_result_table(result_filename)
        self.get_taosBenchmark_process_info(result_filename)
        # self.get_result(result_filename)

        env_setting = self.get_component_by_name("prometheus")
        self.get_process_exporter_info(env_setting, 1)
        self.get_node_exporter_info(env_setting, 1)
