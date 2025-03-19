import threading
import time
from prettytable import PrettyTable
from taostest.components import PrometheusServer
from taostest.performance.perfor_basic import InsertFile, QueryFile
from taostest.util.remote import Remote


class Perf_Base_func():
    def __init__(self, logger, run_log_dir):
        self.logger = logger
        self.run_log_dir = run_log_dir

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

    def threads_run_taosBenchmark(self, iplist, json_data, file_name: list,env_setting):
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
                                              f'taosBenchmark -c {env_setting[0]["spec"]["config_dir"]} -f {json_data[i]["test_log"]}{file_name[i]} 2>&1 | tee /tmp/{i}.log '])))

            t[i].start()
            time.sleep(1)

        for i in t:
            i.join()

        result_files = []

        for i in range(len(t)):
            # rename result file
            filename = self.run_log_dir + '/' + str(i) + "-" + iplist[i]
            result_files.append(filename)
            # get result_files and remove test_log
            remote.get(iplist[i], f"/tmp/{i}.log", filename)
            # remote.cmd(iplist[i], [f'rm -rf {json_data[i]["test_log"]}'])

        return result_files

    def draw_table(self, tablelist: list, datalist: list):
        """
        description:This method is used to draw table(表格) for final result.
        :param tablelist -> list : header list of table
        :param datalist -> list : data
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

        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')

        f.write(str(x) + '\n')
        f.close()

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

        table_list = ["taosBenchmark_id", "times(s)", "tables", "threads", "actual_create","rate(tables/s)"]
        data_list1 = []

        # insert the result data
        for i in range(len(data_list)):
            create_tb_rate = round((data_list[0]["actual_create"] / data_list[0]["times"]), 3)
            data_list1.append(list(data_list[i].values()))
            data_list1[i].insert(0, taosBenchmark_list[i])
            data_list1[i].append(create_tb_rate)

        # draw table

        file_name = self.run_log_dir + '/perf_report.txt'
        total_table_num = 0
        actual_create_num = 0
        for i in range(len(data_list1)):
            total_table_num += data_list1[i][2]
            actual_create_num += data_list1[i][4]
        f = open(file_name, 'a')
        f.write("\n*********************** Create Table Result **********************\n")
        f.close()

        self.draw_table(table_list, data_list1)

        f = open(file_name, 'a')
        f.write("Total number of creating tables :" + str(total_table_num) + "\n")
        f.write("Actual number of creating tables :" + str(actual_create_num) + "\n")
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
            f.write("\n************** " + thread_list[i] + " Insert Result *************\n")
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
            f.write("Actual insert rows of " + thread_list[i] + ":\t" + str(actual_insert_num) + "\trows\n")
            f.write("\n")
            f.close()

    def taosBenchmark_insert_summary_result(self, result_files: list = None, version=None):
        """
        description: This method is used to get the result of statistical information for every taosBenchmark_id
        :param result_files:result files in run_log.dir
        :return:
        """
        j_file = InsertFile()

        thread_proc = j_file.get_dt_total_summary(result_files, version=version)
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
        f.write("\n****************************** Total insert result ******************************\n")
        f.close()

        self.draw_table(table_list, data_list1)
        f = open(file_name, 'a')
        f.write("The insert rate sum is :\t" + str(rate_sum) + "\t rec/s\n\n")
        f.close()

    def get_process_exporter_info(self, env_setting: list = None, interval: int = None, timestamp_start: str = None,
                                  timestamp_end: str = None):
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
        f.write("\n******************************\tprocess_info\t******************************\n")
        f.close()
        for fqdn_key in data_dict["process_exporter"].keys():

            for thread_name in data_dict["process_exporter"][fqdn_key].keys():
                table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
                statistics_data = []
                if len(data_dict["process_exporter"][fqdn_key][thread_name]["cpu_utilization"]) > 0 and len(
                        data_dict["process_exporter"][fqdn_key][thread_name]["mem_usage"]) > 0 and len(
                    data_dict["process_exporter"][fqdn_key][thread_name]["disk_write"]) > 0 and len(
                    data_dict["process_exporter"][fqdn_key][thread_name]["disk_read"]) > 0:
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

    def get_node_exporter_info(self, env_setting: list = None, interval: int = None, timestamp_start: str = None,
                               timestamp_end: str = None):
        """
        :description: This method is used to get the result of node_info
        :param result_files:env_setting in .yaml file
        :return:
        """
        env_setting_dict = env_setting[0]["spec"]["process_exporter"]["config"]["custom_process"]
        remote = Remote(self.logger)
        p_result = PrometheusServer(remote)

        data_dict, dataframe_dict = p_result.get_custom_query_range_datas(env_setting[0],
                                                                          ["net_read", "net_write", "disk_io"],
                                                                          timestamp_start, timestamp_end,
                                                                          interval, env_setting_dict
                                                                          )
        file_name = self.run_log_dir + '/perf_report.txt'
        f = open(file_name, 'a')
        f.write("\n******************************\tnode_info\t******************************\n")
        f.close()
        for fqdn_key in data_dict["node_exporter"].keys():
            statistics_data = []
            table_list = ["index_name", "max_value", "min_value", "avg_value", "p90", "p95", "p99"]
            if len(data_dict["node_exporter"][fqdn_key]["net_read"]) > 0 and len(
                    data_dict["node_exporter"][fqdn_key]["net_write"]) > 0 and len(
                    data_dict["node_exporter"][fqdn_key]["disk_io"]) > 0:
                for index_name in data_dict["node_exporter"][fqdn_key].keys():
                    statistics_index = []
                    for i in "max_value", "min_value", "avg_value", "p90", "p95", "p99":
                        if index_name != "disk_io":
                            statistics_index.append(
                                round(float(data_dict["node_exporter"][fqdn_key][index_name][0][i]) / 1024, 2))
                        else:
                            statistics_index.append(data_dict["node_exporter"][fqdn_key][index_name][0][i])

                    if index_name == "net_write":
                        statistics_index.insert(0, "net_write(Kb/s)")
                    elif index_name == "net_read":
                        statistics_index.insert(0, "net_read(Kb/s)")
                    elif index_name == "disk_io":
                        statistics_index.insert(0, "disk_io(%)")
                    statistics_data.append(statistics_index)

                f = open(file_name, 'a')
                f.write("\n")
                f.write("\n*******************\t" + fqdn_key + "\t****************************\n")
                f.close()
                self.draw_table(table_list, statistics_data)
            else:
                f = open(file_name, 'a')
                f.write(
                    "\n*******************\tno responce data of nodes:\t" + fqdn_key + "\t****************************\n")
                f.close()

    def get_summary_query_result(self, result_filename):
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

    def get_taosBenchmark_query_process_info(self, result_filename):
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
                    statistics_index.append(round(float(proc_dict[taosBenchmark_id][thread_id][i]) / 1000, 2))

                statistics_data.append(statistics_index)
            f = open(file_name, 'a')
            f.write("\n-----------------\t" + taosBenchmark_id + "\t-----------------\n")
            f.close()

            self.draw_table(table_list, statistics_data)

            min_delay, max_delay, avg_value, p90, p95, p99 = [], [], [], [], [], []
            for i in range(len(statistics_data)):
                min_delay.append(statistics_data[i][1])
                avg_value.append(statistics_data[i][2])
                p90.append(statistics_data[i][3])
                p95.append(statistics_data[i][4])
                p99.append(statistics_data[i][5])
                max_delay.append(statistics_data[i][6])

            f = open(file_name, 'a')
            f.write("min_delay =\t" + str(min(min_delay)) + "\tms\n")
            f.write("max_delay =\t" + str(max(max_delay)) + "\tms\n")
            f.write("avg_delay =\t" + str(sum(avg_value) / len(avg_value)) + "\tms\n")
            f.write("p90 =\t" + str(max(p90)) + "\tms\n")
            f.write("p95 =\t" + str(max(p95)) + "\tms\n")
            f.write("p99 =\t" + str(max(p99)) + "\tms\n")
            f.close()

