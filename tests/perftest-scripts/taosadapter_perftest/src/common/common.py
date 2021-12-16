import sys
sys.path.append("../../")
from config.env_init import *
import shutil
import threading
import time
import json
class Common:
    def __init__(self):
        self.ip_list = list()
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.base_jmx_file = os.path.join(self.current_dir, '../../config/taosadapter_performance_test.jmx')
        self.log_dir = os.path.join(self.current_dir, '../../log')

    def exec_local_cmd(self,shell_cmd):
        logger.info(f'executing cmd: {shell_cmd}')
        result = os.popen(shell_cmd).read().strip()
        logger.info(result)
        return result

    def genTelnetMulTagStr(self, count):
        tag_str = ""
        for i in range(1, count):
            if i < (count-1):
                tag_str += f't{i}={i} '
            else:
                tag_str += f't{i}={i}'
        return tag_str

    def genJsonMulTagDict(self, count):
        tag_dict = dict()
        for i in range(1, count):
            tag_dict[f"t{i}"] = f"{i}"
        return tag_dict
        
    def genProtocolLine(self, protocol, tag_count, col_count=None):
        if protocol == "telnet-restful":
            base_str = 'stb_${stb_csv_count} ${row_csv_count} 32.261068286779754 t0=${tb_csv_count} '
            tag_str = self.genTelnetMulTagStr(tag_count)
            telnet_line = base_str + tag_str
            return telnet_line
        elif protocol == "telnet-tcp":
            base_str = 'stb_${stb_csv_count} ${row_csv_count} 32.261068286779754 t0=${tb_csv_count} '
            tag_str = self.genTelnetMulTagStr(tag_count)
            telnet_line = base_str + tag_str + '${__unescape(\r\n)}'
            return telnet_line
        elif protocol == "json":
            base_tag_dict = {"t0":"${tb_csv_count}"}
            dict_merged = base_tag_dict.copy()
            dict_merged.update(self.genJsonMulTagDict(tag_count))
            json_line = '{"metric": "stb_${stb_csv_count}", "timestamp":${row_csv_count}, "value":32.261068286779754, ' + f'"tags": {dict_merged}' + '}'
            return json_line.replace('\'','"')
        elif protocol == "influxdb":
            # TODO
            pass
        else:
            pass

    def genMixStbTbRows(self, filename, stb_count, tb_count, row_count):
        if stb_count == 0:
            stb_count = 1
        if tb_count == 0:
            tb_count = 1
        if row_count == 0:
            row_count = 1
        logger.info(f'generating import data file: {filename}')
        ts_start = 1614530008000
        with open(filename, "w", encoding="utf-8") as f_w:
            for k in range(stb_count):
                for i in range(tb_count):
                    for j in range(row_count):
                        input_line = str(ts_start) + "," + str(i) + "," + str(k) + '\n'
                        ts_start += 1
                        f_w.write(input_line)

    def genJmxFile(self, testcase):
        des_jmx_file_list = list()
        base_jmx_file = os.path.join(self.current_dir, '../../config/taosadapter_performance_test.jmx')
        count_flag = 0
        if config["taosadapter_separate_deploy"]:
            for key in config:
                if "taosd_dnode" in str(key) and "taosd_dnode1" not in str(key):
                    if count_flag < int(config['testcases'][testcase]['taosadapter_count']):
                        count_flag += 1
                    else:
                        break
                    des_jmx_file = os.path.join(self.current_dir, f'../../config/{testcase}_{key}.jmx')
                    shutil.copyfile(base_jmx_file, des_jmx_file)
                    with open(des_jmx_file, 'r', encoding='utf-8') as f:
                        file_data = ""
                        for line in f:
                            if "restful_ip" in line:
                                line = line.replace("restful_ip", config[key]['ip'])
                            if "restful_port" in line:
                                line = line.replace("restful_port", str(config[key]['restful_port']))
                            if "telnet_ip" in line:
                                line = line.replace("telnet_ip", config[key]['ip'])
                            if "telnet_port" in line:
                                line = line.replace("telnet_port", str(config[key]['telnet_port']))
                            # if "db_name" in line:
                            #     line = line.replace("db_name", key)
                            file_data += line
                    with open(des_jmx_file, "w", encoding="utf-8") as f:
                        f.write(file_data)
                    des_jmx_file_list.append(des_jmx_file)
        else:
            des_jmx_file = os.path.join(self.current_dir, f'../../config/taosd_dnode1.jmx')
            shutil.copyfile(base_jmx_file, des_jmx_file)

            with open(des_jmx_file, 'r', encoding='utf-8') as f:
                file_data = ""
                for line in f:
                    if "restful_ip" in line:
                        line = line.replace("restful_ip", config['taosd_dnode1']['ip'])
                    if "restful_port" in line:
                        line = line.replace("restful_port", str(config['taosd_dnode1']['restful_port']))
                    if "telnet_ip" in line:
                        line = line.replace("telnet_ip", config['taosd_dnode1']['ip'])
                    if "telnet_port" in line:
                        line = line.replace("telnet_port", str(config['taosd_dnode1']['telnet_port']))
                    # if "db_name" in line:
                    #     line = line.replace("db_name", "taosd_dnode1")
                    file_data += line
            with open(des_jmx_file, "w", encoding="utf-8") as f:
                f.write(file_data)
            des_jmx_file_list.append(des_jmx_file)
        return des_jmx_file_list

    def getLoopCount(self, stb_count, tb_count, row_count, threads):
        if (stb_count * tb_count * row_count) % threads == 0:
            loop_count = int((stb_count * tb_count * row_count) / threads)
        else:
            loop_count = int((stb_count * tb_count * row_count) / threads) + 1
        return loop_count

    def recreateReportDir(self, path):
        '''
            recreate jmeter report path
        '''
        if os.path.exists(path):
            self.exec_local_cmd(f'ls {path} | grep -v performance_ | xargs rm -rf')
        else:
            os.makedirs(path)

    def genJmeterCmd(self, jmx_file_list):
        jmeter_cmd_list = list()
        for jmx_file in jmx_file_list:
            jmeter_cmd = f'jmeter -n -t {jmx_file}'
            if config['jmeter']['aggregate_report']:
                current_time = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime(time.time()))
                jmx_filename = jmx_file.split('/')[-1].replace('.jmx', '')
                jmx_filelog = f'{jmx_filename}_{current_time}'
                jmeter_report_dir = f'{self.log_dir}/{jmx_filelog}'
                self.recreateReportDir(jmeter_report_dir)
                jmeter_cmd += f' -l {jmeter_report_dir}/{jmx_filelog}.log -e -o {jmeter_report_dir}'
            jmeter_cmd_list.append(jmeter_cmd)
        return jmeter_cmd_list

    def genJmeterThreads(self, jmeter_cmd_list):
        tlist = list()
        for jmeter_cmd in jmeter_cmd_list:
            t = threading.Thread(target=self.exec_local_cmd, args=(jmeter_cmd,))
            tlist.append(t)
        return tlist
    
    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def runJmeter(self):
        for key, value in config['testcases'].items():
            jmx_file_list = list()
            logger.info(f'executing {key}')
            for jmx_file in self.genJmxFile(key)[:value["taosadapter_count"]]:
                jmx_filename = jmx_file.split('/')[-1]
                import_file_name = jmx_filename.replace('jmx', 'txt')
                import_file = os.path.join(self.current_dir, f'../../config/{import_file_name}')
                loop_count = self.getLoopCount(value["stb_count"], value["tb_count"], value["row_count"], value["threads"])
                self.genMixStbTbRows(import_file, value["stb_count"], value["tb_count"], value["row_count"])
                input_line = self.genProtocolLine(value["protocol"], value["tag_count"])
                with open(jmx_file, 'r', encoding='utf-8') as f:
                    file_data = ""
                    for line in f:
                        if value['protocol'] == 'telnet-tcp':
                            if "telnet_tcp_status" in line:
                                line = line.replace("telnet_tcp_status", "true")
                        if value['protocol'] == 'telnet-restful' or value['protocol'] == 'json':
                            if "drop_db_status" in line:
                                line = line.replace("drop_db_status", "true")
                            if "create_db_status" in line:
                                line = line.replace("create_db_status", "true")
                            if "telnet_restful_status" in line:
                                line = line.replace("telnet_restful_status", "true")
                            if "line_protocol" in line:
                                if value['protocol'] == 'telnet-restful':
                                    line = line.replace("line_protocol", 'telnet')
                                elif value['protocol'] == 'json':
                                    line = line.replace("line_protocol", 'json')
                                else:
                                    pass
                            if "db_name" in line:
                                db_name = jmx_filename.split('.')[0]
                                line = line.replace("db_name", db_name)
                        if "import_file" in line:
                            line = line.replace("import_file", import_file)
                        if "input_line" in line:
                            line = line.replace("input_line", input_line)
                        if "perf_threads" in line:
                            line = line.replace("perf_threads", str(value['threads']))
                        if "loop_count" in line:
                            line = line.replace("loop_count", str(loop_count))
                        file_data += line
                with open(jmx_file, "w", encoding="utf-8") as f:
                    f.write(file_data)
                jmx_file_list.append(jmx_file)
            jmeter_cmd_list = self.genJmeterCmd(jmx_file_list)
            self.multiThreadRun(self.genJmeterThreads(jmeter_cmd_list))
            time.sleep(int(''.join(list(filter(str.isdigit, str(value["sleep_time"]))))))


            # else:
            #     import_file = os.path.join(self.current_dir, f'../../config/taosd_dnode1.txt')
            #     loop_count = self.getLoopCount(value["stb_count"], value["tb_count"], value["row_count"], value["threads"])
            #     self.genMixStbTbRows(import_file, value["stb_count"], value["tb_count"], value["row_count"])
            #     input_line = self.genProtocolLine(value["protocol"], value["tag_count"])
            #     jmx_file = self.genJmxFile()[0]
            #     with open(jmx_file, 'r', encoding='utf-8') as f:
            #         file_data = ""
            #         for line in f:
            #             if "import_file" in line:
            #                 line = line.replace("import_file", import_file)
            #             if "input_line" in line:
            #                 line = line.replace("input_line", input_line)
            #             if "perf_threads" in line:
            #                 line = line.replace("perf_threads", str(value['threads']))
            #             if "loop_count" in line:
            #                 line = line.replace("loop_count", str(loop_count))
            #             file_data += line
            #     with open(jmx_file, "w", encoding="utf-8") as f:
            #         f.write(file_data)

if __name__ == '__main__':
    com = Common()
    # print(com.genProtocolLine("telnet", 5))
    # print(com.genJmxFile())
    # print(com.genTelnetLine(10))
    # stb_count = 5
    # tb_count = 10
    # row_count = 1
    # com.genMixStbTbRows("mix.txt", 5, 10, 1)
    print(com.runJmeter())