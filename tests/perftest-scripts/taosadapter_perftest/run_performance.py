from config.env_init import *
from src.common.common import Common
from src.common.dnodes import Dnodes
from src.common.monitor import Monitor
from src.util.jmeter import Jmeter

class RunPerformance:
    def __init__(self):
        self.COM = Common()
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.log_dir = os.path.join(self.current_dir, f'./log')
        if config["jmeter"]["clean_aggregate_report"]:
            self.COM.exec_local_cmd(f'sudo rm -rf {self.log_dir}/testcase*')

    def runJmeter(self):
        for key, value in config['testcases'].items():
            jmx_file_list = list()
            logger.info(f'executing {key}')
            for jmx_file in self.COM.genJmxFile(key)[:value["taosadapter_count"]]:
                jmx_filename = jmx_file.split('/')[-1]
                import_file_name = jmx_filename.replace('jmx', 'txt')
                import_file = os.path.join(self.current_dir, f'./config/{import_file_name}')
                loop_count = self.COM.getLoopCount(value["stb_count"], value["tb_count"], value["row_count"], value["threads"])
                self.COM.genMixStbTbRows(import_file, value["stb_count"], value["tb_count"], value["row_count"])
                input_line = self.COM.genProtocolLine(value["protocol"], value["tag_count"])
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
            jmeter_cmd_list = self.COM.genJmeterCmd(jmx_file_list)
            self.COM.multiThreadRun(self.COM.genJmeterThreads(jmeter_cmd_list))
            time.sleep(int(''.join(list(filter(str.isdigit, str(value["sleep_time"]))))))

if __name__ == '__main__':
    Performance = RunPerformance()
    DNODES = Dnodes()
    MONITOR = Monitor()
    JMETER = Jmeter() 
    if config['deploy_mode'] == "auto":
        if config['taosd_autodeploy']:
            DNODES.deployNodes()
        if config["prometheus"]["autodeploy"]:
            MONITOR.deployAllNodeExporters()
            MONITOR.deployAllProcessExporters()
            MONITOR.deployPrometheus()
            MONITOR.deployGrafana()
        if config["jmeter"]["autodeploy"]:
            JMETER.deployJmeter()
    Performance.runJmeter()


