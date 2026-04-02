from typing import Dict

from ..util.file import dict2yaml
from ..util.remote import Remote
from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame, MetricRangeDataFrame
import datetime
import pandas as pd
import socket
import re
from collections import defaultdict


class PrometheusServer:
    def __init__(self, remote: Remote):
        self._remote: Remote = remote
        self._fqdn: str = None
        self.download_addr: str = None
        self.tar_bin_file_md5: str = None
        self.bin_file_md5: str = None
        self.package_dir: str = None
        self.file_path: str = None
        self.bin_file: str = None

        self.prom_conn: PrometheusConnect = None
        self.process_dict: dict = dict()
        self.device = None
        self.disk = None

    def set_prometheus_initial_env(self):
        self.download_addr = "https://github.com/prometheus/prometheus/releases/download/v2.33.3/prometheus-2.33.3.linux-amd64.tar.gz"
        self.tar_bin_file_md5 = "298945801e1b55695c95958658871b40"
        self.bin_file_md5 = "e6a7503a22a7f9fe0c80dcb293d8c39c"

    def set_node_exporter_initial_env(self):
        self.download_addr = "https://github.com/prometheus/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64.tar.gz"
        self.tar_bin_file_md5 = "9ed75103b9bc65b30e407d4f238f9dea"
        self.bin_file_md5 = "e105cbe3e43cc6e133a83358db009d36"

    def set_process_exporter_initial_env(self):
        self.download_addr = "https://github.com/ncabatoff/process-exporter/releases/download/v0.7.10/process-exporter-0.7.10.linux-amd64.tar.gz"
        self.tar_bin_file_md5 = "15339af547ba89abdb5374f38e6003de"
        self.bin_file_md5 = "f10059bf710a789daf2a8dde84aba566"

    def is_ip(self, str):
        """
        confirm if str is ip
        """
        p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
        if p.match(str):
            return True
        else:
            return False

    def date_sync(self):
        """
        date sync
        """
        cmd = "ntpdate -u ntp.aliyun.com"
        self._remote.cmd(self._fqdn, [cmd])

    def get_ip_by_fqdn(self, fqdn):
        """
        get ip by fqdn
        """
        if self.is_ip(fqdn):
            return fqdn
        else:
            return socket.gethostbyname(fqdn)

    def get_device_by_fqdn(self, fqdn):
        """
        get device by fqdn
        """
        ip_addr = self.get_ip_by_fqdn(fqdn)
        self.device = self._remote.cmd(self._fqdn, [f"ip route | grep {ip_addr} | awk -F '[ \\t*]' '{{print $3}}'"])

    def get_disk(self):
        disk_info = self._remote.cmd(self._fqdn, [f"fdisk -l | grep -A10 Device | grep BIOS | awk '{{print $1}}'"])
        self.disk = "".join(re.findall('[a-zA-Z]', disk_info.split("/")[-1]))

    def check_exists(self, file):
        """
        check file if exists
        """
        if bool(int(self._remote.cmd(self._fqdn, [f'ls {file} > /dev/null 2>&1 && echo 1 || echo 0']))):
            return True

    def check_md5(self, file, md5):
        """
        check file's md5
        """
        if self.check_exists(file):
            bin_file_md5 = self._remote.cmd(self._fqdn, [f'md5sum {file} | cut -d " " -f 1'])
            if str(bin_file_md5) == str(md5):
                return True

    def download(self):
        """
        download from https://prometheus.io/download/
        """
        if not self.check_md5(f'~/{self.download_addr.split("/")[-1]}', self.tar_bin_file_md5):
            self._remote.cmd(self._fqdn, [f'rm -rf ~/{self.download_addr.split("/")[-1]}', f"wget -P ~ {self.download_addr}"])

    def check_install(self, config: Dict):
        if "prometheus-" in self.download_addr:
            if config['spec']['server'] is not None:
                if "run_dir" in config['spec']['server']:
                    self.package_dir = config['spec']['server']['run_dir']
            else:
                self.package_dir = "/opt/prometheus"
        elif "node_exporter" in self.download_addr:
            if "run_dir" in config['spec']['node_exporter']:
                self.package_dir = config['spec']['node_exporter']['run_dir']
            else:
                self.package_dir = "/opt/node_exporter"
        elif "process-exporter" in self.download_addr:
            if "run_dir" in config['spec']['process_exporter']:
                self.package_dir = config['spec']['process_exporter']['run_dir']
            else:
                self.package_dir = "/opt/process_exporter"
        else:
            pass
        self.file_path = f'{self.package_dir}/{self.download_addr.split("/")[-1].replace(".tar.gz", "")}'
        if "process-exporter" in self.file_path:
            self.bin_file = "-".join(self.file_path.replace(".tar.gz", "").split("/")[-1].split("-")[:2])
        else:
            self.bin_file = self.file_path.split('/')[-1].split('-')[0]
        if self.check_md5(f'{self.file_path}/{self.bin_file}', self.bin_file_md5):
            return True

    def check_status(self):
        """
        check if process exists
        """
        pass

    def install(self, config: Dict):
        if not self.check_install(config):
            self.download()
            if self.check_exists(self.file_path):
                self._remote.cmd(self._fqdn, [f'rm -rf {self.file_path}'])
            self._remote.cmd(self._fqdn, [f"mkdir -p {self.package_dir}", f'tar -xf ~/{self.download_addr.split("/")[-1]} -C {self.package_dir}'])

    def uninstall(self, config: Dict):
        if self.check_install(config):
            self._remote.cmd(self._fqdn, [f'rm -rf {self.package_dir}'])

    def gen_instance_name(self, fqdn):
        return f'{fqdn}_node', f'{fqdn}_process'

    def gen_prometheus_yaml_dict(self, config: Dict):
        node_exporter_fqdn_list = list()
        process_exporter_fqdn_list = list()
        prometheus_yaml_dict = {'global': {'scrape_interval': '10s', 'evaluation_interval': '10s', 'scrape_timeout': '3s'}, 'alerting': {'alertmanagers': [{'static_configs': [{'targets': None}]}]}, 'rule_files': None,
                                'scrape_configs': [{'job_name': 'prometheus', 'static_configs': [{'targets': ['localhost:9090']}]}]}
        if config['spec']['server'] is not None:
            if "config" in config['spec']['server']:
                for key, value in config['spec']['server']['config'].items():
                    if key == "global":
                        for global_key, global_value in value.items():
                            prometheus_yaml_dict['global'][global_key] = global_value
                    elif key == "alerting":
                        prometheus_yaml_dict['alerting']['alertmanagers'][0]['static_configs'][0]['targets'] = dict()
                        prometheus_yaml_dict['alerting']['alertmanagers'][0]['static_configs'][0]['targets']['alertmanager'] = value['alertmanager']
                    elif key == "scrape_configs":
                        for scrape_key, scrape_value in value.items():
                            if scrape_key == "job_name":
                                prometheus_yaml_dict['scrape_configs'][0][scrape_key] = scrape_value
                            elif scrape_key == "targets":
                                prometheus_yaml_dict['scrape_configs'][0]['static_configs'][0][scrape_key] = scrape_value
                            else:
                                pass
                    else:
                        pass
        if 'node_exporter' in config['spec']:
            node_exporter_fqdn_list = config['spec']['node_exporter']['fqdn']
        if 'process_exporter' in config['spec']:
            process_exporter_fqdn_list = config['spec']['process_exporter']['fqdn']

        if len(node_exporter_fqdn_list) > 0:
            for fqdn in node_exporter_fqdn_list:
                prometheus_yaml_dict['scrape_configs'].append({'job_name': self.gen_instance_name(fqdn)[0], 'static_configs': [{'targets': [f'{fqdn}:9100'], 'labels': {'instance': self.gen_instance_name(fqdn)[0]}}]})
        if len(process_exporter_fqdn_list) > 0:
            for fqdn in process_exporter_fqdn_list:
                prometheus_yaml_dict['scrape_configs'].append({'job_name': self.gen_instance_name(fqdn)[1], 'static_configs': [{'targets': [f'{fqdn}:9256'], 'labels': {'instance': self.gen_instance_name(fqdn)[1]}}]})
        return prometheus_yaml_dict

    def gen_process_exporter_yaml(self, tmp_dir: str, config: Dict):
        common_process_list = list()
        for fqdn in config['spec']['process_exporter']['fqdn']:
            if 'config' in config['spec']['process_exporter']:
                if 'common_process' in config['spec']['process_exporter']['config']:
                    self.process_dict[fqdn] = config['spec']['process_exporter']['config']['common_process']
                    for process in config['spec']['process_exporter']['config']['common_process']:
                        common_process_list.append({'name': '{{.Comm}}', 'cmdline': [process]})
                else:
                    self.process_dict[fqdn] = ["taosd"]
                    common_process_list.append({'name': '{{.Comm}}', 'cmdline': ["taosd"]})
            process_exporter_yaml_dict = {'process_names': common_process_list}
            dict2yaml(process_exporter_yaml_dict, tmp_dir, f"process_{fqdn}.yaml")

            if 'config' in config['spec']['process_exporter']:
                if 'custom_process' in config['spec']['process_exporter']['config']:
                    for custom_fqdn, process_list in config['spec']['process_exporter']['config']['custom_process'].items():
                        self.process_dict[custom_fqdn] = process_list
                        custom_process_list = list()
                        for process in process_list:
                            custom_process_list.append({'name': '{{.Comm}}', 'cmdline': [process]})
                        process_exporter_yaml_dict = {'process_names': custom_process_list}
                        dict2yaml(process_exporter_yaml_dict, tmp_dir, f"process_{custom_fqdn}.yaml")

    def start_prometheus(self, tmp_dir: str, config: Dict):
        self.set_prometheus_initial_env()
        if 'server' in config['spec']:
            self.stop_prometheus(config)
            for fqdn in config["fqdn"]:
                self._fqdn = fqdn
                self.date_sync()
                self.install(config)
                prometheus_yaml_dict = self.gen_prometheus_yaml_dict(config)
                prometheus_yaml_filename = f"prometheus_{fqdn}.yaml"
                dict2yaml(prometheus_yaml_dict, tmp_dir, prometheus_yaml_filename)
                self._remote.put(self._fqdn, f"{tmp_dir}/{prometheus_yaml_filename}", self.file_path)
                self._remote.cmd(self._fqdn, [f'screen -d -m {self.file_path}/prometheus --config.file={self.file_path}/{prometheus_yaml_filename}'])

    def stop_prometheus(self, config: Dict):
        for fqdn in config['fqdn']:
            if bool(int(self._remote.cmd(fqdn, ['systemctl -l | grep prometheus > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["systemctl stop prometheus > /dev/null 2&>1"])
            elif bool(int(self._remote.cmd(fqdn, ['ps -ef | grep -w prometheus | grep -v grep > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["ps -ef | grep -w prometheus | grep -v grep | awk \'{print $2}\' | xargs kill -9 > /dev/null 2&>1"])
            else:
                pass

    def start_node_exporter(self, config: Dict):
        self.set_node_exporter_initial_env()
        if 'node_exporter' in config['spec']:
            self.stop_node_exporter(config)
            for fqdn in config['spec']['node_exporter']['fqdn']:
                self._fqdn = fqdn
                self.date_sync()
                self.install(config)
                self._remote.cmd(self._fqdn, [f'screen -d -m {self.file_path}/node_exporter'])

    def stop_node_exporter(self, config: Dict):
        for fqdn in config['spec']['node_exporter']['fqdn']:
            if bool(int(self._remote.cmd(fqdn, ['systemctl -l | grep node_exporter > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["systemctl stop node_exporter > /dev/null 2&>1"])
            elif bool(int(self._remote.cmd(fqdn, ['ps -ef | grep -w node_exporter | grep -v grep > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["ps -ef | grep -w node_exporter | grep -v grep | awk \'{print $2}\' | xargs kill -9 > /dev/null 2&>1"])
            else:
                pass

    def start_process_exporter(self, tmp_dir: str, config: Dict):
        self.set_process_exporter_initial_env()
        if 'process_exporter' in config['spec']:
            self.stop_process_exporter(config)
            for fqdn in config['spec']['process_exporter']['fqdn']:
                self._fqdn = fqdn
                self.date_sync()
                self.install(config)
                self.gen_process_exporter_yaml(tmp_dir, config)
                process_yaml_filename = f"process_{fqdn}.yaml"
                self._remote.put(self._fqdn, f"{tmp_dir}/{process_yaml_filename}", self.file_path)
                self._remote.cmd(self._fqdn, [f'screen -d -m {self.file_path}/process-exporter --config.path={self.file_path}/{process_yaml_filename}'])

    def stop_process_exporter(self, config: Dict):
        for fqdn in config['spec']['process_exporter']['fqdn']:
            if bool(int(self._remote.cmd(fqdn, ['systemctl -l | grep process-exporter > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["systemctl stop process-exporter > /dev/null 2&>1"])
            elif bool(int(self._remote.cmd(fqdn, ['ps -ef | grep -w process-exporter | grep -v grep > /dev/null 2>&1 && echo 1 || echo 0']))):
                self._remote.cmd(fqdn, ["ps -ef | grep -w process-exporter | grep -v grep | awk \'{print $2}\' | xargs kill -9 > /dev/null 2&>1"])
            else:
                pass

    def setup(self, tmp_dir: str, config: Dict):
        self.start_prometheus(tmp_dir, config)
        self.start_node_exporter(config)
        self.start_process_exporter(tmp_dir, config)

    def destroy(self, config: Dict):
        self.stop_prometheus(config)
        self.stop_node_exporter(config)
        self.stop_process_exporter(config)

    def init_prom_connect(self, config: Dict):
        for fqdn in config["fqdn"]:
            self._fqdn = fqdn
            self.prom_conn = PrometheusConnect(url=f'http://{self._fqdn}:9090', disable_ssl=True)
            return self.prom_conn.check_prometheus_connection()

    def timestamp2string(self, timestamp):
        """
        trans timestamp to strftime
        """
        try:
            d = datetime.datetime.fromtimestamp(timestamp)
            return d.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return e

    def gen_fqdn_process_dict(self, config: Dict):
        """
        gen key-value pair of {fqdn:process}
        """
        for fqdn in config['spec']['process_exporter']['fqdn']:
            if 'config' in config['spec']['process_exporter']:
                if 'common_process' in config['spec']['process_exporter']['config']:
                    self.process_dict[fqdn] = config['spec']['process_exporter']['config']['common_process']
                else:
                    self.process_dict[fqdn] = ["taosd"]
            if 'config' in config['spec']['process_exporter']:
                if 'custom_process' in config['spec']['process_exporter']['config']:
                    for custom_fqdn, process_list in config['spec']['process_exporter']['config']['custom_process'].items():
                        self.process_dict[custom_fqdn] = process_list

    def gen_custom_promql_dict(self, process_name: str, instance: str):
        """
        gen promql
        """
        promql_dict = dict()
        promql_dict[
            "cpu_utilization"] = f'avg(irate(namedprocess_namegroup_cpu_seconds_total{{groupname="{process_name}", instance="{instance}", mode="user"}}[10m])) by (groupname, instance) * 100 + avg(irate(namedprocess_namegroup_cpu_seconds_total{{groupname="{process_name}", instance="{instance}", mode="system"}}[10m])) by (groupname, instance) * 100'
        promql_dict["mem_usage"] = f'namedprocess_namegroup_memory_bytes{{groupname="{process_name}", instance="{instance}", memtype="resident"}}'
        promql_dict["disk_write"] = f'rate(namedprocess_namegroup_write_bytes_total{{groupname="{process_name}", instance="{instance}"}}[10m])'
        promql_dict["disk_read"] = f'rate(namedprocess_namegroup_read_bytes_total{{groupname="{process_name}", instance="{instance}"}}[10m])'
        promql_dict["net_write"] = f'irate(node_network_transmit_bytes_total{{instance="{instance}",device="{self.device}"}}[5m])*8'
        promql_dict["net_read"] = f'irate(node_network_receive_bytes_total{{instance="{instance}",device="{self.device}"}}[5m])*8'
        promql_dict["disk_io"] = f'irate(node_disk_io_time_seconds_total{{instance="{instance}", device="{self.disk}"}}[5m])*100'
        return promql_dict

    def get_custom_query_current(self, collect_type: str, process_name: str, instance: str):
        """
        :param collect_type [cpu_utilization]: (str) cpu utilization(%)
                            [mem_usage]: (str) mem usage(Mb)
                            [disk_write]: (str) disk write
                            [disk_read]: (str) disk read
                            [net_write]: (str) net write
                            [net_read]: (str) net read
        :param process_name: (str) monitoring process
        :param instance: (str) monitoring instance
        """
        query_res = self.prom_conn.custom_query(self.gen_custom_promql_dict(process_name, instance)[collect_type])
        if len(query_res) > 0:
            query_res[0]["value"][0] = self.timestamp2string(query_res[0]["value"][0])
            query_res[0]["value"][1] = format(float(query_res[0]["value"][1]), '.2f')
            dataframe = MetricSnapshotDataFrame(query_res).loc[:, ["value"]]
            dataframe["collect_type"] = collect_type
            dataframe["node"] = self._fqdn
            if collect_type in ["cpu_utilization", "mem_usage", "disk_write", "disk_read"]:
                dataframe["process"] = process_name
            elif collect_type in ["net_write", "net_read"]:
                dataframe["device"] = self.device
            else:
                pass

            dataframe["value"] = pd.to_numeric(dataframe["value"], downcast="float")
            if collect_type == "mem_usage":
                dataframe["value"] = dataframe["value"] / 1024 / 1024
                dataframe["value"] = dataframe["value"].round(decimals=2)
            return query_res, dataframe
        else:
            return [], []

    def get_custom_query_current_datas(self, config: Dict, type_list: list, custom_process_dict: dict = None):
        """
        :param config: (dict) input dict from frame
        :param type_list [cpu_utilization]: (list) cpu utilization(%)
                        [mem_usage]: (list) mem usage(Mb)
                        [disk_write]: (list) disk write
                        [disk_read]: (list) disk read
                        [net_write]: (list) net write
                        [net_read]: (list) net read
        :param custom_process_dict: (dict) a dict described monitor process. eg: {"k8smaster": ["taosd"], "k8sslave": ["taosadapter"]}
        """
        self.init_prom_connect(config)
        self.gen_fqdn_process_dict(config)
        current_data_dict = defaultdict(dict)
        current_dataframe_dict = defaultdict(dict)

        for fqdn, process_list in self.process_dict.items():
            self._fqdn = fqdn
            current_data_dict["node_exporter"][self._fqdn] = dict()
            current_dataframe_dict["node_exporter"][self._fqdn] = dict()
            current_data_dict["process_exporter"][self._fqdn] = dict()
            current_dataframe_dict["process_exporter"][self._fqdn] = dict()
            # set device
            if "net_read" in type_list or "net_write" in type_list:
                self.get_device_by_fqdn(self._fqdn)

            if "disk_io" in type_list:
                self.get_disk()
            if custom_process_dict is not None:
                for custom_fqdn, custom_process_list in custom_process_dict.items():
                    if custom_fqdn == self._fqdn:
                        process_list = custom_process_list

            for process in process_list:
                current_data_dict["process_exporter"][self._fqdn][process] = dict()
                current_dataframe_dict["process_exporter"][self._fqdn][process] = dict()

                for collect_type in type_list:
                    if collect_type in ["cpu_utilization", "mem_usage", "disk_write", "disk_read"]:
                        query_res, dataframe = self.get_custom_query_current(collect_type, process, self.gen_instance_name(self._fqdn)[1])
                        current_data_dict["process_exporter"][self._fqdn][process][collect_type] = query_res
                        current_dataframe_dict["process_exporter"][self._fqdn][process][collect_type] = dataframe
                    else:  # ["net_write", "net_read"]
                        query_res, dataframe = self.get_custom_query_current(collect_type, "", self.gen_instance_name(self._fqdn)[0])
                        current_data_dict["node_exporter"][self._fqdn][collect_type] = query_res
                        current_dataframe_dict["node_exporter"][self._fqdn][collect_type] = dataframe
        return current_data_dict, current_dataframe_dict

    def get_custom_query_range(self, collect_type: str, process_name: str, instance: str, start_time: str, end_time: str, step: str, params: str = None):
        """
        :param collect_type [cpu_utilization]: (str) cpu utilization(%)
                            [mem_usage]: (str) mem usage(Mb)
                            [disk_write]: (str) disk write
                            [disk_read]: (str) disk read
                            [net_write]: (str) net write
                            [net_read]: (str) net read
        :param process_name: (str) monitoring process
        :param instance: (str) monitoring instance
        :param start_time: (datetime) A datetime(ms precision) str that specifies the query range start time.
        :param end_time: (datetime) A datetime(ms precision) str that specifies the query range end time. eg: "2022-05-21 00:00:00.521"
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (str) reserve
        """
        if step is None:
            s_count = datetime.datetime.timestamp(datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f')) - datetime.datetime.timestamp(datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f'))
            if int(s_count / 60) < 10:
                step = 1
            else:
                step = int(s_count / 60 * 0.2)
        query_res = self.prom_conn.custom_query_range(
            query=self.gen_custom_promql_dict(process_name, instance)[collect_type],
            start_time=datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f'),
            end_time=datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f'),
            step=str(step),
            params=params
        )
        if len(query_res) > 0:
            for timestamp_values_list in query_res[0]["values"]:
                timestamp_values_list[0] = self.timestamp2string(timestamp_values_list[0])
                timestamp_values_list[1] = format(float(timestamp_values_list[1]), '.2f')
            dataframe = MetricRangeDataFrame(query_res).loc[:, ["value"]]
            dataframe["collect_type"] = collect_type
            dataframe["node"] = self._fqdn
            if collect_type in ["cpu_utilization", "mem_usage", "disk_write", "disk_read"]:
                dataframe["process"] = process_name
            elif collect_type in ["net_write", "net_read"]:
                dataframe["device"] = self.device
            elif collect_type in ["disk_io"]:
                dataframe["disk"] = self.disk
            else:
                pass
            dataframe["value"] = pd.to_numeric(dataframe["value"], downcast="float")
            query_res[0]["max_value"] = format(float(max(query_res[0]["values"], key=lambda x: float(x[1]))[1]), '.2f')
            query_res[0]["min_value"] = format(float(min(query_res[0]["values"], key=lambda x: float(x[1]))[1]), '.2f')
            if collect_type == "mem_usage":
                dataframe["value"] = dataframe["value"] / 1024 / 1024
                dataframe["value"] = dataframe["value"].round(decimals=2)
                query_res[0]["max_value"] = format(float(query_res[0]["max_value"]) / 1024 / 1024, '.2f')
                query_res[0]["min_value"] = format(float(query_res[0]["min_value"]) / 1024 / 1024, '.2f')

            query_res[0]["avg_value"] = format(float(dataframe['value'].mean()), '.2f')
            query_res[0]["p90"], query_res[0]["p95"], query_res[0]["p99"] = dataframe['value'].quantile([.50, .95, .99]).round(decimals=2).values
            return query_res, dataframe
        else:
            return [], []

    def get_custom_query_range_datas(self, config: Dict, type_list: list, start_time: str, end_time: str, step: str = None, custom_process_dict: dict = None):
        """
        :param config: (dict) input dict from frame
        :param type_list [cpu_utilization]: (list) cpu utilization(%)
                        [mem_usage]: (list) mem usage(Mb)
                        [disk_write]: (list) disk write
                        [disk_read]: (list) disk read
                        [net_write]: (list) net write
                        [net_read]: (list) net read
        :param start_time: (datetime) A datetime(ms precision) str that specifies the query range start time.
        :param end_time: (datetime) A datetime(ms precision) str that specifies the query range end time. eg: "2022-05-21 00:00:00.521"
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param custom_process_dict: (dict) a dict described monitor process. eg: {"k8smaster": ["taosd"], "k8sslave": ["taosadapter"]}
        """
        self.init_prom_connect(config)
        self.gen_fqdn_process_dict(config)
        range_data_dict = defaultdict(dict)
        range_dataframe_dict = defaultdict(dict)
        for fqdn, process_list in self.process_dict.items():
            self._fqdn = fqdn
            range_data_dict["node_exporter"][self._fqdn] = dict()
            range_dataframe_dict["node_exporter"][self._fqdn] = dict()
            range_data_dict["process_exporter"][self._fqdn] = dict()
            range_dataframe_dict["process_exporter"][self._fqdn] = dict()
            # set device
            if "net_read" in type_list or "net_write" in type_list:
                self.get_device_by_fqdn(self._fqdn)
            if "disk_io" in type_list:
                self.get_disk()
            if custom_process_dict is not None:
                for custom_fqdn, custom_process_list in custom_process_dict.items():
                    if custom_fqdn == self._fqdn:
                        process_list = custom_process_list

            for process in process_list:
                range_data_dict["process_exporter"][self._fqdn][process] = dict()
                range_dataframe_dict["process_exporter"][self._fqdn][process] = dict()

                for collect_type in type_list:
                    if collect_type in ["cpu_utilization", "mem_usage", "disk_write", "disk_read"]:
                        query_res, dataframe = self.get_custom_query_range(collect_type, process, self.gen_instance_name(self._fqdn)[1], start_time, end_time, step)
                        range_data_dict["process_exporter"][self._fqdn][process][collect_type] = query_res
                        range_dataframe_dict["process_exporter"][self._fqdn][process][collect_type] = dataframe
                    else:  # ["net_write", "net_read", "disk_io"]:
                        query_res, dataframe = self.get_custom_query_range(collect_type, "", self.gen_instance_name(self._fqdn)[0], start_time, end_time, step)
                        range_data_dict["node_exporter"][self._fqdn][collect_type] = query_res
                        range_dataframe_dict["node_exporter"][self._fqdn][collect_type] = dataframe
        return range_data_dict, range_dataframe_dict

    def export_res2md(self, filename: str, data_dict: dict, dataframe_dict: dict):
        """
        :param filename: (str) output filename (with path)
        :param data_dict: (dict) get_custom_query_current_datas[0] || get_custom_query_range_datas[0]
        :param dataframe_dict: (dict) get_custom_query_current_datas[1] || get_custom_query_range_datas[1]
        """
        with open(filename, 'a') as f:
            for exporter_name, monitor_data_dict in dataframe_dict.items():
                if exporter_name == "process_exporter":
                    for fqdn, fqdn_dict in monitor_data_dict.items():
                        for process, collect_type_dict in fqdn_dict.items():
                            for collect_type, query_dataframe in collect_type_dict.items():
                                if len(query_dataframe) > 0:
                                    if "max_value" in data_dict["process_exporter"][fqdn][process][collect_type][0]:
                                        max_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["max_value"]
                                        min_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["min_value"]
                                        avg_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["avg_value"]
                                        p90_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["p90"]
                                        p95_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["p95"]
                                        p99_value = data_dict["process_exporter"][fqdn][process][collect_type][0]["p99"]
                                        query_summary = f'|------- max: {max_value}, min: {min_value}, avg: {avg_value}, p90: {p90_value}, p95: {p95_value}, p99: {p99_value} ------|\n\n'
                                    else:
                                        query_summary = ""

                                    if collect_type == "cpu_utilization":
                                        collect_type += " (%)"
                                    elif collect_type == "mem_usage":
                                        collect_type += " (M)"
                                    elif collect_type == "disk_write" or collect_type == "disk_read":
                                        collect_type += " (Byte/s)"
                                    else:
                                        pass
                                    f.write(f'|------- fqdn: {fqdn}, process: {process}, collect_type: {collect_type} ------|\n\n')
                                    f.write(query_summary)
                                    f.write(str(query_dataframe.to_markdown()))
                                    f.write('\n\n\n')
                                else:
                                    f.write(f'|------- fqdn: {fqdn}, process: {process}, collect_type: {collect_type} ------|\n\n')
                                    f.write("no response data\n\n\n")

                elif exporter_name == "node_exporter":
                    for fqdn, fqdn_dict in monitor_data_dict.items():
                        for collect_type, query_dataframe in fqdn_dict.items():
                            if len(query_dataframe) > 0:
                                if "max_value" in data_dict["node_exporter"][fqdn][collect_type][0]:
                                    max_value = data_dict["node_exporter"][fqdn][collect_type][0]["max_value"]
                                    min_value = data_dict["node_exporter"][fqdn][collect_type][0]["min_value"]
                                    avg_value = data_dict["node_exporter"][fqdn][collect_type][0]["avg_value"]
                                    p90_value = data_dict["node_exporter"][fqdn][collect_type][0]["p90"]
                                    p95_value = data_dict["node_exporter"][fqdn][collect_type][0]["p95"]
                                    p99_value = data_dict["node_exporter"][fqdn][collect_type][0]["p99"]
                                    query_summary = f'|------- max: {max_value}, min: {min_value}, avg: {avg_value}, p90: {p90_value}, p95: {p95_value}, p99: {p99_value} ------|\n\n'
                                else:
                                    query_summary = ""
                                if collect_type == "net_write" or collect_type == "net_read":
                                    collect_type += " (byte/s)"
                                elif collect_type == "disk_io":
                                        collect_type += " (%)"
                                else:
                                    pass
                                f.write(f'|------- fqdn: {fqdn}, collect_type: {collect_type} ------|\n\n')
                                f.write(query_summary)
                                f.write(str(query_dataframe.to_markdown()))
                                f.write('\n\n\n')
                            else:
                                f.write(f'|------- fqdn: {fqdn}, collect_type: {collect_type} ------|\n\n')
                                f.write("no response data\n\n\n")
                else:
                    pass