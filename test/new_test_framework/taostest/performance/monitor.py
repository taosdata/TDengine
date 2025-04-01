import json
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass

import requests

try:
    import taos
except:
    pass


# import promethus


@dataclass
class CheckTime:
    startime: str = None
    endtime: str = None
    timepoint: str = None


class Monitor(ABC):
    """
    the base class to get all monitor metric data
    """

    def __init__(self, conn):
        # self.cursor = taos.connect(conn)
        pass

    def check_metric(func):
        def wrapper(self, msg):
            if msg.lower() == "cpu_engine":
                metric = "cpu_engine"
            elif msg.lower() == "cpu_system":
                metric = "cpu_system"
            elif msg.lower() == "mem_engine" or msg.lower() == "memory_engine":
                metric = "mem_engine"
            elif msg.lower() == "mem_system" or msg.lower() == "memory_system":
                metric = "mem_system"
            elif msg.lower() == "disk_engine_used" or msg.lower() == "disk_engine":
                metric = "disk_engine"
            elif msg.lower() == "disk_read" or msg.lower() == "io_read":
                metric = "io_read_disk"
            elif msg.lower() == "disk_write" or msg.lower() == "io_write":
                metric = "io_write_disk"
            elif msg.lower() == "net_in":
                metric = "net_in"
            elif msg.lower() == "net_out":
                metric = "net_out"
            else:
                raise TypeError("metric is not allowed type")
            return metric

        return wrapper

    @check_metric
    def get_metric(self, metric):
        return metric

        # def get_metric_info(self, msg):
        metric = self.get_metric(msg)
        if metric == "cpu_engine":
            self.cpu_engine_info(checktime)
        if metric == "cpu_system":
            self.cpu_system_info(checktime)
        if metric == "mem_engine":
            self.mem_engine_info(checktime)
        if metric == "mem_system":
            self.mem_system_info(checktime)
        if metric == "disk_engine":
            self.disk_engine_info(checktime)
        if metric == "io_read_disk":
            self.disk_read_info(checktime)
        if metric == "io_write_disk":
            self.disk_write_info(checktime)
        if metric == "net_in":
            self.net_in_info(checktime)
        if metric == "net_out":
            self.net_out_info(checktime)
        if metric == "insert_rate":
            self.insert_rate_info(checktime)

    @abstractmethod
    def mem_engine_info(self, checktime) -> list:
        pass

    @abstractmethod
    def mem_system_info(self, checktime) -> list:
        pass

    @abstractmethod
    def cpu_engine_info(self, checktime) -> list:
        pass

    @abstractmethod
    def cpu_system_info(self, checktime) -> list:
        pass

    @abstractmethod
    def disk_engine_info(self, checktime) -> list:
        pass

    @abstractmethod
    def disk_read_info(self, checktime) -> list:
        pass

    @abstractmethod
    def disk_write_info(self, checktime) -> list:
        pass

    @abstractmethod
    def net_in_info(self, checktime):
        pass

    @abstractmethod
    def net_out_info(self, checktime):
        pass

    def insert_rate(self, checktime):
        # 从结果数据文件中获取插入的速率，单位rows/sec
        pass

    def query_time(self, checktime):
        # 从结果数据文件中获取查询的响应时间，单位s
        pass

    def __delattr__(self, __name: str) -> None:
        self.cursor.close()


class TaosMonitor(Monitor):
    """
    从taosd的monitor服务中获取taosd性能指标数据，不能监控非taosd进程的指标
    This class is used to get performance indicator data of taosd from monitor.
    It can't get data which not belongs to taosd
    """

    # def __init__(self, conn):
    #     super().__init__(conn)
    #     self.__startime     =   None
    #     self.__endtime      =   None
    #     self.__timepoint    =   None

    def checkmonitor(self):
        pass

    # def check_metric(func):
    #     return super().check_metric()

    # def get_metric_info(self, msg):
    #     return super().get_metric_info(msg)

    def get_info(self, metric, checktime: CheckTime):
        metric = self.get_metric(msg=metric)
        startime, endtime, timepoint = checktime.startime, checktime.endtime, checktime.timepoint
        try:
            if startime is not None:
                if not endtime:
                    print(1)
                    # self.cursor.excute(f"select {metric} from {monitor_dbname} where ts >= {startime}")
                if endtime:
                    print(2)
                    # self.cursor.excute(f"select {metric} from {monitor_dbname} where ts >= {startime} and ts <= {endtime}")

            elif endtime is not None:
                print(3)
                # self.cursor.excute(f"select {metric} from {monitor_dbname} where ts < {endtime}")

            elif timepoint is not None:
                print(4)
                # self.cursor.excute(f"select {metric} from {monitor_dbname} where ts = {timepoint}")

            else:
                print(5)
                # self.cursor.excute(f"select {metric} from {monitor_dbname} where ts = {nowtime}")

            # return self.cursor.fetchall()
            print(metric)
        except:
            traceback.print_exc()

    def cpu_engine_info(self, checktime: CheckTime):
        return self.get_info(metric="cpu_engine", checktime=checktime)

    def cpu_system_info(self, checktime: CheckTime):
        return self.get_info(metric="cpu_system", checktime=checktime)

    def mem_engine_info(self, checktime: CheckTime):
        return self.get_info(metric="mem_engine", checktime=checktime)

    def mem_system_info(self, checktime: CheckTime):
        return self.get_info(metric="mem_system", checktime=checktime)

    def disk_engine_info(self, checktime: CheckTime):
        return self.get_info(metric="disk_engine", checktime=checktime)

    def disk_read_info(self, checktime: CheckTime):
        return self.get_info(metric="disk_read", checktime=checktime)

    def disk_write_info(self, checktime: CheckTime):
        return self.get_info(metric="disk_write", checktime=checktime)

    def net_in_info(self, checktime: CheckTime):
        return self.get_info(metric="net_in", checktime=checktime)

    def net_out_info(self, checktime: CheckTime):
        return self.get_info(metric="net_out", checktime=checktime)

    def __delattr__(self, __name: str) -> None:
        self.conn


# Promethus_Server 变量需要写进框架的配置文件
Promethus_Server = "http://192.168.1.101:9090"


class PromethusMonitor(Monitor):
    """
    从promethus中获取被监控对象的监控指标，可以获取非taosd进程的指标
    """

    def __init__(self, conn):
        super().__init__(conn)
        pass

    def get_targets_response(self, address=Promethus_Server):
        """[describe]
            本方法返回targets的响应报文
        Args:
            address ([string]): [a http-url of premuthus server, like : "http://localhost:9090"]
        """
        url = address + "/api/v1/targets"
        try:
            return json.loads(requests.request('GET', url=url))
        except:
            print('An exception occurred')
            return

    def get_up_targets_status(self, target_response: get_targets_response):
        """[describe]
            本方法获取status为up的监测节点
        Args:
            address ([string]): [return up-status target list]
        """
        targets = target_response.json()['data']['activeTargets']
        up_target_list = []
        up_target_num = 0
        for target in targets:
            if target['health'] == 'up':
                up_target_num += 1
                up_target_list.append(target['labels']['instance'])
        return up_target_list, up_target_num

    def get_down_targets_status(self, target_response: get_targets_response):
        """[describe]
            本方法获取status不为up的监测节点
        Args:
            address ([string]): [return non-up-status target list]
        """
        targets = target_response.json()['data']['activeTargets']
        down_target_list = []
        down_target_num = 0
        for target in targets:
            if target['health'] != 'up':
                down_target_num += 1
                down_target_list.append(target['labels']['instance'])
        return down_target_list, down_target_num

    def get_query_expr(self, metric, monitor_interval, chektime: CheckTime):
        expr = f"irate({metric}{{mode='idle'}}[{monitor_interval}])"

    def get_query_response(address, query_expr):
        """[summary]
            本方法获取promethus的监控数据
        Args:
            address ([string]): [a http-url of premuthus server, like : "http://localhost:9090"]
            query_expr ([string]): [the promethusQL of get monitor-metric]
            
        Returns:
            [dict]: [the monitor-metric dict]
        """
        url = address + "/api/v1/query?query=" + query_expr
        try:
            return json.loads(requests.get(url=url).content.decode("utf8", "ignore"))
        except Exception as e:
            raise e

    def order_usage(usage_dict, current_time, monitor_interval):
        """
        """

    def cpu_engine_info(self, checktime) -> list:
        return super().cpu_engine_info(checktime)

    pass

    def cpu_system_info(self, checktime) -> list:
        return super().cpu_system_info(checktime)


def getMonitor(monitor_type=TaosMonitor, kwargs={}):
    """
    description: factor method to get a monitor
    param monitor_type: the type of get metric, value choice from (getMonitorInfo, getPromethusInfo)
    """
    return monitor_type(kwargs)


if __name__ == '__main__':
    checktime = CheckTime(startime=None, endtime=None)
    monitor = getMonitor()
    monitor.disk_read_info(checktime)
