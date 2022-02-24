import random
import time
import taos
from datetime import datetime
import multiprocessing as mp
from taostest.components import TaosD
from taostest.util.remote import Remote
from taostest.logger import Logger

from taostest import TDCase, T

kill_times = 50


def kill_and_start(run_log_dir: str, taosd_config: dict, mq):
    """不定时停止和启动第一个taosd"""
    logger = Logger(run_log_dir + "/kill_start.log")
    remote = Remote(logger)
    taosd = TaosD(remote)
    for i in range(kill_times):
        time.sleep(random.randint(2, 5))
        taosd.kill_and_start(taosd_config, sleep_seconds=3)  # sleep_seconds 是 kill 和 start 之间等待的秒数
    mq.put("done")
    logger.info("exit kill and start process")


class TestWalRestore(TDCase):

    def __init__(self):
        super(TestWalRestore, self).__init__()
        self.conn1: taos.TaosConnection = None
        self.conn2: taos.TaosConnection = None
        self.taosd_cfg_lis = None
        self.client_cfg_lis = None
        self.total_rows_1 = None
        self.total_rows_2 = None
        self.try_insert_total = 0
        self.error_times = 0

    def prepare_table(self, conn):
        conn.execute("create database if not exists wal_test")
        conn.execute("use wal_test")
        conn.execute("create stable if not exists st (ts timestamp ,int_val int , double_val double) tags(name binary(20))")

    def init(self):
        self.taosd_cfg_lis = self.get_component_by_name("taosd")
        self.client_cfg_lis = self.get_component_by_name("taospy")
        self.conn1 = self.tdSql.get_connection(self.client_cfg_lis[0])
        self.conn2 = self.tdSql.get_connection(self.client_cfg_lis[1])
        self.prepare_table(self.conn1)
        self.prepare_table(self.conn2)

    def gen_values(self, ts, n):
        """
        拼sql的values部分
        """
        int_val = 0
        double_vale = 0.000001
        values = []
        for i in range(n):
            n_ts = ts + i
            int_val = int_val + 1
            double_val = double_vale + 0.0001
            values.append(f"({n_ts}, {int_val}, {double_val})")

        return " ".join(values)

    def run(self):
        mq = mp.Queue()
        p1 = mp.Process(target=kill_and_start, args=(self.run_log_dir, self.taosd_cfg_lis[0], mq))
        p1.start()
        ts = int(time.time() * 1000) - 10 ** 11
        while mq.empty():
            try:
                for i in range(2000):
                    ts += 1000
                    tag = ["A", "B", "C"][i % 3]
                    tb_name = ["t1", "t2", "t3"][i % 3]
                    values = self.gen_values(ts, 1000)
                    self.try_insert_total += 1000
                    sql = f"insert into {tb_name} using st tags('{tag}') values {values}"
                    self.conn1.execute(sql)
                    # 只有当对conn1写入没有抛异常的情况下，才会写入conn2。
                    self.conn2.execute(sql)
            except BaseException as e:
                self.logger.error("%s", e)
                self.error_times += 1
        return self.check_total_record()

    def check_total_record(self):
        self.total_rows_1 = self.conn1.query("select count(*) from st").fetch_all()[0][0]
        self.total_rows_2 = self.conn2.query("select count(*) from st").fetch_all()[0][0]
        suc = self.total_rows_1 == self.total_rows_2
        if not suc:
            self.set_error_msg("error")
        return suc

    def desc(self) -> str:
        """
        启动两个TDengine，命名为d1,d2。
        写入端（如何做不管）先向d1写入数据，对返回成功的数据写入d2，如果d1返回失败的数据则略过d2。
        一个循环脚本，循环执行，sleep一个随机时间（限定范围，比如0到60秒之间），然后kill-9 d1，然后再start d1
        运行足够长时间，比如10分钟，60分钟（可配置），100小时等，等运行结束后，比较d1和d2中的数据，如果数据量不对，说明有
        数据丢失。
        """
        return "测试写入wal的数据在taosd重启后是否恢复"

    def cleanup(self):
        pass

    def get_report(self, start_time: datetime, stop_time: datetime) -> str:
        report = f"try_insert_total={self.try_insert_total} total_rows_1 = {self.total_rows_1}, total_rows_2 = {self.total_rows_2}, kill_times = {kill_times}, error_times = {self.error_times}"
        self.logger.info(report)
        return report

    def author(self) -> str:
        return "dingbo"

    def tags(self):
        return T.Abnormal.Software.KillProcess
