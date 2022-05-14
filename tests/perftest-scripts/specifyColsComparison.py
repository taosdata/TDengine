from loguru import logger
import time
import os
import json
import sys
from fabric import Connection

# apt install -y sudo python3-pip
# pip3 install fabric loguru

class specifyColsCompared:
    def __init__(self):
        # remote server
        self.remote_hostname = "vm85"
        self.remote_sshport = "22"
        self.remote_username = "root"
        self.remote_password = "tbase125!"

        # TDengine pkg path
        self.autoDeploy = False
        self.install_package = '/root/share/TDengine-server-2.4.0.0-Linux-amd64.tar.gz'

        # test element
        self.update_list = [1, 2]
        self.column_count_list = [100, 500, 2000]

        # perfMonitor config
        self.thread_count = 10
        self.taosc_port = 6030
        self.http_port = 6041
        self.database = "test"
        self.table_count = 10
        self.tag_count = 5
        self.col_count = 50000
        self.batch_size = 1
        self.sleep_time = 20

        self.current_time = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time()))
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.log_file = os.path.join(self.current_dir, f'./performance.log')
        if self.remote_username == "root":
            self.remote_dir = "/root"
        else:
            self.remote_dir = f'/home/{self.remote_username}'
        self.conn = Connection(self.remote_hostname, user=self.remote_username, port=self.remote_sshport, connect_timeout=120, connect_kwargs={"password": self.remote_password})
        logger.add(self.log_file)
        logger.info(f'init env success, log will be export to {self.log_file}')

    def initLog(self):
        # init log
        self.exec_local_cmd(f'echo "" > {self.log_file}')

    def exec_local_cmd(self,shell_cmd):
        # exec local cmd
        try:
            result = os.popen(shell_cmd).read().strip()
            return result
        except Exception as e:
            logger.error(f"exec cmd: {shell_cmd} failed----{e}")

    def checkStatus(self, process):
        # check process status
        try:
            process_count = self.conn.run(f'ps -ef | grep -w {process} | grep -v grep | wc -l', pty=False, warn=True, hide=False).stdout
            if int(process_count.strip()) > 0:
                logger.info(f'check {self.remote_hostname} {process} existed')
                return True
            else:
                logger.info(f'check {self.remote_hostname} {process} not exist')
                return False
        except Exception as e:
            logger.error(f"check status failed----{e}, please check by manual")

    def deployPerfMonitor(self):
        # deploy perfMonitor
        logger.info('deploying perfMonitor')
        if os.path.exists(f'{self.current_dir}/perfMonitor'):
            os.remove(f'{self.current_dir}/perfMonitor')
        self.exec_local_cmd(f'wget -P {self.current_dir} http://39.105.163.10:9000/perfMonitor && chmod +x {self.current_dir}/perfMonitor')
        package_name = self.install_package.split('/')[-1]
        package_dir = '-'.join(package_name.split("-", 3)[0:3])
        self.exec_local_cmd(f'tar -xvf {self.install_package} && cd {package_dir} && echo -e "\n" | ./install.sh')

    def dropAndCreateDb(self):
        try:
            self.conn.run(f'taos -s "drop database if exists {self.database}"')
            self.conn.run(f'taos -s "create database if not exists {self.database}"')
        except Exception as e:
            logger.error(f"drop db failed----{e}, please check by manual")

    def uploadPkg(self):
        # upload TDengine pkg
        try:
            logger.info(f'uploading {self.install_package} to {self.remote_hostname}:{self.remote_dir}')
            self.conn.put(self.install_package, self.remote_dir)
        except Exception as e:
            logger.error(f"pkg send failed----{e}, please check by manual")

    def deployTDengine(self):
        # deploy TDengine
        try:
            package_name = self.install_package.split('/')[-1]
            package_dir = '-'.join(package_name.split("-", 3)[0:3])
            self.uploadPkg()
            self.conn.run(f'sudo rmtaos', pty=False, warn=True, hide=False)
            logger.info('installing TDengine')
            logger.info(self.conn.run(f'cd {self.remote_dir} && tar -xvf {self.remote_dir}/{package_name} && cd {package_dir} && echo -e "\n"|./install.sh', pty=False, warn=True, hide=False))
            logger.info('start TDengine')
            logger.info(self.conn.run('sudo systemctl start taosd', pty=False, warn=True, hide=False))
            for deploy_elm in ['taosd', 'taosadapter']:
                if self.checkStatus(deploy_elm):
                    logger.success(f'{self.remote_hostname}: {deploy_elm} deploy success')
                else:
                    logger.error(f'{self.remote_hostname}: {deploy_elm} deploy failed, please check by manual')
                    sys.exit(1)
        except Exception as e:
            logger.error(f"deploy TDengine failed----{e}, please check by manual")

    def genInsertJsonFile(self, thread_count, table_count, row_count, batch_size, column_count, partical_col_num, update, drop="yes", result_file=None):
        # gen json file
        json_file = os.path.join(self.current_dir, f'./insert.json')
        if result_file == None:
            result_file = self.log_file
        else:
            result_file = self.log_file.replace('performance.log', 'unused_performance.log')

        jdict = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": self.remote_hostname,
            "rest_host": self.remote_hostname,
            "port": self.taosc_port,
            "rest_port": self.http_port,
            "user": "root",
            "password": "taosdata",
            "thread_count": thread_count,
            "thread_count_create_tbl": 1,
            "result_file": result_file,
            "databases": [{
                "dbinfo": {
                    "name": self.database,
                    "drop": drop,
                    "update": update
                },
                "super_tables": [{
                    "name": "stb",
                    "childtable_count": table_count,
                    "childtable_prefix": "stb_",
                    "batch_create_tbl_num": 1,
                    "insert_mode": "rand",
                    "insert_iface": "rest",
                    "insert_rows": row_count,
                    "insert_interval": 0,
                    "batch_rows": batch_size,
                    "max_sql_len": 1048576,
                    "timestamp_step": 1000,
                    "start_timestamp": "2021-01-01 00:00:00.000",
                    "tags_file": "",
                    "partical_col_num": partical_col_num,
                    "columns": [{"type": "INT", "count": column_count}],
                    "tags": [{"type": "BINARY", "len": 16, "count": self.tag_count}]
                }]
            }]
        }
        with open(json_file, "w", encoding="utf-8") as f_w:
            f_w.write(json.dumps(jdict))

    def runTest(self):
        self.initLog()
        if self.autoDeploy:
            self.deployTDengine()
            self.deployPerfMonitor()

        # blank insert
        update = 0
        for col_count in self.column_count_list:
            for partical_col_num in [int(col_count * 0), int(col_count * 0.1), int(col_count * 0.3)]:
                logger.info(f'update: {update} || col_count: {col_count} || partical_col_num: {partical_col_num} test')
                self.genInsertJsonFile(self.thread_count, self.table_count, self.col_count, self.batch_size, col_count, partical_col_num, update)
                self.exec_local_cmd(f'{self.current_dir}/perfMonitor -f insert.json')
                time.sleep(self.sleep_time)
        
        # update = 1/2
        for update in self.update_list:
            for col_count in self.column_count_list:
                for partical_col_num in [int(col_count * 0.1), int(col_count * 0.3)]:
                    logger.info(f'update: {update} || col_count: {col_count} || partical_col_num: {partical_col_num} test')
                    self.genInsertJsonFile(self.thread_count, self.table_count, self.col_count, 100, col_count, int(col_count * 0), update, drop="yes", result_file="unused")
                    self.exec_local_cmd(f'{self.current_dir}/perfMonitor -f insert.json')
                    time.sleep(self.sleep_time)
                    self.genInsertJsonFile(self.thread_count, self.table_count, self.col_count, self.batch_size, col_count, partical_col_num, update, drop="no")
                    self.exec_local_cmd(f'{self.current_dir}/perfMonitor -f insert.json')
                    time.sleep(self.sleep_time)

if __name__ == '__main__':
    runPerf = specifyColsCompared()
    runPerf.runTest()
