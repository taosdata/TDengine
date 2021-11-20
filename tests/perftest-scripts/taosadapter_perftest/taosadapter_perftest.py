from fabric import Connection
from loguru import logger
import shutil
import os
import time

class TaosadapterPerftest():
    def __init__(self):
        self.ip     = "192.168.1.85" 
        self.port   = "22"     
        self.user   = "root"      
        self.passwd = "tbase125!"
        self.telnetCreateStbJmxFile = "opentsdb_telnet_createStb.jmx"
        self.telnetCreateTbJmxFile = "opentsdb_telnet_createTb.jmx"
        self.telnetInsertRowsFile = "opentsdb_telnet_insertRows.jmx"
        self.jsonCreateStbJmxFile = "opentsdb_json_createStb.jmx"
        self.jsonCreateTbJmxFile = "opentsdb_json_createTb.jmx"
        self.jsonInsertRowsFile = "opentsdb_json_insertRows.jmx"
        self.logfile = "taosadapter_perftest.log"
        self.createStbThreads = 100
        self.createTbThreads = 100
        self.insertRowsThreads = 24

        logger.add(self.logfile)
    
    def exec_remote_cmd(self, cmd):
        """
            remote exec shell cmd
        """
        try:
            c = Connection(self.ip, user=self.user, port=self.port, connect_timeout=120, connect_kwargs={"password": self.passwd})
            result = c.run(cmd, pty=False, warn=True, hide=True).stdout
            c.close()
            return result
        except Exception as e:
            logger.error(f"exec cmd {cmd} failed：{e}");

    def exec_local_cmd(self, shell_cmd):
        '''
            exec local shell cmd
        '''
        result = os.popen(shell_cmd).read().strip()
        return result

    def modifyJxmLooptimes(self, filename, looptimes):
        '''
            modify looptimes
        '''
        with open(filename, "r", encoding="utf-8") as f:
            lines = f.readlines()
        with open(filename, "w", encoding="utf-8") as f_w:
            for line in lines:
                if "looptimes" in line:
                    line = line.replace("looptimes", looptimes)
                f_w.write(line)

    def cleanAndRestartTaosd(self):
        '''
            restart taosd and clean env
        '''
        logger.info("restarting taosd and taosadapter")
        self.exec_remote_cmd("systemctl stop taosd")
        self.exec_remote_cmd("rm -rf /var/lib/taos/* /var/log/taos/*")
        self.exec_remote_cmd("systemctl start taosd")
        time.sleep(60)
        
    def recreateReportDir(self, path):
        '''
            recreate jmeter report path
        '''
        if os.path.exists(path):
            self.exec_local_cmd(f'rm -rf {path}/*')
        else:
            os.makedirs(path)

    def cleanLog(self):
        '''
            clean log
        '''
        with open(self.logfile, 'w') as f:
            f.seek(0)
            f.truncate()

    def outputParams(self, protocol, create_type):
        '''
            procotol is "telnet" or "json"
            create_type is "stb" or "tb" or "rows"
        '''
        if protocol == "telnet":
            if create_type == "stb":
                return self.telnetCreateStbJmxFile, self.createStbThreads
            elif create_type == "tb":
                return self.telnetCreateTbJmxFile, self.createTbThreads
            elif create_type == "rows":
                return self.telnetInsertRowsFile, self.insertRowsThreads
            else:
                logger.error("create type error!")
        else:
            if create_type == "stb":
                return self.jsonCreateStbJmxFile, self.createStbThreads
            elif create_type == "tb":
                return self.jsonCreateTbJmxFile, self.createTbThreads
            elif create_type == "rows":
                return self.jsonInsertRowsFile, self.insertRowsThreads
            else:
                logger.error("create type error!")

    def insertTDengine(self, procotol, create_type, count):
        '''
            create stb/tb or insert rows
        '''
        jmxfile, threads = self.outputParams(procotol, create_type)
        handle_file = str(count) + jmxfile
        report_dir = f'testreport/{handle_file}'
        self.recreateReportDir(report_dir)
        shutil.copyfile(jmxfile, handle_file)
        replace_count = int(count/threads)
        self.modifyJxmLooptimes(handle_file, str(replace_count))
        result = self.exec_local_cmd(f"jmeter -n -t {handle_file} -l {report_dir}/{handle_file}.txt -e -o {report_dir}")
        logger.info(result)

if __name__ == '__main__':
    taosadapterPerftest = TaosadapterPerftest()
    taosadapterPerftest.cleanLog()
    
    logger.info('------------ Start testing the scenarios in the report chapter 3.4.1 ------------')
    for procotol in ["telnet", "json"]:
        taosadapterPerftest.cleanAndRestartTaosd()
        logger.info(f'----- {procotol} protocol ------- Creating 30W stable ------------')
        taosadapterPerftest.insertTDengine(procotol, "stb", 300000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- Creating 100W table with stb "cpu.usage_user" ------------')
        taosadapterPerftest.insertTDengine(procotol, "tb", 1000000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- inserting 100W rows ------------')
        taosadapterPerftest.insertTDengine(procotol, "rows", 1000000)
        time.sleep(120)

        taosadapterPerftest.cleanAndRestartTaosd()
        logger.info(f'----- {procotol} protocol ------- Creating 50W stable ------------')
        taosadapterPerftest.insertTDengine(procotol, "stb", 500000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- Creating 500W table with stb "cpu.usage_user" ------------')
        taosadapterPerftest.insertTDengine(procotol, "tb", 5000000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- inserting 500W rows ------------')
        taosadapterPerftest.insertTDengine(procotol, "rows", 5000000)
        time.sleep(120)

        taosadapterPerftest.cleanAndRestartTaosd()
        logger.info(f'----- {procotol} protocol ------- Creating 100W stable ------------')
        taosadapterPerftest.insertTDengine(procotol, "stb", 1000000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- Creating 1000W table with stb "cpu.usage_user" ------------')
        taosadapterPerftest.insertTDengine(procotol, "tb", 10000000)
        time.sleep(120)
        logger.info(f'----- {procotol} protocol ------- inserting 1000W rows ------------')
        taosadapterPerftest.insertTDengine(procotol, "rows", 10000000)
        time.sleep(120)

    










