import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

insertJson = '''{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "localhost",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 10,
    "thread_count": 10,
    "create_table_thread_count": 10,
    "result_file": "./insert-2-2-1.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 3600,
    "prepared_rand": 3600,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "ts5617",
                "drop": "yes",
                "vgroups": 10,
                "precision": "ms",
		"buffer": 512,
		"cachemodel":"'both'",
		"stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb_2_2_1",
                    "child_table_exists": "no",
                    "childtable_count": 10000,
                    "childtable_prefix": "d_",
                    "auto_create_table": "yes",
                    "batch_create_tbl_num": 10,
                    "data_source": "csv",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 20000,
                    "childtable_limit": 0,
                    "childtable_offset": 0,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "2024-11-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./td_double10000_juchi.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {"type": "DOUBLE", "name": "val"},
                        { "type": "INT", "name": "quality"}
                    ],
                    "tags": [
                        {"type": "INT", "name": "id", "max": 100, "min": 1}
                    ]
                }
            ]
        }
    ]
}'''

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):

        with open('ts-5617.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f ts-5617.json")
        if os.system("taosBenchmark -f ts-5617.json") != 0:
            tdLog.exit("taosBenchmark -f ts-5617.json")


        start_time = time.time()
        tdSql.execute(f'create stream s1 fill_history 1 into ts5617.st1 tags(tname varchar(20)) subtable(tname) as select last(val), last(quality) from ts5617.stb_2_2_1 partition by tbname tname interval(1800s);')
        end_time = time.time()
        if end_time - start_time > 1:
            tdLog.err("create stream too long")

        tdSql.query("show streams")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "init")

        while 1:
            tdSql.query("show streams")
            tdLog.info(f"streams is creating ...")
            if tdSql.getData(0, 1) == "ready":
                break
            else:
                time.sleep(10)
        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())



def initEnv():
    updatecfgDict2 = {'debugFlag': 131, 'serverPort': 7030}
    tdDnodes2.init(dnode2)
    tdDnodes2.deploy(1, updatecfgDict2)
    tdDnodes2.start(1)

    updatecfgDict1 = {'debugFlag': 131, 'serverPort': 6030}
    tdDnodes1.init(dnode1)
    tdDnodes1.deploy(1, updatecfgDict1)
    tdDnodes1.start(1)

    with open('taosx-performance.json', 'w') as file:
        file.write(insertJson)

    changeVgroups = f"sed -i '/vgroups/c\    \"vgroups\": {vgroups},' taosx-performance.json"
    os.system(changeVgroups)

    changeTables = f"sed -i '/childtable_count/c\    \"childtable_count\": {tables},' taosx-performance.json"
    os.system(changeTables)

    changeRows = f"sed -i '/insert_rows/c\    \"insert_rows\": {rows_of_each_table},' taosx-performance.json"
    os.system(changeRows)
    os.system("taosBenchmark -f taosx-performance.json")

def stopTaosd(str):
    psCmd = f"ps -ef | grep -w taosd | grep -v grep | grep {str}" + " | awk '{print $2}' | xargs"
    processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
    print(f"kill taosd pid={processID}")
    if processID:
        cmd = f"kill -9 {processID}"
        os.system(cmd)

def cleanDb():
    dropTopic = f"{taosd}/bin/taos -c {dnode1}/{cfg} -s \"drop topic if exists test\""
    print("dropTopic:%s" % dropTopic)
    os.system(dropTopic)

    dropDb = f"{taosd}/bin/taos -c {dnode2}/{cfg} -s \"drop database if exists test\""
    print("dropDb:%s" % dropDb)
    os.system(dropDb)

    createDb = f"{taosd}/bin/taos -c {dnode2}/{cfg} -s \"create database test vgroups {vgroups}\""
    print("createDb:%s" % createDb)
    os.system(createDb)


def restartTaosd():
    cmd1 = f"{taosd}/bin/taosd -c {dnode1}/{cfg} > /dev/null 2>&1 &"
    cmd2 = f"{taosd}/bin/taosd -c {dnode2}/{cfg} > /dev/null 2>&1 &"
    print("start taosd1 :%s" % cmd1)
    print("start taosd2 :%s" % cmd2)
    os.system(cmd1)
    os.system(cmd2)


def runTaosx():
    cmd = f"{taosx} run -f \"tmq://root:taosdata@localhost:6030/test?group.id=taosx-new-`date +%s`&timeout={taosxTimeout}s&experimental.snapshot.enable=false&auto.offset.reset=earliest&prefer=raw&libraryPath={taosd}/lib/libtaos.so\" -t \"taos://root:taosdata@localhost:7030/test?libraryPath={taosd}/lib/libtaos.so\" > {taosxLog}"
    print("run taosx:%s" % cmd)
    os.system(cmd)


def parseArgs(argv):
    if len(argv) < 6:
        print(
            "Usage: python3 taosx-performance.py path_to_taosx path_to_taosd init vgroups tables rows_of_each_table [path]")
        sys.exit(1)

    global taosx
    global taosd
    global init
    global vgroups
    global tables
    global rows_of_each_table
    global path
    global dnode1
    global dnode2
    taosx = argv[0]
    taosd = argv[1]
    init = argv[2]
    vgroups = argv[3]
    tables = argv[4]
    rows_of_each_table = argv[5]
    if len(argv) == 7:
        path = argv[6]

    if not os.path.isdir(path):
        mkCmd = f"mkdir {path}"
        os.system(mkCmd)
        # os.system("git clone https://github.com/brendangregg/FlameGraph.git taosx-perf")
def getCost():
    costCmd = f"cat {taosxLog}| grep 'time cost'" + "| awk '{print $3}' | xargs"
    cost = subprocess.check_output(costCmd, shell=True).decode("utf-8").strip()
    return cost

def runOnce(argv):
    parseArgs(argv)

    os.chdir(path)
    print("current dir:" + os.getcwd())

    stopTaosd("dnode2")
    stopTaosd("dnode1")
    time.sleep(2)

    if init == "true":
        initEnv()
    else:
        restartTaosd()

    cleanDb()
    runTaosx()

    cost = getCost()
    print("cost:%s" % cost)
    timeCost.append(cost)

'''
python3 taosx-performance.py once path_to_taosx path_to_taosd init vgroups tables rows_of_each_table [path]
'''
paras = [
    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  1, 5000, 200, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 1, 5000, 200, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  1, 5000, 1000, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 1, 5000, 1000, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  1, 5000, 2000, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 1, 5000, 2000, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  2, 10000, 100, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 2, 10000, 100, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  2, 10000, 500, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 2, 10000, 500, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  2, 10000, 1000, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 2, 10000, 1000, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  4, 20000, 500, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 4, 20000, 500, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  8, 40000, 250, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 8, 40000, 250, "/tmp/taosx_perf"),

    ("/root/taosx/taosx/target/release/taosx", "~/TDinternal/community/debug/build/bin/taosd", "true",  16, 100000, 100, "/tmp/taosx_perf"),
    ("/root/taosx/taosx/target/release/taosx", "~/TDengine/debug/build/bin/taosd",             "false", 16, 100000, 100, "/tmp/taosx_perf"),
]
if __name__ == "__main__":
    print("run performance start")

    once = sys.argv[1]
    if once == "once":
        runOnce(sys.argv[2:])
    else:
        for i in range(len(paras)):
            runOnce(paras[i])
            if i % 2 == 1 :
                print(f"opti cost:{float(timeCost[0]) - taosxTimeout}")
                print(f"old  cost:{float(timeCost[1]) - taosxTimeout}")
                tmp = str(paras[i]) + f" speedup:{(float(timeCost[1]) - taosxTimeout)/(float(timeCost[0]) - taosxTimeout)}"
                speedupStr.append(tmp)
                print(tmp + "\n\n\n")
                timeCost.clear()

    print("performance result:\n" + str(speedupStr))
    tdLog.info("run performance end")
