import taos
import sys
import time
import socket
import os
import threading

sys.path.append("../../pytest")

from new_test_framework.utils import tdLog, TDDnodes
from taos.tmq import *

sys.path.append("./7-tmq")
from tmqCommon import *

tdDnodes1 = TDDnodes()
tdDnodes2 = TDDnodes()
vgroups = 1
tables = 1
rows_of_each_table = 1
path = "./taosx_perf"
dnode1 = "./dnode1"
dnode2 = "./dnode2"
cfg = "sim/dnode1/cfg"
taosx = "taosx"
taosd = "taosd"
taosxLog = "taosx.log"
taosxTimeout = 2
timeCost = []
speedupStr = []
insertJson = '''{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "connection_pool_size": 20,
  "thread_bind_vgroup": "yes",
  "thread_count": 20,
  "create_table_thread_count": 16,
  "result_file": "./insert_res.txt",
  "confirm_parameter_prompt": "no",
  "num_of_records_per_req": 10,
  "prepared_rand": 10000,
  "chinese": "no",
  "escape_character": "yes",
  "continue_if_fail": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "test",
        "drop": "yes",
        "vgroups": 8,
        "precision": "ms",
        "WAL_RETENTION_PERIOD": 864000
      },
      "super_tables": [
        {
          "name": "meters",
          "child_table_exists": "no",
          "childtable_count": 10000,
          "childtable_prefix": "d",
          "auto_create_table": "yes",
          "batch_create_tbl_num": 500,
          "data_source": "rand",
          "insert_mode": "taosc",
          "non_stop_mode": "no",
          "line_protocol": "line",
          "insert_rows": 1000,
          "childtable_limit": 0,
          "childtable_offset": 0,
          "interlace_rows": 1,
          "insert_interval": 0,
          "partial_col_num": 0,
          "timestamp_step": 10,
          "start_timestamp": "2020-10-01 00:00:00.000",
          "sample_format": "csv",
          "sample_file": "./sample.csv",
          "use_sample_ts": "no",
          "tags_file": "",
          "generate_row_rule": 2,
          "columns": [
            {"type": "TINYINT", "name": "current", "max": 128, "min": 1 },
            { "type": "BOOL", "name": "phaseewe" },
            { "type": "BINARY", "name": "str", "len":16374,
              "values": [
                "{kkey00000k: kvalue00000k, kkey00001k: kvalue00001k, kkey00002k: kvalue00002k, kkey00003k: kvalue00003k, kkey00004k: kvalue00004k, kkey00005k: kvalue00005k, kkey00006k: kvalue00006k, kkey00007k: kvalue00007k, kkey00008k: kvalue00008k, kkey00009k: kvalue00009k, kkey00010k: kvalue00010k, kkey00011k: kvalue00011k, kkey00012k: kvalue00012k, kkey00013k: kvalue00013k, kkey00014k: kvalue00014k, kkey00015k: kvalue00015k, kkey00016k: kvalue00016k, kkey00017k: kvalue00017k, kkey00018k: kvalue00018k, kkey00019k: kvalue00019k, kkey00020k: kvalue00020k, kkey00021k: kvalue00021k, kkey00022k: kvalue00022k, kkey00023k: kvalue00023k, kkey00024k: kvalue00024k, kkey00025k: kvalue00025k, kkey00026k: kvalue00026k, kkey00027k: kvalue00027k, kkey00028k: kvalue00028k, kkey00029k: kvalue00029k, kkey00030k: kvalue00030k, kkey00031k: kvalue00031k, kkey00032k: kvalue00032k, kkey00033k: kvalue00033k, kkey00034k: kvalue00034k, kkey00035k: kvalue00035k, kkey00036k: kvalue00036k, kkey00037k: kvalue00037k, kkey00038k: kvalue00038k, kkey00039k: kvalue00039k, kkey00040k: kvalue00040k, kkey00041k: kvalue00041k, kkey00042k: kvalue00042k, kkey00043k: kvalue00043k, kkey00044k: kvalue00044k, kkey00045k: kvalue00045k, kkey00046k: kvalue00046k, kkey00047k: kvalue00047k, kkey00048k: kvalue00048k, kkey00049k: kvalue00049k, kkey00050k: kvalue00050k, kkey00051k: kvalue00051k, kkey00052k: kvalue00052k, kkey00053k: kvalue00053k, kkey00054k: kvalue00054k, kkey00055k: kvalue00055k, kkey00056k: kvalue00056k, kkey00057k: kvalue00057k, kkey00058k: kvalue00058k, kkey00059k: kvalue00059k, kkey00060k: kvalue00060k, kkey00061k: kvalue00061k, kkey00062k: kvalue00062k, kkey00063k: kvalue00063k, kkey00064k: kvalue00064k, kkey00065k: kvalue00065k, kkey00066k: kvalue00066k, kkey00067k: kvalue00067k, kkey00068k: kvalue00068k, kkey00069k: kvalue00069k, kkey00070k: kvalue00070k, kkey00071k: kvalue00071k, kkey00072k: kvalue00072k, kkey00073k: kvalue00073k, kkey00074k: kvalue00074k, kkey00075k: kvalue00075k, kkey00076k: kvalue00076k, kkey00077k: kvalue00077k, kkey00078k: kvalue00078k, kkey00079k: kvalue00079k, kkey00080k: kvalue00080k, kkey00081k: kvalue00081k, kkey00082k: kvalue00082k, kkey00083k: kvalue00083k, kkey00084k: kvalue00084k, kkey00085k: kvalue00085k, kkey00086k: kvalue00086k, kkey00087k: kvalue00087k, kkey00088k: kvalue00088k, kkey00089k: kvalue00089k, kkey00090k: kvalue00090k, kkey00091k: kvalue00091k, kkey00092k: kvalue00092k, kkey00093k: kvalue00093k, kkey00094k: kvalue00094k, kkey00095k: kvalue00095k, kkey00096k: kvalue00096k, kkey00097k: kvalue00097k, kkey00098k: kvalue00098k, kkey00099k: kvalue00099k, kkey00100k: kvalue00100k, kkey00101k: kvalue00101k, kkey00102k: kvalue00102k, kkey00103k: kvalue00103k, kkey00104k: kvalue00104k, kkey00105k: kvalue00105k, kkey00106k: kvalue00106k, kkey00107k: kvalue00107k, kkey00108k: kvalue00108k, kkey00109k: kvalue00109k, kkey00110k: kvalue00110k, kkey00111k: kvalue00111k, kkey00112k: kvalue00112k, kkey00113k: kvalue00113k, kkey00114k: kvalue00114k, kkey00115k: kvalue00115k, kkey00116k: kvalue00116k, kkey00117k: kvalue00117k, kkey00118k: kvalue00118k, kkey00119k: kvalue00119k, kkey00120k: kvalue00120k, kkey00121k: kvalue00121k, kkey00122k: kvalue00122k, kkey00123k: kvalue00123k, kkey00124k: kvalue00124k, kkey00125k: kvalue00125k, kkey00126k: kvalue00126k, kkey00127k: kvalue00127k, kkey00128k: kvalue00128k, kkey00129k: kvalue00129k, kkey00130k: kvalue00130k, kkey00131k: kvalue00131k, kkey00132k: kvalue00132k, kkey00133k: kvalue00133k, kkey00134k: kvalue00134k, kkey00135k: kvalue00135k, kkey00136k: kvalue00136k, kkey00137k: kvalue00137k, kkey00138k: kvalue00138k, kkey00139k: kvalue00139k, kkey00140k: kvalue00140k, kkey00141k: kvalue00141k, kkey00142k: kvalue00142k, kkey00143k: kvalue00143k, kkey00144k: kvalue00144k, kkey00145k: kvalue00145k, kkey00146k: kvalue00146k, kkey00147k: kvalue00147k, kkey00148k: kvalue00148k, kkey00149k: kvalue00149k, kkey00150k: kvalue00150k, kkey00151k: kvalue00151k, kkey00152k: kvalue00152k, kkey00153k: kvalue00153k, kkey00154k: kvalue00154k, kkey00155k: kvalue00155k, kkey00156k: kvalue00156k, kkey00157k: kvalue00157k, kkey00158k: kvalue00158k, kkey00159k: kvalue00159k, kkey00160k: kvalue00160k, kkey00161k: kvalue00161k, kkey00162k: kvalue00162k, kkey00163k: kvalue00163k, kkey00164k: kvalue00164k, kkey00165k: kvalue00165k, kkey00166k: kvalue00166k, kkey00167k: kvalue00167k, kkey00168k: kvalue00168k, kkey00169k: kvalue00169k, kkey00170k: kvalue00170k, kkey00171k: kvalue00171k, kkey00172k: kvalue00172k, kkey00173k: kvalue00173k, kkey00174k: kvalue00174k, kkey00175k: kvalue00175k, kkey00176k: kvalue00176k, kkey00177k: kvalue00177k, kkey00178k: kvalue00178k, kkey00179k: kvalue00179k, kkey00180k: kvalue00180k, kkey00181k: kvalue00181k, kkey00182k: kvalue00182k, kkey00183k: kvalue00183k, kkey00184k: kvalue00184k, kkey00185k: kvalue00185k, kkey00186k: kvalue00186k, kkey00187k: kvalue00187k, kkey00188k: kvalue00188k, kkey00189k: kvalue00189k, kkey00190k: kvalue00190k, kkey00191k: kvalue00191k, kkey00192k: kvalue00192k, kkey00193k: kvalue00193k, kkey00194k: kvalue00194k, kkey00195k: kvalue00195k, kkey00196k: kvalue00196k, kkey00197k: kvalue00197k, kkey00198k: kvalue00198k, kkey00199k: kvalue00199k}",
                "NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL","NULL",
                "NULerL","NdrerULL","NU232LL","NUL23L","NU23LL","NU23LL","NUL23L","NUL32L"
              ]
            },
            { "type": "BIGINT", "name": "cnt", "max" : 2563332323232, "min":1 },
            { "type": "DOUBLE", "name": "phase", "max": 1000, "min": 0 }
          ],
          "tags": [
            {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
            {"type": "BINARY",  "name": "location", "len": 16,
              "values": ["San Francisco", "Los Angles", "San Diego",
                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                "Sunnyvale", "Santa Clara", "Cupertino"]
            }
          ]
        }
      ]
    }
  ]
}'''


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

class TestCase:
  def test_dummy(self):
    """summary: xxx

    description: xxx

    Since: xxx

    Labels: xxx

    Jira: xxx

    Catalog:
    - xxx:xxx

    History:
    - xxx
    - xxx

    """
    pass