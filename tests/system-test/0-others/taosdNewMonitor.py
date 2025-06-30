import taos
import sys
import time
import socket
# import pexpect
import os
import http.server
import gzip
import threading
import json
import pickle

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

telemetryPort = '6043'
serverPort = '7080'
hostname = socket.gethostname()

class RequestHandlerImpl(http.server.BaseHTTPRequestHandler):
    hostPort = hostname + ":" + serverPort

    def telemetryInfoCheck(self, infoDict=''):
        if len(infoDict) == 0:
            return
        
        if  "ts" not in infoDict[0] or len(infoDict[0]["ts"]) == 0:
            tdLog.exit("ts is null!")

        if "protocol" not in infoDict[0] or infoDict[0]["protocol"] != 2:
            tdLog.exit("protocol is null!")

        if "tables" not in infoDict[0]:
            tdLog.exit("tables is null!")

        if infoDict[0]["tables"][0]["name"] != "taosd_dnodes_info":
            tdLog.exit("taosd_dnodes_info is null!")
        
        # dnode_info  ====================================

        dnode_infos =  ['disk_engine', 'system_net_in', 'vnodes_num', 'system_net_out', 'uptime', 'has_mnode', 'io_read_disk', 'error_log_count',
        'io_read', 'cpu_cores', 'has_qnode', 'has_snode', 'disk_total', 'mem_engine', 'info_log_count', 'cpu_engine', 'io_write_disk',
        'debug_log_count', 'disk_used', 'mem_total', 'io_write', 'masters', 'cpu_system',
        'trace_log_count', 'mem_free']
        index = 0
        for elem in dnode_infos:
            tdLog.debug(f"elem: {index},{elem}")
            if infoDict[0]["tables"][0]["metric_groups"][0]["metrics"][index]["name"] != elem:
                tdLog.exit(f"{elem} is null!")
            index += 1

        if infoDict[0]["tables"][1]["name"] != "taosd_dnodes_log_dirs":
            tdLog.exit("taosd_dnodes_log_dirs is null!")

        # logdir
        if infoDict[0]["tables"][1]["metric_groups"][0]["tags"][3]["name"] != "data_dir_name":
            tdLog.exit("data_dir_name is null!")

        if infoDict[0]["tables"][1]["metric_groups"][0]["metrics"][0]["name"] != "total":
            tdLog.exit("total is null!")

        if infoDict[0]["tables"][1]["metric_groups"][0]["metrics"][1]["name"] != "used":
            tdLog.exit("used is null!")
        
        if infoDict[0]["tables"][1]["metric_groups"][0]["metrics"][2]["name"] != "avail":
            tdLog.exit("avail is null!")

        if infoDict[0]["tables"][2]["name"] != "taosd_dnodes_data_dirs":
            tdLog.exit("taosd_dnodes_data_dirs is null!")

        # data_dir
        if infoDict[0]["tables"][2]["metric_groups"][0]["tags"][3]["name"] != "data_dir_name":
            tdLog.exit("data_dir_name is null!")

        if infoDict[0]["tables"][2]["metric_groups"][0]["metrics"][0]["name"] != "avail":
            tdLog.exit("total is null!")

        if infoDict[0]["tables"][2]["metric_groups"][0]["metrics"][1]["name"] != "total":
            tdLog.exit("used is null!")
        
        if infoDict[0]["tables"][2]["metric_groups"][0]["metrics"][2]["name"] != "used":
            tdLog.exit("avail is null!")
        
        if infoDict[0]["tables"][3]["name"] != "taosd_cluster_info":
            tdLog.exit("taosd_cluster_info is null!")

        # cluster_info  ====================================

        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][0]["name"] != "cluster_uptime":
            tdLog.exit("cluster_uptime is null!")
        
        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][1]["name"] != "dbs_total":
            tdLog.exit("dbs_total is null!")
        
        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][4]["name"] != "vgroups_total":
            tdLog.exit("vgroups_total is null!")

        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][5]["name"] != "vgroups_alive":
            tdLog.exit("vgroups_alive is null!")

        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][10]["name"] != "connections_total":
            tdLog.exit("connections_total is null!")

        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][13]["name"] != "dnodes_total":
            tdLog.exit("dnodes_total is null!")

        # grant_info  ====================================
        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][15]["name"] != "grants_expire_time":
            tdLog.exit("grants_expire_time is null!")

        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][16]["name"] != "grants_timeseries_used":
            tdLog.exit("grants_timeseries_used is null!")
        
        if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][17]["name"] != "grants_timeseries_total":
            tdLog.exit("grants_timeseries_total is null!")

        if infoDict[0]["tables"][4]["name"] != "taosd_sql_req":
            tdLog.exit("taosd_sql_req is null!")

        # vgroup_infos  ====================================

        vgroup_infos_nums = len(infoDict[0]["tables"][5]["metric_groups"])   
    
        for  index in range(vgroup_infos_nums):
            if infoDict[0]["tables"][5]["metric_groups"][index]["metrics"][0]["name"] != "tables_num":
                tdLog.exit("tables_num is null!")

            if infoDict[0]["tables"][5]["metric_groups"][index]["metrics"][1]["name"] != "status":
                tdLog.exit("status is null!")
        
        if infoDict[0]["tables"][6]["name"] != "taosd_dnodes_status":
            tdLog.exit("taosd_dnodes_status is null!")

        if infoDict[0]["tables"][7]["name"] != "taosd_mnodes_info":
            tdLog.exit("taosd_mnodes_info is null!")

        if infoDict[0]["tables"][8]["name"] != "taosd_vnodes_info":
            tdLog.exit("taosd_vnodes_info is null!")

    def do_GET(self):
        """
        process GET request
        """

    def do_POST(self):
        """
        process POST request
        """
        contentEncoding = self.headers["Content-Encoding"]

        if contentEncoding == 'gzip':
            req_body = self.rfile.read(int(self.headers["Content-Length"]))
            plainText = gzip.decompress(req_body).decode()
        else:
            plainText = self.rfile.read(int(self.headers["Content-Length"])).decode()

        print(plainText)
        # 1. send response code and header
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()

        # 2. send response content
        #self.wfile.write(("Hello World: " + req_body + "\n").encode("utf-8"))

        # 3. check request body info
        infoDict = json.loads(plainText)
        #print("================")
        # print(infoDict)
        self.telemetryInfoCheck(infoDict)

        time.sleep(1)

        # 4. shutdown the server and exit case
        assassin = threading.Thread(target=self.server.shutdown)
        assassin.daemon = True
        assassin.start()
        print ("==== shutdown http server ====")

class TDTestCase:
    global hostname
    global serverPort
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP )
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    rpcDebugFlagVal = '143'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    clientCfgDict["fqdn"]          = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"]       = hostname

    updatecfgDict["monitorFqdn"]       = hostname
    updatecfgDict["monitorPort"]          = '6043'
    updatecfgDict["monitor"]            = '1'
    updatecfgDict["monitorInterval"]        = "5"
    updatecfgDict["monitorMaxLogs"]        = "10"
    updatecfgDict["monitorComp"]        = "1"
    updatecfgDict["monitorForceV2"]        = "1"

    updatecfgDict["audit"]            = '0'

    print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        # time.sleep(2)
        vgroups = "4"
        sql = "create database db3 vgroups " + vgroups
        tdSql.query(sql)
        sql = "create table db3.stb (ts timestamp, f int) tags (t int)"
        tdSql.query(sql)
        sql = "create table db3.tb using db3.stb tags (1)"
        tdSql.query(sql)

        count = 0
        while count < 100:
            sql = "insert into db3.tb values (now, %d)"%count
            tdSql.execute(sql)
            count += 1

        # create http server: bing ip/port , and  request processor
        if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
            RequestHandlerImplStr = base64.b64encode(pickle.dumps(RequestHandlerImpl)).decode()
            cmdStr = "import pickle\nimport http\nRequestHandlerImpl=pickle.loads(base64.b64decode(\"%s\".encode()))\nclass NewRequestHandlerImpl(RequestHandlerImpl):\n    hostPort = \'%s\'\nhttp.server.HTTPServer((\"\", %s), NewRequestHandlerImpl).serve_forever()"%(RequestHandlerImplStr,hostname+":"+serverPort,telemetryPort)
            tdDnodes.dnodes[0].remoteExec({}, cmdStr)
        else:
            serverAddress = ("", int(telemetryPort))
            http.server.HTTPServer(serverAddress, RequestHandlerImpl).serve_forever()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
