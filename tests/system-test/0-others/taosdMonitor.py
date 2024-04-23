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
        if  "ts" not in infoDict or len(infoDict["ts"]) == 0:
            tdLog.exit("ts is null!")

        if "dnode_id" not in infoDict or infoDict["dnode_id"] != 1:
            tdLog.exit("dnode_id is null!")

        if "dnode_ep" not in infoDict:
            tdLog.exit("dnode_ep is null!")

        if "cluster_id" not in infoDict:
            tdLog.exit("cluster_id is null!")

        if "protocol" not in infoDict or infoDict["protocol"] != 1:
            tdLog.exit("protocol is null!")

        if "cluster_info" not in infoDict :
            tdLog.exit("cluster_info is null!")

        # cluster_info  ====================================

        if "first_ep" not in infoDict["cluster_info"] or infoDict["cluster_info"]["first_ep"] == None:
            tdLog.exit("first_ep is null!")

        if "first_ep_dnode_id" not in infoDict["cluster_info"] or infoDict["cluster_info"]["first_ep_dnode_id"] != 1:
            tdLog.exit("first_ep_dnode_id is null!")

        if "version" not in infoDict["cluster_info"] or infoDict["cluster_info"]["version"] == None:
            tdLog.exit("first_ep_dnode_id is null!")

        if "master_uptime" not in infoDict["cluster_info"] or infoDict["cluster_info"]["master_uptime"] == None:
            tdLog.exit("master_uptime is null!")

        if "monitor_interval" not in infoDict["cluster_info"] or infoDict["cluster_info"]["monitor_interval"] !=5:
            tdLog.exit("monitor_interval is null!")

        if "vgroups_total" not in infoDict["cluster_info"] or infoDict["cluster_info"]["vgroups_total"] < 0:
            tdLog.exit("vgroups_total is null!")

        if "vgroups_alive" not in infoDict["cluster_info"] or infoDict["cluster_info"]["vgroups_alive"] < 0:
            tdLog.exit("vgroups_alive is null!")

        if "connections_total" not in infoDict["cluster_info"] or infoDict["cluster_info"]["connections_total"] < 0 :
            tdLog.exit("connections_total is null!")

        if "dnodes" not in infoDict["cluster_info"] or infoDict["cluster_info"]["dnodes"] == None :
            tdLog.exit("dnodes is null!")

        dnodes_info = { "dnode_id": 1,"dnode_ep": self.hostPort,"status":"ready"}

        for k ,v in dnodes_info.items():
            if k not in infoDict["cluster_info"]["dnodes"][0] or v != infoDict["cluster_info"]["dnodes"][0][k] :
                tdLog.exit("dnodes info is null!")

        mnodes_info = { "mnode_id":1, "mnode_ep": self.hostPort,"role": "leader" }

        for k ,v in mnodes_info.items():
            if k not in infoDict["cluster_info"]["mnodes"][0] or v != infoDict["cluster_info"]["mnodes"][0][k] :
                tdLog.exit("mnodes info is null!")

        # vgroup_infos  ====================================

        if "vgroup_infos" not in infoDict or infoDict["vgroup_infos"]== None:
            tdLog.exit("vgroup_infos is null!")

        vgroup_infos_nums = len(infoDict["vgroup_infos"])

        for  index in range(vgroup_infos_nums):
            if "vgroup_id" not in infoDict["vgroup_infos"][index] or infoDict["vgroup_infos"][index]["vgroup_id"]<0:
                tdLog.exit("vgroup_id is null!")
            if "database_name" not in infoDict["vgroup_infos"][index] or len(infoDict["vgroup_infos"][index]["database_name"]) < 0:
                tdLog.exit("database_name is null!")
            if "tables_num" not in infoDict["vgroup_infos"][index]:
                tdLog.exit("tables_num is null!")
            if "status" not in infoDict["vgroup_infos"][index] or len(infoDict["vgroup_infos"][index]["status"]) < 0 :
                tdLog.exit("status is null!")
            if "vnodes" not in infoDict["vgroup_infos"][index] or infoDict["vgroup_infos"][index]["vnodes"] ==None :
                tdLog.exit("vnodes is null!")
            if "dnode_id" not in infoDict["vgroup_infos"][index]["vnodes"][0] or infoDict["vgroup_infos"][index]["vnodes"][0]["dnode_id"] < 0 :
                tdLog.exit("vnodes is null!")

        # grant_info  ====================================

        if "grant_info" not in infoDict or infoDict["grant_info"]== None:
            tdLog.exit("grant_info is null!")

        if "expire_time" not in infoDict["grant_info"] or not infoDict["grant_info"]["expire_time"] > 0:
            tdLog.exit("expire_time is null!")

        if "timeseries_used" not in infoDict["grant_info"]:# or not infoDict["grant_info"]["timeseries_used"] > 0:
            tdLog.exit("timeseries_used is null!")

        if "timeseries_total" not in infoDict["grant_info"] or not infoDict["grant_info"]["timeseries_total"] > 0:
            tdLog.exit("timeseries_total is null!")

        # dnode_info  ====================================

        if "dnode_info" not in infoDict or infoDict["dnode_info"]== None:
            tdLog.exit("dnode_info is null!")

        dnode_infos =  ['uptime', 'cpu_engine', 'cpu_system', 'cpu_cores', 'mem_engine', 'mem_system', 'mem_total', 'disk_engine',
        'disk_used', 'disk_total', 'net_in', 'net_out', 'io_read', 'io_write', 'io_read_disk', 'io_write_disk', 'req_select',
        'req_select_rate', 'req_insert', 'req_insert_success', 'req_insert_rate', 'req_insert_batch', 'req_insert_batch_success',
        'req_insert_batch_rate', 'errors', 'vnodes_num', 'masters', 'has_mnode', 'has_qnode', 'has_snode']
        for elem in dnode_infos:
            if elem not in infoDict["dnode_info"] or  infoDict["dnode_info"][elem] < 0:
                tdLog.exit(f"{elem} is null!")

        # dnode_info  ====================================

        if "disk_infos" not in infoDict or infoDict["disk_infos"]== None:
            tdLog.exit("disk_infos is null!")

        # bug for data_dir
        if "datadir" not in infoDict["disk_infos"] or len(infoDict["disk_infos"]["datadir"]) <=0 :
            tdLog.exit("datadir is null!")

        if "name" not in infoDict["disk_infos"]["datadir"][0] or len(infoDict["disk_infos"]["datadir"][0]["name"]) <= 0:
            tdLog.exit("name is null!")

        if "level" not in infoDict["disk_infos"]["datadir"][0] or infoDict["disk_infos"]["datadir"][0]["level"] < 0:
            tdLog.exit("level is null!")

        if "avail" not in infoDict["disk_infos"]["datadir"][0] or infoDict["disk_infos"]["datadir"][0]["avail"] <= 0:
            tdLog.exit("avail is null!")

        if "used" not in infoDict["disk_infos"]["datadir"][0] or infoDict["disk_infos"]["datadir"][0]["used"] <= 0:
            tdLog.exit("used is null!")

        if "total" not in infoDict["disk_infos"]["datadir"][0] or infoDict["disk_infos"]["datadir"][0]["total"] <= 0:
            tdLog.exit("total is null!")


        if "logdir" not in infoDict["disk_infos"] or infoDict["disk_infos"]["logdir"]== None:
            tdLog.exit("logdir is null!")

        if "name" not in infoDict["disk_infos"]["logdir"] or len(infoDict["disk_infos"]["logdir"]["name"]) <= 0:
            tdLog.exit("name is null!")

        if "avail" not in infoDict["disk_infos"]["logdir"] or infoDict["disk_infos"]["logdir"]["avail"] <= 0:
            tdLog.exit("avail is null!")

        if "used" not in infoDict["disk_infos"]["logdir"] or infoDict["disk_infos"]["logdir"]["used"] <= 0:
            tdLog.exit("used is null!")

        if "total" not in infoDict["disk_infos"]["logdir"] or infoDict["disk_infos"]["logdir"]["total"] <= 0:
            tdLog.exit("total is null!")

        if "tempdir" not in infoDict["disk_infos"] or infoDict["disk_infos"]["tempdir"]== None:
            tdLog.exit("tempdir is null!")

        if "name" not in infoDict["disk_infos"]["tempdir"] or len(infoDict["disk_infos"]["tempdir"]["name"]) <= 0:
            tdLog.exit("name is null!")

        if "avail" not in infoDict["disk_infos"]["tempdir"] or infoDict["disk_infos"]["tempdir"]["avail"] <= 0:
            tdLog.exit("avail is null!")

        if "used" not in infoDict["disk_infos"]["tempdir"] or infoDict["disk_infos"]["tempdir"]["used"] <= 0:
            tdLog.exit("used is null!")

        if "total" not in infoDict["disk_infos"]["tempdir"] or infoDict["disk_infos"]["tempdir"]["total"] <= 0:
            tdLog.exit("total is null!")

        # log_infos  ====================================

        if "log_infos" not in infoDict or infoDict["log_infos"]== None:
            tdLog.exit("log_infos is null!")

        if "summary" not in infoDict["log_infos"] or len(infoDict["log_infos"]["summary"])!= 4:
            tdLog.exit("summary is null!")

        if "total" not in infoDict["log_infos"]["summary"][0] or infoDict["log_infos"]["summary"][0]["total"] < 0 :
            tdLog.exit("total is null!")

        if "level" not in infoDict["log_infos"]["summary"][0] or infoDict["log_infos"]["summary"][0]["level"] not in ["error" ,"info" , "debug" ,"trace"]:
            tdLog.exit("level is null!")

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
    updatecfgDict["monitorForceV2"]        = "0"

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
