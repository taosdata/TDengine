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
        if  "ts" not in infoDict[0] or len(infoDict[0]["ts"]) == 0:
            tdLog.exit("ts is null!")

        if "protocol" not in infoDict[0] or infoDict[0]["protocol"] != 2:
            tdLog.exit("protocol is null!")

        if "tables" not in infoDict[0]:
            tdLog.exit("tables is null!")

        if infoDict[0]["tables"][0]["name"] != "taosd_dnodes_info":
            tdLog.exit("taosd_dnodes_info is null!")

        if infoDict[0]["tables"][1]["name"] != "taosd_dnodes_log_dirs":
            tdLog.exit("taosd_dnodes_log_dirs is null!")

        if infoDict[0]["tables"][2]["name"] != "taosd_dnodes_data_dirs":
            tdLog.exit("taosd_dnodes_data_dirs is null!")
        
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

        if infoDict[0]["tables"][4]["name"] != "taosd_vgroups_info":
            tdLog.exit("taosd_vgroups_info is null!")

        # vgroup_infos  ====================================

        if "vgroup_infos" not in infoDict or infoDict["vgroup_infos"]== None:
            tdLog.exit("vgroup_infos is null!")

        vgroup_infos_nums = len(infoDict[0]["tables"][3]["metric_groups"][0]["metrics"])   

        for  index in range(vgroup_infos_nums):
            if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][0]["metrics"][0]["name"] != "tables_num":
                tdLog.exit("tables_num is null!")

            if infoDict[0]["tables"][3]["metric_groups"][0]["metrics"][0]["metrics"][0]["name"] != "status":
                tdLog.exit("status is null!")

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
