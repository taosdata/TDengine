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
        if  "records" not in infoDict or len(infoDict["records"]) == 0:
            tdLog.exit("records is null!")

        if "operation" not in infoDict["records"][0] or infoDict["records"][0]["operation"] != "delete":
            tdLog.exit("operation is null!")

        if "details" not in infoDict["records"][0] or infoDict["records"][0]["details"] != "delete from db3.tb":
            tdLog.exit("details is null!")

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
    updatecfgDict["monitor"]            = '0'
    updatecfgDict["monitorInterval"]        = "5"
    updatecfgDict["monitorMaxLogs"]        = "10"
    updatecfgDict["monitorComp"]        = "1"
    updatecfgDict["monitorForceV2"]        = "0"

    updatecfgDict["audit"]            = '1'

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

        sql = "delete from db3.tb"
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
