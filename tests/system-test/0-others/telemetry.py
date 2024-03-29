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

telemetryPort = '80'

#{
#	"instanceId":	"5cf4cd7a-acd4-43ba-8b0d-e84395b76a65",
#	"reportVersion":	1,
#	"os":	"Ubuntu 20.04.3 LTS",
#	"cpuModel":	"Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz",
#	"numOfCpu":	6,
#	"memory":	"65860292 kB",
#	"version":	"3.0.0.0",
#	"buildInfo":	"Built at 2022-05-07 14:09:02",
#	"gitInfo":	"2139ccceb0946cde86b6b553b11e338f1ba437e5",
#	"email":	"user@taosdata.com",
#	"numOfDnode":	1,
#	"numOfMnode":	1,
#	"numOfVgroup":	32,
#	"numOfDatabase":	2,
#	"numOfSuperTable":	0,
#	"numOfChildTable":	100,
#	"numOfColumn":	200,
#	"numOfPoint":	300,
#	"totalStorage":	400,
#	"compStorage":	500
#}

def telemetryInfoCheck(infoDict=''):
    if  "instanceId" not in infoDict or len(infoDict["instanceId"]) == 0:
        tdLog.exit("instanceId is null!")

    if "reportVersion" not in infoDict or infoDict["reportVersion"] != 1:
        tdLog.exit("reportVersion is null!")

    if "os" not in infoDict:
        tdLog.exit("os is null!")

    if "cpuModel" not in infoDict:
        tdLog.exit("cpuModel is null!")

    if "numOfCpu" not in infoDict or infoDict["numOfCpu"] == 0:
        tdLog.exit("numOfCpu is null!")

    if "memory" not in infoDict:
        tdLog.exit("memory is null!")

    if "version" not in infoDict:
        tdLog.exit("version is null!")

    if "buildInfo" not in infoDict:
        tdLog.exit("buildInfo is null!")

    if "gitInfo" not in infoDict:
        tdLog.exit("gitInfo is null!")

    if "email" not in infoDict:
        tdLog.exit("email is not exists!")

    if "numOfDnode" not in infoDict or infoDict["numOfDnode"] < 1:
        tdLog.exit("numOfDnode is null!")

    if "numOfMnode" not in infoDict or infoDict["numOfMnode"] < 1:
        tdLog.exit("numOfMnode is null!")

    if "numOfVgroup" not in infoDict or infoDict["numOfVgroup"] <= 0:
        tdLog.exit("numOfVgroup is null!")

    if "numOfDatabase" not in infoDict or infoDict["numOfDatabase"] <= 0:
        tdLog.exit("numOfDatabase is null!")

    if "numOfSuperTable" not in infoDict or infoDict["numOfSuperTable"] < 0:
        tdLog.exit("numOfSuperTable is null!")

    if "numOfChildTable" not in infoDict or infoDict["numOfChildTable"] < 0:
        tdLog.exit("numOfChildTable is null!")

    if "numOfColumn" not in infoDict or infoDict["numOfColumn"] < 0:
        tdLog.exit("numOfColumn is null!")

    if "numOfPoint" not in infoDict or infoDict["numOfPoint"] < 0:
        tdLog.exit("numOfPoint is null!")

    if "totalStorage" not in infoDict or infoDict["totalStorage"] < 0:
        tdLog.exit("totalStorage is null!")

    if "compStorage" not in infoDict or infoDict["compStorage"] < 0:
        tdLog.exit("compStorage is null!")


class RequestHandlerImpl(http.server.BaseHTTPRequestHandler):
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

        print("monitor info:\n%s"%plainText)

        # 1. send response code and header
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()

        # 2. send response content
        #self.wfile.write(("Hello World: " + req_body + "\n").encode("utf-8"))

        # 3. check request body info
        infoDict = json.loads(plainText)
        #print("================")
        #print(infoDict)
        telemetryInfoCheck(infoDict)

        # 4. shutdown the server and exit case
        assassin = threading.Thread(target=self.server.shutdown)
        assassin.daemon = True
        assassin.start()
        print ("==== shutdown http server ====")

class TDTestCase:
    hostname = socket.gethostname()
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP)
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    serverPort = '7080'
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

    updatecfgDict["telemetryReporting"]       = '1'
    updatecfgDict["telemetryServer"]          = hostname
    updatecfgDict["telemetryPort"]            = telemetryPort
    updatecfgDict["telemetryInterval"]        = "3"

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

        # create http server: bing ip/port , and  request processor
        if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
            RequestHandlerImplStr = base64.b64encode(pickle.dumps(RequestHandlerImpl)).decode()
            telemetryInfoCheckStr = base64.b64encode(pickle.dumps(telemetryInfoCheck)).decode()
            cmdStr = "import pickle\nimport http\ntelemetryInfoCheck=pickle.loads(base64.b64decode(\"%s\".encode()))\nRequestHandlerImpl=pickle.loads(base64.b64decode(\"%s\".encode()))\nhttp.server.HTTPServer((\"\", %d), RequestHandlerImpl).serve_forever()"%(telemetryInfoCheckStr,RequestHandlerImplStr,int(telemetryPort))
            tdDnodes.dnodes[0].remoteExec({}, cmdStr)
        else:
            serverAddress = ("", int(telemetryPort))
            http.server.HTTPServer(serverAddress, RequestHandlerImpl).serve_forever()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())





