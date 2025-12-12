from new_test_framework.utils import tdLog, tdSql, etool, tdDnodes, tdCom

import os
import platform 
import time
import subprocess
import re



class TestTLSDemo:
    def setup_class(cls):
        #print(f"setup_class: {cls.updatecfgDict}")    
        tdLog.debug(f"start to excute {__file__}")
        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)
        
    def genTLSFile(self):
        if platform.system().lower() == "linux":    
            os.system("sh {os.path.dirname(os.path.realpath(__file__))}/tlsFileGen.sh")

    def restartAndupdateCfg(self):

        clientcfgDict = {} 
        updatecfgDict = {}

        if platform.system().lower() == "linux":    
            #os.system("sh {os.path.dirname(os.path.realpath(__file__))}/tlsFileGen.sh")
            clientcfgDict = {
                "enableTls"        :"1",  
                "forceReadConfig": "1",   
                "tlsCliKeyPath"         :"/tmp/client.key", 
                "tlsCliCertPath"         : "/tmp/client.crt", 
                "tlsSvrKeyPath"         :"/tmp/server.key", 
                "tlsSvrCertPath"         :"/tmp/server.crt", 
                "tlsCaPath"              :"/tmp/ca.crt"  
            }
            updatecfgDict = {
                "clientCfg" : clientcfgDict,
                "enableTls"        :"1", 
                "forceReadConfig": "1",
                "tlsCliKeyPath"         :"/tmp/client.key", 
                "tlsCliCertPath"         : "/tmp/client.crt", 
                "tlsSvrKeyPath"         :"/tmp/server.key", 
                "tlsSvrCertPath"         :"/tmp/server.crt", 
                "tlsCaPath"              :"/tmp/ca.crt"  
            } 
        tdSql.close()
        time.sleep(10)
        tdDnodes.stop(1)

         
        #tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)
        

        
        # python tdsql  ----> taos-c-drvi 
        # python newsql ---
        tdDnodes.starttaosd(1)  

    def stop_and_restart(self):   
        self.basicTest(tdSql)  

        self.genTLSFile()
        self.restartAndupdateCfg()

        if platform.system().lower() == "linux":
            self.testCmd("show databases")
            self.testCmd("select 1")

    def testCmd(self, cmd):

        cfg = tdDnodes.sim.getCfgDir() 

        shellCmd = f"taos -c {cfg} -s \"{cmd}\""
        print(f"execute {shellCmd}")
        result = subprocess.run(shellCmd, shell=True, capture_output=True, text=True)
        output = result.stdout
        print(f"output {output}")      

        if "name"not in output.lower() and "1" not in output: 
            raise ValueError(f"output {output} not correct")

        lines = output.split("\n")
        print(f"lines {lines}")
        if len(lines) < 4:
            raise ValueError(f"output {output} not correct")

    def basicTest(self, cli):
        cli.query("select 1")
        cli.query("show databases")


    # def updateTls(self, cli):
    #     #cli.execute("alter dnodes reload tls") 
    def test_tls_demo(self):
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
        self.stop_and_restart()
        # for i in range(10):
        #     time.sleep(3)
        #     self.updateTls(tdSql)
        #     self.basicTest(tdSql)
        

        
        
