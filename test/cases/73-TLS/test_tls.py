from new_test_framework.utils import tdLog, tdSql, etool, tdDnodes, tdCom

import os
import platform 



class TestTLSDemo:
    def setup_class(cls):
        #print(f"setup_class: {cls.updatecfgDict}")    
        tdLog.debug(f"start to excute {__file__}")
        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)
        
    def genTLSFile(self):
        if platform.system().lower() == "linux":    
            os.system("sh {os.path.dirname(os.path.realpath(__file__))}/tlsFileGen.sh")

    def restartAndupdateCfg(self):
        clientcfgDict1 = {
        "enableTls"        :"1",  
        "forreadConfig": "1",   
        "tlsCliKeyPath"         :"/tmp/client.crt", 
        "tlsCliCertPath"         : "/tmp/client.crt", 
        "tlsSvrKeyPath"         :"/tmp/server.crt", 
        "tlsSvrCertPath"         :"/tmp/server.crt", 
        "tlsCaPath"              :"/tmp/ca.crt"  
        }

        updatecfgDict1 = {
        "enableTls"        :"1", 
        "forceReadConfig": "1",
        "tlsCliKeyPath"         :"/tmp/server.crt", 
        "tlsCliCertPath"         : "/tmp/server.crt", 
        "tlsSvrKeyPath"         :"/tmp/server.crt", 
        "tlsSvrCertPath"         :"/tmp/server.crt", 
        "tlsCaPath"              :"/tmp/ca.crt",  
        'clientCfg' : clientcfgDict1 } 


        tdDnodes.stop(1)
        tdDnodes.deploy(1, updatecfgDict1)
        tdDnodes.starttaosd(1)  
          
    def stop_and_restart(self):   
        self.genTLSFile()
        self.basicTest(tdSql)  
        tdSql.close()
        self.restartAndupdateCfg()

        newClient = tdCom.newTdSql()
        self.basicTest(newClient)    

    def basicTest(self, cli):
        cli.query("select 1")
        cli.checkData(0,0,1)

        cli.query("show databases");

    def test_tls_demo(self):
        self.stop_and_restart()
        
