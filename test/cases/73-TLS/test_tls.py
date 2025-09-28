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

        clientcfgDict = {} 
        updatecfgDict = {}

        if platform.system().lower() == "linux":    
            #os.system("sh {os.path.dirname(os.path.realpath(__file__))}/tlsFileGen.sh")
            clientcfgDict = {
                "enableTls"        :"1",  
                "forceReadConfig": "1",   
                "tlsCliKeyPath"         :"/tmp/client.crt", 
                "tlsCliCertPath"         : "/tmp/client.crt", 
                "tlsSvrKeyPath"         :"/tmp/server.crt", 
                "tlsSvrCertPath"         :"/tmp/server.crt", 
                "tlsCaPath"              :"/tmp/ca.crt"  
            }
            updatecfgDict = {
                "clientCfg" : clientcfgDict,
                "enableTls"        :"1", 
                "forceReadConfig": "1",
                "tlsCliKeyPath"         :"/tmp/server.crt", 
                "tlsCliCertPath"         : "/tmp/server.crt", 
                "tlsSvrKeyPath"         :"/tmp/server.crt", 
                "tlsSvrCertPath"         :"/tmp/server.crt", 
                "tlsCaPath"              :"/tmp/ca.crt"  
            } 
        tdDnodes.stop(1)
        #tdDnodes.simDeployed = False
        tdDnodes.sim.deploy(updatecfgDict)
        tdDnodes.deploy(1, updatecfgDict)
        

        tdDnodes.starttaosd(1)  


          
    def stop_and_restart(self):   
        self.basicTest(tdSql)  
        tdSql.close()

        self.genTLSFile()
        self.restartAndupdateCfg()

        newClient = tdCom.newTdSql()
        self.basicTest(newClient)    

    def basicTest(self, cli):
        cli.query("select 1")
        cli.query("show databases")

    def test_tls_demo(self):
        self.stop_and_restart()
        
