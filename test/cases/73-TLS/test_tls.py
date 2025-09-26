from new_test_framework.utils import tdLog, tdSql, etool, sc
import os
import platform 



class TestTLSDemo:
    if platform.system().lower() != "linux":
        updateCfgDict = {
        "enableTls"        : "0", 
        } 
    else:
        clientCfgDict = {
        "tlsCliKeyPath"         :"/tmp/server.crt", 
        "tlsCliCertPath"         : "/tmp/server.crt", 
        "tlsSvrKeyPath"         :"/tmp/server.crt", 
        "tlsSvrCertPath"         :"/tmp/server.crt", 
        "tlsCaPath"              :"/tmp/ca.crt"  
        }

        updateCfgDict = {
        "enableTls"        :"1", 
        "tlsCliKeyPath"         :"/tmp/server.crt", 
        "tlsCliCertPath"         : "/tmp/server.crt", 
        "tlsSvrKeyPath"         :"/tmp/server.crt", 
        "tlsSvrCertPath"         :"/tmp/server.crt", 
        "tlsCaPath"              :"/tmp/ca.crt",  
        'clientCfg' : clientCfgDict } 
        

    def setup_class(cls):
        print(f"setup_class: {cls.updateCfgDict}")    
        tdLog.debug(f"start to excute {__file__}")
        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)
        
    def initEnv1(self):
        if platform.system().lower() == "linux":    
            os.system("sh {os.path.dirname(os.path.realpath(__file__))}/tlsFileGen.sh")

    def stop_and_restart(self):   
        self.initEnv1()
        self.basicTest()  

        tdSql.execute("alter all dnodes 'enableTLS 1'")


        tdSql.close()

        sc.dnodeStop(1)
        sc.dnodeStart(1)

        tdSql.connect(user="root")

        self.basicTest() 

    def basicTest(self):
        tdSql.query("select 1")
        tdSql.checkData(0,0,1)

        tdSql.query("show databases");
        #self.stop_and_restart() 

    def test_tls_demo(self):
        self.stop_and_restart()
        
