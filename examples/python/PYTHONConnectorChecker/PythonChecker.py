import taos
import time 
import sys
import getopt
class ConnectorChecker:
    def init(self):  
        self.host = "127.0.0.1"
        self.dbName = "test"
        self.tbName = "weather"
        self.user = "root"
        self.password = "taosdata"
        
       
    def sethdt(self,FQDN,dbname,tbname):
        if(FQDN):
            self.host=FQDN
        if(dbname):
            self.dbname=dbname
        if(tbname):
            self.tbName
    def printSql(self,sql,elapsed):
        print("[ "+"OK"+" ]"+" time cost: %s ms, execute statement ====> %s"
                   %(elapsed,sql))
    def executeQuery(self,sql):
        try:
            start=time.time()
            execute = self.cl.execute(sql)
            elapsed = (time.time()-start)*1000
            self.printSql(sql,elapsed)
            data = self.cl.fetchall()
            numOfRows = self.cl.rowcount
            numOfCols = len(self.cl.description)
            for irow in range(numOfRows):
                print("Row%d: ts=%s, temperature=%d, humidity=%f" %(irow, data[irow][0], data[irow][1],data[irow][2]))
        except Exception as e:
            print("Failure sql: %s,exception: %s" %sql,str(e))  
    def execute(self,sql):
        try:
            start=time.time()
            execute = self.cl.execute(sql)
            elapsed = (time.time()-start)*1000
            self.printSql(sql,elapsed)
           
        except Exception as e:
            print("Failure sql: %s,exception: %s" %
                sql,str(e))
    def close(self):
        print("connetion closed.")
        self.cl.close()
        self.conn.close()
    def createDatabase(self):
        sql="create database if not exists %s" % self.dbName
        self.execute(sql)
    def useDatabase(self):
        sql="use %s" % self.dbName
        self.execute(sql)
    def createTable(self):
        sql="create table if not exists %s.%s (ts timestamp, temperature float, humidity int)"%(self.dbName,self.tbName)
        self.execute(sql)
    def checkDropTable(self):
        sql="drop table if exists " + self.dbName + "." + self.tbName + ""
        self.execute(sql)
    def checkInsert(self):
        sql="insert into test.weather (ts, temperature, humidity) values(now, 20.5, 34)"
        self.execute(sql)
    def checkSelect(self):
        sql = "select * from test.weather"
        self.executeQuery(sql)
    def srun(self):
        try:
            self.conn = taos.connect(host=self.host,user=self.user,password=self.password)
            #self.conn = taos.connect(self.host,self.user,self.password)
        except Exception as e:
            print("connection failed: %s"%self.host)
            exit(1)
        print("[ OK ] Connection established.")
        self.cl = self.conn.cursor()

def main(argv):
    FQDN=''
    dbname=''
    tbname=''
    try:
      opts, args = getopt.getopt(argv,"h:d:t:",["FQDN=","ifile=","ofile="])
    except getopt.GetoptError:
        print ('PYTHONConnectorChecker.py -h <FQDN>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--FQDN"):
            FQDN=arg
        elif opt in ("-d", "--dbname"):
            dbname = arg
        elif opt in ("-t", "--tbname"):
            tbname = arg
   
    checker = ConnectorChecker()
    checker.init()
    checker.sethdt(FQDN,dbname,tbname)
    checker.srun()
    checker.createDatabase()
    checker.useDatabase()
    checker.checkDropTable()
    checker.createTable()
    checker.checkInsert()
    checker.checkSelect()
    checker.checkDropTable()
    checker.close()



if __name__ == "__main__":
    main(sys.argv[1:])
           

