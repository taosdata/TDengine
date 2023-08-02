if (! "RJDBC" %in% installed.packages()[, "Package"]) {
  install.packages('RJDBC', repos='http://cran.us.r-project.org')
}

# ANCHOR: demo
library("DBI")
library("rJava")
library("RJDBC")

args<- commandArgs(trailingOnly = TRUE)
driver_path = args[1] # path to jdbc-driver for example: "/root/taos-jdbcdriver-3.2.4-dist.jar"
driver = JDBC("com.taosdata.jdbc.TSDBDriver", driver_path)
conn = dbConnect(driver, "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata")
dbGetQuery(conn, "SELECT server_version()")
dbSendUpdate(conn, "create database if not exists rtest")
dbSendUpdate(conn, "create table if not exists rtest.test (ts timestamp, current float, voltage int, devname varchar(20))")
dbSendUpdate(conn, "insert into rtest.test values (now, 1.2, 220, 'test')")
dbGetQuery(conn, "select * from rtest.test")
dbDisconnect(conn)
# ANCHOR_END: demo
