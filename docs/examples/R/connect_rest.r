if (! "RJDBC" %in% installed.packages()[, "Package"]) {
  install.packages('RJDBC', repos='http://cran.us.r-project.org')
}

# ANCHOR: demo
library("DBI")
library("rJava")
library("RJDBC")

args<- commandArgs(trailingOnly = TRUE)
driver_path = args[1] # path to jdbc-driver for example: "/root/taos-jdbcdriver-3.2.4-dist.jar"
driver = JDBC("com.taosdata.jdbc.rs.RestfulDriver", driver_path)
conn = dbConnect(driver, "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata")
dbGetQuery(conn, "SELECT server_version()")
dbDisconnect(conn)
# ANCHOR_END: demo
