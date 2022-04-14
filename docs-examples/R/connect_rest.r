library("DBI")
library("rJava")
library("RJDBC")
driver_path = "/home/debug/build/lib/taos-jdbcdriver-2.0.38-dist.jar"
driver = JDBC("com.taosdata.jdbc.TSDBDriver", driver_path)
conn = dbConnect(driver, "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata")
dbGetQuery(conn, "SELECT server_version()")
dbDisconnect(conn)