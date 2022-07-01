
#install.packages('RJDBC', repos='http://cran.us.r-project.org')

library('DBI')
library('rJava')
library('RJDBC')
# get jdbc from input parameters
args<- commandArgs(trailingOnly = TRUE)
# arg[1] for jdbc path,arg[2] fro jdbc name 
# like "/src/connector/jdbc/target/taos-jdbcdriver-2.0.37-dist.jar"
jdbc<- paste(args[1],args[2],sep="/")


print("***************************taos-jdbcDriver support RJDBC sample *************************************")
#JNI
# Get JBDC-JNI connection
drv<-JDBC("com.taosdata.jdbc.TSDBDriver",jdbc, identifier.quote="\"")
conn<-dbConnect(drv,"jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata","root","taosdata")

#Restful
#drv<-JDBC("com.taosdata.jdbc.TSDBDriver",jdbc, identifier.quote="\"")
#conn<-dbConnect(drv,"jdbc:TAOS-RS://127.0.0.1:6041/test?user=root&password=taosdata","root","taosdata")

# Get connection information
dbGetInfo(conn)

# create database
dbSendUpdate(conn,"create database if not exists r_example_db keep 3650")

# use db
dbSendUpdate(conn,"use r_example_db")
dbSendUpdate(conn,"create table if not exists test(ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnry binary(50),nchr nchar(50)) tags(k int,v binary(20))")

# insert data
dbSendUpdate(conn,"insert into test_s01 using test tags(1,'sub_01') values(now,1,2,3,4,'binary1','nchar1')(now+1s,5,6,7,8,'binary2','nchar2')(now+2s,9,0,1,2,'binary3','nchar3')")

# View all tables
print("==============dbGetQuery==================")
table1<-dbGetQuery(conn,"select * from test")
print(table1)

# Functional support for RJDBC

# List all tables
print("=============dbListTables===================")
dbListTables(conn,"test%")

print("=============dbExistsTable===================")
# Is there table "test"
dbExistsTable(conn,"test")

# Connect summary information
print("=============summary===================")
summary(conn)
print("=============dbGetInfo===================")
dbGetInfo(conn)

 
# Read all the data from the test table
print("=============dbReadTable===================")
dbReadTable(conn,"test")


# Execute any non-query SQL statements
dbSendUpdate(conn, "create table if not exists t1(ts timestamp,id int,u nchar(40))")
dbGetTables(conn, "t")
sampleData=data.frame(ts=c('2022-03-22 00:00:00.000','2022-03-22 10:00:00.000'),id=c(1,2),u=c('涛思数据','TDengine'))
print("=============dbWriteTable===================")
dbWriteTable(conn, "t1", sampleData, overwrite=FALSE, append=TRUE)
dbReadTable(conn,"t1")

# Delete table t1
dbRemoveTable(conn,"t1")

# Extracting data on demand using SQL statements
print("=============dbGetQuery===================")
dbGetQuery(conn, "select * from test_s01")

# Drop database
dbSendUpdate(conn,"drop database if exists r_example_db")

# Close the connection
print("=============dbDisconnect===================")
dbDisconnect(conn)

print("***************************taos-jdbcDriver support RJDBC sample done*************************************")
