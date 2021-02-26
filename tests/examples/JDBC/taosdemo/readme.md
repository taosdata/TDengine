```
cd tests/examples/JDBC/taosdemo
mvn clean package -Dmaven.test.skip=true
# 先建表，再插入的
java -jar target/taosdemo-2.0-jar-with-dependencies.jar -host [hostname] -database [database] -doCreateTable true -superTableSQL "create table weather(ts timestamp, f1 int) tags(t1 nchar(4))" -numOfTables 1000 -numOfRowsPerTable 100000000 -numOfThreadsForInsert 10 -numOfTablesPerSQL 10 -numOfValuesPerSQL 100
# 不建表，直接插入的
java -jar target/taosdemo-2.0-jar-with-dependencies.jar -host [hostname] -database [database] -doCreateTable false -superTableSQL "create table weather(ts timestamp, f1 int) tags(t1 nchar(4))" -numOfTables 1000 -numOfRowsPerTable 100000000 -numOfThreadsForInsert 10 -numOfTablesPerSQL 10 -numOfValuesPerSQL 100
```

需求：
1. 可以读lowa的配置文件
2. 支持JDBC-JNI和JDBC-restful
3. 读取配置文件，持续执行查询