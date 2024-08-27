```
cd tests/examples/JDBC/taosdemo
mvn clean package -Dmaven.test.skip=true
# 先建表，再插入的
java -jar target/taosdemo-2.0.1-jar-with-dependencies.jar -host <hostname> -database <db name> -doCreateTable true -superTableSQL "create table weather(ts timestamp, f1 int) tags(t1 nchar(4))" -numOfTables 1000 -numOfRowsPerTable 100000000 -numOfThreadsForInsert 10 -numOfTablesPerSQL 10 -numOfValuesPerSQL 100
# 不建表，直接插入的
java -jar target/taosdemo-2.0.1-jar-with-dependencies.jar -host <hostname> -database <db name> -doCreateTable false -superTableSQL "create table weather(ts timestamp, f1 int) tags(t1 nchar(4))" -numOfTables 1000 -numOfRowsPerTable 100000000 -numOfThreadsForInsert 10 -numOfTablesPerSQL 10 -numOfValuesPerSQL 100
```

如果发生错误 Exception in thread "main" java.lang.UnsatisfiedLinkError: no taos in java.library.path
请检查是否安装 TDengine 客户端安装包或编译 TDengine 安装。如果确定已经安装过还出现这个错误，可以在命令行 java 后加 -Djava.library.path=/usr/lib 来指定寻找共享库的路径。
