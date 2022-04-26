这个example中，我们适配了java常见的连接池:
* HikariCP（默认）
* druid
* dbcp
* c3p0

### 说明
ConnectionPoolDemo的程序逻辑：
1. 创建到host的connection连接池
2. 创建名称为pool_test的database，创建表超级weather，创建tableSize个子表
3. 总共插入totalNumber条数据。

### 如何运行这个例子：

```shell script
mvn clean package assembly:single
java -jar target/connectionPools-1.0-SNAPSHOT-jar-with-dependencies.jar -host 127.0.0.1
```
使用mvn运行ConnectionPoolDemo的main方法，可以指定参数
```shell script
Usage: 
java -jar target/connectionPools-1.0-SNAPSHOT-jar-with-dependencies.jar
-host : hostname
-poolType <c3p0| dbcp| druid| hikari>
-poolSize <poolSize>
-tableSize <tableSize>
-batchSize : 每条Insert SQL中values的数量
-sleep : 每次插入任务提交后的
```

### 日志
使用log4j，将日志和错误分别输出到了debug.log和error.log中