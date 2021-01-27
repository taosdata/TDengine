这个example中，我们适配了java常见的连接池:
* c3p0
* dbcp
* druid
* HikariCP

### 说明
ConnectionPoolDemo的程序逻辑：
1. 创建到host的connection连接池
2. 创建名称为pool_test的database，创建表超级weather，创建tableSize个子表
3. 不断向所有子表进行插入。

### 如何运行这个例子：
```shell script
# mvn exec:java -Dexec.mainClass="com.taosdata.example.ConnectionPoolDemo" -Dexec.args="-host localhost"
```
使用mvn运行ConnectionPoolDemo的main方法，可以指定参数
```shell script
Usage: 
mvn exec:java -Dexec.mainClass="com.taosdata.example.ConnectionPoolDemo" -Dexec.args="<args>"
-host : hostname
-poolType <c3p0| dbcp| druid| hikari>
-poolSize <poolSize>
-tableSize <tableSize>
-batchSize : 每条Insert SQL中values的数量
-sleep : 每次插入任务提交后的
```

### 如何停止程序：
ConnectionPoolDemo不会自己停止，会一直执行插入，需要手动Ctrl+C运行。

### 日志
使用log4j，将日志和错误分别输出到了debug.log和error.log中