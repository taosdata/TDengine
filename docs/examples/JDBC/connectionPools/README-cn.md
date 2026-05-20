这个 example 中，我们适配了 java 常见的连接池：

* HikariCP（默认）
* druid
* dbcp
* c3p0

### 说明

ConnectionPoolDemo 的程序逻辑：

1. 创建到 host 的 connection 连接池
2. 创建名称为 pool_test 的 database，创建表超级 weather，创建 tableSize 个子表
3. 总共插入 totalNumber 条数据。

### 如何运行这个例子

```shell script
mvn clean package
java -jar target/ConnectionPoolDemo-jar-with-dependencies.jar -host 127.0.0.1
```

使用 mvn 运行 ConnectionPoolDemo 的 main 方法，可以指定参数

```shell script
Usage: 
java -jar target/ConnectionPoolDemo-jar-with-dependencies.jar
-host : hostname
-poolType <c3p0| dbcp| druid| hikari>
-poolSize <poolSize>
-tableSize <tableSize>
-batchSize : 每条Insert SQL中values的数量
-sleep : 每次插入任务提交后的
```

### 日志

使用 log4j，将日志和错误分别输出到了 debug.log 和 error.log 中
