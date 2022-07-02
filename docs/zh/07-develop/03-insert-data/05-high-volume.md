---
title: 高效写入
---

## 高效写入原理

本节介绍如何高效地向 TDengine 写入数据。高效写入数据要考虑几个因素：数据在不同表（或子表）之间的分布，即要写入数据的相邻性；单次写入的数据量；并发连接数。一般来说，每批次只向同一张表（或子表）写入数据比向多张表（或子表）写入数据要更高效；每批次写入的数据量越大越高效（但超过一定阈值其优势会消失）；同时写入数据的并发连接数越多写入越高效（但超过一定阈值反而会下降，取决于服务端处理能力）。

为了更高效地向 TDengine 写入数据，客户端程序要充分且恰当地利用以上几个因素。在单次写入中尽量只向同一张表（或子表）写入数据，每批次写入的数据量经过测试和调优设定为一个最适合当前系统处理能力的数值，并发写入的连接数同样经过测试和调优后设定为一个最适合当前系统处理能力的数值，以实现在当前系统中的最佳写入速度。同时，TDengine 还提供了独特的参数绑定写入，这也是一个有助于实现高效写入的方法。

## 高效写入方案

下面的示例程序展示了如何高效写入数据：
- TDengine 客户端程序从消息队列或者其它数据源不断读入数据，在示例程序中采用生成模拟数据的方式来模拟读取数据源
- 单个连接向 TDengine 写入的速度无法与读数据的速度相匹配，因此客户端程序启动多个线程，每个线程都建立了与 TDengine 的连接，每个线程都有一个独占的固定大小的消息队列
- 客户端程序将接收到的数据根据所属的表名（或子表名）HASH 到不同的线程，即写入该线程所对应的消息队列，以此确保属于某个表（或子表）的数据一定会被一个固定的线程处理
- 各个子线程在将所关联的消息队列中的数据读空后或者读取数据量达到一个预定的阈值后将该批数据写入 TDengine，并继续处理后面接收到的数据


## Java 示例程序

在 Java 示例程序中采用拼接 SQL 的写入方式。

### 主程序

主程序负责创建队列，并启动读线程和写线程。

```java title="主程序"
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java:main}}
```

### 读任务的实现

<details>
<summary>读任务的实现</summary>

```java 
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/ReadTask.java:ReadTask}}
```

</details>

### 写任务的实现

<details>
<summary>写任务的实现</summary>

```java 
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/WriteTask.java:WriteTask}}
```
</details>

### SQLWriter 类的实现

SQLWriter 类封装了拼 SQL 和写数据的逻辑。注意，所有的表都没有提前创建，而是写入出错的时候，再以超级表为模板建表，然后重现执行 INSERT 语句。

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/SQLWriter.java:SQLWriter}}
```
### 执行示例程序

可用在本地集成开发环境里直接运行示例程序，注意要提前配置环境变量 TDENGINE_JDBC_URL。若要在远程服务器上执行示例程序，可按照下面步骤操作：
1. 打包示例代码。在目录 TDengine/docs/examples/java 下执行：
   ```
   mvn package
   ```
2. 远程服务器上创建 examples 目录：
   ```
   mkdir -p examples/java
   ```
3. 复制依赖到服务器指定目录，比如：
   ```
   scp -r .\target\lib user@host:~/examples/java
   scp -r .\target\javaexample-1.0.jar user@host:~/examples/java
   ```
4. 配置环境变量，例如：
   ```
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```
5. 用 java 命令启动示例程序
   ```
   java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count>
   ```

## Python 示例程序

在 Python 示例程序中采用参数绑定的写入方式。(开发中)
<!-- ```python title="Python 示例程序"

``` -->
