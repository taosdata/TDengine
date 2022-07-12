---
title: 高效写入
---

## 高效写入原理

本节介绍如何高效地向 TDengine 写入数据。高效写入数据要考虑几个因素：数据在不同表（或子表）之间的分布，即要写入数据的相邻性；单次写入的数据量；并发连接数。一般来说，每批次只向同一张表（或子表）写入数据比向多张表（或子表）写入数据要更高效；每批次写入的数据量越大越高效（但超过一定阈值其优势会消失；同时写入数据的并发连接数越多写入越高效（但超过一定阈值反而会下降，取决于服务端处理能力）。

为了更高效地向 TDengine 写入数据，客户端程序要充分且恰当地利用以上几个因素。在单次写入中尽量只向同一张表（或子表）写入数据，每批次写入的数据量经过测试和调优设定为一个最适合当前系统处理能力的数值，并发写入的连接数同样经过测试和调优后设定为一个最适合当前系统处理能力的数值，以实现在当前系统中的最佳写入速度。同时，TDengine 还提供了独特的参数绑定写入，这也是一个有助于实现高效写入的方法。

## 高效写入方案

下面的示例程序展示了如何高效写入数据：

- TDengine 客户端程序从消息队列或者其它数据源不断读入数据，在示例程序中采用生成模拟数据的方式来模拟读取数据源
- 单个连接向 TDengine 写入的速度无法与读数据的速度相匹配，因此客户端程序启动多个线程，每个线程都建立了与 TDengine 的连接，每个线程都有一个独占的固定大小的消息队列
- 客户端程序将接收到的数据根据所属的表名（或子表名）HASH 到不同的线程，即写入该线程所对应的消息队列，以此确保属于某个表（或子表）的数据一定会被一个固定的线程处理
- 各个子线程在将所关联的消息队列中的数据读空后或者读取数据量达到一个预定的阈值后将该批数据写入 TDengine，并继续处理后面接收到的数据

![TDengine 高效写入线程模型](highvolume.webp)

## Java 示例程序

在 Java 示例程序中采用拼接 SQL 的写入方式。

### 主程序

主程序负责：

1. 创建消息队列
2. 启动写线程
3. 启动读线程
4. 每隔 10 秒统计一次写入速度

主程序默认暴露了 4 个参数，每次启动程序都可调节，用于测试和调优：

1. 读线程个数。默认为 1。
2. 写线程个数。默认为 3。
3. 模拟生成的总表数。默认为 1000。将会平分给各个读线程。
4. 每批最大数据量。默认为 3000。

<details>
<summary>主程序</summary>
```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java:main}}
```
</details>


队列容量(taskQueueCapacity)也是与性能有关的参数，可通过修改程序调节。一般来讲，队列容量越大，入队被阻塞的概率越小，队列的吞吐量越大，但是内存占用也会越大。

### 读任务的实现

读任务负责从数据源读数据。每个读任务都关联了一个模拟数据源。每个模拟数据源可生成一点数量表的数据。不同的模拟数据源生成不同表的数据。

读任务采用阻塞的方式写消息队列。也就是说，一旦队列满了，写操作就会阻塞。

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

SQLWriter 类封装了拼 SQL 和写数据的逻辑。注意，所有的表都没有提前创建，而是写入出错的时候，再以超级表为模板批量建表，然后重新执行 INSERT 语句。

<details>
<summary>SQLWriter 类的实现</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/SQLWriter.java:SQLWriter}}
```

</details>

### 执行示例程序

执行程序前需配置环境变量 `TDENGINE_JDBC_URL`。如果 TDengine Server 部署在本机，且用户名、密码和端口都是默认值，那么可配置：

```
TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
```

若要在本地集成开发环境执行示例程序，只需：

1. clone TDengine 仓库
   ```
   git clone git@github.com:taosdata/TDengine.git --depth 1
   ```
2. 用集成开发环境打开 `docs/examples/java` 目录。
3. 在开发环境中配置环境变量 `TDENGINE_JDBC_URL`。如果已配置了全局的环境变量 `TDENGINE_JDBC_URL` 可跳过这一步。
4. 运行类 `com.taos.example.highvolume.FastWriteExample`。

若要在服务器上执行示例程序，可按照下面的步骤操作：

1. 打包示例代码。在目录 TDengine/docs/examples/java 下执行：
   ```
   mvn package
   ```
2. 远程服务器上创建 examples 目录：
   ```
   mkdir -p examples/java
   ```
3. 复制依赖到服务器指定目录：
   - 复制依赖包，只用复制一次
      ```
      scp -r .\target\lib <user>@<host>:~/examples/java
      ```
      
   - 复制本程序的 jar 包，每次更新代码都需要复制
      ```
      scp -r .\target\javaexample-1.0.jar <user>@<host>:~/examples/java
      ```
4. 配置环境变量。
   编辑 `~/.bash_profile` 或 `~/.bashrc` 添加如下内容例如：
   ```
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```
   以上使用的是本地部署 TDengine Server 时默认的 JDBC URL。你需要根据自己的实际情况更改。

5. 用 java 命令启动示例程序，命令模板：
   ```
   java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count> <total_table_count> <max_batch_size>
   ```
6. 结束测试程序。测试程序不会自动结束，在获取到当前配置下稳定的写入速度后，按 <kbd>CTRL</kbd> + <kbd>C</kbd> 结束程序。
   下面是一次实际运行的截图： 

   ```
   [bding@vm95 java]$ java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample 1 9 1000 2000
   17:01:01.131 [main] INFO  c.t.e.highvolume.FastWriteExample - readTaskCount=1, writeTaskCount=9 tableCount=1000 maxBatchSize=2000
   17:01:01.286 [WriteThread-0] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.354 [WriteThread-1] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.360 [WriteThread-2] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.366 [WriteThread-3] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.433 [WriteThread-4] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.438 [WriteThread-5] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.443 [WriteThread-6] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.448 [WriteThread-7] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.454 [WriteThread-8] INFO  c.taos.example.highvolume.WriteTask - started
   17:01:01.454 [ReadThread-0] INFO  com.taos.example.highvolume.ReadTask - started
   17:01:11.615 [main] INFO  c.t.e.highvolume.FastWriteExample - count=18766442 speed=1876644
   17:01:21.775 [main] INFO  c.t.e.highvolume.FastWriteExample - count=38947464 speed=2018102
   17:01:32.428 [main] INFO  c.t.e.highvolume.FastWriteExample - count=58649571 speed=1970210
   17:01:42.577 [main] INFO  c.t.e.highvolume.FastWriteExample - count=79264890 speed=2061531
   17:01:53.265 [main] INFO  c.t.e.highvolume.FastWriteExample - count=99097476 speed=1983258
   17:02:04.209 [main] INFO  c.t.e.highvolume.FastWriteExample - count=119546779 speed=2044930
   17:02:14.935 [main] INFO  c.t.e.highvolume.FastWriteExample - count=141078914 speed=2153213
   17:02:25.617 [main] INFO  c.t.e.highvolume.FastWriteExample - count=162183457 speed=2110454
   17:02:36.718 [main] INFO  c.t.e.highvolume.FastWriteExample - count=182735614 speed=2055215
   17:02:46.988 [main] INFO  c.t.e.highvolume.FastWriteExample - count=202895614 speed=2016000
   ```

## Python 示例程序

在 Python 示例程序中采用多进程的架构。写任务中同样采用拼装 SQL 的方式写入。
### 主进程

<details>

<summary>main 函数</summary>

```python
{{#include docs/examples/python/highvolume_example.py:main}}
```

</details>

<details>
<summary>DataBaseMonitor</summary>

```python
{{#include docs/examples/python/highvolume_example.py:DataBaseMonitor}}
```

</details>

### 读进程

<details>

<summary>启动函数</summary>

```python
{{#include docs/examples/python/highvolume_example.py:read}}
```

</details>

<details>

<summary>MockDataSource</summary>

```python
{{#include docs/examples/python/highvolume_example.py:MockDataSource}}
```

</details>

### 写进程


<details>

<summary>启动函数</summary>

```python
{{#include docs/examples/python/highvolume_example.py:write}}
```

</details>

<details>

<summary>SQLWriter</summary>

```python
{{#include docs/examples/python/highvolume_example.py:SQLWriter}}
```

</details>

### 执行示例程序

```
git clone git@github.com:taosdata/TDengine.git
git checkout -t 2.6
cd docs/examples/python/
python3  highvolume_example.py
```