## 调用 fileToDatabaseRaw 从指定文件列表向 TQueue 写入数据的测试工具
其中 A 目录中是只要 TQueue 有数据就消费的程序，B 目录中是检查到 TQueue 中有数据，当记录满 100 个或者 10s 后仍不满 100 个时才消费的程序，tool 目录中是将数据写入 TQueue 的程序，测试数据和脚本，使用方法见下文描述。

### 使用方法

#### 写入测试数据
./write.sh 含测试数据的目录 要写入的TQ名称

#### 消费
./streamComputingA -t=TQ -e=结果表
./streamComputingB -t=TQ -e=结果表
