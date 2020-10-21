# Tessting Report

Notes:

***
Since trial version of HiveMQ only supports max 24 clients, this testing cannot simulate real IoT scenarios. It only shows you I/O performance between HiveMQ and TDengine service.
***

## The Evironment

Regarding testing resource, we selected two **DSv4** series Virtual Machines. Detail as below:

|Series|vCPU|Memory(GiB)|Temproary Storage(GiB)|Max Disk|IOPS/MBPs)|
|:-|:-|:-|:-|:-|:-|
|Standard_D4s_v3|4|16|32|8|8000/64 (100)|

This kind of Vritual Machine has a good balance of Azure quota and pricing. Azure has deployed a large number of this kind of resources.  All VMs will be running on Premium SSD for best disk performance.

Operating System is **Ubuntu 18.04**. 

The first one installed mqttloader, HiveMQ and hivemq-tdengine-extension. The second one installed TDengine service individually.

Below list shows you the software versions:

+ hivemq-tdengine-extension: v1.1.0
+ mqttloader: v0.72
+ HiveMQ: v4.4.1
+ TDengine: v.2.0.3

Since it's a dedicate on I/O performance testing, the environment ignored latency between MQTT client and HiveMQ. The mqttloader will raise connecting request on the local of HiveMQ service.

## Performance Testing

For the reviewer who concern about performance should care about subscriber informations.

These two indicators are the bigger the better:
Maximum throughput[msg/s]
Average throughput[msg/s]

These two indicators are the less the better:
Maximum latency[ms]
Average latency[ms]

### 1 publisher, 1 subscriber, each publisher publishes 1,000 messages

```shell
./mqttloader -b tcp://127.0.0.1:1883 -p 1 -s 1 -m 1000

-----Publisher-----
Maximum throughput[msg/s]: 1000
Average throughput[msg/s]: 1000.00
Number of published messages: 1000
Per second throughput[msg/s]: 1000

-----Subscriber-----
Maximum throughput[msg/s]: 346
Average throughput[msg/s]: 125.00
Number of received messages: 1000
Per second throughput[msg/s]: 346, 54, 85, 124, 150, 32, 132, 77
Maximum latency[ms]: 6964
Average latency[ms]: 3016.71
```
### 10 Publishers 10 Subscribers Each publisher publishes 100 messages

```shell
./mqttloader -b tcp://127.0.0.1:1883 -p 10 -s 10 -m 100

-----Publisher-----
Maximum throughput[msg/s]: 1000
Average throughput[msg/s]: 1000.00
Number of published messages: 1000
Per second throughput[msg/s]: 1000

-----Subscriber-----
Maximum throughput[msg/s]: 7050
Average throughput[msg/s]: 5000.00
Number of received messages: 10000
Per second throughput[msg/s]: 7050, 2950
Maximum latency[ms]: 1308
Average latency[ms]: 650.41
```

### 10 Publishers 10 Subscribers Each publisher publishes 1000 messages

```shell
./mqttloader -b tcp://127.0.0.1:1883 -p 10 -s 10 -m 1000

-----Publisher-----
Maximum throughput[msg/s]: 10000
Average throughput[msg/s]: 10000.00
Number of published messages: 10000
Per second throughput[msg/s]: 10000

-----Subscriber-----
Maximum throughput[msg/s]: 8881
Average throughput[msg/s]: 6666.67
Number of received messages: 100000
Per second throughput[msg/s]: 2469, 8881, 7173, 8207, 8423, 7577, 8458, 8473, 8339, 7669, 8260, 6991, 4870, 3790, 420
Maximum latency[ms]: 13495
Average latency[ms]: 6416.67
```
