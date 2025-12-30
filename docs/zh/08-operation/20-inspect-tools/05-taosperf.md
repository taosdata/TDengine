---
sidebar_label: 基准性能测试工具
title: 基准性能测试工具
toc_max_heading_level: 4
---

## 背景

TDengine TSDB 部署环境的磁盘读写和网络传输能力直接影响数据库的查询和写入性能，为了更便捷的评估部署环境性能指标，基准性能测试工具可通过第三方工具 dd 或 iperf3 评估磁盘读写和网络传输性能

## 工具使用方法

### local 模式

工具支持通过 help 参数查看支持的语法

```help
usage: taosperf local [-h] [--config CONFIG] [--backend] [--test-item {all,disk,network}] [--result RESULT] [--log-level {debug,info}] [--ip IP]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -f CONFIG
                        Path to config file
  --backend, -b         Run process in backend
  --test-item {all,disk,network}, -ti {all,disk,network}
                        Test item
  --result RESULT, -r RESULT
                        Path to result file
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
  --ip IP, -i IP        Server IP
```

#### 参数详细说明

- `config`：工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。参数时配置文件默认路径为工具运行当前目录。
- `result`：结果文件的存储目录，不配置 result 参数时默认路径为工具运行当前目录。
- `backend`：后台运行安装工具，默认前台运行。
- `test-item`：检查项目。可选项目为 all（disk 和 network）、disk 和 network。默认值为 all。
- `log-level`：输出日志级别，目前支持 debug 和 info，模式为 info。
- `ip`：指定目标机器 IP 地址，当配置 ip 后仅测试本机与目标机器间的网络传输能力。

### ssh 模式

工具支持通过 help 参数查看支持的语法

```help
usage: taosperf ssh [-h] [--config CONFIG] [--backend] [--test-item {all,disk,network}] [--result RESULT] [--log-level {debug,info}]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -f CONFIG
                        Path to config file
  --backend, -b         Run process in backend
  --test-item {all,disk,network}, -ti {all,disk,network}
                        Test item
  --result RESULT, -r RESULT
                        Path to result file
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
```

#### 参数详细说明

- `config`：工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。参数时配置文件默认路径为工具运行当前目录。
- `result`：结果文件的存储目录，不配置 result 参数时默认路径为工具运行当前目录。
- `backend`：后台运行安装工具，默认前台运行。
- `test-item`：检查项目。可选项目为 all（disk 和 network）, disk 和 network。默认值为 all。
- `log-level`：输出日志级别，目前支持 debug 和 info，模式为 info。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

# 集群信息，包含每个节点的连接信息和每个节点上需要测试磁盘性能的存储路径
nodes:
  - ip: 192.168.1.101
    user: root
    password: yourpassword
    port: 22
    dirs:
      - dir: /data1
      - dir: /data2
  - ip: 192.168.1.102
    user: root
    password: yourpassword
    port: 22
    dirs:
      - dir: /data1
      - dir: /data2
  - ip: 192.168.1.103
    user: root
    password: yourpassword
    port: 22
    dirs:
      - dir: /data1
      - dir: /data2

# 测试磁盘和网络能力的第三方工具运行方式
cmd:
  - io:
      dd:
        write: dd if=/dev/zero of=[file] bs=400K count=50000 oflag=direct conv=fsync
        read: dd if=[file] of=/dev/null bs=1M count=20000 iflag=direct
      fio:
        write: fio --name=db_write_test --filename=[file] --size=5G --bsrange=480k-500k --rw=randwrite --ioengine=libaio --direct=1 --numjobs=4 --iodepth=32 --group_reporting
        read: fio --name=db_read_test --filename=[file] --size=5G --bs=1M --rw=randread --ioengine=libaio --direct=1 --numjobs=4 --iodepth=32 --group_reporting
  - network:
      ping: ping -c 10000 -i 0.1 -s 1024 [ip]
      iperf3: iperf3 -c [ip]  -P 4 -t 120
```

## 结果文件

工具运行后会生成测试报告 perf_report.md，其内容包含集群磁盘和网络配置基本信息和测试结果两部分

```markdown
# 性能检查报告

时间：2025-05-30 17:21:02  

## 磁盘配置

| FQDN   | 磁盘名称 | 磁盘型号          | 序列号            | 磁盘类型 | 磁盘空间 |
|--------|--------|------------------|------------------|--------|--------|
| u1-11  | sda    | INTEL_SSDSC2BB48 | 55cd2e414d66c8cd | SSD    | 447.1G |
| u1-11  | sdb    | INTEL_SSDSC2BB48 | 55cd2e414d72e38b | SSD    | 447.1G |
| u1-11  | sdc    | SAMSUNG_MZ7L31T9 | 5002538f3431f2c9 | SSD    | 1.8T   |
| u1-11  | sdd    | ST2000NX0273     | 5000c500f06b8e1f | HDD    | 1.8T   |

#### 网络配置

| FQDN   | 网卡名称   | 带宽       |
|--------|--------|----------|
| u1-11  | eno3   | 1000Mb/s |

## 磁盘性能

### 1. 写盘命令

#### 1.1 dd 命令

dd if=/dev/zero of=[file] bs=400K count=50000 oflag=direct conv=fsync  

#### 1.2 fio 命令

fio --name=db_write_test --filename=[file] --size=5G --bsrange=480k-500k --rw=randwrite --ioengine=libaio --direct=1 --numjobs=4 --iodepth=32 --group_reporting  

### 2. 读盘命令

#### 2.1 dd 命令

dd if=[file] of=/dev/null bs=1M count=20000 iflag=direct 

#### 2.2 fio 命令

fio --name=db_read_test --filename=[file] --size=5G --bs=1M --rw=randread --ioengine=libaio --direct=1 --numjobs=4 --iodepth=32 --group_reporting  

### 3. 测试结果

| FQDN   | 磁盘路径   | 测试工具   | 写入速度      | 读取速度     |
|--------|--------|--------|-----------|----------|
| u1-11  | /data1 | fio    | 493MiB/s  | 528MiB/s |
| u1-11  | /data2 | fio    | 87.0MiB/s | 106MiB/s |

```

## 应用示例

测试本机的磁盘读写性能和本机到其它节点的网络传输能力

```shell
./taosperf -m local
```

测试本机的磁盘读写性能和本机到指定节点 ( 192.168.1.1 ) 的网络传输能力

```shell
./taosperf -m local -i 192.168.1.1
```

测试集群所有节点的磁盘读写性能和相互间的网络传输能力

```shell
./taosperf -m ssh
```

指定配置文件并测试磁盘读写和网传输能力

```shell
./taosperf -m ssh -f /path_to_file/perf.yaml
```

测试集群所有节点间的网络传输能力

```shell
./taosperf -m ssh -ti network
```

测试集群所有节点的磁盘读写性能

```shell
./taosperf -m ssh -ti disk
```
