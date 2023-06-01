## 功能

release.py 脚本为 taosx 及 taosx-agent 打包服务，支持 Windows 及 Linux 打包

## 环境要求

- rust 开发环境（taosX 依赖）
- golang 1.20及以上（taos-opc、taos-mqtt 依赖）
- PI System 授权(AF SDK)（pi-connector 依赖）
- jdk1.8+、maven3(taos-influxdb 依赖)
- Inno setup 6.2 及以上

## 版本号说明
### taosx
taosx 版本号从 Cargo.toml 文件可以取到：
~~~
[package]
name = "taosx"
version = "0.5.1"
~~~
其他子模块版本号各自维护，一般在启动时候 -v 参数可查看，或者通过日志查看。

## 参数说明

支持参数：
- -h: 查看本帮助信息
- -c: cpu type [aarch32 | aarch64 | x64 | x86 | mips64 | loongarch64 ...]
- -b: build mode,可选 Debug\Release,默认 Release
- -l: 需要同时打包的连接器列表，可以多个空格隔开； 当前支持：opc pi mqtt influxdb.该参数不传表示包含支持的所有连接器（linux 下无 pi），注意 taosx 及 taosx-agent 不是连接器，一定在安装包里
- -s: submodel build mode, 各个模块单独配置 Debug/Release，该配置比-b参数优先，没有配置的模块使用-b配置
- -s: ```examples, -s pi debug``` 表示 pi 模块使用 debug 模式，无论-b参数如何配置（支持对 taosx, taosx-agent, pi, opc, mqtt, influxdb 分别配置）
- -s: ```examples, -s pi debug taosx release``` 表示 pi 模块使用 debug 模式，taosx 使用 release 模式，无论 -b 参数如何配置
- -t: 脚本快速测试，单独测试某一过程（仅支持 windows, 支持 taosx,agent,opc,pi,mqtt,package）
- -t pi: 示例，测试 pi 编译安装
- -t package: 已经安装好的服务打包测试( taosx taosx-agent 必须已经编译安装过)
- 连接器可带版本号编译，和连接器名空格隔开
- example: ```python release.py -c x64 -s pi debug```  所有连接器集合打包，除了 pi 使用 debug 模式，其他都是 release 模式
- example: ```python release.py -c x64 -b debug -l pi opc -s pi release taosx release```  pi 及 opc 连接器集合打包，除了 pi 和 taosx 使用 release 模式，其他模块都是 debug 模式

## 安装说明

- 输出路径：taosx\release
- 文件名：
    - windows:   taosx-{version}-windows-installer.exe
    - linux:     taosX-{version}-Linux-x64.tar.gz
- windows 使用安装程序进行安装，使用 uninstall_taosx.exe 进行卸载。taosx-srv.exe 和 taosx-agent.exe 可以以服务模式启动 taosx 和 taos-agent
- 命令窗口执行 ```sc start/stop taosx``` 管理 taosx 服务
- 命令窗口执行 ```sc start/stop taosx-agent``` 管理 taosx-agent 服务
- 使用 uninstall_taosx.exe 卸载 taosx
- windows 安装目录为```C:\Program Files\taosX```，目录结构如下：
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-agent.exe
├── plugins
│   ├── influxdb
│   │   └── taosx-inflxdb.jar
│   ├── mqtt
│   │   └── taosx-mqtt.exe
│   └── opc
│       └── taosx-opc.exe
│   ├── influxdb
│   │   └── taosx-inflxdb.exe
│   └── pi
│       └── taosx-pi.exe
│       └── taosx-pi-backfill.exe
│       └── ...
└── config
│   ├── agent.example.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~
- linux 下需要安装程序先解压，后安装使用，示例如下：
``` bash
# 解压文件
tar -zxf taosX-0.5.1-Linux-x64.tar.gz
cd taosX-0.5.1-Linux-x64
# 安装
sudo ./install.sh
# 验证
taosx -V 
# taosx 0.5.1-b9827b00-dirty (built linux-x86_64 2023-05-31 09:11:13 +08:00)
taosx-agent -V 
# taosx-agent 0.1.0-33c1e5e4 (built linux-x86_64 2023-05-26 14:24:13 +08:00)

# start taosX and taosx-agent system service
sudo systemctl start taosx
sudo systemctl start taosx-agent

# check status of tasx and taosx-agent serverice
sudo systemctl status taosx
sudo systemctl status taosx-agent

# stop taosx and taosx-agent
sudo systemctl stop taosx
sudo systemctl stop taosx-agent

# 卸载
sudo rmtaox
```
- linux 下文件路径说明
  1. taosX, Agent, Explorer: /usr/local/bin
  2. connectors: /usr/local/taosX/plugins
  3. logs for tasoX and Agent: /usr/local/taosX/logs
  4. rmtaosX.sh:  /user/local/taosx