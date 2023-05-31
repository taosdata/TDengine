## 功能

release.py 脚本为 taosx及taosx-agent 打包服务，支持 Windows及Linux 打包

## 环境要求

- rust 开发环境（taosX 依赖）
- golang 1.20及以上（taos-opc、taos-mqtt 依赖）
- PI System 授权(AF SDK)（pi-connector 依赖）
- jdk1.8+、maven3(taos-influxdb依赖)
- Inno setup 6.2及以上

## 版本号说明
### taosx
taosx版本号从Cargo.toml文件可以取到：
~~~
[package]
name = "taosx"
version = "0.5.1"
~~~
其他子模块版本号各自维护，一般在启动时候-v参数可查看，或者通过日志查看。

## 参数说明

支持参数：
- -h: 查看本帮助信息
- -c: cpu type [aarch32 | aarch64 | x64 | x86 | mips64 | loongarch64 ...]
- -b: build mode,可选Debug\Release,默认Release
- -l: 需要同时打包的连接器列表，可以多个，空格隔开； 当前支持：opc pi mqtt influxdb.改参数不传表示包含所有连接器
- -s: submodel build mode, 各个模块单独配置Debug/Release，该配置比-b参数有限，没有配置的模块使用-b配置
- -t: 脚本快速测试，单独测试某一过程（仅支持windows, 支持taosx,agent,opc,pi,mqtt,package）
- -t pi: 示例，测试 pi 编译安装
- -t package: 已经安装好的服务打包测试( taosx taosx-agent必须已经编译安装过)
- 连接器可带版本号编译，和连接器名空格隔开
- example: python release.py -c pi 

## 安装说明

- 输出路径：taosx\release
- 文件名：
    - windows:   taosx-{version}-windows-installer.exe
    - linux:     taosX-{version}-Linux-x64.tar.gz
- windows使用安装程序进行安装，使用uninstall_taosx.exe进行卸载。taosx-srv.exe和taosx-agent.exe可以以服务模式启动taosx和taos-agent
- windows安装目录为C:\Program Files\taosX，目录结构如下：
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-srv.exe
│   ├── taosx-srv.xml
│   ├── taosx-agent.exe
│   ├── taosx-agent-srv.exe
│   ├── taosx-agent-srv.xml
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
│       └── ***
└── config
│   ├── agent.example.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~
- linux下需要安装程序先解压，后安装使用，示例如下：
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
- linux下文件路径说明
  1. taosX, Agent, Explorer: /usr/local/bin
  2. connectors: /usr/local/taosX/plugins
  3. logs for tasoX and Agent: /usr/local/taosX/logs
  4. rmtaosX.sh:  /user/local/taosx