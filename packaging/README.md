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
- -t: 脚本快速测试，单独测试某一过程（支持taosx,agent,opc,pi,mqtt,package）
- -t pi: 示例，测试 pi 编译安装
- -t package: 已经安装好的服务打包测试( taosx taosx-agent必须已经编译安装过)
- 连接器可带版本号编译，和连接器名空格隔开
- example: python release.py -c pi 

## 安装说明

- 输出路径：taosx\release
- 文件名：
    - taosx-agent and OPC:   taosx-{version}-windows-installer.exe
- 均安装在默认安装目录(C:\TDengine)下
    - taosx           C:\TDengine\bin
    - taosx-agent     C:\TDengine\bin
    - taosx-agent-srv C:\TDengine\bin
    - taosx-agent cfg C:\TDengine\cfg
    - pi C:\TDengine\xplugins\pi
    - opc C:\TDengine\xplugins\opc
    - mqtt C:\TDengine\xplugins\mqtt
    - influxdb C:\TDengine\xplugins\mqtt