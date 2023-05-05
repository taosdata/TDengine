## 功能

release.py 脚本为 taosx 打包服务，当前仅支持 Windows 打包

## 环境要求

- rust 开发环境（taosX 依赖）
- golang 1.20及以上（opc、mqtt 依赖）
- PI System 授权（pi 依赖）
- Inno setup 6.2及以上

## 参数说明

支持参数：

- -c: 需要同时打包的连接器列表，可以多个，空格隔开； 当前支持：opc pi mqtt
- -t: 脚本快速测试，单独测试某一过程
- -t pi: 测试 pi 编译安装
- -t opc: 测试 opc 编译安装
- -t mqtt: 测试 mqtt 编译安装
- -t taosx: 测试 taosx 编译安装
- -t package: 已经安装好的服务打包测试( taosx 必须已经编译安装过)

## 安装说明

- 输出路径：taosx\release
- 文件名：
    - taosX and OPC:   taosX-v{version}-opc-installer.exe
    - taosX and PI:   taosX-v{version}-pi-installer.exe
    - taosX and MQTT:   taosX-v{version}-mqtt-installer.exe
- 均安装在默认安装目录(C:\TDengine)下
    - taosX C:\TDengine\bin
    - pi C:\TDengine\xplugins\pi
    - opc C:\TDengine\xplugins\opc
    - mqtt C:\TDengine\xplugins\mqtt