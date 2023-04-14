## 功能
release.py脚本为taosx打包服务，当前仅支持windows打包
## 参数说明
支持参数：
- -c: 需要同时打包的连接器列表，可以多个，空格隔开；
    当前支持：opc pi
- -t: 脚本快速测试，单独测试某一过程
    - -t pi: 测试pi编译安装
    - -t opc: 测试pi编译安装
    - -t taosx: 测试taosx编译安装
    - -t package: 已经安装好的服务打包测试(taosx必须已经编译安装过)