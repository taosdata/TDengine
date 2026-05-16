# tsync

## 部署
获取源代码
```shell
git@github.com:taosdata/TDinternal.git
```
执行部署脚本
```shell script
cd TDinternal/enterprise/src/kit/tsync
bin/deploy.sh [host] [deployDir]
```

## 运行
### produce-to-tq + consume-to-tdengine
（1）启动consume-to-tdengine

编辑配置文件
```shell script
vim config/consume-to-tdengine.json

{
  "consumer": {
    "host": "192.168.1.139",
    ...
  },
  ...
  "destination": {
    "taosd": {
      "host": "192.168.1.140",
      ...
    },
    ...
  }
}
```
执行命令
```shell script
./consume-to-tdengine-start.sh
```
（2）启动produce-to-tq
编辑配置文件
```shell script
vim config/produce-to-tq.json

{
  "producer": {
    "host": "192.168.17.156",
    ...
  },
  ...
}
```
执行命令
```shell script
./produce-to-tq.sh
```

### produce-to-tq + consume-to-net + net-to-tq
（1）启动net-to-tq
（2）启动consume-to-tq
（3）启动produce-to-tdengine