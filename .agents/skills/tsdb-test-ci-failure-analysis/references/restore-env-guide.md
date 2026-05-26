# 恢复失败环境完整指南

本文档提供恢复 TDengine CI 失败用例当时 Docker 环境的完整步骤。

## 前置条件

- 已安装 `sshpass`（用于无交互式 SSH）
- 目标机器已安装 Docker
- 目标机器上存在 `tdengine-ci:0.1` 镜像

## 完整步骤

### 1. 从 CI URL 提取参数

```
URL: http://192.168.1.49:8081/PR-34969_8441_6_20260331-095848/cases/02-Databases/08-Keep/test_mlevel_except.py.0.34.1.txt

SSH_HOST  = 192.168.1.49
PR_NUM    = 34969
BUILD_NUM = 8441
CONTAINER_NAME = pr-34969-8441  (命名格式: pr-<PR号>-<Build号>)
```

### 2. 上传 restore-docker.sh 到目标机器

```bash
# 方式一：使用 sshpass（需要知道密码）
sshpass -f ~/.tdengine-ci/credentials scp \
    /path/to/skills/tsdb-test-ci-failure-analysis/scripts/restore-docker.sh \
    root@192.168.1.49:/var/lib/jenkins/workspace/restore-docker.sh

# 方式二：直接 scp（会提示输入密码）
scp skills/tsdb-test-ci-failure-analysis/scripts/restore-docker.sh \
    root@192.168.1.49:/var/lib/jenkins/workspace/restore-docker.sh
```

### 3. 运行 restore-docker.sh

```bash
sshpass -f ~/.tdengine-ci/credentials ssh root@192.168.1.49 \
    "cd /var/lib/jenkins/workspace && \
     chmod +x restore-docker.sh && \
     ./restore-docker.sh -p 34969 -n 8441 -c pr-34969-8441"
```

脚本执行完成后会输出：
```
* run the following command to enter the container:
         docker exec -it pr-34969-8441 bash
* log and coredump files are located in /home/log
* build files are located in /home/TDinternal/debug/build
* source files are located in /home/TDinternal
```

### 4. 进入容器并配置环境

```bash
# 登录目标机器
ssh root@192.168.1.49

# 进入容器
docker exec -it pr-34969-8441 bash

# 在容器内：设置环境变量（持久化到 .bashrc）
cat >> ~/.bashrc << 'EOF'
export PATH=$PATH:/home/TDinternal/debug/build/bin
export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib
EOF
source ~/.bashrc

# 建立库文件符号链接
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null
ln -s /home/TDinternal/community/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s /home/TDinternal/community/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s /home/TDinternal/community/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s /home/TDinternal/community/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null
```

### 5. 目录结构说明

容器内各目录用途：

| 路径 | 说明 |
|------|------|
| `/home/log` | 失败用例的日志和 coredump 文件 |
| `/home/TDinternal/debug/build` | 编译产物（bin、lib 等） |
| `/home/TDinternal` | 源代码根目录 |
| `/home/TDinternal/community` | community 子模块源码 |

### 6. 分析 coredump（如有）

```bash
# 列出 coredump 文件
ls /home/log/core*

# 使用 gdb 分析（需要对应的可执行文件）
gdb /home/TDinternal/debug/build/bin/taosd /home/log/core.xxxxx

# 常用 gdb 命令
(gdb) bt          # 查看堆栈
(gdb) bt full     # 查看完整堆栈（含局部变量）
(gdb) info threads  # 查看所有线程
(gdb) thread apply all bt  # 所有线程堆栈
```

## restore-docker.sh 参数说明

```
./restore-docker.sh
         -p PR number      (必需) PR 号码，支持带 PR- 前缀或纯数字
         -n build number   (必需) 第几次构建（Build 号）
         -c container name (必需) 容器名称，建议格式: pr-<PR号>-<Build号>
         -h help           显示帮助
```

## 常见问题

### 容器已存在
如果提示 `container xxx exists`，说明同名容器已存在。可以：
- 使用不同的容器名 `-c pr-34969-8441-2`
- 或先删除旧容器：`docker rm -f pr-34969-8441`

### 找不到日志目录
如果脚本输出 `no log dir found`，可能是：
- 构建 ID 提取有误，检查 `-p` 和 `-n` 参数
- 日志已被清理

### PR 不在当前机器
如果脚本输出 `PR-xxx:yyy not found`，脚本会自动搜索其他 CI 机器，
并提示该 PR 可能在哪台机器上。
