---
name: tsdb-test-ci-restore-failed-env
description: "恢复 TDengine CI 失败用例的调试环境。给定一个失败用例的 Jenkins URL，自动解析 PR 号和构建编号，SSH 到测试机器，创建 Docker 容器并恢复源码、构建产物和 corefile，完成环境准备后输出进入容器的命令及调试路径。关键词：restore, failed test, CI, corefile, docker, 失败用例, 恢复环境, PR 调试"
metadata:
  author: mmwang
  version: 1.0.0
  owner_team: engine
---

# tsdb-test-ci-restore-failed-env

给定一个 TDengine Jenkins 失败用例 URL，自动解析参数、SSH 登录测试机、创建 Docker 容器并配置环境，方便开发人员直接进入容器调试。

## When to Use

- 收到 CI 失败通知，需要在失败时的原始环境中复现和调试问题
- 需要分析 corefile 定位崩溃原因
- 需要在与 CI 完全一致的构建产物和源码上运行测试

## Input

用户需提供以下信息之一：

| 参数 | 说明 | 示例 |
|------|------|------|
| 失败用例 URL | Jenkins 失败日志完整 URL（首选） | `http://192.168.1.52:8081/PR-34969_8366_1_20260327-172106/cases/...txt` |
| PR 号 + 构建编号 | 手动指定 | PR: `34969`, Build: `8366` |

**默认值：**
- 目标机器：`192.168.1.52`（可从 URL 自动解析）
- 目标机器密码：`12345678`
- 容器名：`pr-<PR号>-<构建编号>`（例：`pr-34969-8366`）
- restore.sh 路径：`/var/lib/jenkins/workspace/restore.sh`

## Output

完成后输出：
1. 容器名称和进入命令（`docker exec -it <容器名> bash`）
2. 日志和 corefile 路径：`/home/log`
3. 构建产物路径：`/home/TDinternal/debug/build`
4. 源码路径：`/home/TDinternal`
5. 已配置的环境变量和符号链接摘要

## Procedure

### 步骤 1：解析 URL，提取参数

从用户提供的 URL 中提取以下参数：

URL 格式：`http://<HOST>:<PORT>/PR-<PR_NUM>_<BUILD_NUM>_<SEQ>_<DATE>/cases/...`

示例：
```
URL: http://192.168.1.52:8081/PR-34969_8366_1_20260327-172106/cases/18-StreamProcessing/20-UseCase/test_idmp_vehicle.py.1.6.18.txt
解析结果：
  HOST      = 192.168.1.52
  PR_NUM    = 34969
  BUILD_NUM = 8366
  CONTAINER = pr-34969-8366
```

提取规则：
- `PR_NUM`：`PR-` 之后、第一个 `_` 之前的数字
- `BUILD_NUM`：第一个 `_` 之后、第二个 `_` 之前的数字
- `CONTAINER`：`pr-${PR_NUM}-${BUILD_NUM}`

### 步骤 2：SSH 登录目标机器

```bash
# 使用 sshpass 或交互式密码认证登录
ssh root@192.168.1.52
# 密码：12345678
```

> 提示：如需非交互式执行，可使用：
> ```bash
> sshpass -p '12345678' ssh -o StrictHostKeyChecking=no root@192.168.1.52
> ```

### 步骤 3：运行 restore.sh 创建容器

```bash
cd /var/lib/jenkins/workspace/
./restore.sh -p <PR_NUM> -n <BUILD_NUM> -c <CONTAINER>
```

示例（基于上述解析结果）：
```bash
cd /var/lib/jenkins/workspace/
./restore.sh -p 34969 -n 8366 -c pr-34969-8366
```

脚本运行成功后会输出类似以下信息：
```
* run the following command to enter the container:
         docker exec -it pr-34969-8366 bash
* log and coredump files are located in /home/log
* build files are located in /home/TDinternal/debug/build
* source files are located in /home/TDinternal
```

### 步骤 4：进入容器并配置环境

```bash
docker exec -it <CONTAINER> bash
```

进入容器后执行以下命令完成环境配置：

```bash
# 配置 PATH 和库路径
export PATH=$PATH:/home/TDinternal/debug/build/bin
export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib

# 创建库文件符号链接
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null

# 创建头文件符号链接
ln -s /home/TDinternal/community/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s /home/TDinternal/community/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s /home/TDinternal/community/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s /home/TDinternal/community/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null
```

### 步骤 5：验证环境并输出摘要

在容器内执行以下验证：

```bash
# 验证 taosd 可执行
which taosd && taosd --version

# 确认 corefile（如有）
ls /home/log/

# 确认构建产物
ls /home/TDinternal/debug/build/bin/
```

向用户输出结构化摘要：

```
✅ 环境恢复完成
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
容器名称  : pr-<PR_NUM>-<BUILD_NUM>
进入命令  : docker exec -it pr-<PR_NUM>-<BUILD_NUM> bash
日志/Core : /home/log
构建产物  : /home/TDinternal/debug/build
源码      : /home/TDinternal
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Safety

- **禁止**删除或修改 `/var/lib/jenkins/workspace/` 下的任何文件
- **禁止**对容器内的源码或构建产物进行破坏性修改
- **禁止**在不确认容器名的情况下执行 `docker rm`
- **禁止**将密码等凭证写入任何文件或日志
- 若 `restore.sh` 报错，立即停止并将错误信息返回用户，不要重试

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-ci-restore-failed-env version=1.0.0 author=user`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

