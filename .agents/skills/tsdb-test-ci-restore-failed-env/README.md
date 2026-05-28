# Restore Failed Env Skill

此 Skill 封装了 TDengine CI 失败用例的环境恢复流程。给定一个 Jenkins 失败日志 URL，自动解析 PR 号和构建编号，SSH 登录测试机，创建 Docker 容器并还原源码、构建产物和 corefile，最终输出可直接使用的调试环境。

## 功能

- 从 Jenkins URL 自动解析 PR 号、构建编号和目标机器地址
- SSH 登录目标机器并运行 `restore.sh` 创建 Docker 容器
- 还原源码（`/home/TDinternal`）、构建产物（`/home/TDinternal/debug/build`）和日志 / corefile（`/home/log`）
- 在容器内配置 `PATH`、`LD_LIBRARY_PATH` 及所有必要的动态库和头文件符号链接
- 输出结构化摘要，包含进入容器的命令和关键路径

## 验证过的工作流

1. 从 URL 解析 `PR_NUM`、`BUILD_NUM`、目标主机
2. SSH 登录目标机器（`ssh root@<HOST>`）
3. 进入 `/var/lib/jenkins/workspace/`，执行 `./restore.sh -p <PR_NUM> -n <BUILD_NUM> -c pr-<PR_NUM>-<BUILD_NUM>`
4. 执行 `docker exec -it <容器名> bash` 进入容器
5. 配置环境变量和符号链接
6. 验证环境并输出摘要

## URL 解析规则

Jenkins URL 格式：

```
http://<HOST>:<PORT>/PR-<PR_NUM>_<BUILD_NUM>_<SEQ>_<DATE>/cases/...
```

示例：

```
http://192.168.1.52:8081/PR-34969_8366_1_20260327-172106/cases/18-StreamProcessing/...txt
                              ↑       ↑
                          PR_NUM   BUILD_NUM
容器名 → pr-34969-8366
```

## 文件

- `SKILL.md`：Skill 主定义，包含适用场景、输入输出、完整操作步骤和安全边界

## 路径速查

| 路径 | 内容 |
|------|------|
| `/home/TDinternal` | 源码 |
| `/home/TDinternal/debug/build` | 构建产物（bin/lib） |
| `/home/log` | 日志文件和 corefile |

## 注意事项

- 目标机器默认为 `192.168.1.52`，可从 URL 自动解析
- `restore.sh` 脚本报错时立即停止，不重试，将错误信息返回用户
- 不要删除或修改 `/var/lib/jenkins/workspace/` 下的任何文件
- 如需进一步分析 corefile，可配合 `tsdb-test-gdb-single-thread-debug` Skill 使用
