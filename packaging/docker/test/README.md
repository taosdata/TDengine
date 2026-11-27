# entrypoint 回归测试

`../bin/entrypoint.sh` 是社区镜像（`packaging/docker/Dockerfile`）的启动脚本，只负责
**进程监管**：拉起 taosd / taosadapter / taoskeeper / taos-explorer，并在它们异常时让容器退出。

这类逻辑静态检查一个都拦不住 —— 例如「容器假活」「子进程被直接 SIGKILL」都只有真跑容器才
能发现。所以这里有两层：

| 层 | 文件 | 需要 docker | 在哪跑 |
|---|---|---|---|
| 静态守卫 | `test_entrypoint_community.py` | 否 | CI（`python3 -m unittest -v test_entrypoint_community`） |
| 行为验证 | `run-local.sh` | 是 | 本地（改脚本后必跑） |

## 本地跑

```bash
cd packaging/docker
bash test/run-local.sh        # 加 -k 保留容器便于排查
```

预期输出 `11 passed / 0 failed`。需要本地 docker daemon，首次会拉 `debian:bookworm-slim`。

## 覆盖的场景

| 场景 | 期望 |
|---|---|
| 正常启动 | 容器 running，4 个子进程都在 |
| `kill -9 taosd` | 容器 **exit 137**（旧脚本会保持 running = 假活） |
| `kill -9 taosadapter` | 同样退出，验证「任一子进程」而非只看 taosd |
| taosd 启动 1 秒后退出 | 数秒内中止并打印具体进程，不空等探测超时 |
| `docker stop` | 子进程收到 SIGTERM 并完成 flush 后容器 exit 0 |
| `TAOS_DISABLE_ADAPTER=1` | 未启动的服务不被追踪，taosd 死亡仍能感知 |

## 实现方式

真实二进制用 `stubs*/` 下的脚本替身：

- `stubs/` 正常进程，PID 写到 `/tmp/<name>.pid` 供测试从宿主机 kill
- `stubs-fail/` taosd 1 秒后退出、metrics 探测故意放慢（暴露「启动期空等」问题）
- `stubs-slow/` taosd 收到 SIGTERM 后 flush 2 秒（区分「优雅退出」和「被 SIGKILL」）

镜像由 `Dockerfile.test` 构建，context 是 `packaging/docker/`，用 `--build-arg STUBS=`
切换替身目录。

## 注意

- 这些测试**不验证 taosd 本身**，只验证脚本的进程管理契约。
- `v3/tsdb/` 下另有一套更完整的 entrypoint 与测试，与本目录无关，别混。
- 基础镜像只装了 `curl gdb locales net-tools netcat-openbsd tini tzdata valgrind`；
  **没有 procps**，因此脚本里不能用 `pkill` / `pgrep` / `ps`，静态测试会拦住。
