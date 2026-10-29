# TDengine TSDB Docker 项目

简体中文 | [English](README.md)

本项目提供了 TDengine TSDB 应用程序的 Docker 化版本。它包含了使用 Docker 构建、运行和部署 TDengine TSDB 应用程序所需的所有文件。（v3.3.8.8 及以上使用该项目）

## 项目结构

```
source/taos-community/packaging/docker/v3/tsdb/
                              # TDengine TSDB 的 v3.3.8.8+ 的 Docker 构建文件
│── Dockerfile.base           # 基础镜像的 Dockerfile
│── Dockerfile                # 构建 TDengine TSDB  Docker 镜像的指令文件
│── entrypoint.sh             # TDengine TSDB 应用程序的初始化脚本
│── README.md                 # 英文项目文档
│── README-CN.md              # 中文项目文档
│── test-docker.sh            # Docker 测试脚本

```

## 前置条件

- Docker：确保您的机器上已安装并运行 Docker
- 确保下载 TDengine TSDB Enterprise 或者 TSDB OSS 版本的安装包到 source/taos-community/packaging/docker/v3/tsdb 目录下
  - [TDengine TSDB Enterprise 下载](https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise&version=3.3.8.8&platform=Docker&architecture=x64)
  - [TDengine TSDB OSS 下载](https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS&version=3.3.8.8&platform=Docker&architecture=x64)


支持 3.3.6.67 及以后的 3.3.6 企业版构建，安装包支持 `.tar.gz` 和 `.tar.xz`。

离线交付可以直接导出和加载镜像，无需镜像仓库：

```bash
docker save tdengine/tsdb-ee:<version> | gzip > tdengine-tsdb-docker.tar.gz
docker load -i tdengine-tsdb-docker.tar.gz
```

## 构建 Docker 镜像

要构建 TDengine TSDB Docker 镜像，请导航到项目目录并运行以下命令。可以通过`BUILD_ENTERPRISE`参数来选择构建 Enterprise 版本还是 Community 版本。基础镜像为两个版本共用，无需传入构建参数。
以下示例展示了如何构建 arm 平台下的 TDengine TSDB Enterprise 版本的镜像：

**注意**: 请将 `<version>` 替换为实际的版本号。

```bash
cd source/taos-community/packaging/docker/v3/tsdb
# 构建基础镜像
docker build -t tdengine/tsdb-base:1.2 \
             -f Dockerfile.base .

# 构建 TDengine TSDB Enterprise 镜像
docker build --rm -f Dockerfile \
             --network=host \
             -t tdengine/tsdb-ee:<version> . \
             --build-arg BUILD_ENTERPRISE=1 \
             --build-arg pkgFile=tdengine-tsdb-enterprise-<version>-linux-arm64.tar.xz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost

```

`BUILDKIT_SANDBOX_HOSTNAME=localhost` 为可选参数。Dockerfile 优先使用安装器的
`--image-build` 模式，避免写入构建主机地址；旧安装器则清理生成的 `fqdn`，并仅将
已知的本地服务连接字段恢复为 `localhost`，使数据库使用运行容器的主机名。

如果希望构建 TDengine TSDB Community 版本的镜像，请使用如下命令：

```bash

cd source/taos-community/packaging/docker/v3/tsdb
# 构建基础镜像
docker build -t tdengine/tsdb-base:1.2 \
             -f Dockerfile.base . 

# 构建 TDengine TSDB Community 镜像

docker build --rm -f Dockerfile \
             --network=host \
             -t tdengine/tsdb:<version> . \
             --build-arg pkgFile=tdengine-tsdb-oss-<version>-linux-arm64.tar.xz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost

```

### 使用 zstd 压缩减小拉取体积

Docker 默认使用 gzip 压缩镜像层。改用 zstd 压缩后，在镜像内容完全不变的前提下，
发布（拉取）体积默认可减少约 12%，使用 19 级压缩可减少约 23%：

| 压缩方式 | 拉取体积 |
| -------- | -------- |
| gzip（默认） | 444 MB |
| zstd，3 级 | 389 MB |
| zstd，19 级 | 339 MB |

该方式需要使用 `docker buildx`，并直接推送到镜像仓库：

```bash
docker buildx build --rm -f Dockerfile \
             --network=host \
             -t <registry>/tdengine/tsdb-ee:<version> . \
             --build-arg BUILD_ENTERPRISE=1 \
             --build-arg pkgFile=tdengine-tsdb-enterprise-<version>-linux-arm64.tar.gz \
             --build-arg BUILDKIT_SANDBOX_HOSTNAME=localhost \
             --output type=image,compression=zstd,compression-level=19,force-compression=true,push=true
```

**兼容性说明**：zstd 压缩的镜像层要求 Docker 23 及以上或 containerd 1.5 及以上，
更低版本的客户端无法拉取。如需兼容旧版本客户端，请继续使用默认的 gzip 构建方式。

## 镜像内容说明

为控制镜像体积，以下内容**未**包含在镜像中：

| 已移除内容 | 体积 | 移除原因 |
| ---------- | ---- | -------- |
| `openjdk-17-jre-headless` | 185 MB | 仅下方基于 JVM 的 taosX 插件需要 |
| taosX `influxdb`、`opentsdb`、`pspace` 插件 | 97 MB | 依赖 JRE 的 Java 插件 |
| `taos-jdbcdriver-*.jar` | 26 MB | 镜像内任何组件均未使用 |
| 软件包文档、man 手册、locale 源文件 | 22 MB | 运行时不需要 |

移除 JRE 的同时也会移除其依赖（`libnss3`、`libnspr4`、`libpcsclite1`、`libpng16`、
字体等），因此实际减少的体积大于上表之和：镜像未压缩体积从 2.02 GB 降至 1.46 GB，
压缩后体积从 634 MB 降至 444 MB（使用 zstd 19 级压缩可降至 339 MB）。

原生的 taosX `opc` 插件**仍然保留**，因为它不需要 JVM。

注意事项：

- 如果需要 InfluxDB、OpenTSDB、PI/pspace 的 taosX 数据接入能力，或需要使用 JDBC
  驱动，请使用保留了这些内容的镜像。
- 调试工具（`gdb`、`valgrind`、`vim-tiny`、`curl`、`netcat`、`net-tools`、
  `iputils-ping`）均有意保留。
- 基础镜像标签为 `1.2`。如果本地缓存了 `tsdb-base:1.1`，构建仍会成功，但产出的镜像
  会保留 JRE，因此更新后请重新构建基础镜像。

## 快速开始

快速运行容器示例：

```bash
docker run -d \
  -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6060:6060 \
  -p 6044-6049:6044-6049 \
  -p 6044-6045:6044-6045/udp \
  --name tdengine-tsdb \
  tdengine/tsdb-ee:<version>
```

## 健康检查与密码配置

镜像通过 `taos-check` 提供两种探测模式：

- `taos-check startup`：检查 TDengine 是否完成启动。
- `taos-check service`：检查 SQL 服务是否已经能够对外提供服务。

其中，`taos-check startup` 和 `taos-check service` 从 `3.4.1.0` 版本开始支持。

`taos-check service` 与容器入口脚本使用相同的密码来源：

- `TAOS_ROOT_PASSWORD_FILE`
- `TAOS_ROOT_PASSWORD`
- 默认密码 `taosdata`

关于密码与升级行为，这里只区分 `3.3.8.8` 前后：

- 对于 `3.3.8.8` 之前的旧版本，如果历史上已经修改过 root 密码，仍需按旧镜像的方式处理 `.docker-entrypoint-root-password-changed` 标记文件后再启动。
- 对于 `3.3.8.8` 及以上版本，可以通过 `TAOS_ROOT_PASSWORD` 或 `TAOS_ROOT_PASSWORD_FILE` 提供当前实际密码，容器也可以直接升级；但如果之前修改过 root 密码，在重启容器、重建 Pod 或升级镜像之前，仍需确保部署配置中的密码与数据库实际密码一致，否则依赖 SQL 认证的健康检查以及其他使用 root 账号鉴权的组件可能会失败。

## 其他部署方式
更多部署方式可以参考 [TDengine Docker 部署](https://docs.taosdata.com/operation/deployment/#docker-%E9%83%A8%E7%BD%B2)。

## 镜像修改验证流程

本镜像采用 All-in-One 语义：entrypoint 启动的任一常驻服务退出（包括退出码 0），容器以非零状态退出，由 Docker restart policy 恢复。被禁用或不存在的组件不参与监管。正常 SIGTERM/SIGINT 优雅停止，容器退出码为 0。

修改 entrypoint、Dockerfile 或安装脚本时，先在 Linux 运行不依赖 Docker 的回归测试（需要 Python 3.8+、Bash、procps）：

```bash
cd source/taos-community/packaging/docker/v3/tsdb
python3 -m unittest -v test_entrypoint test_image_build test_image_lifecycle_harness
```

MR 的 `docker-entrypoint-regression` job 自动执行上述测试。仅包含 Docker 镜像相关改动的 MR 可添加 `ci:docker-only` 标签，避免每次推送触发引擎全量 CI 和 SQL fuzz；后续加入其他代码改动前须移除此标签。专项验证也可在 Run pipeline 设置 `RUN_DOCKER_ENTRYPOINT_TESTS_ONLY=true`，只运行镜像回归及适用的基础检查，跳过 Linux/macOS 全量 CI 和 SQL fuzz。测试使用真实 entrypoint 函数和独立子进程，覆盖任一服务退出、启动阶段退出、优雅停止、停止超时、退出码保留、临时后台注册任务和全部服务禁用等情况。它们不能替代真实安装包的镜像验收。

按本文构建镜像后，在有 Docker 的隔离 Linux 主机执行真实镜像验收：

```bash
python3 test_image_lifecycle.py \
  --image tdengine/tsdb-ee-amd64:3.4.3.6.1026 \
  --edition enterprise \
  --report-dir /tmp/tsdb-image-validation-unique
```

社区版使用 `--edition oss` 并指定实际构建的社区版镜像。报告目录必须不存在，每次执行使用新目录。验收验证全部预期服务启动、SQL 写入及停止后重启的数据保留、逐个终止常驻服务后容器退出 1、`unless-stopped` 自动恢复及数据保留、禁用可选组件和全部禁用行为。测试不映射主机端口；只终止本次创建的带唯一标签容器中的进程。

每个用例结束后自动移除本次容器和其匿名卷，并保存日志、容器状态、镜像 ID 和结果 JSON。测试不删除输入镜像。归档报告后，由执行人按确切测试 tag 删除测试镜像，例如 `docker image rm tdengine/tsdb-ee-amd64:3.4.3.6.1026`；不要使用全局 prune。构建失败时也应核对本次构建产生的残留资源。MR 描述须记录安装包 URL/SHA256、镜像 tag/ID、测试结果和清理结果，未执行的版本或场景必须注明。

快速回归也验证验收工具本身的失败清理：创建后启动失败、命令超时、创建前失败、报告磁盘写入失败、多容器清理隔离和标签保护。真实镜像验收还校验 Dockerfile 的 tini 入口、持久卷声明、必需可执行文件和 taosd 动态库，以及镜像内 entrypoint/健康检查脚本与当前源码一致，避免误测旧镜像；并通过故意指定不存在的入口，验证真实启动失败后容器及匿名卷被删除。Dockerfile 构建本身仍须使用前文的真实安装包构建命令，不能由模拟测试替代。

原有 `test-docker.sh` 保留为密码初始化、单节点/三节点升级和数据兼容性扩展测试，不作为默认冒烟入口。它当前包含固定旧镜像版本、固定容器名/宿主机数据目录，且部分数据不匹配仅警告，不能直接据其成功提示判定验收通过。使用前须调整版本、隔离资源并完善失败断言。日常 MR 跑快速回归；发布镜像或修改 Dockerfile/entrypoint 时构建实际镜像并运行上述生命周期冒烟；涉及密码、初始化或升级逻辑时还须补充对应扩展场景。社区版、企业版及不同架构需分别验证，未执行的矩阵不能视为已通过。
