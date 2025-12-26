# TDengine 安装脚本使用指南

## 1. 脚本概述

TDengine 提供了一个功能强大的安装脚本 `install.sh`，用于在 Linux 和 macOS 系统上快速安装 TDengine 数据库。该脚本支持服务器和客户端两种安装模式，并提供了多种自定义安装选项。

## 2. 核心功能点

### 2.1 安装模式支持

- **服务器模式**：安装完整的 TDengine 数据库服务，包括服务器进程、客户端工具和相关组件
- **客户端模式**：仅安装 TDengine 客户端工具，用于连接远程 TDengine 服务器

### 2.2 安装路径自定义

- 支持默认路径安装（`/usr/local/taos` 或 `$HOME/taos`）
- 支持自定义安装路径
- 自动创建必要的目录结构

### 2.3 服务管理

- 自动检测系统服务管理器（systemd 或 SysVinit）
- 支持服务的自动注册和启动
- 支持服务的启用和禁用

### 2.4 环境适配

- 支持主流 Linux 发行版（Ubuntu、Debian、CentOS、Fedora 等）
- 支持 macOS 系统 (待调试，未发布)
- 自动检测系统架构和配置

### 2.5 安装配置

- 支持交互式和静默安装模式
- 支持 FQDN（完全限定域名）的交互式配置
- 自动创建数据目录、日志目录和配置目录

### 2.6 权限管理

- 支持 root 用户和普通用户安装
- 自动设置适当的文件和目录权限
- 非 root 用户模式下的特殊目录处理

### 2.7 组件安装

根据安装模式和版本类型，自动安装相应的组件：
- 客户端工具：taos、taosBenchmark、taosDump、taosDemo 等
- 服务器组件：taosd、taosAdapter、taosX、taos-explorer、taosKeeper 等
- 示例代码和文档
- 连接器和插件

## 3. 使用方法

### 3.1 基本用法

```bash
# 下载并解压 TDengine 安装包
# 进入安装包目录
./install.sh
```

### 3.2 命令行参数

| 参数 | 说明 | 可选值 | 默认值 |
|------|------|--------|--------|
| `-h` | 显示帮助信息 | - | - |
| `-v` | 指定安装类型 | `server` 或 `client` | `server` |
| `-e` | 交互式 FQDN 设置 | `yes` 或 `no` | `yes` |
| `-d` | 自定义安装目录 | 任意有效路径 | 默认路径 |
| `-s` | 静默安装模式 | - | 交互式安装 |
| `-q` | 静默安装并指定安装目录 | 任意有效路径 | - |

### 3.3 安装示例

#### 3.3.1 服务器模式安装

```bash
# 交互式安装（默认服务器模式）
./install.sh

# 静默安装服务器
./install.sh -s

# 自定义路径安装服务器
./install.sh -d /opt/tdengine

# 静默模式+自定义路径安装服务器
./install.sh -q /opt/tdengine

# 安装服务器并禁用 FQDN 交互
./install.sh -e no
```

#### 3.3.2 客户端模式安装(暂不支持)

```bash
# 交互式安装客户端
./install.sh -v client

# 静默安装客户端
./install.sh -v client -s

# 自定义路径安装客户端
./install.sh -v client -d /opt/tdengine-client

# 安装客户端并禁用 FQDN 交互
./install.sh -v client -e no
```

## 4. 安装流程

### 4.1 交互式安装流程

1. 选择安装类型（服务器或客户端）
2. 选择安装路径（默认或自定义）
3. 配置 FQDN（可选，仅服务器模式）
4. 确认安装信息
5. 开始安装
6. 完成安装并显示服务状态

### 4.2 静默安装流程

1. 根据命令行参数确定安装配置
2. 自动创建安装目录和必要文件
3. 安装相应组件
4. 配置服务（仅服务器模式）
5. 完成安装

## 5. 目录结构

### 5.1 指定路径模式

TDengine安装脚本支持用户自定义安装路径，通过命令行参数或交互式选择来指定安装位置。

#### 5.1.1 指定路径的方式

**1. 命令行参数指定**
- 使用 `-d` 参数在非静默模式下指定安装路径
- 使用 `-q` 参数在静默模式下指定安装路径

**2. 路径规范化处理**
- 安装脚本会自动对输入路径进行规范化处理，确保末尾不包含斜杠
- 实际安装目录会在指定路径后自动添加 `taos` 后缀（例如：指定路径为 `/opt/myapp`，实际安装目录为 `/opt/myapp/taos`）

#### 5.1.2 指定路径示例

**非静默模式指定路径**：
```bash
# Root用户指定安装路径
./install.sh -d /opt/taos-install

# 普通用户指定安装路径
./install.sh -d /home/user/taos-install
```

**静默模式指定路径**：
```bash
# Root用户静默安装并指定路径
./install.sh -q /opt/taos-install

# 普通用户静默安装并指定路径
./install.sh -q /home/user/taos-install
```

### 5.2 服务器模式安装目录

| 目录 | 用途 | 默认路径（root） | 默认路径（普通用户） | 指定路径模式 |
|------|------|------------------|----------------------|--------------|
| 安装目录 | 程序文件 | `/usr/local/taos` | `$HOME/taos` | `${指定路径}/taos` |
| 数据目录 | 数据库数据 | `/var/lib/taos` | `$HOME/taos/data` | `$HOME/taos/data`（普通用户） |
| 日志目录 | 日志文件 | `/var/log/taos` | `$HOME/taos/log` | `$HOME/taos/log`（普通用户） |
| 配置目录 | 配置文件 | `/etc/taos` | `$HOME/taos/cfg` | `$HOME/taos/cfg`（普通用户） |
| 命令链接 | 系统命令链接 | `/usr/bin` | `$HOME/.local/bin` | 保持不变 |
| 库文件链接 | 系统库文件链接 | `/usr/lib`/`/usr/lib64` | `$HOME/.local/lib` | 保持不变 |
| 头文件链接 | 系统头文件链接 | `/usr/include` | `$HOME/.local/include` | 保持不变 |

### 5.3 客户端模式安装目录

| 目录 | 用途 | 默认路径（root） | 默认路径（普通用户） | 指定路径模式 |
|------|------|------------------|----------------------|--------------|
| 安装目录 | 程序文件 | `/usr/local/taos` | `$HOME/taos` | `${指定路径}/taos` |
| 配置目录 | 配置文件 | `/etc/taos` | `$HOME/taos/cfg` | `$HOME/taos/cfg`（普通用户） |
| 命令链接 | 系统命令链接 | `/usr/bin` | `$HOME/.local/bin` | 保持不变 |
| 库文件链接 | 系统库文件链接 | `/usr/lib`/`/usr/lib64` | `$HOME/.local/lib` | 保持不变 |
| 头文件链接 | 系统头文件链接 | `/usr/include` | `$HOME/.local/include` | 保持不变 |

## 6. 注意事项

### 6.1 系统要求

- Linux 发行版：Ubuntu 16.04+、Debian 9+、CentOS 7+、Fedora 28+ 等
- macOS：10.14+（Mojave 及以上版本）
- 最低内存：2GB
- 最低磁盘空间：10GB

### 6.2 权限要求

- **root 用户安装**：可以安装完整的服务器和客户端，自动配置系统服务
- **普通用户安装**：可以安装客户端和用户服务级别的服务器组件

### 6.3 数据迁移

- 安装前请确保数据目录为空，避免覆盖现有数据
- 从旧版本升级时，请先备份数据

### 6.4 防火墙设置

- 服务器模式需要开放 6030-6042 端口
- 客户端模式需要能够访问服务器的相应端口

### 6.5 卸载方法

安装完成后，使用 `rmtaos` 命令卸载 TDengine 服务。卸载时会提示确认，确保卸载前备份数据。`rmtaos` 指向 `/usr/local/taos/bin/uninstall.sh`。
rmtaos 会自动切换服务和客户端模式的卸载脚本，也会自动检测默认路径和安装路径是否存在 TDengine 服务，若不存在则提示用户指定路径。

```bash
# 服务器模式卸载
rmtaos 

# 指定路径卸载
rmtaos -d /opt/taos-install/taos
```

## 7. 常见问题

### 7.1 安装失败怎么办？

- 检查系统是否满足最低要求
- 检查是否有足够的磁盘空间和内存
- 检查是否有适当的权限
- 查看安装日志获取详细错误信息

### 7.2 如何验证安装成功？

```bash
# 检查 TDengine 客户端版本
taos -V

# （仅服务器模式）检查服务状态
systemctl status taosd

# 连接 TDengine 服务器
taos
```

### 7.3 如何修改配置？

- 服务器配置文件：`/etc/taos/taos.cfg`（root 用户）或 `$HOME/taos/cfg/taos.cfg`（普通用户）
- 其他组件配置文件位于相应的配置目录

### 7.4 如何启动/停止服务？

```bash
# 使用 systemd 管理服务
systemctl start taosd  # 启动服务
systemctl stop taosd   # 停止服务
systemctl restart taosd # 重启服务
systemctl status taosd  # 查看服务状态
```

## 8. 高级配置

### 8.1 自定义服务配置

可以通过修改服务配置文件来自定义服务行为：

- systemd 服务文件：`/etc/systemd/system/taosd.service`（root 用户）或 `$HOME/.config/systemd/user/taosd.service`（普通用户）

---

## 9. 联系方式

如果您在安装或使用 TDengine 过程中遇到问题，请联系我们：

- 官方网站：https://www.taosdata.com
- 技术支持：support@taosdata.com
- 社区论坛：https://ask.taosdata.com/latest
- GitHub：https://github.com/taosdata/TDengine
