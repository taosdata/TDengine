---
sidebar_label: 用安装包快速体验
title: 用安装包快速体验 TDengine TSDB
description: 使用安装包快速体验 TDengine TSDB
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgList from "/src/components/PkgList";

本页介绍如何使用安装包快速安装 TDengine TSDB Enterprise，并完成一次基础体验：启动服务、进入 shell、写入数据和查询数据。如果你希望为 TDengine 贡献代码，或对内部技术实现感兴趣，请参考 [TDengine GitHub 主页](https://github.com/taosdata/TDengine)。

## 前提条件

请先确认已经完成以下准备：

1. 已经选择要安装 TDengine 的操作系统环境，并确认机器满足最低要求。更多信息见 [支持的平台](../../12-operations-and-tooling/05-supported-platforms.md) 与 [系统要求](../../12-operations-and-tooling/02-operations/01-planning.md)。
2. Linux 环境需要具备 `sudo` 权限；Windows 环境需要使用管理员权限运行相关命令。
3. **（仅 Windows）** 安装程序会检查 Microsoft Visual C++ Redistributable 2015-2022 x64 14.44 或更高版本。若缺失或过旧，确认提示后可由安装程序安装自带的 14.50.35719.0 运行库。若安装失败，请从 [Microsoft Visual C++ Redistributable 最新受支持下载](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170) 获取最新包。

## 安装

<Tabs>
<TabItem label="Linux 系统" value="tarinst">

1. 从列表中下载 tar.gz 安装包：
    <PkgList productName="TDengine TSDB-Enterprise" platform="Linux-Generic" excludeSbom />
2. 进入到安装包所在目录，使用 `tar` 解压安装包，以 x64 架构为例：

    ```bash tsdb-ee
    tar -zxvf tdengine-tsdb-enterprise-{{VERSION}}-linux-x64.tar.gz
    ```

3. 进入解压后的目录，执行 `install.sh` 安装脚本。

    ```bash
    sudo ./install.sh
    ```

</TabItem>
<TabItem label="Windows 系统" value="windows">

1. 从列表中下载最新 Windows 安装程序：
    <PkgList productName="TDengine TSDB-Enterprise" platform="Windows" excludeSbom />
2. 运行安装程序，根据提示完成 TDengine 的安装。默认安装目录为 `C:\\TDengine`。你可以选择安装根目录；如果所选路径未以 `TDengine` 结尾，安装程序会自动在其下创建 `TDengine` 目录。例如，选择 `D:\\apps` 后，实际安装目录为 `D:\\apps\\TDengine`；直接选择 `D:\\apps\\TDengine` 时，不会重复创建目录。升级安装会继续使用已有安装目录，不能更改该目录。

</TabItem>
</Tabs>

更多类型和版本的安装包，请前往 [TDengine 产品下载中心](https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise) 下载。

## 启动服务

<Tabs>
<TabItem label="Linux 系统" value="linux">

完成安装后，在终端执行以下脚本，启动所有服务：

```bash
sudo start-all.sh
```

TDengine 的所有组件均使用 systemd 进行服务管理。可以使用以下命令查看服务状态：

```bash
sudo systemctl status taosd
sudo systemctl status taosadapter
sudo systemctl status taoskeeper
sudo systemctl status taos-explorer
```

如果服务状态中包含 `Active: active (running) since ...`，说明服务已经启动成功。

</TabItem>
<TabItem label="Windows 系统" value="windows">

安装程序会将实际安装目录添加到 `PATH` 环境变量。安装完成后，以管理员身份重新打开 cmd 窗口，进入实际安装目录，然后使用 `start-all.bat` 统一管理服务。无参数时默认执行启动，也支持 `status` 和 `stop` 子命令。

启动所有服务：

```cmd
start-all.bat
```

如果只需启动服务，并跳过连通性检查以及 Snode 和 Xnode 初始化，可以使用 `-S` 参数：

```cmd
start-all.bat -S
```

查看服务状态：

```cmd
start-all.bat status
```

停止所有服务：

```cmd
start-all.bat stop
```

如果需要分别查看各个 Windows Service 的原始状态，可以使用以下命令：

```cmd
sc query taosd
sc query taosadapter
sc query taosx
sc query taoskeeper
sc query taos-explorer
```

如果 `start-all.bat status` 中显示服务状态为 `running`，或者 `sc query` 输出中包含 `RUNNING`，则说明对应服务已经启动成功。

</TabItem>
</Tabs>

import Getstarted from './resource/_get_started.mdx'

<Getstarted />
