## 功能

release.py 脚本为 taosx 及 taosx-agent 打包服务，支持 Windows 及 Linux 打包。
安装包也会包含 taos-explorer 及各类连接器

release.py 也可为 taos-agent 及连接器不依赖 taosx 独立打包，详细见参数控制 ( -o agent )

## 环境要求

- rust 开发环境（taosX 依赖）
- golang 1.20及以上（taos-opc、taos-mqtt 依赖）
- PI System 授权(AF SDK)（pi-connector 依赖）
- jdk1.8+、maven3(taos-influxdb 依赖)
- Rust 1.65+ node.js 16+, yarn( taos-explorer 依赖)
- Inno setup 6.2 及以上
- 在运行release.py之前需要通过命令 pip install toml 来安装toml模块

## 版本号说明

### taosx

taosx 版本号从 Cargo.toml 文件可以取到：

```toml
[workspace.package]
version = "1.0.0"
```

其他子模块版本号各自维护，一般在启动时候 -v 参数可查看，或者通过日志查看。

## 参数说明

支持参数：

- -h: 查看本帮助信息
- -c: cpu type [aarch32 | aarch64 | x64 | x86 | mips64 | loongarch64 ...]
- -o: package target [taosx | agent] taosx 安装包还是 taos-agent 安装包，默认 taosx
- -b: build mode,可选 Debug\Release,默认 Release
- -l: 需要同时打包的连接器列表，可以多个空格隔开； 当前支持：opc pi mqtt influxdb.该参数不传表示包含支持的所有连接器（linux 下无 pi），注意 taosx\taosx-agent\taos-explorer 不是连接器，一定在安装包里
- -s: submodel build mode, 各个模块单独配置 Debug/Release，该配置比-b参数优先，没有配置的模块使用-b配置
- -s: ```examples, -s pi debug``` 表示 pi 模块使用 debug 模式，无论-b参数如何配置（支持对 taosx, taosx-agent, pi, opc, mqtt, influxdb, taos-explorer 分别配置）
- -s: ```examples, -s pi debug taosx release``` 表示 pi 模块使用 debug 模式，taosx 使用 release 模式，无论 -b 参数如何配置
- -t: 脚本快速测试，单独测试某一过程（仅支持 windows, 支持 taosx,agent,opc,pi,mqtt,package, explorer）
- -t pi: 示例，测试 pi 编译安装
- -t package: 已经安装好的服务打包测试( taosx taosx-agent 必须已经编译安装过)
- 连接器可带版本号编译，和连接器名空格隔开
- example: ```python release.py -c x64 -s pi debug```  所有连接器集合打包，除了 pi 使用 debug 模式，其他都是 release 模式
- example: ```python release.py -c x64 -b debug -l pi opc -s pi release taosx release```  pi 及 opc 连接器集合打包，除了 pi 和 taosx 使用 release 模式，其他模块都是 debug 模式

## taosX 安装说明

- 输出路径：taosx\release
- 文件名：
    - windows:   taosx-{version}-windows-installer.exe
    - linux:     taosx-{version}-linux-x64.tar.gz
- windows 使用安装程序进行安装，使用 uninstall_taosx.exe 进行卸载。taosx\taosx-agent\taos-explorer 均已安装为服务
- 命令窗口执行 ```sc start/stop taosx``` 管理 taosx 服务
- 命令窗口执行 ```sc start/stop taosx-agent``` 管理 taosx-agent 服务
- 命令窗口执行 ```sc start/stop taos-explorer``` 管理 taos-explorer 服务
- 使用 uninstall_taosx.exe 卸载 taosx
- windows 安装目录为```C:\Program Files\taosX```，目录结构如下：

    ```text
    ├── bin
    │   ├── taosx.exe
    │   ├── taosx-agent.exe
    │   ├── taos-explorer.exe
    ├── plugins
    │   ├── influxdb
    │   │   └── taosx-inflxdb.jar
    │   ├── mqtt
    │   │   └── taosx-mqtt.exe
    │   └── opc
    │       └── taosx-opc.exe
    │   └── pi
    │       └── taosx-pi.exe
    │       └── taosx-pi-backfill.exe
    │       └── ...
    └── config
    │   ├── agent.toml
    │   ├── explorer.toml
    ├── uninstall_taosx.exe
    ├── uninstall_taosx.dat
    ```

- linux 下需要安装程序先解压，后安装使用，示例如下：

    ```bash
    # 解压文件
    tar -zxf taosx-1.0.0-linux-x64.tar.gz
    cd taosx-1.0.0-linux-x64
    # 安装
    sudo ./install.sh
    # 验证
    taosx -V
    # taosx 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:00 +08:00)
    taosx-agent -V
    # taosx-agent 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:01 +08:00)

    # start taosx and taosx-agent system service
    sudo systemctl start taosx
    sudo systemctl start taosx-agent
    sudo systemctl start taos-explorer

    # check status of tasx and taosx-agent serverice
    sudo systemctl status taosx
    sudo systemctl status taosx-agent
    sudo systemctl status taos-explorer

    # stop taosx and taosx-agent
    sudo systemctl stop taosx
    sudo systemctl stop taosx-agent
    sudo systemctl stop taos-explorer

    # 卸载
    cd /usr/local/taosx
    sudo ./uninstall.sh
    ```

- linux 下文件路径说明
  1. taosx, taosx-gent, taos-explorer: /usr/bin
  2. connectors: /usr/local/taosx/plugins
  3. uninstall.sh:  /usr/local/taosx
  4. config files: /etc/taos/


## taosx-agent 安装说明

- 输出路径：taosx\release
- 文件名：
    - windows:   taosx-agent-{version}-windows-installer.exe
    - linux:     taosx-agent-{version}-linux-x64.tar.gz
- windows 使用安装程序进行安装，使用 uninstall_taosx-agent 进行卸载。taosx-agent 安装为服务
- 命令窗口执行 `sc start/stop taosx-agent` 管理 taosx-agent 服务
- windows 安装目录为 `C:\Program Files\taosX`，目录结构如下：

  ```
  ├── bin
  │   ├── taosx-agent.exe
  │   ├── ...
  ├── plugins
  │   ├── influxdb
  │   │   └── taosx-inflxdb.jar
  │   ├── mqtt
  │   │   └── taosx-mqtt.exe
  │   └── opc
  │       └── taosx-opc.exe
  │   └── pi
  │       └── taosx-pi.exe
  │       └── taosx-pi-backfill.exe
  │       └── ...
  └── config
  │   ├── agent.toml
  ├── uninstall_taosx-agent.exe
  ├── uninstall_taosx-agent.dat
  ```

- linux 下需要安装程序先解压，后安装使用，示例如下：

  ``` bash
  # 解压文件
  tar -zxf taosx-agent-1.0.0-linux-x64.tar.gz
  cd taosx-agent-1.0.0-linux-x64
  # 安装
  sudo ./install.sh
  # 验证
  taosx-agent -V
  # taosx-agent 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:01 +08:00)

  # start taosx-agent system service
  sudo systemctl start taosx-agent

  # check status of taosx-agent serverice
  sudo systemctl status taosx-agent

  # stop taosx-agent
  sudo systemctl stop taosx-agent

  # 卸载
  cd /usr/local/taosx
  sudo ./uninstall.sh
  ```

- linux 下文件路径说明
  1. taosx-gent: /usr/bin
  2. connectors: /usr/local/taosx/plugins
  3. uninstall.sh:  /usr/local/taosx
  4. config files: /etc/taos/
