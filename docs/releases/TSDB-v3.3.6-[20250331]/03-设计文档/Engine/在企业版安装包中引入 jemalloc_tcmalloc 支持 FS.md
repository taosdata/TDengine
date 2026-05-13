# 在企业版安装包中引入 jemalloc/tcmalloc 支持 FS

## 1. 背景

为满足不同场景下的性能优化和内存管理需求，企业版安装包需支持多种内存分配器（tcmalloc/jemalloc）的灵活切换，通过脚本化方式简化配置流程，提升系统稳定性与可调试性。
本特性旨在提供内存分配策略的多样化选择，默认配置针对企业级生产环境优化。
需求 jira：https://jira.taosdata.com:18080/browse/TS-6119

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/07/01 | 0.1 | 陈浩然 | 形成文档 |
| 2025/07/09 | 0.2 | 陈浩然 | 根据 review 意见修改 |

## 3. 定义

- **tcmalloc**：Google开发的内存分配器，优化多线程内存分配性能
- **jemalloc**：Facebook开发的内存分配器，以低碎片率著称
- **set_taos_malloc.sh**：内存分配策略切换脚本，位于/usr/local/taos/bin目录
- **内存检查模式**：启用内存泄漏检测和边界检查的调试模式

## 4. 内存分配模式配置

新增`set_taos_malloc.sh`脚本支持以下参数：
```bash

## 5. 语法格式

set_taos_malloc.sh -m <mode>

## 6. 参数说明

-m: 内存分配模式（必填），取值范围0-4
  0: 默认模式，使用默认的 glibc 管理内存（社区版默认）
  1: tcmalloc优化模式（企业版默认，待与Bo Xiao确认）
  2: tcmalloc内存检查模式
  3: jemalloc优化模式
  4: jemalloc内存检查模式
```

### 6.1 内存分配器的编译选项

1. tcmalloc 使用默认编译选项 `./configure --disable-static --enable-shared CXXFLAGS="-fPIC -g"`
2. jemalloc 使用默认编译选项 `./configure --enable-prof `

### 6.2 内存分配器模式

配置如下环境变量，启用不同的内存分配器：
- tcmalloc优化模式
` export LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so.4.5.18`
- tcmalloc 检测模式
```shell {wrap}
LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so
HEAPCHECK=strict
HEAPPROFILE=${install_dir}/taos/log/tcmalloc/prof
HEAP_PROFILE_ALLOCATION_INTERVAL=8147483648
HEAP_PROFILE_INUSE_INTERVAL=536870912
```

- jemalloc 优化模式
```shell {wrap}
export LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so.2
export MALLOC_CONF="percpu_arena:percpu,metadata_thp:auto,dirty_decay_ms:10000"
```

- jemalloc 检查模式
```shell {wrap}
export LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so.2
export MALLOC_CONF="percpu_arena:percpu,abort_conf:true,prof:true,prof_prefix:${install_dir}/taos/log/prof,prof_active:true,lg_prof_sample:20"
```

### 6.3 默认启动配置

- 企业版默认启用tcmalloc优化模式（模式1）
- 社区版默认使用系统默认分配器（模式0）
*注：最终默认值待关胜亮与Bo Xiao讨论后确定*

### 6.4 配置生效机制

1. Linux 操作系统下 taosd.service 会增加环境变量文件 `EnvironmentFile=/usr/local/taos/cfg/set_``taos_env.conf`
2. 执行脚本后，会更新系统的内存分配器的自定义环境配置文件 `set_taos_malloc_env.sh` 和 `set_taos_malloc_env.conf`，文件分别存放的路径为` {install_dir}/taos/bin` 和` /etc/default/taos/ `
3. 命令行前台启动 TDengine 组件时，需要手动 `source set_taos_malloc_env.sh`，这个会在执行完以后有命令行提示
4. 使用服务启动 TDengine 组件时，只需要重新启动服务即可，比如 ：`sudo systemctl restart taosd`
5. 配置变更记录会写入`${install_dir}/taos/log/``set_taos_malloc.log`
6. 第一次安装时，安装脚本会设置默认的内存分配模式，因为之前的测试有问题，暂时企业版和社区版全部设置为 模式 0，详见 jira：https://jira.taosdata.com:18080/browse/TD-34955。

### 6.5 自定义内存检查模式

如果希望自定义内存检查模式，则切换到 2 或者 4 的模式
- 服务模式：手动配置`set_taos_malloc_env.conf`，重启服务即可。
- 命令行前台启动模式：手动配置`set_taos_malloc_env.sh`，source 该脚本，

### 6.6 结果输出示例：

内存分配模式修改结果信息打印：
```bash
root@zk-ubuntu /usr/local/taos/bin $ ./set_taos_malloc.sh -m 3 
---------------------------------------------
Memory allocator setting complete!
  Mode: 3 (jemalloc optimization)
  Shell env file:    /usr/local//taos/bin/set_taos_malloc_env.sh
  Systemd env file:  /usr/local//taos/cfg/set_taos_malloc_env.conf

To use in shell:    source /usr/local//taos/bin/set_taos_malloc_env.sh
To use in systemd:  Just restart your service, EnvironmentFile is already configured.
```

## 7. 性能

- **tcmalloc优化模式**：多线程场景下内存分配有优化
- **jemalloc优化模式**：内存碎片率降低
- **检查模式**：性能损耗，仅建议在调试环境使用

## 8. 兼容性

无兼容性影响，内存分配器切换不改变TAOS数据存储格式和API接口

## 9. 运维

 目前只支持 linux 的 server 安装包上，client 安装包不支持这个配置

### 9.1 安装要求

- 安装包需内置以下动态库文件，区分 arm64 和 x64 版本： 
  - libtcmalloc.so.4 (tcmalloc版本2.16)
  - libjemalloc.so.2 (jemalloc版本5.3)

### 9.2 安装建立软链：

```shell
ln -s /usr/local/taos/driver/libtcmalloc.so.4.5.18  /usr/local/taos/driver/libtcmalloc.so
ln -s /usr/local/taos/driver/llibjemalloc.so.2  /usr/local/taos/driver/libtcmalloc.so
```

### 9.3 安装包位置

目前存在安装路径 /usr/local/taos/driver/ 路径

### 9.4 状态检查

```bash

## 10. 验证动态库加载状态

ldd /usr/local/taos/bin/taosd | grep -E "tcmalloc|jemalloc"
或者启动后检查：
grep -E 'jemalloc|tcmalloc' /proc/$(pidof taosd)/maps  
```

## 11. 使用场景

| 模式 | 适用场景 | 推荐环境 |
| --- | --- | --- |
| 0 | 兼容性测试 | 开发/测试环境 |
| 1 | 生产环境部署 | 企业版生产集群 |
| 2 | 内存泄漏排查 | 开发/测试环境 |
| 3 | 高内存碎片场景 | 数据归档节点 |
| 4 | 内存越界调试 | 开发/测试环境 |

## 12. 约束和限制

- 约束：仅支持64位Linux系统（以下是 libso 的编译环境）
  - arm 系统支持 gcc 2.27 版本及以上，内核版本 4.15.0
  - x64 系统支持 gcc 2.17 版本及以上，内核版本 3.10.0
- 限制：检查模式会显著降低性能，禁止在生产环境使用

## 13. 常见错误和排查

| 错误现象 | 可能原因 | 解决方案 |
| --- | --- | --- |
| 模式切换无效 | 权限不足 | 使用root用户执行脚本 |

## 14. 可观测性

- set_taos_malloc.sh 的日志文件会记录当前内存分配器类型

## 15. 安装和卸载

### 15.1 安装流程

1. 安装脚本自动将动态库文件部署至 ${install_dir}taos/driver
2. set_taos_malloc.sh 脚本软链接到 /usr/bin/ 下
3. 依据版本类型（企业/社区）设置默认模式

### 15.2 卸载处理

卸载时自动清理动态库文件和配置变更
- 不保留 data 和 config 时，会删除本功能引入的所有文件。
- 如果保留 data 和 config，会保留 log 目录和 cfg 目录中本功能引入的日志文件和配置文件。

## 16. 文档

添加到官网的性能优化章节

## 17. 参考文档

1. tcmalloc官方文档: [https://google.github.io/tcmalloc/](https://google.github.io/tcmalloc/)
2. jemalloc官方文档: [https://jemalloc.net/](https://jemalloc.net/) 
3. [本地编译 tcmalloc 和 jemalloc 并验证](https://taosdata.feishu.cn/wiki/Io7ow2d8viWgbfkfxREcXFjInih)
4. [使用 google perftools 快速定位内存增长](https://taosdata.feishu.cn/docx/ME7MdrVzSoFMgfxU5FLcnwYpnvf)

## 18. 附录
