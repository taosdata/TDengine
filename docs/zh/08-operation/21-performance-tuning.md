# 性能优化

## 内存优化

TDengine TSDB 提供了内存分配器配置脚本 `set_taos_malloc.sh`，用于优化内存分配策略。通过该脚本，用户可以轻松切换不同的内存分配器模式，以提高 TDengine TSDB 的性能和稳定性。

### TDengine TSDB 内存分配器配置脚本

`set_taos_malloc.sh` 是 TDengine TSDB 提供的内存分配器配置脚本，用于一键切换 taosd/taosadapter 等服务的内存分配策略。

### 作用

- 支持 glibc 默认分配器、tcmalloc、jemalloc 等多种模式。
- 可自动生成 shell 环境变量文件和 systemd 环境变量文件。
- 支持静默模式（不输出提示信息）。

### 基本用法

#### 参数说明

- `-m <mode>`  
  指定内存分配器模式。  
  - 0：glibc 默认分配器  
  - 1：tcmalloc 优化  
  - 2：tcmalloc 定制检查，用于内存泄漏检测
  - 3：jemalloc 优化  
  - 4：jemalloc 定制检查，用于内存泄漏检测

#### 结果说明

- 脚本会自动生成/更新如下环境变量文件：
  - Shell 环境变量文件：`/usr/local/taos/bin/set_taos_malloc_env.sh`
  - systemd taosd 环境变量文件：`/etc/default/taosd`
  - systemd taosadapter 环境变量文件：`/etc/default/taosadapter`
- 日志文件：`/var/log/taos/set_taos_malloc.log`

`/usr/local/taos` 和 `/var/log/taos` 是 TDengine TSDB 默认的安装路径和日志路径。

### 注意事项

- 修改后，需要重启 taosd/taosadapter 服务或重新 source 环境变量文件让配置生效。
- 仅支持在 TDengine TSDB 安装目录`/usr/local/taos/bin/`下运行。

如需更多高级用法，请参考脚本内注释或联系运维支持。
