# TDengine macOS 编译指南

## 环境要求

- macOS 系统
- XCode 命令行工具
- CMake
- Homebrew 依赖：argp-standalone, gflags, pkgconfig

## 安装依赖

```bash
brew install argp-standalone gflags pkgconfig
```

## 编译命令

```bash
mkdir debug && cd debug
cmake .. -DCMAKE_POLICY_VERSION_MINIMUM=3.5
cmake --build .
```

## 输出目录

- 二进制文件: `debug/build/bin/`
  - `taosd` - 服务器
  - `taos` / `taosql` - 客户端
  - `taosudf` - UDF 支持
  - `tsim` - 测试工具

- 库文件: `debug/build/lib/`
  - `libtaos.dylib` - 动态库
  - `libtaos_static.a` - 静态库

## 快速测试

```bash
# 启动服务器
./debug/build/bin/taosd -c /etc/taos/taos.cfg

# 连接服务器
./debug/build/bin/taos
```

**注意**: `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` 参数是必需的，用于解决 CMake 4.x 版本兼容性问题。
