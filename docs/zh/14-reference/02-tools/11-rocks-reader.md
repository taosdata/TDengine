---
title: rocks-reader 参考手册
sidebar_label: rocks-reader
toc_max_heading_level: 4
---

`rocks-reader` 是 TDengine 内部调试工具，用于读取 TSDB 缓存目录 `cache.rdb`（RocksDB 格式）中的缓存条目，并按指定条件将结果输出到标准输出。

它主要用于排查/分析 TDengine vnode 侧 `last` / `last_row` 缓存的内容。

## 工具获取

通常在你构建 TDengine 源码时会编译得到 `rocks-reader` 可执行文件。具体可执行文件路径取决于你的构建方式，例如可能在：

- `debug/build/bin/rocks-reader`
- `release/build/bin/rocks-reader`

你也可以在构建目录下自行查找 `rocks-reader` 可执行文件。

## 运行方式

在命令行运行，并通过参数指定要读取的 `cache.rdb` 目录。例如：

```bash
./rocks-reader -t all -v all -p /var/lib/taos/vnode/vnode2/tsdb/cache.rdb
```

> 注意：`-p` 需要指向 RocksDB 的数据库目录（TDengine 的 `cache.rdb` 目录），通常是一个目录而不是普通文件。

## 命令行参数

```bash
Usage: rocks-reader [options]
Options:
  -t <type>   缓存类型过滤：last / last_row / all（默认：all）
  -v <flag>   value 状态过滤：none / null / value / all（默认：all）
  -p <path>   指定 RocksDB 目录（cache.rdb 所在目录，默认：./cache.rdb）
  -h,--help,-help  显示帮助信息
```

### `-t <type>`：缓存类型过滤

- `last`：只打印 `last` 缓存条目
- `last_row`：只打印 `last_row` 缓存条目
- `all`：打印两类缓存条目（默认）

### `-v <flag>`：值状态过滤

- `none`：只打印 value 状态为 `none` 的条目
- `null`：只打印 value 状态为 `null` 的条目
- `value`：只打印 value 状态为 `value`（真实值）的条目
- `all`：打印上述所有状态（默认）

### `-p <path>`：RocksDB 目录

- 指定 `cache.rdb` 的目录路径（默认 `./cache.rdb`）

## 输出格式

输出为多行文本，每条记录一行，形如：

```text
[LAST|LAST_ROW] uid: <uid>, cid: <cid>, value: <...>
```

其中：

- `[LAST]` / `[LAST_ROW]`：对应 `-t` 选择的缓存类型
- `uid`：缓存条目对应的行/对象标识
- `cid`：列标识

当 `-v` 选择了 `none/null` 时，`value` 字段分别为：

- `value: none`
- `value: null`

当 `-v value` 时，`value` 的展示策略与字段类型有关：

1. 字符类（如 `VARCHAR` / `NCHAR` / `JSON`）：按实际长度输出字符串（不会要求数据以 `\0` 结尾）
2. 其余非固定长度或大对象类：输出前 32 字节的十六进制预览，形式类似：

   ```text
   value(hex, len=<n>): <first-32-bytes-as-hex>...
   ```

3. 固定长度数值类型：直接输出数值（按 `int64_t` 展示）

## 使用示例

### 1. 只查看 `last` 缓存中的真实值

```bash
./rocks-reader -t last -v value -p /var/lib/taos/vnode/vnode2/tsdb/cache.rdb
```

### 2. 只查看 `last_row` 缓存中的 `null`

```bash
./rocks-reader -t last_row -v null -p /var/lib/taos/vnode/vnode2/tsdb/cache.rdb
```

### 3. 全量查看（`last/last_row` + `none/null/value`）

```bash
./rocks-reader -t all -v all -p /var/lib/taos/vnode/vnode2/tsdb/cache.rdb
```

## 常见问题

### `failed to open rocksdb`

常见原因包括：

- `-p` 指向的路径不是实际的 `cache.rdb` RocksDB 目录
- 该目录权限不足
- RocksDB 目录被其他进程占用/锁定（例如 `taosd` 正在运行并使用该目录）

