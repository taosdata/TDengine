# taosX 内存优化

## 1. 背景

taosX 的内存占用和持续增长问题在不同数据源下偶有出现，我们在解决所有内存泄漏以后，仍然在某些运行条件下出现这类问题。尤其在使用 Agent 进行数据同步时，不同数据源都一定程度上出现了积压，taosX 服务内存上涨并在任务结束后内存没有显著降低。相关 Jira 有：
- 
  TD-27728

- 
  TD-25705

- 
  TD-27896

- 
  TD-29000

- 
  TD-26849

- 
  TD-27883

这里列出的问题大都已经解决（TD-29000 为本次优化记录 Jira），此文档将目前内存优化的方案一一列出。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/03/19 | 0.1 | @霍琳贺 | Draft |

## 3. 定义

- GNU C Allocator:  GNU C库 （glibc） 中的 `malloc` 实现是从 ptmalloc（pthreads malloc）派生而来的，参考 https://www.gnu.org/software/libc/manual/html_node/The-GNU-Allocator.html 。
- Rust Global Allocator：在 Rust 中，使用 [#[global_allocator]](https%3A%2F%2Frust-lang.github.io%2Frfcs%2F1974-global-allocators.html) API 可以替换系统默认的内存分配器（glibc on Linux）。

## 4. 行为说明

内存优化对 taosx 的用户可见行为没有影响。在此仅对其优化方案进行说明。

### 4.1 限制内存队列长度

在 taosx 实现中，我们大量使用 flume 和 tokio channel 来进行多线程或异步任务之间的数据交换。将 `flume::unbounded()` Channel 替换为`flume::bounded(size)` 大小受控的 channel 可以降低内存分配的大小。在后续的开发中，始终使用长度受限的 channel 避免无限制的内存分配。

### 4.2 减少内存分配

在异步任务中，
- 使用 `Arc` 共享变量以减少 String 类型和 Arrow Records 处理时的内存分配。
- 使用 `bytes` 替换 `Vec<u8>` 以减少内存分配。

### 4.3 RPC 写入优化

对使用 Agent 的数据传输任务，统一数据接收和写入机制，减少冗余代码，提升写入性能，以降低数据交换的阻塞时间。

### 4.4 使用 [mimalloc](https://github.com/microsoft/mimalloc) 内存分配器

通过对比 Rust 中不同的内存分配器方案和最终性能对比。我们最终选择 mimalloc 作为默认内存分配器以替代 glibc。其优势在于：
- 在基准测试中 mimalloc 优于其他分配器（jemalloc，tcmalloc，Hoard等），并且通常使用更少的内存（*mimalloc* outperforms other leading allocators (*jemalloc*, *tcmalloc*, *Hoard*, etc), and often uses less memory. A nice property is that it does consistently well over a wide range of benchmarks. There is also good huge OS page support for larger server programs.）。
- 在多线程间进行数据交换的场景下效率更高（*mimalloc* is quite a bit faster than *tcmalloc* and *jemalloc* probably due to the object migration between different threads.）。
- 在不对称的工作负载（内存分配集中在个别线程中）下表现更稳定。（we see that the *mimalloc* technique of having non-contended sharded thread free lists pays off as it outperforms others by a very large margin. Only *rpmalloc*, *tbb*, and *glibc* also scale well on this benchmark.）
- 针对 Windows 系统的优化（It also includes a robust way to override the default allocator in [Windows](https://github.com/microsoft/mimalloc#override_on_windows).）。
使用 mimalloc 内存分配器，我们预期：
1. 在内存分配少的场景中内存占用与 glibc 相当，性能相当。
2. 在频繁的小内存分配（如字符串内存分配）场景中性能表现更好。
3. 产生更少的内存碎片，降低频繁内存分配下的内存占用。
4. Windows 下 Agent 性能有所提升。
5. 内存持续上涨、任务执行完毕后内存不降现象有所改善。

## 5. 性能

### 5.1 原生连接性能相当

```bash
❯ hyperfine -m 5 -L malloc glibc,mi './taosx-{malloc} run -f taos:///test -t "taos:///db{malloc}?assert"'
Benchmark 1: ./taosx-glibc run -f taos:///test -t "taos:///dbglibc?assert"
  Time (mean ± σ):     13.163 s ±  1.344 s    [User: 22.358 s, System: 6.978 s]
  Range (min … max):   10.759 s … 13.767 s    5 runs

Benchmark 2: ./taosx-mi run -f taos:///test -t "taos:///dbmi?assert"
  Time (mean ± σ):     13.179 s ±  1.354 s    [User: 21.921 s, System: 7.098 s]
  Range (min … max):   10.757 s … 13.816 s    5 runs
```

### 5.2 SQL 写入方式性能提高

使用 Sparse 模式模拟 SQL 写入方案，可以观察到可观的性能提升。得益于内存分配器对部分场景的优化，预期可以提升所有其他数据源（MQTT、Kafka、PI 等）接入的性能。
```bash
❯ hyperfine -m 1 -L malloc glibc,mi './taosx-{malloc} run -f "taos:///test?sparse&unit=1s" -t "taos:///db{malloc}?assert"'
Benchmark 1: ./taosx-glibc run -f "taos:///test?sparse&unit=1s" -t "taos:///dbglibc?assert"
  Time (abs ≡):        60.321 s               [User: 772.603 s, System: 63.934 s]
 
Benchmark 2: ./taosx-mi run -f "taos:///test?sparse&unit=1s" -t "taos:///dbmi?assert"
  Time (abs ≡):        46.801 s               [User: 616.884 s, System: 17.138 s]
 
Summary
  './taosx-mi run -f "taos:///test?sparse&unit=1s" -t "taos:///dbmi?assert"' ran
    1.29 times faster than './taosx-glibc run -f "taos:///test?sparse&unit=1s" -t "taos:///dbglibc?assert"'
```

### 5.3 Windows 系统下 taosx 性能提高

Windows 系统下的 taosx 运行速度可以得到一定提升。
```sql
PS > hyperfine -m 1 -L m std,mi '.\taosx-{m}.exe run -t \"taos+ws://192.168.0.201:6041/db{m}?assert\"  -f \"taos+ws://192.168.0.201:6041/test?sparse&unit=1m\" '
Benchmark 1: .\taosx-std.exe run -t "taos+ws://192.168.0.201:6041/dbstd?assert"  -f "taos+ws://192.168.0.201:6041/test?sparse&unit=1m"
  Time (abs ≡):        52.711 s               [User: 24.499 s, System: 1.701 s]

Benchmark 2: .\taosx-mi.exe run -t "taos+ws://192.168.0.201:6041/dbmi?assert"  -f "taos+ws://192.168.0.201:6041/test?sparse&unit=1m"
  Time (abs ≡):        41.806 s               [User: 7.343 s, System: 2.264 s]

Summary
  .\taosx-mi.exe run -t "taos+ws://192.168.0.201:6041/dbmi?assert"  -f "taos+ws://192.168.0.201:6041/test?sparse&unit=1m"  ran
    1.26 times faster than .\taosx-std.exe run -t "taos+ws://192.168.0.201:6041/dbstd?assert"  -f "taos+ws://192.168.0.201:6041/test?sparse&unit=1m"
```

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

无。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

此部分的变化对于打包脚本是透明的，无需更改。

## 13. 参考文档

1. Microsoft MiMalloc. https://github.com/microsoft/mimalloc
2. Rust RFC for Global Allocators. https://rust-lang.github.io/rfcs/1974-global-allocators.html
