# rdb-selftest 原理

三个程序合起来回答一个问题：**这台机器能不能可靠地写入和读回 rocksdb 数据？**

思路是绕开 taosd，用同样的方式反复写读，让间歇性故障暴露出来。

---

## rdbwrite：照抄生产的写入方式

关键在于"照抄"。任何一处不同，测的就不是同一条路径了。

对齐 `tsdbOpenRocksCache()`（`tsdbCache.c`）的四点：

| 项 | 为什么必须一致 |
|---|---|
| `SLastKey` + `myCmp` 比较器 | 比较器决定 key 顺序，顺序决定记录怎么打包进块，**块布局才是 checksum 覆盖的对象** |
| 关闭 WAL | 生产是 `disable_WAL(1)`，写入路径不同则测不到同一条 |
| 默认 block-based table | 默认即 `kXXH3`，和生产报错里的 `type = 4` 一致 |
| 每 4096 条 flush batch | 照 `rocksMayWrite()` 的 `ROCKS_BATCH_SIZE`，最后再 `rocksdb_flush()` 落 SST |

### 数据是确定性生成的

这是设计上最关键的一点：记录由 `(index, seed)` 算出来，不是随机的。

```
key   = f(index, seed)          // splitmix64 散列，铺开到 2 万个 uid × 64 列
value = [4B crc32c][8B index][payload...]
```

于是 **rdbread 不需要 rdbwrite 告诉它任何东西**，自己就能重算出每条记录应该是什么。
不用中间文件，不用传状态。

value 里那个 crc32c 覆盖 **payload + key 两部分**，所以"值本身完好但挂在错误的 key 上"
也能被发现。

---

## rdbread：三层检查，从粗到细

```
第 1 层  rocksdb 有没有报块校验错          ← 生产上那个错误
第 2 层  每个值是否逐字节等于写进去的       ← rocksdb 查不出来的那类
第 3 层  同一个 key 反复读是否返回相同字节   ← -r N 开启
```

**第 2 层是这套工具的价值所在。** rocksdb 的块校验只保证"块的字节没变"，
它不知道块里的内容语义上对不对。如果某个 value 被换成了另一个合法的 value，
块校验照样通过 —— rocksdb 原理上无法检测这种情况。

而我们知道每条记录**应该**是什么（确定性生成），所以能查。value 自带的 crc32c
就是干这个的：

```
stored_crc  = value 里那 4 字节
computed_crc = 对读回来的 payload + key 重算
不等 → 值被改了，但 rocksdb 没报错
```

第 3 层针对间歇性故障：SST 落盘后**不可变**，所以同一个 key 两次读出不同字节，
无法用"文件变了"解释，只能是读取路径的问题。

读选项设了 `verify_checksums = 1`，和 compaction 一样
（`compaction_job.cc:1083` 无条件设置）。

---

## run.sh：循环施压，失败后自动定性

单次跑通只证明"这一次没事"。间歇性故障大部分时候是安静的，所以要循环。

```
每轮：  建新库 → rdbwrite 写 → (可选清 page cache) → rdbread 读回校验
```

几个开关对应不同的排查目的：

| 开关 | 用途 |
|---|---|
| 默认每轮新建库 | 失败可归因到本轮，不受上一轮残留污染 |
| `--keep-db` | 写一次、之后只重读 → **隔离读路径和写路径** |
| `--drop-cache` | 每轮读前清页缓存 → 强制读真的落到设备 |
| `-j N` | 并发跑，增加内存和 I/O 压力 |
| `-r N` | 每轮多读几遍，查读取可重复性 |

### 失败后那一步才是重点

失败时 run.sh **保留数据库，并自动用 rdbsst 复查**。这一步决定诊断结论：

| rdbsst 复查 | 结论 |
|---|---|
| 仍然 MISMATCH | 盘上字节确实是错的 → 介质、控制器、RAID 写缓存及电池、文件系统 |
| OK / MATCH | 盘上字节是好的，那次失败的读是瞬时的 → 内存、CPU cache、HBA、线缆、控制器缓存 |

逻辑依据还是 SST 不可变：两次读之间没有东西能改文件。所以"当时读失败、现在读正常"
只能解释为读取过程出了问题，而不是文件坏了。

---

## rdbsst：只用 rocksdb 自己的接口

用来复查单个 SST，四步全是 rocksdb 的代码：

```
SstFileReader::Open()             文件能不能打开
SstFileReader::VerifyChecksum()   全部块校验
SstFileReader::NewIterator()      读 key（-i 可选）
ComputeBuiltinChecksum()          重算指定块（-o/-s）
```

第四步存在的理由：`VerifyChecksum()` **只在失败时才带数字**，成功时只回一个 OK。
所以指定 `-o/-s` 时会单独重算那个块，**成功失败都打印 stored 和 computed**，
这样才能和日志里的数字对照。

> `-i` 默认关闭，因为 `SstFileReader` 没有传比较器的接口，只能按字节序迭代，
> 而 cache.rdb 是 `myCmp` 排序的。比较器不匹配会让迭代提前停止，key 数偏少是
> 正常现象、不是损坏。`VerifyChecksum()` 不受影响 —— 它按文件偏移遍历所有块，
> 从不比较 key。

---

## 一句话总结

`rdbwrite`/`rdbread` 复现生产的写读链路并逐字节校验，`run.sh` 循环加压让间歇性
故障现形，`rdbsst` 在失败后判定"坏在盘上"还是"坏在读的路上" —— 后者是把问题
指向内存还是存储链路的分水岭。

跑干净不等于硬件健康，只是与运行时长和负载成正比的弱证据。
