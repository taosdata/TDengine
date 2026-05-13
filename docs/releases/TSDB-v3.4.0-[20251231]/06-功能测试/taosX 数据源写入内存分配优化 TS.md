# taosX 数据源写入内存分配优化 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-25 | 2025-11-25 | 1.0 | @霍琳贺 | 性能测试 |

## 2. 测试目标

1. 验证对已有功能的正确性无影响
2. 验证优化后的内存使用和性能提升程度

## 3. 参考文档

TD-38728

## 4. 测试结论

1. 功能无影响，CI 全部通过
  ![](./images/img_F6cUbLDdwod9ymxwHEEcDZyanie.png)

1. 内存分配大大减少，极端情况下写入性能提升 15%，内存降低 50%；内存占用在不同模式下均有所降低。

  | 测试项 | 测试时机 | 指标 | 优化前 | 优化后 | 变化 |
| --- | --- | --- | --- | --- | --- |
| 第一次优化 | 执行时间 | 72s | 61s | -15% |
|  | 内存占用 | 248.7 MiB | 130.0 MiB | -47% |
|  | 运行中内存分配 | 162.9 GiB | 12.9 GiB | -92% |
| 第一次优化 | 执行时间 | 6.17s | 6.06s | -1.7% |
|  | 内存占用 | 292.0 MiB | 249.0 MiB | -18% |
|  | 运行中内存分配 | 25.0 GiB | 22.9 GiB | -8% |

  
## 5. 功能测试

1. CI 集成测试用例全部通过

## 6. 性能测试

### 6.1 测试要点

1. 使用优化前和优化后的用例分别进行 CSV 数据写入
2. 分别使用 batch_size=1/1000 进行测试，并解释性能差异
   - batch_size=1 的极端情况内存分配频率高，优化结果会比较明显；
   - batch_size=1000 内存分配频率也应降低，性能提升；
3. 使用 MIMALLOC_SHOW_STATS=1 输出内存信息。

### 6.2 测试用例

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| 性能测试 | 1. 100 万行 meters 数据 CSV 格式入库，batch_size=1 | PASS |
|  | 2. 1000 万行 meters 数据 CSV 格式入库，batch_size=1000 |  |

### 6.3 测试结果

| 测试项 | 测试时机 | 指标 | 优化前 | 优化后 | 变化 |
| --- | --- | --- | --- | --- | --- |
| 第一次优化 | 执行时间 | 72s | 61s | -15% |
|  | 内存占用 | 248.7 MiB | 130.0 MiB | -47% |
|  | 运行中内存分配 | 162.9 GiB | 12.9 GiB | -92% |
| 第一次优化 | 执行时间 | 6.17s | 6.06s | -01.7% |
|  | 内存占用 | 292.0 MiB | 249.0 MiB | -18% |
|  | 运行中内存分配 | 25.0 GiB | 22.9 GiB | -8% |

#### 6.3.1 batch_size=1 {folded="true"}

优化前：
```plaintext {wrap}
time MIMALLOC_SHOW_STATS=1 ../taosx-benches/taosx-old run -f csv:./meters.csv -t "taos:///csv3" --parser @meters.json
heap stats:     peak       total     current       block      total#   
  reserved:     3.0 GiB     3.0 GiB     3.0 GiB                          
 committed:     3.0 GiB     3.0 GiB  -159.9 GiB                          
     reset:     0      
    purged:   162.9 GiB
   touched:     3.9 MiB     3.9 MiB   -43.0 GiB                          
  segments:   119         123          56                                not all freed
-abandoned:    59          60           0                                ok
   -cached:     0           0           0                                ok
     pages:     0           0        -697.1 Ki                           not all freed
-abandoned:   379         380           0                                ok
 -extended:     0      
   -retire:     0      
    arenas:     3      
 -rollback:     0      
     mmaps:    87      
   commits:     0      
    resets:     0      
    purges:    16.6 Ki 
   guarded:     0      
   threads:    83          85          22                                not all freed
  searches:     0.0 avg
numa nodes:     1
   elapsed:    72.762 s
   process: user: 865.909 s, system: 224.019 s, faults: 628, rss: 248.7 MiB, commit: 3.0 GiB
MIMALLOC_SHOW_STATS=1 ../taosx-benches/taosx-old run -f csv:./meters.csv -t    865.91s user 224.02s system 1497% cpu 1:12.79 total
```

优化后：
```plaintext {wrap}
heap stats:     peak       total     current       block      total#   
  reserved:     3.0 GiB     3.0 GiB     3.0 GiB                          
 committed:     3.0 GiB     3.0 GiB    -9.9 GiB                          
     reset:     0      
    purged:    12.9 GiB
   touched:     3.3 MiB     3.3 MiB  -474.0 MiB                          
  segments:   103         106          49                                not all freed
-abandoned:    51          53           0                                ok
   -cached:     0           0           0                                ok
     pages:     0           0         -10.2 Ki                           not all freed
-abandoned:   316         330           0                                ok
 -extended:     0      
   -retire:     0      
    arenas:     3      
 -rollback:     0      
     mmaps:    77      
   commits:     0      
    resets:     0      
    purges:     2.7 Ki 
   guarded:     0      
   threads:    74          76          22                                not all freed
  searches:     0.0 avg
numa nodes:     1
   elapsed:    61.614 s
   process: user: 546.741 s, system: 254.170 s, faults: 148, rss: 130.0 MiB, commit: 3.0 GiB
```

#### 6.3.2 batch_size=1000

优化前：
```plaintext {wrap}
heap stats:     peak       total     current       block      total#   
  reserved:     3.0 GiB     3.0 GiB     3.0 GiB                          
 committed:     3.0 GiB     3.0 GiB   -22.0 GiB                          
     reset:     0      
    purged:    25.0 GiB
   touched:     3.5 MiB     3.5 MiB    -1.5 GiB                          
  segments:   108         110          52                                not all freed
-abandoned:    53          53           0                                ok
   -cached:     0           0           0                                ok
     pages:     0           0         -24.5 Ki                           not all freed
-abandoned:   387         387           0                                ok
 -extended:     0      
   -retire:     0      
    arenas:     3      
 -rollback:     0      
     mmaps:    81      
   commits:     0      
    resets:     0      
    purges:     2.5 Ki 
   guarded:     0      
   threads:    78          78          22                                not all freed
  searches:     0.0 avg
numa nodes:     1
   elapsed:     6.153 s
   process: user: 27.087 s, system: 2.107 s, faults: 630, rss: 292.3 MiB, commit: 3.0 GiB
MIMALLOC_SHOW_STATS=1 ../taosx-benches/taosx-old run -f  -t "taos:///csv"    27.09s user 2.12s system 472% cpu 6.181 total
```

第一次优化后：
```plaintext {wrap}
heap stats:     peak       total     current       block      total#   
  reserved:     4.0 GiB     4.0 GiB     4.0 GiB                          
 committed:     2.0 GiB     4.0 GiB   -18.6 GiB                          
     reset:     0      
    purged:    22.6 GiB
   touched:     3.3 MiB     3.3 MiB    -1.5 GiB                          
  segments:   103         104          49                                not all freed
-abandoned:    49          50           0                                ok
   -cached:     0           0           0                                ok
     pages:     0           0         -24.1 Ki                           not all freed
-abandoned:   384         390           0                                ok
 -extended:     0      
   -retire:     0      
    arenas:     4      
 -rollback:     0      
     mmaps:    79      
   commits:     0      
    resets:     0      
    purges:     2.1 Ki 
   guarded:     0      
   threads:    75          76          22                                not all freed
  searches:     0.0 avg
numa nodes:     1
   elapsed:     6.064 s
   process: user: 24.719 s, system: 1.947 s, faults: 0, rss: 249.1 MiB, commit: 2.0 GiB
MIMALLOC_SHOW_STATS=1 ./target/release/taosx run -f  -t "taos:///csv" --parse  24.72s user 1.95s system 439% cpu 6.075 total
```

第二次优化后：
```plaintext {wrap}
heap stats:     peak       total     current       block      total#   
  reserved:     4.0 GiB     4.0 GiB     4.0 GiB                          
 committed:     2.0 GiB     4.0 GiB   -18.6 GiB                          
     reset:     0      
    purged:    22.6 GiB
   touched:     3.3 MiB     3.3 MiB    -1.5 GiB                          
  segments:   103         104          49                                not all freed
-abandoned:    49          50           0                                ok
   -cached:     0           0           0                                ok
     pages:     0           0         -24.1 Ki                           not all freed
-abandoned:   384         390           0                                ok
 -extended:     0      
   -retire:     0      
    arenas:     4      
 -rollback:     0      
     mmaps:    79      
   commits:     0      
    resets:     0      
    purges:     2.1 Ki 
   guarded:     0      
   threads:    75          76          22                                not all freed
  searches:     0.0 avg
numa nodes:     1
   elapsed:     6.064 s
   process: user: 24.719 s, system: 1.947 s, faults: 0, rss: 249.1 MiB, commit: 2.0 GiB
MIMALLOC_SHOW_STATS=1 ./target/release/taosx run -f  -t "taos:///csv" --parse  24.72s user 1.95s system 439% cpu 6.075 total
```
