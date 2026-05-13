# numOfVnodeQueryThreads 与 qps 相关性 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-25 | 2025-11-25 | 0.1 | @张天毅 | 创建 |

## 2. 相关文档

JIRA: [TS-6252](https://jira.taosdata.com:18080/browse/TS-6252)

## 3. 测试目标

验证numOfVnodeQueryThreads与qps在一般情况下的负相关性，并验证调大LRU cachesize和分片数有助于改变这一行为

## 4. 测试结论

增加query线程数的同时，需要调整LRU cachesize和分片数到合适的数值，才能真正提高qps。在资源受限时，盲目提高query线程数会导致性能下降。

## 5. 测试

#### 5.0.1 测试环境

Cpu: Intel u9-285H 16 cores
Mem: 64GB

#### 5.0.2 准备数据

通过taosgen生成10w子表，每个子表含有10条数据；之后通过taosBenchmark进行查询
脚本见附件

#### 5.0.3 实验

##### 5.0.3.1 Baseline

Cachesize 设为**512MB**
```c
SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, 0, .5);  // 1
```


| numOfVnodeQueryThreads | QPS | 变化 |
| --- | --- | --- |
| 2 | 7.396 | - |
| 4 | 9.427 | +27.5% |
| 8 | 6.993 | -25.8% |
| 16 | 5.440 | -22.2% |

##### 5.0.3.2 调大 LRU 分片数

Cachesize 设为**512MB**
```c
SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, 3, .5);  // 2^3 = 8
```


| numOfVnodeQueryThreads | QPS | 变化 |
| --- | --- | --- |
| 2 | 11.886 | - |
| 4 | 18.237 | +53.4% |
| 8 | 20.109 | +10.3% |
| 16 | 14.573 | -27.5% |

##### 5.0.3.3 调大cachesize

Cachesize 设为**10240MB**
```c
SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, 0, .5);  // 1
```


| numOfVnodeQueryThreads | QPS | 变化 |
| --- | --- | --- |
| 2 | 11.906 | - |
| 4 | 11.558 | -2.92% |
| 8 | 6.755 | -41.6% |
| 16 | 4.833 | -28.5% |

##### 5.0.3.4 同时调大 LRU 分片数和cachesize

Cachesize 设为**10240MB**
```c
SLRUCache *pCache = taosLRUCacheInit(cfgCapacity, -1, .5);  // 10240 / 512 = 20
```


| numOfVnodeQueryThreads | QPS | 变化 |
| --- | --- | --- |
| 2 | 9.088 | - |
| 4 | 16.618 | +82.9% |
| 8 | 24.240 | +45.9% |
| 16 | 32.209 | +32.9% |
| 32 | 30.686 | -4.7% |

#### 5.0.4 附件

1. taosgen数据生成脚本
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: NJUUbzPovognk8xvIepcYzVsn6f)

</view>

1. taosBenchmark查询脚本
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: QFv1b9MIBoVgfUxkrracZJpZnvh)

</view>
