# [Test Report] TS-4054 变长字符串性能

### 1. 概述：

测试需求：[[测试需求] 变长字符串性能](https://taosdata.feishu.cn/wiki/TkmswFTFsivvd4kJ6kIc2QwJned) 
测试目的主要是验证当写入的内容完全相同时，字符串列的类型和长度定义对写入性能、压缩比的影响。

### 2. 测试环境：

192.168.1.35(taosd)：8
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）24核
Mem: DDR3  32 GB * 2
Disk: 2792GB
192.168.1.61（客户端）：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz （2）40核
Mem：DDR4 16GB * 16
Disk:  893GB

### 3. 测试用例：

| 场景序号 | 字段类型 | 字段长度 | 基准值 | 随机程度 |
| --- | --- | --- | --- | --- |
| 场景一 | VARCHAR | 8 | "taosdata" | 不发生变化 |
| 场景二 | VARCHAR | 128 | "taosdata" | 不发生变化 |
| 场景三 | VARCHAR | 2k | "taosdata" | 不发生变化 |
| 场景四 | VARCHAR | 16k | "taosdata" | 不发生变化 |
| 场景五 | VARCHAR | 32k | "taosdata" | 不发生变化 |
| 场景六 | NCHAR | 8k | "taosdata" | 不发生变化 |
| 场景七 | NCHAR | 8K | "你好" | 不发生变化 |
| 场景八 | VARCHAR | 32K | 英文字符填满 16k | 完全随机 |
| 场景九 | NCHAR | 8k | 英文字符填满 16k | 与场景八相同 |

测试结果：

| 场景序号 | 耗时 | 写入速度 | 压缩比 | CPU | Mem | 磁盘IO | 网络 | jso | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 场景一 | start：2023-11-7 15:07:42 end：2023-11-7 15:17:50 10分8秒（608秒） | 3289474 条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_RCTzb4gLnoXvQTxmwbGcD0mcn1d.png) | ![](./images/img_Cd0BbM02kofzzCxJjmacHHG3nLg.png) | ![](./images/img_S86HbmoXzoaKFlxcM6Ic8ZRrnze.png) | ![](./images/img_AIppbh5iRo5NEnxq59vcHC03nvh.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: C42RbbSHVo2isqxDX5zc6FtlnTe) |
| 场景二 | start：2023-11-7 17:05:16 end：2023-11-7 17:16:21 11分5秒（665秒） | 3007519 条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_Jp92bUGkPoatrKxbQ9icc8kXncg.png) | ![](./images/img_SK5tb6puWo92NixB0yRcAhBOnDc.png) | ![](./images/img_SOY6bkpXxoINTjxR41rcBD3zn3y.png) | ![](./images/img_Z7X8bqrotoX6GKxIJZocpDZcnmd.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: LVbmbSertoIoUfxiJKTcf1B9nCh) |
| 场景三 | start：2023-11-7 19:00:42 end：2023-11-7 19:11:21 10分39秒 （639秒） | 3129890 条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_WBzbbhaShojPASxvKE1cAzXqnyd.png) | ![](./images/img_BYQ5bapYqo701ZxI91Bcj5A8ndd.png) | ![](./images/img_A3kvbpUdLoGTiuxrdMvcUdHhnY1.png) | ![](./images/img_JrMbb0CvUoDf7IxPo8eczdvGnPc.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: I9n4b6Q8Xou8i1xxYLUciUVxnNe) |
| 场景四 | start：2023-11-7 19:19:56 end：2023-11-7 19:31:05 11分9秒 （669秒） | 2989536条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_IzikbWVqFoEoeXxmtJrcqUGFnoh.png) | ![](./images/img_C0O5bGSe8ouJXyxPLocclS07nxe.png) | ![](./images/img_GNq4bEsDYoEAEzxkuQkc0nACnCb.png) | ![](./images/img_BZ4Gby2qbovzscxIkiicq7LsnDc.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: U7IDbHcaLoPRdmx450FcXS6bnyb) |
| 场景五 | start：2023-11-8 08:09:12 end：2023-11-8 08:30:21 21分9秒 （1269秒） | 1576044条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_MJAlbeGutobDLUxcxqTcjX9Snud.png) | ![](./images/img_WXsnbqbjsoTvNOxrRGXcAJYFn8c.png) | ![](./images/img_EBhZbsJdcoyUjlxqumRc3Z6bnz2.png) | ![](./images/img_IXuQb5HsloflDSxALBFcLc8FnCh.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: MdqXb4XE6oB3K2xc0CUcNysdn9X) |
| 场景六 | start：2023-11-9 13:44:27 end：2023-11-9 14:16:41 32分14秒 （1934秒） | 1034126条/秒 | 理论值：29.8GB 实际值：342MB 压缩比：98.88% | ![](./images/img_KDjZbwGeMolfD3xC2wzcT0twnHb.png) | ![](./images/img_HN4NbuJWHowjIHxqulkcZR5cnIc.png) | ![](./images/img_IlqBbMSTZoe6tLxIo4QcVabCnNe.png) | ![](./images/img_OjLSbg8SholaHhxN3fBcZbEEnjg.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: BPRibwlGro5eTox141oco8wdn5g) |
| 场景七 | start：2023-11-8 13:53:51 end：2023-11-8 14:05:13 11分22秒 （682秒） | 2932551条/秒 | 理论值：29.8GB 实际值：130MB 压缩比：99.57% | ![](./images/img_KBMWbUc0moM5DNxW3L1cw375nwc.png) | ![](./images/img_LU6Gbo5XBoypScxNFQrcCe1ln5e.png) | ![](./images/img_PEJLbkR9wofNx8xSKpccBouwnOf.png) | ![](./images/img_ZfOqbFPqQoxaw4x0WrYcGIvznsh.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: JjkKbfCNioV2lcxcwvYcZhu9nZg) |
| 场景八 | start：2023-11-9 15:42:17 end：2023-11-10 03:58:35 12小时16分18秒 （44100秒） | 2268条/秒 | 理论值：1.49TB 实际值：6.45GB 压缩比：99.58% | ![](./images/img_Enxjbiv94ol0lYxgRKIczvTknAh.png) | ![](./images/img_CsViblC1rogZFhxNvuxcGM1nnSd.png) | ![](./images/img_YdVhbCpnPo7y5ax9nS0cdKv0nAe.png) | ![](./images/img_NYAdbKytwoVRMdxwe8EcsUIBnJe.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: AWDJbC4Zto8q5uxqYqAcujVtnZf) |
| 场景九 | start：2023-11-10 08:16:56 end：2023-11-11 17:04:44 32小时47分48秒 （118064秒） | 847条/秒 | 理论值：1.49TB 实际值：13.4GB 压缩比：99.12% | ![](./images/img_DFn6bY9uAoS7hDxkBf7ctOi4nje.png) | ![](./images/img_ZhNBby0gao0brPxOuhLcud5wn8e.png) | ![](./images/img_Dla2biOpvo3g7dxoDsPcSEFJnAb.png) | ![](./images/img_Q2vobE1aBok4QMxKAweccy6Enzf.png) | > ⚠ 嵌入文件，需在飞书中查看 (token: K03vbh0nQo038XxxEsiczJTWnKh) |

### 4. 总结：

 压缩比=（理论值 - 实际值）/ 理论值
预期结果

|  | 写入数据 | 字段类型 | 占用最大空间 | 支持最大字符 | 测试目标 | 预期 |
| --- | --- | --- | --- | --- | --- | --- |
| 场景 1 到 5 对比 | 完全相同 | 相同 | 不断增长 | 不断增长 | 长度定义的影响 | 相同 |
| 场景 4 和 6 对比 | 业务内容相同 | 不同 | 不同 | 相同 | nchar 和 varchar 的不同 | 4 比 6 好 |
| 场景 5 和 6 对比 | 业务内容相同 | 不同 | 相同 | 不同 | nchar 和 varchar 的不同 | 5 比 6 好 |
| 场景 4 和 7 对比 | 真实长度相同 | 不同 | 不同 | 相同 | nchar 和 varchar 的不同 | 4 比 7 好 |
| 场景 5 和 7 对比 | 真实长度相同 | 不同 | 相同 | 不同 | nchar 和 varchar 的不同 | 相同 |
| 场景 8 和 9 对比 | 业务内容相同 | 不同 | 相同 | 不同 | nchar 和 varchar 的不同 | 相同 |

1. 场景1到5对比，随着VARCHAR定义的长度增加，写入速度会有下降，尤其是场景1与场景5对比，场景5的网络曲线抖动明显，CPU、内存消耗明显升高，磁盘写入下降；在磁盘占用上，5个场景一致, 在资源占用及写入速度上与预期不相符
2. 场景4与6对比，在资源占用率上6比4高，在写入速度上，6比4要慢2倍左右，同时在磁盘占用上，6比4要多出1倍以上，符合4比6好的预期，即同样固定长度的字符串，VARCHAR比NCHAR要好，符合4比6好预期
3. 场景5与场景6对比，在cpu、内存的消耗上差别不大，在写入速度上5比补充场景6要快很多，磁盘占用上5比6要小，符合5比补充场景6好的预期
4. 场景4与7对比，在资源占用率，写入速度及磁盘占用上，4比7略好，但不够明显，基本符合4比7好的预期
5. 场景5与7对比，两个场景占用最大空间不同，在资源占用率上，5比7消耗要大，在写入速度上，5比7要慢，说明在写入字符长度一定的情况下，增加占用最大空间会使写入速度下降，预期相同，在资源占用与写入速度上与预期不相符
6. 场景8与9对比，由于写入速度下降严重，减少写入数据量为每个子表10000条数据，总计写入1亿条数据。两个场景下资源占用基本相同，写入速度上NCHAR比VARCHAR慢很多，占用空间上NCHAR是VARCHAR的一倍以上，与预期不相符。
