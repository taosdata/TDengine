# TDengine 安全漏洞审查报告

## 1. 审查范围

- **仓库**：`taosdata/TDengine`
- **组件**：`taosd`
- **报告来源**: `Jeft Tao`
- **审查对象**：`./tdengine_tsdb_security_review.md` 中列出的安全问题
- **审查方式**：逐项回溯源码、类型定义、解析链路与输入约束，核实漏洞是否真实可达
- **审查人员**: `Linhe Huo`

## 2. 结论摘要

本次复核后，报告中的问题分为三类：

| 结论 | 条目 |
|---|---|
| **确认有效的安全漏洞** | T1、T2、T3、T4、T5、G3 |
| **代码缺陷但不建议按安全漏洞计入** | T6 |
| **证据不足 / 非安全问题 / 当前代码已不成立** | T7、T8、G1、G2 |

## 3. 有效安全漏洞

### 3.1 T1：`tsdbCache.c` 未校验 `numOfPKs` 导致堆缓冲区溢出

- **文件**：`source/dnode/vnode/src/tsdb/tsdbCache.c:372-388`
- **严重级别**：High
- **置信度**：9/10
- **类别**：`heap_buffer_overflow`

**问题描述**

`tsdbCacheDeserialize()` 从 RocksDB 持久化 value 中读取 `numOfPKs`，并直接以此为循环上界写入 `SRowKey.pks[i]`。  
而 `SRowKey.pks` 固定大小为 `TD_MAX_PK_COLS = 2`，当 `numOfPKs > 2` 时会发生越界写。

**利用前提**

需要攻击者能够离线篡改 `cache.rdb` 中对应 value。

**安全化 PoC 思路**

1. 在隔离环境中使用 ASan 构建 `taosd`。
2. 生成一条正常 cache 记录并停服务。
3. 离线修改对应 RocksDB value，将 `numOfPKs` 改为大于 2。
4. 重启或触发 cache 读取路径。
5. 观察 `tsdbCacheDeserialize()` 中的 heap OOB write。

**修复建议**

- 读取 `numOfPKs` 后立即校验：`numOfPKs <= TD_MAX_PK_COLS`
- 对每次定长/变长字段读取增加边界检查

**飞书工作项**

- [6977578592](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977578592)

---

### 3.2 T2：`tsdbDataFileRW.c` 中 brin block 解析导致栈缓冲区溢出

- **文件**：`source/dnode/vnode/src/tsdb/tsdbDataFileRW.c:257-280`
- **严重级别**：High
- **置信度**：9/10
- **类别**：`stack_buffer_overflow`

**问题描述**

`brinBlk->numOfPKs` 直接来自磁盘元数据，随后被用作循环上界写入固定大小栈数组 `firstInfos[TD_MAX_PK_COLS]` 和 `lastInfos[TD_MAX_PK_COLS]`。  
当 `numOfPKs > 2` 时会发生栈越界写。

**利用前提**

需要攻击者能够离线篡改 `.head` 文件中对应 brin block 元数据，并保持页校验一致。

**安全化 PoC 思路**

1. 在隔离环境中生成正常数据文件。
2. 离线修改某个 `SBrinBlk` 的 `numOfPKs > 2`。
3. 同步修正页 checksum。
4. 触发 brin block 读取路径。
5. 观察栈越界写。

**修复建议**

- 在 decode 前校验 `brinBlk->numOfPKs <= TD_MAX_PK_COLS`
- 对异常元数据返回 `TSDB_CODE_FILE_CORRUPTED`

**飞书工作项**

- [6977620109](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977620109)

---

### 3.3 T3：`tsdbSttFileRW.c` 中 statis block 解析导致栈缓冲区溢出

- **文件**：`source/dnode/vnode/src/tsdb/tsdbSttFileRW.c:446-461`
- **严重级别**：High
- **置信度**：9/10
- **类别**：`stack_buffer_overflow`

**问题描述**

`statisBlk->numOfPKs` 从 `.stt` 文件元数据进入后，直接驱动对固定大小栈数组 `firstKeyInfos[]` / `lastKeyInfos[]` 的写入。  
当 `numOfPKs > 2` 时会发生栈越界。

**利用前提**

需要攻击者能离线篡改 `.stt` 文件并保持页校验一致。

**安全化 PoC 思路**

1. 生成正常 `.stt` 文件。
2. 离线修改 `SStatisBlk.numOfPKs > 2`。
3. 修正 checksum。
4. 触发 statis block 读取路径。
5. 观察栈越界写。

**修复建议**

- 在 decode 前校验 `statisBlk->numOfPKs <= TD_MAX_PK_COLS`

**飞书工作项**

- [6977587019](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977587019)

---

### 3.4 T4：`tsdbUtil2.c` 中 statis record 填充导致堆缓冲区溢出

- **文件**：`source/dnode/vnode/src/tsdb/tsdbUtil2.c:217-224`
- **严重级别**：High
- **置信度**：9/10
- **类别**：`heap_buffer_overflow`

**问题描述**

`tStatisBlockGetRecord()` 以 `statisBlock->numOfPKs` 为循环上界，写入 `record->firstKey.pks[]` / `record->lastKey.pks[]`。  
这两个数组同样只有 `TD_MAX_PK_COLS = 2` 个槽位，因此是明确的下游越界写 sink。

**说明**

该问题依赖上游异常 `numOfPKs` 已进入 `statisBlock`，因此更适合作为 **T3 的下游 sink**；但从代码层面它仍然是独立成立的越界点。

**修复建议**

- 在 `tStatisBlockGetRecord()` 开头增加防御式校验：`statisBlock->numOfPKs <= TD_MAX_PK_COLS`

**飞书工作项**

- [6977781989](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977781989)

---

### 3.5 T5：`tsdbCache.c` 反序列化缺少逐步边界检查导致越界读

- **文件**：`source/dnode/vnode/src/tsdb/tsdbCache.c:368-394`
- **严重级别**：High
- **置信度**：8/10
- **类别**：`out_of_bounds_read`

**问题描述**

`tsdbCacheDeserialize()` 在读取 version、`numOfPKs`、`SValue` 和变长 payload 时，没有逐字段边界检查，而是在尾部统一判断 `offset > size`。  
因此截断后的 V1 cache value 会先触发越界读，再进入错误返回路径。

**利用前提**

攻击者需能离线篡改 `cache.rdb` 中 value。

**安全化 PoC 思路**

1. 生成合法 V1 cache 记录。
2. 离线截断尾部字段，使前缀仍可通过。
3. 重启或触发读取路径。
4. 观察 OOB read。

**修复建议**

- 每次读取前检查 `offset + sizeof(field) <= size`
- 对变长字段检查 `offset + nData <= size`
- 优先改为 `tDecoder` 风格解码

**飞书工作项**

- [6977553022](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977553022)

---

### 3.6 G3：`clientRawBlockWrite.c` 中负数长度进入解码器导致越界读

- **文件**：`source/client/src/clientRawBlockWrite.c:384-386`
- **严重级别**：Medium
- **置信度**：8/10
- **类别**：`integer_underflow` / `out_of_bounds_read`

**问题描述**

代码对 `metaRspLen` 直接执行：

```c
int32_t len = metaRsp->metaRspLen - sizeof(SMsgHead);
tDecoderInit(&coder, data, len);
```

当 `metaRspLen < sizeof(SMsgHead)` 时，`len` 变为负数；而 `tDecoderInit()` 的 `size` 参数是 `uint32_t`，负数会被解释成超大无符号值，导致后续解码越界读取。

**利用前提**

需要恶意服务端或中间人能够控制 `SMqMetaRsp`。

**安全化 PoC 思路**

1. 用隔离环境下的 mock server 返回畸形 `SMqMetaRsp`
2. 令 `metaRspLen < sizeof(SMsgHead)`
3. 触发 `clientRawBlockWrite` 对该响应的解析
4. 观察解码器越界读取

**修复建议**

- 在减法前校验：`metaRspLen > sizeof(SMsgHead)`
- 不满足条件时直接返回解析错误

**飞书工作项**

- [6977491719](https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/6977491719)

## 4. 不建议按安全漏洞计入的问题

### 4.1 T6：NULL 检查对象写错

- **文件**：`source/dnode/vnode/src/tsdb/tsdbRead2.c:691-699`
- **结论**：代码缺陷成立，但更偏向稳定性问题
- **原因**：触发依赖分配失败，主要后果是崩溃，不属于高置信可利用安全漏洞

### 4.2 T7：`int64_t -> int32_t` 截断

- **结论**：证据不足
- **原因**：截断点存在，但未确认形成当前报告声称的明确可利用越界链路

### 4.3 T8：内存泄漏

- **结论**：非安全问题
- **原因**：属于资源泄漏，不建议记为安全漏洞

## 5. 误报 / 当前代码不成立项

### 5.1 G1：`tjsonGetStringValue()` 裸拷贝

- **结论**：风险点存在，但当前报告的影响面和利用链不成立
- **原因**：
  - 当前仓库中实际调用点远少于报告描述
  - 报告举例的配置解析路径与当前代码不一致
  - 未确认到可绕过既有长度约束的真实外部输入链

### 5.2 G2：`thttp.c` `sprintf` 栈溢出

- **结论**：当前代码已不成立
- **原因**：
  - 当前源码已使用 `snprintf`
  - `monitorFqdn` 进入该路径前已受长度限制
  - 属于过时报告

## 6. 飞书工作项创建结果

所有有效漏洞已创建为 **Defect / 潜在漏洞**，字段已补充：

- **组件**：`taosd`
- **仓库**：`TDengine`
- **Owner**：`Simon Guan`
- **模板**：`Taos Dev`
- **已填写字段**：
  - 来源
  - 严重级别
  - 漏洞类型
  - 复现步骤
  - 根因分析
  - 解决方案
  - 影响范围
  - 安全测试建议

| 工作项 ID | 标题 | 类型 |
|---|---|---|
| 6977578592 | Potential Vuln: tsdbCache.c unchecked numOfPKs leads heap buffer overflow | 潜在漏洞 |
| 6977553022 | Potential Vuln: tsdbCache.c missing bounds checks leads out-of-bounds read | 潜在漏洞 |
| 6977620109 | Potential Vuln: tsdbDataFileRW.c unchecked brin numOfPKs leads stack buffer overflow | 潜在漏洞 |
| 6977587019 | Potential Vuln: tsdbSttFileRW.c unchecked statis numOfPKs leads stack buffer overflow | 潜在漏洞 |
| 6977781989 | Potential Vuln: tsdbUtil2.c statis record fill numOfPKs leads heap buffer overflow | 潜在漏洞 |
| 6977491719 | Potential Vuln: clientRawBlockWrite.c negative metaRspLen leads decoder out-of-bounds read | 潜在漏洞 |

## 7. 最终结论

本次复核后，**应认定为有效安全漏洞的条目为 6 个：T1、T2、T3、T4、T5、G3**。
其余条目分别属于稳定性缺陷、证据不足，或基于过时代码形成的误报，不建议纳入本次安全漏洞处置范围。
