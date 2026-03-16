# 审计功能直接写入本地集群实现方案

## 一、需求分析

### 当前架构
```
操作触发 → auditRecord/auditAddRecord → HTTP POST → taoskeeper → 写入audit库
```

### 目标架构
```
操作触发 → 构造审计消息 → RPC消息 → mnode路由 → vnode → 写入audit库
```

### 核心目标
- 绕过taoskeeper，不通过HTTP发送审计消息
- 直接将审计信息保存到本地集群的audit库中
- 利用现有的vnode写入机制和RPC消息系统

## 二、技术方案设计

### 方案选择：通过Mnode路由到Vnode（推荐）

**优势：**
1. 符合TDengine现有架构设计
2. 复用mnode的数据库路由和vgroup查询机制
3. 无需修改vnode代码，审计数据作为普通写入处理
4. 支持集群环境下的分布式写入
5. 自动处理Raft共识和数据复制

**架构流程：**
```
审计触发点
  ↓
构造审计数据（SAuditRecord）
  ↓
发送RPC消息到本地mnode（TDMT_MND_AUDIT_WRITE）
  ↓
mnode处理器：
  - 查询audit数据库
  - 获取对应的vgroup信息
  - 构造SSubmitReq2消息
  - 发送到目标vnode
  ↓
vnode正常处理写入（TDMT_VND_SUBMIT）
  ↓
数据写入audit库
```

## 三、详细实现步骤

### 步骤1：定义新的消息类型

**文件：** `community/include/common/tmsgdef.h`

在mnode消息段添加新的消息类型：
```c
TD_DEF_MSG_TYPE(TDMT_MND_AUDIT_WRITE, "audit-write", NULL, NULL)
TD_DEF_MSG_TYPE(TDMT_MND_BATCH_AUDIT_WRITE, "batch-audit-write", NULL, NULL)
```

**说明：**
- `TDMT_MND_AUDIT_WRITE`：单条审计记录写入
- `TDMT_MND_BATCH_AUDIT_WRITE`：批量审计记录写入
- 复用现有的 `SAuditReq` 和 `SBatchAuditReq` 结构体

### 步骤2：在Mnode添加审计写入处理器

**文件：** `community/source/dnode/mnode/impl/src/mndDnode.c`

#### 2.1 注册消息处理器

在 `mndInitDnode()` 函数中添加：
```c
mndSetMsgHandle(pMnode, TDMT_MND_AUDIT_WRITE, mndProcessAuditWriteReq);
mndSetMsgHandle(pMnode, TDMT_MND_BATCH_AUDIT_WRITE, mndProcessBatchAuditWriteReq);
```

#### 2.2 实现审计写入处理器

**核心逻辑：**
```c
static int32_t mndProcessAuditWriteReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SAuditReq auditReq = {0};

  // 1. 反序列化审计请求
  if (tDeserializeSAuditReq(pReq->pCont, pReq->contLen, &auditReq) != 0) {
    return -1;
  }

  // 2. 获取audit数据库对象
  SDbObj *pDb = mndAcquireAuditDb(pMnode);
  if (pDb == NULL) {
    mError("audit database not found");
    tFreeSAuditReq(&auditReq);
    return -1;
  }

  // 3. 获取audit数据库的vgroup列表
  SArray *pVgList = taosArrayInit(4, sizeof(SVgObj*));
  mndBuildDBVgroupInfo(pDb, pMnode, pVgList);

  // 4. 选择第一个vgroup（audit库通常只有一个vgroup）
  SVgObj *pVg = taosArrayGetP(pVgList, 0);

  // 5. 构造SSubmitReq2消息
  SSubmitReq2 *pSubmitReq = mndBuildAuditSubmitReq(pMnode, &auditReq);

  // 6. 序列化提交请求
  int32_t contLen = 0;
  void *pCont = NULL;
  tEncodeSize(tEncodeSSubmitReq2, pSubmitReq, contLen, code);
  pCont = taosMemoryMalloc(contLen);
  SEncoder encoder = {0};
  tEncoderInit(&encoder, pCont, contLen);
  tEncodeSSubmitReq2(&encoder, pSubmitReq);
  tEncoderClear(&encoder);

  // 7. 构造RPC消息发送到vnode
  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_VND_SUBMIT;
  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;

  // 8. 获取vgroup的endpoint并发送
  SEpSet epSet = mndGetVgroupEpset(pMnode, pVg);
  tmsgSendReq(&epSet, &rpcMsg);

  // 9. 清理资源
  tFreeSAuditReq(&auditReq);
  tDestroySSubmitReq2(pSubmitReq);
  mndReleaseDb(pMnode, pDb);
  taosArrayDestroy(pVgList);

  return 0;
}
```

#### 2.3 构造审计数据的Submit请求

**核心函数：**
```c
static SSubmitReq2* mndBuildAuditSubmitReq(SMnode *pMnode, SAuditReq *pAuditReq) {
  // 1. 创建SSubmitReq2结构
  SSubmitReq2 *pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2));
  pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData));

  // 2. 获取audit表的元数据（表名：t_operations_<cluster_id>）
  char tableName[TSDB_TABLE_NAME_LEN];
  snprintf(tableName, sizeof(tableName), "t_operations_%s", pMnode->clusterId);

  // 3. 查询表的UID和schema version
  // 需要调用mnode的表查询接口获取表元数据

  // 4. 构造SSubmitTbData
  SSubmitTbData tbData = {0};
  tbData.flags = SUBMIT_REQ_COLUMN_DATA_FORMAT;
  tbData.suid = <super_table_uid>;  // operations超级表的UID
  tbData.uid = <child_table_uid>;   // 子表的UID
  tbData.sver = <schema_version>;
  tbData.aRowP = taosArrayInit(1, sizeof(SRow*));

  // 5. 构造行数据
  // 审计表schema：
  // ts timestamp, user_name varchar(25), operation varchar(20),
  // db varchar(65), resource varchar(193), client_address varchar(64),
  // details varchar(50000), affected_rows bigint unsigned, duration double

  SRow *pRow = createAuditRow(pAuditReq);
  taosArrayPush(tbData.aRowP, &pRow);

  // 6. 添加到提交请求
  taosArrayPush(pReq->aSubmitTbData, &tbData);

  return pReq;
}

static SRow* createAuditRow(SAuditReq *pAuditReq) {
  // 构造包含审计数据的SRow
  // 列顺序：ts, user_name, operation, db, resource, client_address,
  //        details, affected_rows, duration

  // 使用tdatablock或trow模块的API构造行数据
  // 具体实现需要参考现有的行构造代码
}
```

### 步骤3：修改审计发送逻辑

**文件：** `enterprise/src/plugins/audit/src/audit.c`

#### 3.1 修改 `auditSend()` 函数

```c
static int32_t auditSend(SJson *pJson) {
  // 新增配置项：tsAuditDirectWrite（是否直接写入本地集群）
  if (tsAuditDirectWrite) {
    // 直接写入本地集群模式
    return auditSendToLocalCluster(pJson);
  } else {
    // 原有的HTTP发送模式（保持兼容）
    return auditSendViaHttp(pJson);
  }
}
```

#### 3.2 实现本地集群写入函数

```c
static int32_t auditSendToLocalCluster(SJson *pJson) {
  // 1. 从JSON提取审计数据
  SAuditReq auditReq = {0};
  extractAuditReqFromJson(pJson, &auditReq);

  // 2. 序列化审计请求
  int32_t contLen = 0;
  void *pCont = NULL;
  tEncodeSize(tSerializeSAuditReq, &auditReq, contLen, code);
  pCont = taosMemoryMalloc(contLen);
  tSerializeSAuditReq(pCont, contLen, &auditReq);

  // 3. 构造RPC消息
  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_MND_AUDIT_WRITE;
  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;

  // 4. 发送到本地mnode
  SEpSet epSet = {0};
  // 获取本地mnode的endpoint
  // 可以通过dmGetMnodeEpSet()或类似接口获取

  tmsgSendReq(&epSet, &rpcMsg);

  tFreeSAuditReq(&auditReq);
  return 0;
}
```

#### 3.3 批量审计的处理

```c
static int32_t auditSendRecordsInBatchImp() {
  if (tsAuditDirectWrite) {
    // 构造SBatchAuditReq
    SBatchAuditReq batchReq = {0};
    batchReq.auditArr = tsAudit.records;  // 复用现有的记录数组

    // 序列化并发送到mnode
    // 发送TDMT_MND_BATCH_AUDIT_WRITE消息

    // mnode端会循环处理每条记录
  } else {
    // 原有的HTTP批量发送逻辑
  }
}
```

### 步骤4：添加配置项

**文件：** `community/source/common/src/tglobal.c`

添加新的配置变量：
```c
bool tsAuditDirectWrite = true;  // 默认启用直接写入
```

**文件：** `community/include/common/tglobal.h`

添加声明：
```c
extern bool tsAuditDirectWrite;
```

**配置文件支持：**
```ini
# taos.cfg
auditDirectWrite 1  # 1=直接写入本地集群, 0=通过HTTP发送到taoskeeper
```

### 步骤5：确保audit数据库和表存在

#### 5.1 audit数据库初始化

**位置：** mnode启动时或首次审计时

```c
static int32_t mndEnsureAuditDbExists(SMnode *pMnode) {
  SDbObj *pDb = mndAcquireDb(pMnode, "audit");
  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
    return 0;  // 数据库已存在
  }

  // 创建audit数据库
  // CREATE DATABASE IF NOT EXISTS audit VGROUPS 1 BUFFER 16 PRECISION 'ns'
  SCreateDbReq createReq = {0};
  strcpy(createReq.db, "audit");
  createReq.numOfVgroups = 1;
  createReq.buffer = 16;
  createReq.precision = TSDB_TIME_PRECISION_NANO;

  // 调用mnode的创建数据库接口
  return mndCreateDb(pMnode, &createReq);
}
```

#### 5.2 audit超级表和子表初始化

```c
static int32_t mndEnsureAuditTableExists(SMnode *pMnode) {
  // 1. 创建超级表 operations
  // CREATE STABLE IF NOT EXISTS audit.operations (
  //   ts timestamp, user_name varchar(25), operation varchar(20),
  //   db varchar(65), resource varchar(193), client_address varchar(64),
  //   details varchar(50000), affected_rows bigint unsigned, duration double
  // ) TAGS (cluster_id varchar(64))

  // 2. 创建子表
  // CREATE TABLE IF NOT EXISTS audit.t_operations_<cluster_id>
  //   USING audit.operations TAGS ('<cluster_id>')

  // 具体实现需要调用mnode的建表接口
}
```

## 四、关键技术点

### 4.1 消息路由

- **mnode到vnode的消息发送：** 使用 `tmsgSendReq()` 函数
- **endpoint获取：** 使用 `mndGetVgroupEpset()` 获取vgroup的endpoint
- **vgroup选择：** audit库通常只有1个vgroup，选择第一个即可

### 4.2 数据序列化

- **审计请求：** 使用现有的 `tSerializeSAuditReq()` 和 `tDeserializeSAuditReq()`
- **提交请求：** 使用 `tEncodeSSubmitReq2()` 和 `tDecodeSSubmitReq2()`
- **行数据：** 使用 `trow.h` 中的SRow构造函数

### 4.3 表元数据查询

需要在mnode中查询audit表的元数据：
- 超级表UID（operations）
- 子表UID（t_operations_<cluster_id>）
- Schema version

可以使用mnode的内部接口：
- `mndAcquireStb()` - 获取超级表对象
- `mndAcquireChildTable()` - 获取子表对象

### 4.4 异步处理和错误处理

- **异步发送：** RPC消息发送是异步的，不需要等待响应
- **错误处理：** 如果发送失败，可以记录日志，但不影响主流程
- **重试机制：** 可选，如果需要保证审计数据不丢失

## 五、实现优先级

### 阶段1：基础功能（必须）
1. 定义新的消息类型
2. 实现mnode的审计写入处理器（单条记录）
3. 修改audit.c的发送逻辑
4. 添加配置项

### 阶段2：完善功能（重要）
1. 实现批量审计写入
2. 添加audit数据库和表的自动创建
3. 完善错误处理和日志

### 阶段3：优化功能（可选）
1. 添加重试机制
2. 性能优化（批量写入优化）
3. 监控和统计

## 六、测试方案

### 6.1 单元测试
- 测试审计消息的序列化和反序列化
- 测试SSubmitReq2的构造
- 测试vgroup查询和路由

### 6.2 集成测试
- 测试单条审计记录写入
- 测试批量审计记录写入
- 测试audit数据库不存在时的自动创建
- 测试集群环境下的审计写入

### 6.3 性能测试
- 对比HTTP方式和直接写入方式的性能
- 测试高并发场景下的审计写入
- 测试审计写入对主业务的影响

## 七、兼容性考虑

### 7.1 配置兼容
- 保留原有的HTTP发送模式
- 通过 `auditDirectWrite` 配置项切换模式
- 默认启用直接写入模式

### 7.2 升级兼容
- 升级时自动创建audit数据库和表
- 支持从taoskeeper模式平滑切换到直接写入模式

### 7.3 降级方案
- 如果直接写入失败，可以fallback到HTTP模式
- 提供配置项强制使用HTTP模式

## 八、潜在问题和解决方案

### 8.1 audit数据库不存在
**问题：** 首次使用时audit数据库可能不存在
**解决：** mnode启动时或首次审计时自动创建

### 8.2 表元数据查询失败
**问题：** 无法获取表的UID和schema version
**解决：** 实现表元数据缓存机制，定期刷新

### 8.3 vnode写入失败
**问题：** vnode可能因为各种原因写入失败
**解决：** 记录错误日志，可选实现重试机制

### 8.4 性能影响
**问题：** 审计写入可能影响主业务性能
**解决：**
- 使用异步发送
- 批量写入优化
- 可配置的审计级别

## 九、代码文件清单

### 需要修改的文件
1. `community/include/common/tmsgdef.h` - 添加消息类型
2. `community/source/dnode/mnode/impl/src/mndDnode.c` - 添加消息处理器
3. `enterprise/src/plugins/audit/src/audit.c` - 修改发送逻辑
4. `community/source/common/src/tglobal.c` - 添加配置项
5. `community/include/common/tglobal.h` - 添加配置声明

### 需要新增的文件（可选）
1. `community/source/dnode/mnode/impl/src/mndAudit.c` - 审计相关的mnode处理逻辑
2. `community/source/dnode/mnode/impl/inc/mndAudit.h` - 审计相关的头文件

## 十、总结

本方案通过在mnode层添加审计写入处理器，将审计数据转换为标准的vnode写入请求，实现了绕过taoskeeper直接写入本地集群的目标。

**核心优势：**
1. 复用现有的vnode写入机制，无需修改vnode代码
2. 利用mnode的路由能力，支持集群环境
3. 保持与现有审计系统的兼容性
4. 实现简单，风险可控

**实施建议：**
1. 先实现单条审计记录的写入，验证可行性
2. 再实现批量写入和自动建库建表
3. 充分测试后再部署到生产环境
4. 保留HTTP模式作为降级方案
