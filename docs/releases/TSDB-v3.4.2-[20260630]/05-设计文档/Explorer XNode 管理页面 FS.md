## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-15 | 2026-04-15 | 1.0 | yanyuxing | 初版：补充 Explorer 集群页 XNode 管理能力，以及 DataIn 任务入口的 XNode 前置校验 |

## 2. 背景

当前 Explorer 的 Cluster 页面缺少独立的 XNode 管理区块，用户无法在 UI 中查看、创建、删除 XNode。

同时，DataIn 的 **Create New Task** 和 **Import Task** 两个入口依赖 XNode 执行任务，但旧行为没有在入口处做前置检查，用户往往会在更晚的步骤才发现环境未就绪。此次改动的目标是：

1. 在 Explorer 的 Cluster 页面补齐 XNode 的可视化管理能力。
2. 在 DataIn 入口处提前检查 XNode 是否存在，并给出明确引导。
3. 对 Import Task 保持浏览器文件选择器的正常唤起行为，不因异步检查破坏用户点击上下文。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| XNode | TDengine 集群中的 XNode 节点，用于支持对应任务执行能力 |
| Cluster 页面 | Explorer 路由 `/management/cluster` 对应的系统管理/集群页面 |
| DataIn 任务页 | Explorer 路由 `/dataIn/Task` 对应的数据接入任务列表页面 |
| 入口校验 | 用户点击 **Create New Task** 或 **Import Task** 时，先执行 `SHOW XNODES` 判断是否存在至少一个 XNode |
| 缺失引导弹框 | 当系统中不存在 XNode 时，Explorer 弹出的确认框，提供 **Cancel** 和 **Go Create** 两个按钮 |

## 4. 行为说明

### 4.1 Cluster 页面新增 XNode 区块

Cluster 页面新增一个独立的 **XNodes** 区块，行为与已有集群信息区块保持一致，展示数据来自 SQL 代理接口执行：

```sql
SHOW XNODES;
```

#### 4.1.1 列表字段

列表展示以下列：

| 列名 | 字段 | 说明 |
| --- | --- | --- |
| Endpoint | `endpoint` | XNode 地址。若底层返回字段名为 `url`，Explorer 在前端归一化为 `endpoint` 后展示 |
| Status | `status` | XNode 当前状态 |
| Create Time | `create_time` | XNode 创建时间 |
| Action | - | 目前提供删除按钮 |

#### 4.1.2 列表刷新行为

1. 列表为空时显示空表，不额外报错。
2. 页面初始化时自动执行一次 `SHOW XNODES`。
3. 创建成功或删除成功后，自动重新拉取列表。

### 4.2 创建 XNode

XNodes 区块右上角提供新增入口，点击后弹出创建对话框。

#### 4.2.1 对话框字段

对话框字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| Endpoint | 是 | XNode 地址，去除首尾空白后参与校验 |
| User | 否 | 可选用户名 |
| Password | 否 | 可选密码；若填写用户名，则密码必填；若填写密码，则用户名必填 |
| token | 否 | 可选 token |

#### 4.2.2 校验规则

校验规则：

1. Endpoint 不能为空。
2. User 和 Password 必须同时为空或同时非空。
3. User 必须匹配 `^[A-Za-z_][A-Za-z0-9_]*$`。

#### 4.2.3 创建 SQL

##### 4.2.3.1 仅填写 Endpoint

```sql
CREATE XNODE '192.168.1.10:6043';
```

##### 4.2.3.2 填写 Endpoint、User、Password、Token

```sql
CREATE XNODE '192.168.1.10:6043' USER taosx_user PASS 'secret123';
CREATE XNODE '192.168.1.10:6043' TOKEN 'token';
```

#### 4.2.4 交互行为

1. 点击 **Confirm** 后，Explorer 通过 SQL 代理执行创建 SQL。
2. 创建成功后关闭弹框并刷新 XNode 列表。
3. 创建失败时保留当前弹框内容，由后端错误提示或 SQL 执行错误反馈给用户。
4. 点击 **Cancel** 或关闭弹框时，清空表单与校验状态。

#### 4.2.5 浏览器输入行为

1. 表单设置 `autocomplete="off"`。
2. Password 输入框设置 `autocomplete="new-password"`，降低浏览器自动填充已有账户密码的概率。

### 4.3 删除 XNode

XNodes 列表每行提供删除按钮。点击后弹出确认框，确认后执行：

#### 4.3.1 删除 SQL

```sql
DROP XNODE 1;
```

#### 4.3.2 删除说明

1. 删除 SQL 中的 `1` 为当前行 `id` 字段。
2. 若 `id` 不是整数，前端直接提示 `invalidXnodeId` 对应文案，不发送 SQL。
3. 删除成功后提示成功消息并刷新列表。

### 4.4 DataIn 入口校验

DataIn 页面新增统一的 XNode 存在性校验能力，用于拦截以下两个入口：

#### 4.4.1 适用入口

1. **Create New Task**
2. **Import Task**

#### 4.4.2 校验 SQL

```sql
SHOW XNODES;
```

#### 4.4.3 判断规则

1. 只检查返回结果是否至少有一行。
2. 不检查 `status` 字段，不区分 online/offline。

#### 4.4.4 Create New Task

点击 **Create New Task** 后：

1. 若 `SHOW XNODES` 返回至少一条记录，正常跳转到 `/dataIn/add`。
2. 若没有任何 XNode，弹出缺失引导弹框。

#### 4.4.5 Import Task

点击 **Import Task** 后：

1. 若系统已知存在 XNode，直接打开浏览器文件选择器。
2. 若系统已知不存在 XNode，弹出缺失引导弹框，不打开文件选择器。
3. 若页面预加载的 XNode 状态尚未返回，则乐观地直接打开文件选择器，避免因等待异步检查丢失浏览器用户激活上下文。

#### 4.4.6 缺失引导弹框

当系统中不存在 XNode 时，Explorer 弹出确认框：

- 标题：`XNode Required`
- 内容：提示用户需要先前往 Cluster 页面创建 XNode
- 按钮：`Cancel` / `Go Create`

##### 4.4.6.1 按钮行为

1. 点击 **Cancel**：关闭弹框，停留在当前页面，不执行原始动作。
2. 点击 **Go Create**：跳转到 `/management/cluster`，由用户自行完成 XNode 创建。

##### 4.4.6.2 国际化范围

以下用户可见文案需要提供中英文翻译：

1. XNode 区块标题
2. Endpoint / Status / Create Time / Action 列标题
3. 创建弹框标题、字段名、校验错误
4. DataIn 入口缺失引导弹框标题、正文、按钮文本

### 4.5 错误处理

#### 4.5.1 处理策略总览

| 场景 | 行为 |
| --- | --- |
| `SHOW XNODES` 查询失败（Cluster 页面） | 列表保持当前状态，前端记录错误，不额外新增页面级提示 |
| `SHOW XNODES` 查询失败（DataIn 预加载） | `xnodesExist` 保持未知状态，Import Task 仍允许直接打开文件选择器 |
| 创建 XNode 参数校验失败 | 在弹框内显示字段级错误，不发送 SQL |
| 创建 XNode SQL 执行失败 | 保持弹框打开，不自动清空用户输入 |
| 删除 XNode 时 `id` 非整数 | 直接提示错误，不发送 SQL |
| 用户取消缺失引导弹框 | 关闭弹框，保持当前页面状态 |

## 5. 性能

### 5.1 影响概览

影响较小。

1. Cluster 页面新增一次 `SHOW XNODES` 查询，返回数据量通常较小。
2. DataIn 页面初始化新增一次 `SHOW XNODES` 预查询，用于优化 Import Task 行为。
3. Create New Task 仍在点击时执行一次实时校验，确保行为正确。

预期不会对写入、查询、启动等核心路径造成可感知性能影响。

## 6. 安全

### 6.1 风险控制

1. 创建 XNode 时，前端对 Endpoint 和 Password 中的 SQL 特殊字符进行转义后再拼接 SQL，降低拼接型注入风险。
2. User 字段限制为字母、数字和下划线组合，且首字符不能为数字。
3. Password 输入框使用密码展示控件，并通过 `autocomplete="new-password"` 降低浏览器误填充风险。
4. DataIn 缺失引导仅暴露“是否存在 XNode”这一必要状态，不返回额外集群敏感信息。

## 7. 兼容性

### 7.1 兼容性说明

兼容现有 Explorer 路由与 SQL 代理能力。

1. 未修改已有 DataIn 任务创建接口与导入接口。
2. 旧版本若不包含 XNode 区块，用户仍可通过其他方式创建 XNode；本改动仅增强 Explorer 可用性。
3. DataIn 的入口校验属于前端行为变化，不改变服务端协议。

## 8. 运维

### 8.1 部署要求

无额外部署步骤。

### 8.2 运行前提

只要 Explorer 前端版本升级到包含本特性的版本，功能即可生效。运行前提仍是 Explorer 可以通过既有 SQL 代理成功执行：

```sql
SHOW XNODES;
```

## 9. 使用场景

### 9.1 UC-1：管理员在 Cluster 页面查看现有 XNode

管理员进入 `/management/cluster`，在 XNodes 区块中查看当前所有 XNode 的 Endpoint、状态和创建时间。

### 9.2 UC-2：管理员在 Cluster 页面新增 XNode

管理员点击新增按钮，填写 Endpoint 与可选认证信息，提交后完成创建并刷新列表。

### 9.3 UC-3：管理员删除错误配置的 XNode

管理员在列表中选择某个 XNode，确认删除后刷新列表。

### 9.4 UC-4：普通用户创建 DataIn 任务前被引导补齐前置条件

用户在 `/dataIn/Task` 点击 **Create New Task**，若系统中没有 XNode，则收到明确提示并跳转到 Cluster 页面。

### 9.5 UC-5：普通用户导入任务时保留原生文件选择体验

用户在 `/dataIn/Task` 点击 **Import Task**，若系统中已有 XNode，则立即打开文件选择器，不因异步检查导致点击无响应。

### 9.6 UC-6：普通用户取消引导

当系统中没有 XNode 时，用户可以在缺失引导弹框中点击 **Cancel**，继续留在当前任务列表页。

## 10. 约束和限制

### 10.1 约束

1. 本特性依赖 Explorer 现有 SQL 代理接口可用。
2. DataIn 入口校验仅在 Explorer Web UI 生效，不约束其他客户端。

### 10.2 限制

1. 入口校验只判断 XNode 是否存在，不判断该 XNode 是否可连通或是否处于可执行状态。
2. 当 DataIn 页面预加载失败时，Import Task 会优先保证文件选择器体验，不会强制阻断。
3. 删除操作依赖列表返回的 `id` 字段必须为整数。

## 11. 常见错误和排查

### 11.1 排查对照表

| 错误现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| Cluster 页面 XNodes 列表为空 | 当前确实没有 XNode，或 `SHOW XNODES` 查询失败 | 在 SQL Console 中手动执行 `SHOW XNODES;` |
| 创建弹框提示用户名格式错误 | User 不满足命名规则 | 检查是否以字母或下划线开头，后续仅包含字母、数字、下划线 |
| 创建弹框提示用户名和密码必须同时填写 | 仅填写了 User 或 Password 其中之一 | 同时填写或同时留空 |
| 删除失败提示无效 XNode ID | 当前行 `id` 字段异常 | 检查 `SHOW XNODES` 返回结构是否包含整数类型 `id` |
| 点击 Create New Task 后被引导去 Cluster | 当前系统中没有 XNode | 在 Cluster 页面先创建至少一个 XNode |
| 点击 Import Task 没有弹出缺失引导 | 页面预加载状态未知时会优先打开文件选择器 | 检查页面初始化时 `SHOW XNODES` 是否执行成功 |

## 12. 可观测性

### 12.1 taos Explorer

- Cluster 页面新增 XNodes 区块。
- DataIn 任务列表页新增 XNode 缺失引导弹框。
- Import Task 的点击行为在存在 XNode 时改为立即拉起文件选择器。

### 12.2 taos shell

- 无行为变化；可继续通过 `SHOW XNODES`、`CREATE XNODE`、`DROP XNODE` 观察结果。

### 12.3 TDinsight

- 无直接行为变化。

## 13. 安装和卸载

### 13.1 交付方式

无额外要求。

本特性随 Explorer 前端资源一起交付，不新增独立安装脚本或卸载脚本。

## 14. 文档

### 14.1 需同步更新的文档

需要评估并同步更新以下文档：

1. 企业版文档中 Explorer 的 Cluster 页面说明，补充 XNode 管理能力。
2. 企业版文档中 DataIn 任务入口说明，补充“缺少 XNode 时需要先创建”的前置条件。
3. 若官网存在 Explorer 功能导览，也应补充 XNode 管理与入口校验行为。

## 15. 参考文档

1. `explorer/src/views/8_administrator/views/components/clusters/xnodes.vue`
2. `explorer/src/components/xnode/AddXnodeDialog.vue`
3. `explorer/src/components/xnode/xnodeDialog.helper.ts`
4. `explorer/src/components/xnode/xnodeGate.helper.ts`
5. `explorer/src/views/2_dataIn/index.vue`
6. `explorer/taos-ui/components/dataIn/components/task-import.vue`
7. `explorer/taos-ui/components/dataIn/views/task/index.vue`
8. `explorer/tests/xnode-entry-gate.spec.ts`

## 16. 附录

### 16.1 前端实现边界

为避免 `taos-ui` 直接依赖 Explorer 私有 UI 组件，本次实现采用以下分层方式：

1. Explorer 应用层负责：
   - 执行 `SHOW XNODES`
   - 构建 `ensureXnodeThen()` 逻辑
   - 弹出缺失引导弹框并执行路由跳转
2. `taos-ui` 负责：
   - 消费 `ensureXnodeThen()` 与预加载状态
   - 在 Create New Task 和 Import Task 上触发校验
   - 保持任务列表和文件导入组件的原有职责

### 16.2 关键设计说明

1. Import Task 不能在用户点击后等待异步请求再调用文件选择器，否则浏览器可能丢失用户激活上下文并静默拒绝打开文件选择器。
2. 因此，页面初始化时会预加载一次 XNode 存在性；若状态未知则优先保证文件选择器体验。
3. 传给 `taos-ui` 的缺失回调避免使用 `on*` 命名，防止 Vue 将其识别为事件监听器而不是普通 prop。
