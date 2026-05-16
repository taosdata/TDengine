# taosX 写入端支持原生连接

## 1. 背景

taosx 服务支持原生连接的任务，但是目前的 explorer 不支持配置为原生连接，目标端也不能配置为原生连接。为了提升在 taosx serve + explorer 模式下的数据迁移性能，考虑对系统配置及流程进行优化。

TD-30066


TD-30082

## 2. 现状及流程

在 explorer 页面中创建的数据迁移任务，目标端与 explorer 登陆的数据库保持一致，而目前 explorer 只配置了 websocket 连接地址，导致数据迁移任务仅支持 websocket 连接，流程如下：
1. 在 explorer.toml 中配置参数 cluster（配置文件不存在或配置项不存在则默认 http://localhost:6041），例如 cluster = "http://localhost:6041"，explorer 将通过 websocket 方式连接 http://localhost:6041 进行 TDengine 的可视化管理
2. 登陆 explorer 页面时需要填写 TDengine 的账号密码，登陆成功后 explorer 会记住账号密码
1. 
1. 
1. 
1. 
1. 创建 datain 任务时，在 explorer 前端选择目标数据库，例如 test_ms，explorer 前端将拼接一个连接串 `taos+``http://root``:taosdata@``localhost:6041/test_ms` 作为任务参数 to 提交到 explorer 后端
2. explorer 后端将任务请求转发到 taosx
3. taosx 通过任务参数 to="taos+http://root:taosdata@localhost:6041/test_ms" 创建到目标库的连接

taosx 创建 TDengine 连接的规则如下：
<quote-container>
以 taos+http://... 为例，解析为 driver=taos & protocol=http
</quote-container>

1. driver=("ws" | "wss" | "http" | "https" | "taosws" | "taoswss") 创建 websocket 连接
2. driver=("taos" | "tmq") & protocol=None 创建 native 连接
3. driver=("taos" | "tmq") & protocol=("ws" | "wss" | "http" | "https") 创建 websocket 连接

## 3. 改造方案讨论

### 3.1 方案 1

在 explorer 中增加配置，由用户选择使用的连接方式：
1. 在 explorer.toml 中增加配置 cluster_native="taos://localhost:6030"，explorer 连接与登陆保持原有行为不变
2. 创建 datain 任务时，explorer 页面中增加目标数据库连接方式选项 websocket/native，如果选择 websocket 则保持原有行为不变，如果选择 native 则使用 cluster_native 拼接 to 参数，例如 to="taos://root:taosdata@localhost:6030/test_ms"
3. taosx 通过新的任务参数 "taos://..." 创建到目标库的 native 连接

### 3.2 方案 2

在 explorer 中增加配置，在创建任务时增加参数，由系统优先选择 native 连接或保持 websocket 连接：
1. 在 explorer.toml 中增加配置 cluster_native="taos://localhost:6030"，explorer 连接与登陆保持原有行为不变
2. 创建 datain 任务时，explorer 页面显示不变，增加任务参数 to_native="taos://root:taosdata@localhost:6030/test_ms"
3. taosx 优先使用 to_native 参数创建到目标库的 native 连接，如果失败，则使用 to 参数创建到目标库的 websocket 连接

### 3.3 方案 3

在 explorer 中增加配置，在创建任务时增加参数，由系统优先选择 native 连接或保持 websocket 连接：
1. 在 explorer.toml 中增加配置 cluster_native="taos://localhost:6030"，explorer 连接与登陆保持原有行为不变
2. 创建 datain 任务时，explore 调用 taosx 接口，判断是否可以使用 native 连接 "taos://root:taosdata@localhost:6030/test_ms" 访问目标库，如果可以，则使用 native 连接下发任务，否则使用原来的 ws 连接下发任务。

### 3.4 方案 4(确定）

在 explorer 后端修改对 cluster 参数的解析与使用，参数可以配置为`http://localhost:6041`或`taos://localhost:6030`的形式，由 explorer 后端判断应使用 native 方式或 websocket 方式进行连接，后续创建数据迁移任务时流程类似于方案 1，如下：
1. 在 explorer.toml 中配置 cluster="http://localhost:6041" 或 cluster="taos://localhost:6030"
2. explorer 后端根据 url 判断连接到 TDengine 的方式为 native 或 websocket 并创建连接
3. explorer 前端的 “数据浏览器” 中执行 sql 语句由 explorer 后端解析/接收后执行 taosquery 等操作  
4. 创建 datain 任务，连通性检查时，既检查数据源的连通性，也检查目标 TDengine 的连通性，如果目标 TDengine 无法连接，需要提示 taosX 不可达 TDengine。
5. 创建 datain 任务时，explorer 前端根据当前的 cluster 配置直接拼接 to 参数，行为不变，后续 taosx 流程不变

## 4. 改造方案对比

| **方案** | **优点** | **缺点** |
| --- | --- | --- |
| 方案 1 | 1. 由用户主动选择连接方式，结果明确，所见即所得 1. 只在 explorer 前后端进行少量修改，对系统现状基本没有影响 | 1. 增加用户操作复杂度（也不复杂，主要是选择困难症） 1. 可能出现选择 native 方式但无法连接的情况 |
| 方案 2 | 1. 对用户无感知，减少用户使用疑虑 1. 系统自动判断连接方式，兼顾功能与性能 | 1. taosx 需要增加任务参数与处理逻辑，破坏现状，可能带来未知问题 |
| 方案 3 | 同2 比方案2 好的一点是：对原来的 taosX 任务执行逻辑没有侵入修改，是增加接口判断。 |  |
