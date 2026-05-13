# taosgen 支持优先读取程序目录下的 libtaos - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-26 | YYYY-MM-DD | 0.1 | 裴亚明 | 初始版本，覆盖核心加载逻辑验证 |

## 2. 测试目标

验证 taosgen 在多环境下的 `libtaos` 加载行为符合设计规范
- 优先级正确性：当 `./lib/libtaos` 存在时，必须优先加载该文件（而非系统 `/usr/lib/libtaos.so` 或 `LD_LIBRARY_PATH` 中的版本）；  
- 回退机制可靠性：当 `./lib/libtaos` 缺失时，能自动回退至系统标准路径加载，并成功初始化客户端；    
- 日志可观测性：控制台/日志中明确输出加载来源路径（含绝对路径），便于运维定位；  
- 多版本共存能力：不同 taosgen 实例（不同安装路径）可独立绑定各自 `./lib/libtaos`，互不干扰。

## 3. 参考文档

[taosgen 支持优先读取程序目录下的 libtaos - FS](https://taosdata.feishu.cn/wiki/W2WUwJweJiHnK8kMtq6clacRnoh)

## 4. 测试结论

用例通过，功能符合预期

## 5. 测试环境

- OS: Ubuntu 22.04 LTS, macOS 12.4

## 6. 功能测试

### 6.1 本地 libtaos 优先加载功能 

#### 6.1.1 测试要点

- 验证 `./lib/libtaos.so`（或 `.dylib`）存在时是否被绝对优先加载；  
- 验证加载路径为 **绝对路径**；  
- 验证 `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` 设置不影响优先级。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 本地库存在 | 在 `taosgen` 同级目录创建 `lib/`，放入 `libtaos.so`等库，运行 `./taosgen` | 成功，日志输出 Loaded libtaos from program directory: /path/to/taosgen/lib/libtaos.so |
| 2 | 本地库缺失 → 回退系统路径 | 删除 `./lib/` 目录，确保系统 `/usr/lib/libtaos.so`存在 | 成功，日志输出 Loaded libtaos from system path: /usr/lib/libtaos.so |
| 3 | 环境变量干扰测试 | 设置 LD_LIBRARY_PATH=/tmp/evil:/usr/lib，./lib/libtaos.so 存在 | 仍加载 `./lib/libtaos.so`，日志路径正确；`/tmp/evil` 中同名库被忽略 |
| 4 | 多实例隔离验证 | 启动两个 taosgen 进程：A（/opt/idmp-a/taosgen）、B（/opt/idmp-b/taosgen） | A、B 显示各自加载不同路径 |

### 6.2 首次加载单例性与日志可观测性

#### 6.2.1 测试要点

- 验证 `libtaos` 仅在首次使用 libtaos 库时加载，重复连接不重复加载； 

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 单例加载验证 | 运行 taosgen，默认执行 TDengine 建库、建表、写入数据等操作，日志中仅出现1次`Loaded libtaos from ...` | 符合设计（首次加载） |

## 7. 兼容性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 旧版 taosgen 无缝降级 | 将新 taosgen（含本特性）替换为旧版（无本地加载逻辑） | 旧版正常启动，连接成功 |
| 2 | 混合部署兼容性 | 同一服务器同时运行：- IDMP v1.0（旧 taosgen）- IDMP v2.0（新 taosgen + `./lib/libtaos`） | 二者均正常连接各自配置的 TDengine 实例，无库冲突 |

## 8. 已知问题和限制

- 由于 libtaos 库的限制，不支持混合使用 Native 和 WebSocket
- 不支持运行时热切换 libtaos 库，加载后无法在进程生命周期内更换库
