# TSDB Windows 适配计划

### 1. 计划背景

TSDB 目前可在 Windows 环境运行，但尚未完成全面测试，且存在一些功能缺口。为保障 Windows 版本功能完整性、运行稳定、性能达标，特制定本适配计划，有序推进各项适配与测试工作。

### 2. 核心目标

1. 完成 Windows 环境下缺失核心功能的适配开发，确保功能正常运行、符合预期；
2. 完成 Windows 版本全量功能测试、性能测试，及时解决现存问题，保障版本运行稳定性；
3. 落地 Windows 环境 Coredump 生成方案，完善问题排查能力，提升故障定位效率；
4. 完成 Windows 版本非重要功能适配，实现与 Linux 环境功能完全对齐，保障跨平台一致性。

### 3. 工作阶段与具体任务

#### 3.1 阶段一：核心功能适配

时间：3.6 - 3.13
工作项：
1. [taosX 适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6646294817)（已完成）
2. [流计算适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6857094454)（已完成）
3. [TDgpt 适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6861933885)（已完成）

#### 3.2 阶段二：测试与发布阶段

##### 3.2.1 全量功能测试

时间：3.6 - 3.30
工作项：[全量测试](https://project.feishu.cn/taosdata_td/job/detail/6854646879)，测试发现的 BUG 将包含 windows 标签
工作内容：执行全量功能测试，同步排查并修复测试过程中发现的各类问题，确保功能无遗漏、无异常

##### 3.2.2 性能测试

时间：3.6 - 3.30
工作项：[性能测试](https://project.feishu.cn/taosdata_td/job/detail/6883801779)
工作内容：运行 Linux 发版前采用的测试脚本，解决显著的性能问题，给出性能测试报告及后续优化项

##### 3.2.3 问题排查能力

时间：3.13 - 3.30
工作项：[Coredump 生成](https://project.feishu.cn/taosdata_td/feature/detail/6861895851)
工作内容：制定 coredump 文件生成策略并在代码中实施，确保可正常查看 Coredump 文件；兼顾 Debug 版本与 Release 版本，明确 Release 版本 Coredump 文件查看限制及应对。

##### 3.2.4 安装包验证

时间：3.13 - 3.30
工作项：[Windows 安装包](https://project.feishu.cn/taosdata_td/feature/detail/6856808946)
工作内容：Windows 下的安装包调整，包括新增的 TDgpt，以及 IDMP 结合测试。

#### 3.3 阶段三：非重要功能适配

时间：4.1 - 6.30
工作项：
1. [共享存储适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6862220600)
2. [MQTT 订阅适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6862031345)
3. [UDF 适配 Windows](https://project.feishu.cn/taosdata_td/feature/detail/6876989393)
4. [扫描不支持的功能小项并适配](https://project.feishu.cn/taosdata_td/feature/detail/6862269465)
说明：以上功能重要性一般，计划于 2026Q2 完成适配，确保与 Linux 环境功能对齐
