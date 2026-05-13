# CONTRIBUTING.md

感谢你对 taosgen 的关注与贡献！本文档用于规范贡献流程与代码标准，确保代码库的一致性、可维护性与高质量。请在提交变更前认真阅读本指南。

## 行为准则
本项目遵循 [Contributor Covenant v3.0 Code of Conduct](./CODE_OF_CONDUCT.md)。所有贡献者都需遵守该准则；如遇违反行为，请按文件说明进行举报。

---

## 开发环境与支持平台
- 语言标准：C++17
- 支持平台：Linux、macOS
- 编译器要求：GCC 9+ / Clang 11+
- 构建系统与依赖管理：
  - CMake 3.19+
  - Conan 2.19+

快速构建（在项目根目录）：
```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build .
```

macOS 如需指定 SDK：
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

---

## 提交流程（Issues 与 PR）
### 提交 Issue
- 先搜索以避免重复
- 必填信息：taosgen 版本/Commit、配置参数、TDengine 版本、操作系统、复现步骤、日志/堆栈等

### 分支与命名
- 从 `main` 分支创建功能/修复分支
- 分支命名：
  - 修复：`fix/[brief]`（如：`fix/deadlock-in-scheduler`）
  - 功能：`feat/[feature-name]`（如：`feat/batch-metrics-exporter`）

### 提交信息（Commit）
- 推荐使用 Conventional Commits：
  - `feat: ...`、`fix: ...`、`docs: ...`、`test: ...`、`refactor: ...`、`perf: ...`、`build: ...`、`ci: ...`、`chore: ...`
- 要求使用 DCO（Developer Certificate of Origin）
  - 请在提交时添加签名：`git commit -s -m "feat: add xxx"`
  - 提交中会自动添加 `Signed-off-by: Your Name <you@example.com>`

### 提交 PR
- PR 内容聚焦单一主题；链接相关 Issue，描述动机、设计、实现与验证
- 必须附带/更新单元测试（修改代码的覆盖率目标：80%+）
- 本地确保构建与测试通过（`ctest`）
- PR 检查清单（在描述中逐项自检）：
  - [ ] 已遵循代码风格与命名规范
  - [ ] 添加/更新了必要的单元测试
  - [ ] 通过本地构建与 `ctest`
  - [ ] 无未使用依赖/头文件
  - [ ] 未引入 ABI 破坏（公共头/接口变更已注明）
  - [ ] 变更附带必要的文档/注释
  - [ ] 提交信息遵循 Conventional Commits 且包含 DCO 签名

---

## 代码规范

### 1) 命名约定
与现有代码（如 `JobScheduler.hpp/cpp`）保持一致：
| 元素                  | 风格                | 示例                                       |
|-----------------------|---------------------|--------------------------------------------|
| 类/结构体             | PascalCase          | `JobScheduler`, `StepExecutionStrategy`     |
| 函数/方法             | snake_case          | `worker_loop()`, `has_failure()`            |
| 变量（局部/成员）     | snake_case          | `remaining_jobs_`, `step_strategy_`         |
| 私有成员              | snake_case + `_`    | `config_`, `dag_`                           |
| 常量/宏               | UPPER_SNAKE_CASE    | `MAX_CONCURRENCY`, `DEFAULT_TIMEOUT_MS`     |

### 2) 格式化与风格
- 缩进 4 空格，行宽 ≤ 120 字符
- 大括号 K&R 风格：
  ```cpp
  if (stop_execution_.load()) {
      return;
  }
  // 单行允许
  if (node == nullptr) return;
  ```
- 注释：类/函数使用 Doxygen 风格；行内说明用 `//`
  ```cpp
  /**
   * @brief 运行任务调度器并执行全部 DAG 任务
   * @return 全部成功返回 true，任一步失败返回 false
   */
  bool run();
  ```
- 强制使用 clang-format（提交前本地执行）；建议配置 pre-commit 钩子统一格式

### 3) 内存与资源管理（关键）
- 全面遵循 RAII
- 禁止使用原始 `new`/`delete` 管理所有权；禁止以裸指针表达所有权
- 首选栈分配；堆对象默认使用 `std::unique_ptr`
  ```cpp
  auto dag = std::make_unique<JobDAG>(config.jobs);    // 推荐
  // JobDAG* dag = new JobDAG(config.jobs);            // 禁止
  ```
- 仅在确有共享所有权时使用 `std::shared_ptr`
- 使用 `std::make_unique`/`std::make_shared` 创建智能指针
- 析构函数不得抛出异常；避免在异常传播路径中执行不必要的分配/IO

### 4) 并发与线程安全
- 明确所有共享状态的所有权与生命周期；避免数据竞争
- 使用 `std::mutex` + `std::lock_guard`/`std::scoped_lock`；避免自旋忙等
- 跨线程通信优先使用线程安全队列/条件变量；避免无界队列
- 使用 `std::atomic` 管理原子标志；避免非原子共享布尔/计数器
- 线程创建集中管理；提供可中断/可取消的退出路径（如 `stop_token`）
- 定时等待使用超时（避免无限阻塞）；确保退出时无悬挂线程

### 5) 错误处理与日志
- 对不可恢复错误使用异常（如 DAG 检测到环）：
  ```cpp
  if (dag_->has_cycle()) {
      throw std::runtime_error("DAG contains cycles");
  }
  ```
- 可恢复错误返回 `bool`/错误码，严禁静默失败
- 使用 `LogUtils` 输出具备上下文的日志（模块/任务/步骤）
- 不在析构函数中抛异常；异常边界处做好清理与回滚

### 6) 依赖与模块边界
- 第三方依赖统一通过 Conan 管理；新增依赖需：
  1) 在相关 `conanfile`/`CMakeLists.txt` 中声明并最小化作用域
  2) 通过 `option()` 控制可选特性，避免强绑定
  3) 说明许可证与兼容性（MIT 兼容）
- 模块间依赖自上而下，禁止循环依赖；保持目录结构清晰（见 README“Appendix”）

### 7) 许可与版权
- 避免引入受限版权内容；遵循第三方依赖许可证条款并保留必要声明

---

## 测试与质量保障

### 单元测试
- 使用 `ctest` 统一运行
- 新增/修改功能必须配套测试；修改代码的测试覆盖率目标：80%+
- 用例放置在对应模块的 `test` 子目录
- 测试函数命名以 `test_` 开头（如 `test_job_scheduler_with_order`）
- 新增测试文件需更新对应目录下的 `CMakeLists.txt`

运行方式：
```shell
# 在 build 目录
ctest --output-on-failure
# 按名称筛选
ctest -R TestJobScheduler
# 并发执行
ctest -j 4
```

### 性能与基准（可选）
- 性能关键路径需附带简单基准或指标对比
- 避免不必要的拷贝/临时对象；优先使用 `string_view`、`span` 等非拥有视图（在确保生命周期安全前提下）

---

## 文档与示例
- 公共接口与复杂实现需包含 Doxygen 风格注释与必要示例
- 修改行为影响用户的功能需同步更新 README/参考手册链接的文档
- 配置项/命令行参数变更需在参考文档中明确标注

---

## CI/CD 与合并条件
- PR 必须在 CI 通过（构建 + 测试 + 可用的静态检查）
- 维护者在 3 个工作日内完成初审；作者需及时响应并修正
- 通过评审、满足标准后方可合并

---

## 安全报告
如发现安全漏洞，请不要在公开 Issue 披露。请邮件联系：support@taosdata.com，我们会尽快响应与修复。

---

## 常见问题与支持
- 有疑问可提 `question` 标签的 Issue，或加入 TDengine 社区交流
- 贡献即默认同意你的代码以 [MIT License](./LICENSE) 授权

感谢你的贡献，欢迎一起让 taosgen 更加卓越！