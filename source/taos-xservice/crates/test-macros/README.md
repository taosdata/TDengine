## taosx-test-macros

为 taosX 集成测试提供的一组辅助测试宏，目前主要包含：

- **`#[integration_test]`**：带参数化能力的测试宏，自动初始化 tracing 日志并输出到文件和 stdout。需要与 **`#[test]`** 或 **`#[tokio::test(...)]`** 一起使用：前者用于同步测试，后者用于异步测试（包括 `flavor`、`worker_threads` 等参数）。

---

### 功能概览

- **手动指定测试运行方式（通过独立 attribute）**
  - 使用标准 **`#[test]`** 或 **`#[tokio::test(...)]`** 作为 runner，紧挨着再写 `#[integration_test(...)]`。
  - **同步示例**：`#[test]` + `#[integration_test(a = [1, 2])]` + `fn my_test(a: i32) { ... }`
  - **异步示例**：`#[tokio::test(flavor = "multi_thread", worker_threads = 1)]` + `#[integration_test(a = [1, 2])]` + `async fn my_test(a: i32) { ... }`

- **自动初始化 tracing 日志**
  - 为每个测试用例创建独立的日志文件。
  - 日志文件位于当前工作目录下的 `log/` 目录中。
  - 文件名格式：
    - 无参数时：`integration_test_${fn_name}_${ts}.log`
    - 有参数化时：`integration_test_${fn_name}_case${i}_${ts}.log`
    - 其中 `${ts}` 为 `YYYYMMDD_HHMMSS` 本地时间戳。
  - 文件内日志为 **无 ANSI 颜色** 纯文本，同时也会输出到 `stdout`。

- **参数化测试（类似 pytest.mark.parametrize）**
  - 支持为测试函数参数配置多组取值，生成多条 `#[test]`。
  - 参数值支持任意合法的 Rust 表达式（字面量、函数调用、构造表达式等）。

- **打印测试描述**
  - 如果测试函数带有文档注释（`/// ...`），宏会在实际执行前通过 `tracing::info!` 打印整段说明，方便在日志中快速定位用例语义。

---

### 基本用法

在 `tests/integration/Cargo.toml` 中已经添加依赖：

```toml
taosx-test-macros = { path = "../../crates/test-macros" }
```

在具体测试模块中使用：

```rust
use taosx_test_macros::integration_test;

/// 验证 E2E 模块结构是否按预期加载
#[test]
#[integration_test]
fn test_e2e_modules_structure() -> anyhow::Result<()> {
    tracing::info!("✓ E2E test modules are properly structured");
    Ok(())
}
```

#### 注意点

- **必须有一个 runner attribute**：
  - 同步：在 `#[integration_test(...)]` 上方写 `#[test]`，函数为普通 `fn`。
  - 异步：在 `#[integration_test(...)]` 上方写 `#[tokio::test]`（或带参数版本，如 `#[tokio::test(flavor = "multi_thread", worker_threads = 1)]`），函数为 `async fn`。

- **参数必须是简单形式**：
  - 只支持形如 `fn f(a: T, b: U)` 这种简单参数列表。
  - 不支持 `self` / 解构模式等复杂写法。

---

### 参数化语法

宏的参数采用「参数名 = 取值列表」的形式：

```rust
#[integration_test(
    // 多值列表（会做笛卡尔积）
    a = [1, 2, 3],
    b = [4, 5],

    // 单个任意表达式
    c = Some(7),
)]
fn param_test(a: i32, b: i32, c: Option<i32>) {
    tracing::info!("a={a}, b={b}, c={c:?}");
}
```

含义说明：

- `a = [1, 2, 3]`、`b = [4, 5]`：
  - 宏会对所有参数做 **笛卡尔积**，上例中一共生成 `3 × 2 = 6` 条测试。
- `c = Some(7)`：
  - 不带方括号时，视为该参数只取一个表达式值。

宏会根据参数组合为每一组生成一个独立测试函数，例如：

- `param_test_case0`
- `param_test_case1`
- ...

每个用例都有各自对应的日志文件。

---

### 日志初始化行为

展开后的测试函数中会创建一个 tracing subscriber：

- 一个 `fmt` layer，输出到 **文件**：
  - 定位到当前目录 `log/`。
  - `with_ansi(false)` 保证文件中没有 ANSI 颜色控制符。
- 一个 `fmt` layer，输出到 **stdout**：
  - 默认保留终端友好的输出风格。

最终通过：

```rust
tracing::subscriber::with_default(subscriber, || {
    // 调用内部 impl 函数
});
```

来在该 subscriber 作用域内执行实际测试逻辑。

---

### 文档注释与 tracing::info!

如果测试函数本身带有文档注释：

```rust
/// 这是一个完整 E2E 测试用例
/// 用于验证多数据源协同工作流程
#[test]
#[integration_test(...)]
fn my_e2e_test(...) { ... }
```

宏会将所有 `///` 行拼接为一段字符串，在内部 `*_impl` 函数开始时执行：

```rust
tracing::info!("{}", "这是一个完整 E2E 测试用例\n用于验证多数据源协同工作流程");
```

这样在日志文件开头就能直接看到用例的说明。

---

### 典型错误与排查

- **编译期报参数未提供取值**  
  说明函数参数列表中的某个参数没有在宏参数里声明相应的赋值，例如：

  ```rust
  #[integration_test(a = [1, 2])]
  fn bad(a: i32, b: i32) { ... } // b 未配置
  ```

  请为所有形参加上 `name = [...]` 或 `name = expr`。

- **报错：requires a test runner attribute**  
  说明缺少 `#[test]` 或 `#[tokio::test(...)]`。请在 `#[integration_test(...)]` 上方补上其一。

---

### 开发与测试

本 crate 包含单元测试与 UI 编译失败测试，用于保证参数解析、笛卡尔积逻辑及错误诊断行为正确。

**运行测试：**

```bash
cd crates/test-macros
cargo test
```

**测试内容：**

- **单元测试**（`src/lib.rs` 内 `#[cfg(test)] mod tests`）  
  - `ParamArgs` 解析：空参数、单参数列表 `a = [1, 2, 3]`、单表达式 `a = 42`、多参数混合。  
  - `build_combinations`：空参数、单参数多值、多参数笛卡尔积、列表与单值混合。

### 后续扩展方向

- 支持异步测试（用户已通过手动写 `#[tokio::test]` 支持）。
- 支持为参数化用例自动生成更友好的测试名（携带参数值摘要）。
- 支持通过 attribute 参数自定义日志目录或前缀。

