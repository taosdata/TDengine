# taosX 项目开发规范

本文档旨在为 taosX 项目的开发者提供统一的开发规范，确保代码质量、可维护性和一致性。

> 代码需要在保证功能正确的前提下，满足可读、可维护、安全、可靠、可测试、高效、可移植的特征要求。

规则都不是完美的，通过禁止在特定情况下有用的特性，可能会对代码实现造成影响。但是我们制定规则的目的是“为了大多数程序员可以得到更多的好处”， 如果在团队运作中认为某个规则无法遵循，希望可以共同改进该规则。 参考该规范之前，希望您具有相应的 Rust 语言基础能力，而不是通过该文档来学习 Rust 语言。

在阅读本文档之前，请确保您已了解以下内容：

- [Rust 编程语言](https://doc.rust-lang.org/book/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html)
- [Rust 代码风格指南](https://doc.rust-lang.org/style-guide/index.html)
- [Rust Unsafe Code Guidelines](https://doc.rust-lang.org/nomicon/)
- [Cargo 包管理工具](https://doc.rust-lang.org/cargo/)

---

<!-- omit in toc -->
## 目录

- [1. 代码风格](#1-代码风格)
	- [1.1 缩进与格式化](#11-缩进与格式化)
	- [1.2 命名规范](#12-命名规范)
		- [1.2.1 taosX 命名约定](#121-taosx-命名约定)
- [2. 模块组织](#2-模块组织)
	- [2.1 模块划分](#21-模块划分)
	- [2.2 公共接口](#22-公共接口)
- [3. 错误处理](#3-错误处理)
	- [3.1 使用 `Result` 和 `Option`](#31-使用-result-和-option)
	- [3.2 错误上下文](#32-错误上下文)
- [4. 日志与调试](#4-日志与调试)
	- [4.1 使用 `tracing` 记录日志](#41-使用-tracing-记录日志)
	- [4.2 日志级别](#42-日志级别)
- [5. 测试](#5-测试)
	- [5.1 单元测试](#51-单元测试)
	- [5.2 集成测试](#52-集成测试)
- [6. 文档注释](#6-文档注释)
	- [6.1 使用 `///` 添加文档注释](#61-使用--添加文档注释)
- [7. 依赖管理](#7-依赖管理)
	- [7.1 使用 `Cargo.toml`](#71-使用-cargotoml)
- [8. 性能优化](#8-性能优化)
	- [8.1 避免不必要的分配](#81-避免不必要的分配)
	- [8.2 使用 `tokio` 异步框架](#82-使用-tokio-异步框架)
- [9. 安全性](#9-安全性)
	- [9.1 避免未定义行为](#91-避免未定义行为)
	- [9.2 输入验证](#92-输入验证)
	- [9.3 避免使用 `.unwrap()`](#93-避免使用-unwrap)
- [10. 提交规范](#10-提交规范)
	- [10.1 提交信息格式](#101-提交信息格式)
	- [10.2 提交前检查](#102-提交前检查)
	- [10.3 提交流程](#103-提交流程)
	- [10.5 代码审查](#105-代码审查)
	- [10.6 合并 PR](#106-合并-pr)
- [附录](#附录)

---

## 1. 代码风格

### 1.1 缩进与格式化

- 使用 **4 个空格**作为缩进。
- 使用 `rustfmt` 工具格式化代码。

示例：

```rust
fn example_function() {
    let x = 42;
    println!("Value: {}", x);
}
```

### 1.2 命名规范

- **模块名**：使用小写字母，单词间用下划线分隔，例如 `data_sources`。
- **结构体与枚举**：使用 PascalCase 命名法，例如 `TaskScheduler`。
- **变量与函数**：使用 snake_case 命名法，例如 `get_task_list`。
- **常量**：使用全大写字母 （SCREAMING_SNAKE_CASE 命名法），单词间用下划线分隔，例如 `MAX_RETRY_COUNT`。

#### 1.2.1 taosX 命名约定

- **taos**：在需要使用时，使用 `CUS_PROMPT` 环境变量。
- **TDengine**：在需要使用时，使用 `CUS_NAME` 环境变量。
- **jid**: Job ID 缩写。
- **tid**: Task ID 缩写。
- **aid**: Agent ID 缩写。

---

## 2. 模块组织

### 2.1 模块划分

- 每个模块应关注单一职责，避免模块过于庞大。
- 使用 `mod.rs` 文件组织模块。

示例：

```rust
src/
├── main.rs
├── serve/
│   ├── mod.rs
│   ├── rpc.rs
│   ├── controller.rs
```

### 2.2 公共接口

- 仅暴露必要的公共接口，使用 `pub` 修饰符。
- 非公共函数和模块应标记为 `pub(crate)` 或私有。

---

## 3. 错误处理

### 3.1 使用 `Result` 和 `Option`

- 使用 `Result<T, E>` 处理可能失败的操作。
- 使用 `Option<T>` 表示可能为空的值。

示例：

```rust
fn read_file(path: &str) -> Result<String, std::io::Error> {
    std::fs::read_to_string(path)
}
```

### 3.2 错误上下文

- 使用 `anyhow` 或 `thiserror` 提供错误上下文。

示例：

```rust
use anyhow::{Context, Result};

fn read_config(path: &str) -> Result<String> {
    std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path))
}
```

---

## 4. 日志与调试

### 4.1 使用 `tracing` 记录日志

- 使用 `tracing` 记录日志，避免使用 `println!`。

示例：

```rust
use tracing::info;

fn main() {
    info!("Application started");
}
```

### 4.2 日志级别

- 使用适当的日志级别：`trace`、`debug`、`info`、`warn`、`error`。

---

## 5. 测试

### 5.1 单元测试

- 每个模块应包含单元测试，测试文件命名为 `mod.rs` 或 `tests.rs`。

示例：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(2 + 2, 4);
    }
}
```

### 5.2 集成测试

- 集成测试放置在 `tests/` 目录下。

---

## 6. 文档注释

### 6.1 使用 `///` 添加文档注释

- 为公共函数、结构体和模块添加文档注释。

示例：

```rust
/// 计算两个数的和。
///
/// # 参数
///
/// * `a` - 第一个数。
/// * `b` - 第二个数。
///
/// # 返回值
///
/// 返回两个数的和。
fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

---

## 7. 依赖管理

### 7.1 使用 `Cargo.toml`

- 避免引入不必要的依赖。
- 定期更新依赖版本。
- 提交功能开发或 bug 修复时，不要更新依赖版本。
- 使用 `cargo update` 更新依赖版本。
- 使用 `cargo audit` 检查依赖的安全性。
- 使用 `cargo tree` 查看依赖树。
- 使用 `cargo fmt` 格式化代码。
- 使用 `cargo clippy` 检查代码质量。

---

## 8. 性能优化

### 8.1 避免不必要的分配

- 使用 `&str` 而非 `String`，除非需要可变字符串。
- 使用 `Cow`（Clone on Write）避免不必要的克隆。
- 使用 `FastStr` 代替 `String`，提高性能。
- 使用 `Vec` 而非 `Box<[T]>`，除非需要固定大小的数组。
- 使用 `&[T]` 而非 `Vec<T>`，除非需要动态大小的数组。

### 8.2 使用 `tokio` 异步框架

- 避免阻塞操作，使用异步 I/O。
- 避免使用同步锁和同步队列，使用异步锁和异步队列。

---

## 9. 安全性

### 9.1 避免未定义行为

- 避免使用 `unsafe`，除非有充分理由。使用 `unsafe` 时，必须提供详细的注释说明。

```rust
unsafe {
    // Explain why this is safe or necessary
    // e.g., using raw pointers in FFI
    let ptr = std::ptr::null_mut();
    ...
}
```

### 9.2 输入验证

- 对用户输入进行严格验证。

### 9.3 避免使用 `.unwrap()`

- 避免使用 `.unwrap()` 和 `.expect()`，使用 `?` 运算符处理错误。
- 使用 `if let` 或 `match` 处理 `Option` 和 `Result`。

---

## 10. 提交规范

### 10.1 提交信息格式

遵循 [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) 规范。

- 提交信息应包含类型、范围和描述。
- 类型包括：`feat`（新功能）、`enh`（已有功能优化）、`fix`（修复 bug）、`docs`（文档变更）、`ci`（CI 变更）、`refactor` 或 `ref`（重构）、`perf`（性能优化）、`test`（测试相关）、`release`： `chore`（其他变更）。
- 范围为可选项，表示影响的模块或功能，当前范围包括
  - `serve`：服务端相关；
  - `api`：REST API 相关；
  - `grpc`：gRPC API 相关；
  - `agent`：Agent 相关；
  - `explorer`：Explorer 相关；
  - `legacy`：基于查询的数据同步，2.x 数据源；
  - `utils`：工具类；
  - `kafka`、`mongodb`、`mysql`、`postgresql`、`oracle`、`influxdb`、`opentsdb`、`pi` ：数据源相关；
  - `packaging`：打包相关；
  - `ipc`：IPC 相关；
  - `core`：其他核心功能；
- 描述应简洁明了，说明变更的目的和影响。
- 提交信息应以动词开头，使用现在时态。

Commit 格式如下：

```text
<type>(<scope>): <description>

[optional body]

[optional footer]
```

示例：

```text
feat(serve): add new gRPC service

Including new endpoints for user management and data retrieval.

Closes [TS-1234](https://jira.taosdata.com:18080/browse/TS-1234)
```

`footer` 中应使用 `Closes` 或 `Fixes` 关键字，后跟相关的 issue 链接，表示该提交修复了该 issue。

```markdown
fix(serve): fix memory leak in gRPC service

Closes [TS-1234](https://jira.taosdata.com:18080/browse/TS-1234)
```

如果该提交与多个 issue 相关，则可以使用 `Closes` 或 `Fixes` 关键字，后跟多个 issue 链接。

```markdown
fix(serve): fix memory leak in gRPC service

Closes
- [TS-1234](https://jira.taosdata.com:18080/browse/TS-1234)
- [TS-5678](https://jira.taosdata.com:18080/browse/TS-5678)
```

### 10.2 提交前检查

- 添加必要的测试用例。
- 确保代码通过 `cargo fmt` 和 `cargo clippy` 检查（执行 `cargo make pre-commit` 即可）。

### 10.3 提交流程

- Bug 修复：向 `main` 分支提交 PR。
- 新功能：向 `3.0` 分支提交 PR。
- 文档更新：用户文档向 TDengine 仓库提交 PR，如果是新功能的文档更新，则向 `3.0` 分支提交 PR，否则向 `main` 分支提交 PR。

如果 PR 涉及到多个模块的变更，建议将 PR 拆分为多个小的 PR 提交。

如果 PR 提交后仍有未解决的问题，建议在 PR 中使用 Draft 标记，表示该 PR 仍在开发中。

如果 PR 提交后需要进行修改，建议使用 `git commit --amend` 命令修改提交信息。

如果 PR 基线与目标分支最新版本差异较大，建议使用 `git rebase` 命令修改提交基线。

代码提交后，使用 GitHub Actions 进行自动化测试和构建。

### 10.5 代码审查

提交 PR 后，等待其他开发者进行代码审查。代码审查应包括以下内容：

- 代码风格是否符合规范；
- 代码逻辑是否正确；
- 是否添加了必要的测试用例；
- 是否添加了必要的文档注释；
- 是否遵循了安全性和性能优化的建议；
- 是否遵循了错误处理的建议；
- 是否遵循了日志与调试的建议；
- 是否遵循了依赖管理的建议；
- 是否遵循了模块组织的建议；
- 是否遵循了提交规范的建议；
- 是否遵循了其他开发规范的建议。

### 10.6 合并 PR

在代码审查通过后，合并 PR。合并 PR 时，应选择 `Squash and merge` 选项，将所有提交合并为一个提交。

例外情况：

- `main` 或 `3.0` 或 `3.3.6` 分支之间进行合并时，使用 `Merge` 选项。

---

## 附录

- [Rust 编程语言](https://doc.rust-lang.org/book/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html)
- [Rust 代码风格指南](https://doc.rust-lang.org/style-guide/index.html)
- [Rust Unsafe Code Guidelines](https://doc.rust-lang.org/nomicon/)
- [Cargo 包管理工具](https://doc.rust-lang.org/cargo/)
- [Cargo Make](https://sagiegurari.github.io/cargo-make/)
- [tracing 文档](https://docs.rs/tracing/)
- [anyhow 文档](https://docs.rs/anyhow/)
