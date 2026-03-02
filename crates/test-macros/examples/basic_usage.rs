//! `taosx-test-macros` 使用示例。
//!
//! 可以在本 crate 目录下运行：
//! ```bash
//! cargo test --examples
//! ```
//! 来查看这些示例是否能正常展开并运行。

fn main() {}

#[cfg(test)]
mod tests {

    use taosx_test_macros::integration_test;

    /// 同步参数化测试示例，使用 `test` 作为 runner。
    ///
    /// 展开后等价于：
    /// - 带有 `#[test]` 的 wrapper 函数
    /// - 多个调用 `sync_example_impl(a, b)` 的用例
    #[integration_test(test, a = [1, 2], b = ["alpha", "beta"])]
    fn sync_example(a: i32, b: &str) {
        tracing::info!("[{}] a={a}, b={b}", test_name);
    }

    /// 异步参数化测试示例，使用 `tokio::test(flavor = \"multi_thread\", worker_threads = 1)`。
    ///
    /// 这也验证了形如
    /// `#[integration_test(tokio::test(flavor = \"multi_thread\", worker_threads = 1), case = [1, 2])]`
    /// 的写法可以被正确解析并展开。
    #[integration_test(
        tokio::test,
        case = [1, 2]
    )]
    async fn async_example(case: i32) {
        tracing::info!("[{}] case={case}", test_name);
    }
}
