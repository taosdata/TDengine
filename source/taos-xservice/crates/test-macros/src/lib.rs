use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Expr, Ident, ItemFn, Lit, LitStr, Result as SynResult, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

struct ParamSpec {
    name: Ident,
    values: Vec<Expr>,
}

struct ParamArgs {
    params: Vec<ParamSpec>,
}

impl Parse for ParamArgs {
    fn parse(input: ParseStream<'_>) -> SynResult<Self> {
        let mut params = Vec::new();
        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            // 支持两种写法：
            // 1) name = [expr1, expr2, ...]  （Punctuated 支持可选的尾随逗号）
            // 2) name = expr                 （单个任意表达式）
            let values: Vec<Expr> = if input.peek(syn::token::Bracket) {
                let content;
                syn::bracketed!(content in input);
                let punctuated: Punctuated<Expr, Token![,]> =
                    content.parse_terminated(Expr::parse, Token![,])?;
                punctuated.into_iter().collect()
            } else {
                vec![input.parse()?]
            };

            params.push(ParamSpec { name, values });

            if input.peek(Token![,]) {
                let _ = input.parse::<Token![,]>();
            } else {
                break;
            }
        }

        Ok(ParamArgs { params })
    }
}

/// Runner: #[test] 或 #[tokio::test(...)]
enum TestRunner {
    Test,
    TokioTest(Option<TokenStream2>),
}

/// 宏参数：首参为 test 或 tokio::test（可带括号），逗号后为 ParamArgs
struct IntegrationTestArgs {
    runner: TestRunner,
    params: ParamArgs,
}

impl Parse for IntegrationTestArgs {
    fn parse(input: ParseStream<'_>) -> SynResult<Self> {
        let runner = if input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "#[integration_test] requires a first argument specifying the test runner: `test` or `tokio::test`, for example #[integration_test(test, ...)] or #[integration_test(tokio::test(flavor = \"multi_thread\", worker_threads = 1), ...)].",
            ));
        } else if input.peek(Ident) {
            let ident: Ident = input.parse()?;
            if ident == "test" {
                TestRunner::Test
            } else if ident == "tokio" && input.peek(Token![::]) {
                let _: Token![::] = input.parse()?;
                let test_ident: Ident = input.parse()?;
                if test_ident == "test" {
                    let tokio_args = if input.peek(syn::token::Paren) {
                        let content;
                        syn::parenthesized!(content in input);
                        // 将括号内的任意 TokenStream2 原样保存下来，以便生成 #[tokio::test(...)]
                        let inner: TokenStream2 = content.parse()?;
                        Some(inner)
                    } else {
                        None
                    };
                    TestRunner::TokioTest(tokio_args)
                } else {
                    return Err(syn::Error::new(
                        test_ident.span(),
                        "#[integration_test] runner must be `test` or `tokio::test`.",
                    ));
                }
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    "#[integration_test] the first argument must be `test` or `tokio::test`.",
                ));
            }
        } else {
            return Err(syn::Error::new(
                input.span(),
                "#[integration_test] the first argument must be `test` or `tokio::test`.",
            ));
        };

        if input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
        }

        let params = input.parse()?;
        Ok(IntegrationTestArgs { runner, params })
    }
}

impl std::fmt::Debug for IntegrationTestArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntegrationTestArgs").finish()
    }
}

pub(crate) fn build_combinations(params: &[ParamSpec]) -> Vec<Vec<Expr>> {
    fn helper(idx: usize, params: &[ParamSpec], current: &mut Vec<Expr>, out: &mut Vec<Vec<Expr>>) {
        if idx == params.len() {
            out.push(current.clone());
            return;
        }

        for v in &params[idx].values {
            current.push(v.clone());
            helper(idx + 1, params, current, out);
            current.pop();
        }
    }

    let mut out = Vec::new();
    let mut current = Vec::new();
    if !params.is_empty() {
        helper(0, params, &mut current, &mut out);
    } else {
        out.push(Vec::new());
    }
    out
}

#[proc_macro_attribute]
pub fn integration_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as IntegrationTestArgs);
    let param_args = &args.params;
    let mut item_fn = parse_macro_input!(input as ItemFn);

    let is_tokio_test = matches!(args.runner, TestRunner::TokioTest(_));
    let runner_attr: TokenStream2 = match &args.runner {
        TestRunner::Test => quote!(#[test]),
        TestRunner::TokioTest(None) => quote!(#[tokio::test]),
        TestRunner::TokioTest(Some(tokio_args)) => quote!(#[tokio::test(#tokio_args)]),
    };

    // 原始函数名和可见性
    let fn_name = item_fn.sig.ident.clone();
    let vis = item_fn.vis.clone();

    // 收集文档注释，合并为一行字符串
    let mut doc_lines: Vec<String> = Vec::new();
    for attr in &item_fn.attrs {
        if attr.path().is_ident("doc") {
            // 形如 #[doc = "..."]
            if let Ok(nv) = attr.meta.require_name_value()
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(lit_str),
                    ..
                }) = &nv.value
            {
                doc_lines.push(lit_str.value());
            }
        }
    }
    let doc_lit: Option<LitStr> = if doc_lines.is_empty() {
        None
    } else {
        Some(LitStr::new(&doc_lines.join("\n"), item_fn.sig.ident.span()))
    };

    // Collect test-related attributes to apply to generated test functions.
    let test_attrs: Vec<Attribute> = item_fn
        .attrs
        .iter()
        .filter(|attr| {
            attr.path()
                .get_ident()
                .map(|id| id == "ignore" || id == "should_panic" || id == "cfg")
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    // Remove test-related attrs from the impl fn (runner 已在宏首参中指定).
    item_fn.attrs.retain(|attr| {
        attr.path()
            .get_ident()
            .map(|id| id != "ignore" && id != "should_panic" && id != "cfg")
            .unwrap_or(true)
    });

    let sig = &item_fn.sig;
    let inputs = &sig.inputs;
    let output = &sig.output;
    let block = &item_fn.block;

    // #[test] 仅支持同步函数；#[tokio::test] 支持 async fn
    if sig.asyncness.is_some() && !is_tokio_test {
        return syn::Error::new_spanned(
            sig.fn_token,
            "#[integration_test] async fn is only supported with the `tokio::test` runner; use #[integration_test(tokio::test, ...)] or #[integration_test(tokio::test(flavor = \"multi_thread\", worker_threads = 1), ...)].",
        )
        .to_compile_error()
        .into();
    }

    // 提取形参标识符
    let mut param_idents = Vec::<Ident>::new();
    for arg in inputs {
        match arg {
            syn::FnArg::Typed(pat_type) => {
                if let syn::Pat::Ident(ref pat_ident) = *pat_type.pat {
                    param_idents.push(pat_ident.ident.clone());
                } else {
                    return syn::Error::new_spanned(
                        &pat_type.pat,
                        "#[integration_test] only simple parameter form is supported, e.g. `fn f(x: T, y: U)`",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            syn::FnArg::Receiver(_) => {
                return syn::Error::new_spanned(
                    arg,
                    "#[integration_test] methods with self are not supported",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    // 建立参数名到下标的映射
    use std::collections::HashMap;
    let mut param_index_map = HashMap::<String, usize>::new();
    for (i, ident) in param_idents.iter().enumerate() {
        param_index_map.insert(ident.to_string(), i);
    }

    // 校验 attribute 中声明的参数名都在函数签名中
    for p in &param_args.params {
        if !param_index_map.contains_key(&p.name.to_string()) {
            return syn::Error::new_spanned(
                &p.name,
                format!(
                    "#[integration_test] parameter `{}` is not in the function signature",
                    p.name
                ),
            )
            .to_compile_error()
            .into();
        }
    }

    // 要求函数中所有参数都由 attribute 提供值，避免出现未初始化的参数
    if !param_idents.is_empty() {
        for ident in &param_idents {
            if !param_args.params.iter().any(|p| p.name == *ident) {
                return syn::Error::new_spanned(
                    ident,
                    format!(
                        "#[integration_test] parameter `{}` must have a value list, e.g. {} = [expr1, expr2]",
                        ident,
                        ident
                    ),
                )
                .to_compile_error()
                .into();
            }
        }
    }

    // 拒绝空取值列表，避免生成零个测试而静默跳过
    for p in &param_args.params {
        if p.values.is_empty() {
            return syn::Error::new_spanned(
                &p.name,
                format!(
                    "#[integration_test] parameter `{}` has an empty value list `[]`; provide at least one value, e.g. {} = [expr]",
                    p.name,
                    p.name
                ),
            )
            .to_compile_error()
            .into();
        }
    }

    // 生成参数组合（笛卡尔积）
    let combinations = build_combinations(&param_args.params);

    // 为每种参数组合生成一个独立的测试用例函数：fn_name_case{idx}；test_name 在方法体内注入为变量
    let mut case_tests: Vec<TokenStream2> = Vec::new();

    for (case_idx, combo) in combinations.iter().enumerate() {
        let combo_map: std::collections::HashMap<Ident, Expr> = param_args
            .params
            .iter()
            .zip(combo.iter())
            .map(|(p, expr)| (p.name.clone(), expr.clone()))
            .collect();

        let case_fn_name = format_ident!("{}_case{}", fn_name, case_idx);
        let _case_idx_lit = syn::LitInt::new(&case_idx.to_string(), fn_name.span());

        // test_name 与 ts 在同一代码块，且 test_name 在 ts 之前注入
        let test_name_binding = quote! {
            let test_name = ::core::stringify!(#case_fn_name);
        };
        let param_bindings: Vec<TokenStream2> = param_idents
            .iter()
            .map(|ident| {
                let expr = combo_map
                    .get(ident)
                    .expect("Internal error: parameter value not found in combo map");
                quote! { let #ident = #expr; }
            })
            .collect();

        let doc_stmt = doc_lit.as_ref().map(|doc| {
            quote! {
                ::tracing::info!("{}", #doc);
            }
        });

        // 用块包成单一表达式，否则 let result = doc_stmt; block 会只取 doc_stmt 的值 (())
        let body_after_setup = quote! {
            {
                #( #param_bindings )*
                #doc_stmt
                #block
            }
        };

        let local_time_format = quote! {
            ::tracing_subscriber::fmt::format().with_timer(::tracing_subscriber::fmt::time::ChronoLocal::default())
        };
        if item_fn.sig.asyncness.is_some() {
            case_tests.push(quote! {
                #(#test_attrs)*
                #runner_attr
                #vis async fn #case_fn_name() #output {
                    #test_name_binding
                    ::dotenv::dotenv().ok();
                    use ::tracing_subscriber::layer::SubscriberExt;
                    let dir = ::std::env::current_dir().expect(
                        "integration_trace_test: failed to get current working directory for log path",
                    );
                    let log_dir = dir.join("log");
                    ::std::fs::create_dir_all(&log_dir)
                        .unwrap_or_else(|e| panic!("integration_trace_test: failed to create log directory {log_dir:?}: {e}"));
                    let file_appender = ::tracing_appender::rolling::RollingFileAppender::builder()
                        .rotation(::tracing_appender::rolling::Rotation::DAILY)
                        .filename_prefix("integration_test")
                        .filename_suffix("log")
                        .build(&log_dir)
                        .expect("failed to create integration test rolling log file");

                    let subscriber = ::tracing_subscriber::registry()
                        .with(::tracing_subscriber::EnvFilter::new("info"))
                        .with(
                            ::tracing_subscriber::fmt::layer()
                                .with_ansi(false)
                                .with_writer(file_appender)
                                .event_format(#local_time_format),
                        )
                        .with(
                            ::tracing_subscriber::fmt::layer()
                                .with_writer(::std::io::stdout)
                                .event_format(#local_time_format),
                        );

                    let _guard = ::tracing::subscriber::set_default(subscriber);
                    let result = #body_after_setup;
                    drop(_guard);
                    result
                }
            });
        } else {
            case_tests.push(quote! {
                #(#test_attrs)*
                #runner_attr
                #vis fn #case_fn_name() #output {
                    #test_name_binding
                    ::dotenv::dotenv().ok();
                    use ::tracing_subscriber::layer::SubscriberExt;
                    let dir = ::std::env::current_dir().expect(
                        "integration_trace_test: failed to get current working directory for log path",
                    );
                    let log_dir = dir.join("log");
                    ::std::fs::create_dir_all(&log_dir)
                        .unwrap_or_else(|e| panic!("integration_trace_test: failed to create log directory {log_dir:?}: {e}"));
                    let file_appender = ::tracing_appender::rolling::RollingFileAppender::builder()
                        .rotation(::tracing_appender::rolling::Rotation::DAILY)
                        .max_log_files(1usize)
                        .filename_prefix("integration_test")
                        .filename_suffix("log")
                        .build(&log_dir)
                        .expect("failed to create integration test rolling log file");

                    let subscriber = ::tracing_subscriber::registry()
                        .with(::tracing_subscriber::EnvFilter::new("info"))
                        .with(
                            ::tracing_subscriber::fmt::layer()
                                .with_ansi(false)
                                .with_writer(file_appender)
                                .event_format(#local_time_format),
                        )
                        .with(
                            ::tracing_subscriber::fmt::layer()
                                .with_writer(::std::io::stdout)
                                .event_format(#local_time_format),
                        );

                    ::tracing::subscriber::with_default(subscriber, || {
                        #body_after_setup
                    })
                }
            });
        }
    }

    let expanded = quote! {
        #( #case_tests )*
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_test_args_parse_test_runner_only() {
        let args: IntegrationTestArgs = syn::parse_str("test").unwrap();
        match args.runner {
            TestRunner::Test => {}
            _ => panic!("expected Test runner"),
        }
        assert!(args.params.params.is_empty());
    }

    #[test]
    fn integration_test_args_parse_tokio_test_no_args() {
        let args: IntegrationTestArgs = syn::parse_str("tokio::test").unwrap();
        match args.runner {
            TestRunner::TokioTest(None) => {}
            _ => panic!("expected TokioTest(None) runner"),
        }
    }

    #[test]
    fn integration_test_args_parse_tokio_test_with_args() {
        let args: IntegrationTestArgs =
            syn::parse_str("tokio::test(flavor = \"multi_thread\", worker_threads = 1)").unwrap();
        match args.runner {
            TestRunner::TokioTest(Some(ts)) => {
                let s = ts.to_string();
                assert!(s.contains("flavor"));
                assert!(s.contains("multi_thread"));
                assert!(s.contains("worker_threads"));
            }
            _ => panic!("expected TokioTest(Some(_)) runner"),
        }
    }

    #[test]
    fn integration_test_args_parse_tokio_test_with_args_and_params() {
        let args: IntegrationTestArgs = syn::parse_str(
            "tokio::test(flavor = \"multi_thread\", worker_threads = 1), a = [1, 2]",
        )
        .unwrap();
        match args.runner {
            TestRunner::TokioTest(Some(ts)) => {
                let s = ts.to_string();
                assert!(s.contains("flavor"));
                assert!(s.contains("multi_thread"));
                assert!(s.contains("worker_threads"));
            }
            _ => panic!("expected TokioTest(Some(_)) runner"),
        }
        assert_eq!(args.params.params.len(), 1);
        assert_eq!(args.params.params[0].name, "a");
    }

    #[test]
    fn integration_test_args_parse_runner_and_params() {
        let args: IntegrationTestArgs = syn::parse_str("test, a = [1, 2]").unwrap();
        match args.runner {
            TestRunner::Test => {}
            _ => panic!("expected Test runner"),
        }
        assert_eq!(args.params.params.len(), 1);
        assert_eq!(args.params.params[0].name, "a");
    }

    #[test]
    fn integration_test_args_reject_invalid_runner() {
        let err = syn::parse_str::<IntegrationTestArgs>("something_else").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("the first argument must be `test` or `tokio::test`")
                || msg.contains("must be `test` or `tokio::test`"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn param_args_parse_empty() {
        let args: ParamArgs = syn::parse_str("").unwrap();
        assert!(args.params.is_empty());
    }

    #[test]
    fn param_args_parse_single_list() {
        let args: ParamArgs = syn::parse_str("a = [1, 2, 3]").unwrap();
        assert_eq!(args.params.len(), 1);
        assert_eq!(args.params[0].name.to_string(), "a");
        assert_eq!(args.params[0].values.len(), 3);
    }

    #[test]
    fn param_args_parse_empty_list_a_equals_empty_bracket() {
        let args: ParamArgs = syn::parse_str("a = []").unwrap();
        assert_eq!(args.params.len(), 1);
        assert_eq!(args.params[0].name.to_string(), "a");
        assert!(
            args.params[0].values.is_empty(),
            "a = [] should parse to one param with empty values (macro rejects it later)"
        );
    }

    #[test]
    fn param_args_parse_single_expr() {
        let args: ParamArgs = syn::parse_str("x = 42").unwrap();
        assert_eq!(args.params.len(), 1);
        assert_eq!(args.params[0].name.to_string(), "x");
        assert_eq!(args.params[0].values.len(), 1);
    }

    #[test]
    fn param_args_parse_multiple() {
        let args: ParamArgs = syn::parse_str("a = [1, 2], b = [3], c = true").unwrap();
        assert_eq!(args.params.len(), 3);
        assert_eq!(args.params[0].name.to_string(), "a");
        assert_eq!(args.params[0].values.len(), 2);
        assert_eq!(args.params[1].name.to_string(), "b");
        assert_eq!(args.params[1].values.len(), 1);
        assert_eq!(args.params[2].name.to_string(), "c");
        assert_eq!(args.params[2].values.len(), 1);
    }

    #[test]
    fn build_combinations_empty_params() {
        let args: ParamArgs = syn::parse_str("").unwrap();
        let combos = build_combinations(&args.params);
        assert_eq!(combos.len(), 1);
        assert!(combos[0].is_empty());
    }

    #[test]
    fn build_combinations_single_param() {
        let args: ParamArgs = syn::parse_str("a = [1, 2, 3]").unwrap();
        let combos = build_combinations(&args.params);
        assert_eq!(combos.len(), 3);
    }

    #[test]
    fn build_combinations_cartesian() {
        let args: ParamArgs = syn::parse_str("a = [1, 2], b = [3, 4]").unwrap();
        let combos = build_combinations(&args.params);
        assert_eq!(combos.len(), 4); // 2 * 2
    }

    #[test]
    fn build_combinations_mixed_list_and_single() {
        let args: ParamArgs = syn::parse_str("a = [1, 2], b = 0").unwrap();
        let combos = build_combinations(&args.params);
        assert_eq!(combos.len(), 2);
    }
}
