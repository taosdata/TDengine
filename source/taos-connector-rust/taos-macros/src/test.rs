use proc_macro2::*;
use quote::*;

#[derive(Debug, PartialEq, Eq)]
enum Requires {
    None,
    TaosOnly,
    WithDatabase,
    WithMulti,
}

impl Default for Requires {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
struct Params {
    is_async: bool,
    fn_name: String,
    requires: Requires,
    is_result: bool,
}

impl Params {
    fn from_tokens(item: TokenStream) -> Self {
        // let item_fn: syn::ItemFn = match syn::parse(item.clone().into()) {
        //     Ok(item) => item,
        //     Err(e) => panic!("#[test] only used in function"),
        // };
        // dbg!(item_fn);
        let tokens: Vec<_> = item.into_iter().collect();
        let mut iter = tokens.iter();

        let mut params = Params::default();
        while let Some(token) = iter.next() {
            use TokenTree::*;
            match token {
                // Function comment
                Punct(punct) if punct.as_char() == '#' => {
                    let _ = iter.next();
                }
                // Check if is async function
                Ident(ident) if *ident == "async" => {
                    params.is_async = true;
                }
                // Parse function name and parameters
                Ident(ident) if *ident == "fn" => {
                    if let Some(Ident(ident)) = iter.next() {
                        params.fn_name = ident.to_string();
                    } else {
                        panic!("can't parse fn name");
                    }

                    if let Some(Group(fn_params)) = iter.next() {
                        let tt: Vec<_> = fn_params.stream().into_iter().collect();
                        params.requires = parse_database_requires(&tt);
                    }
                }
                // Parse return type
                Punct(punct) if punct.as_char() == '-' => {
                    // >
                    let _ = iter.next();
                    let mut return_type_tt = Vec::new();
                    if let Some(ident) = iter.next() {
                        // dbg!(&ty);
                        if let Ident(ty) = ident {
                            if ty.to_string().ends_with("Result") {
                                params.is_result = true;
                                break;
                            }
                        }
                        return_type_tt.push(ident.clone());
                    }

                    for token in iter.by_ref() {
                        if let Group(group) = token {
                            let v = group.delimiter();
                            if v == Delimiter::Brace {
                                break;
                            }
                        }
                        return_type_tt.push(token.clone());
                    }

                    params.is_result = return_type_tt.into_iter().any(|t| {
                        if let Ident(ty) = t {
                            ty.to_string().ends_with("Result")
                        } else {
                            false
                        }
                    });
                }
                _ => {
                    // dbg!(&token);
                }
            }
        }
        params
    }
}

#[derive(Default, Debug)]
struct Attr {
    rt: Option<String>,
    databases: Option<usize>,
    naming: Option<Literal>,
    precision: Option<Literal>,
    dropping: Option<Literal>,
    log_level: Option<Literal>,
}

impl PartialEq for Attr {
    fn eq(&self, other: &Self) -> bool {
        macro_rules! _literal_eq {
            ($attr:ident) => {
                match (self.$attr.as_ref(), other.$attr.as_ref()) {
                    (None, None) => true,
                    (Some(lhs), Some(rhs)) if lhs.to_string() == rhs.to_string() => true,
                    _ => false,
                }
            };
        }
        self.rt == other.rt
            && self.databases == other.databases
            && _literal_eq!(naming)
            && _literal_eq!(precision)
            && _literal_eq!(dropping)
    }
}

impl Attr {
    fn from_iter(iter: impl IntoIterator<Item = TokenTree>) -> Attr {
        let mut iter = iter.into_iter();
        let mut attr = Attr::default();
        while let Some(t) = iter.next() {
            match t {
                TokenTree::Ident(ident) if ident == "databases" => {
                    const EXPECT: &str = "expect `[test(databases = \"\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => {
                            attr.databases = Some(value.to_string().parse().expect(EXPECT));
                        }
                        _ => unreachable!("expect `[test(databases = \"\")]`"),
                    }
                }
                TokenTree::Ident(ident) if ident == "rt" => {
                    const EXPECT: &str = "`[test(rt = \"tokio|async_std\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => attr.rt = Some(value.to_string()),
                        _ => unreachable!("expect {EXPECT}"),
                    }
                }
                TokenTree::Ident(ident) if ident == "naming" => {
                    const EXPECT: &str =
                        "`[test(naming = \"random|uuid-v1|sequential|<custom>\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => attr.naming = Some(value.clone()),
                        _ => unreachable!("expect {EXPECT}"),
                    }
                }
                TokenTree::Ident(ident) if ident == "dropping" => {
                    const EXPECT: &str = "`[test(drop = \"none|before|after|always\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => attr.dropping = Some(value.clone()),
                        _ => unreachable!("expect {EXPECT}"),
                    }
                }
                TokenTree::Ident(ident) if ident == "precision" => {
                    const EXPECT: &str = "`[test(drop = \"ms|us|ns|random|cyclic\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => attr.precision = Some(value.clone()),
                        _ => unreachable!("expect {EXPECT}"),
                    }
                }

                TokenTree::Ident(ident) if ident == "log_level" || ident == "log-level" => {
                    const EXPECT: &str = "`[test(log_level = \"trace|debug|info|warn|error\")]`";
                    let _ = iter.next();
                    let value = iter.next().expect(EXPECT);
                    match value {
                        TokenTree::Literal(value) => attr.log_level = Some(value.clone()),
                        _ => unreachable!("expect {EXPECT}"),
                    }
                }
                _ => (),
            }
        }
        attr
    }

    fn naming_token_stream(&self) -> TokenStream {
        match &self.naming {
            Some(naming) => naming.into_token_stream(),
            None => quote!(()),
        }
    }

    fn drop_token_stream(&self) -> TokenStream {
        match &self.dropping {
            Some(drop) => drop.into_token_stream(),
            None => quote!(()),
        }
    }

    fn precision_token_stream(&self) -> TokenStream {
        match &self.precision {
            Some(precision) => precision.into_token_stream(),
            None => quote!(()),
        }
    }

    fn crate_token_stream(&self) -> TokenStream {
        let crate_name = std::env::var("CARGO_PKG_NAME").unwrap();

        if crate_name == "taos" {
            quote!(crate)
        } else {
            quote!(taos)
        }
    }

    fn databases(&self, requires: &Requires) -> usize {
        match requires {
            Requires::None | Requires::TaosOnly => 0,
            Requires::WithDatabase => 1,
            Requires::WithMulti => self.databases.unwrap_or(1),
        }
    }

    fn log_level(&self) -> TokenStream {
        match &self.log_level {
            Some(log_level) => log_level.into_token_stream(),
            None => quote!(()),
        }
    }

    fn with_params(&self, tokens: TokenStream) -> TokenStream {
        let Params {
            is_async,
            fn_name,
            requires,
            is_result,
        } = Params::from_tokens(tokens.clone());

        let fn_name = Ident::new(&fn_name, Span::call_site());
        let naming = self.naming_token_stream();
        let drop = self.drop_token_stream();
        let precision = self.precision_token_stream();
        let log_level = self.log_level();
        let _crate = self.crate_token_stream();
        let databases = self.databases(&requires);

        let common = quote! {
            #_crate::helpers::tests::Common::default()
                .log_level(#log_level)
                .init()?;
        };

        let builder = quote! {
            let __taos = #_crate::helpers::tests::Builder::default()
               .naming(#naming)
               .precision(#precision)
               .dropping(#drop)
               .databases(#databases)
               .build()?;
            let _taos = __taos.taos();
        };
        let builder = match requires {
            Requires::None => quote!(
                #fn_name()
            ),
            Requires::TaosOnly => quote! {
                #builder
                #fn_name(_taos)
            },
            Requires::WithDatabase => quote! {
                #builder
                let _database = __taos.default_database();
                #fn_name(_taos, _database)
            },
            Requires::WithMulti => quote! {
                #builder
                let _databases = __taos.databases();
                #fn_name(_taos, &_databases)
            },
        };

        match (is_async, is_result) {
            (true, true) => {
                quote! {
                    #[tokio::test]
                    async fn #fn_name() -> anyhow::Result<()> {
                        #tokens
                        #common
                        #builder.await?;
                        Ok(())
                    }
                }
            }
            (true, false) => {
                quote! {
                    #[tokio::test]
                    async fn #fn_name() -> anyhow::Result<()> {
                        #tokens
                        #common
                        #builder.await;
                        Ok(())
                    }
                }
            }
            (false, true) => {
                quote! {
                    #[std::prelude::v1::test]
                    fn #fn_name() -> anyhow::Result<()> {
                        #tokens
                        #common
                        #builder?;
                        Ok(())
                    }
                }
            }
            _ => {
                quote! {
                    #[std::prelude::v1::test]
                    fn #fn_name() -> anyhow::Result<()> {
                        #tokens
                        #common
                        #builder;
                        Ok(())
                    }
                }
            }
        }
    }
}

fn parse_database_requires(params: &[TokenTree]) -> Requires {
    let mut iter = params.iter();
    let mut has_taos = false;
    let mut ret = Requires::None;
    while let Some(t) = iter.next() {
        use TokenTree::*;
        match t {
            Ident(ident) if *ident == "taos" || *ident == "_taos" => {
                let _ = iter.next();
                let _ = iter.next();
                let ty = iter.next().unwrap();
                assert!(ty.to_string().ends_with("Taos"));
                has_taos = true;
                ret = Requires::TaosOnly;
            }
            Ident(ident) if *ident == "database" || *ident == "_database" => {
                assert!(
                    has_taos,
                    "please use test fn parameters: `taos: &Taos, database: &str`"
                );
                ret = Requires::WithDatabase;
            }
            Ident(ident) if *ident == "databases" || *ident == "_databases" => {
                assert!(
                    has_taos,
                    "please use test fn parameters: `taos: &Taos, databases: &[String]`"
                );
                ret = Requires::WithMulti;
            }
            _ => (),
        }
    }
    ret
}

pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    Attr::from_iter(TokenStream::from(attr))
        .with_params(TokenStream::from(item))
        .into()
}

#[cfg(test)]
mod tests {
    use proc_macro2::*;
    use quote::quote;

    use super::Attr;

    #[test]
    fn default_sync() {
        let attr = Attr::default();
        let fn_item = quote! {
            fn test_fn() { }
        };
        let tokens = attr.with_params(fn_item);
        let expect = quote! {
            #[std::prelude::v1::test]
            fn test_fn() -> anyhow::Result<()> {
                fn test_fn() { }

                taos::helpers::tests::Common::default()
                    .log_level(())
                    .init()?;
                test_fn();
                Ok(())
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }

    #[test]
    fn default_async() {
        let attr = Attr::default();
        let fn_item = quote! {
            async fn async_simple() { }
        };
        let tokens = attr.with_params(fn_item);
        let expect = quote! {
            #[tokio::test]
            async fn async_simple() -> anyhow::Result<()> {
                async fn async_simple() { }
                taos::helpers::tests::Common::default()
                    .log_level(())
                    .init()?;
                async_simple().await;
                Ok(())
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }

    #[test]
    fn sync_with_result() {
        let attr = Attr::default();
        let fn_item = quote! {
            fn test() -> Result<()> {
                Ok(())
            }
        };
        let tokens = attr.with_params(fn_item);
        let expect = quote! {
            #[std::prelude::v1::test]
            fn test() -> anyhow::Result<()> {
                fn test() -> Result<()> {
                    Ok(())
                }
                taos::helpers::tests::Common::default()
                    .log_level(())
                    .init()?;
                test()?;
                Ok(())
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }

    #[test]
    fn async_with_result() {
        let attr = Attr::default();
        let fn_item = quote! {
            async fn test() -> Result<()> {
                Ok(())
            }
        };
        let tokens = attr.with_params(fn_item);
        let expect = quote! {
            #[tokio::test]
            async fn test() -> anyhow::Result<()> {
                async fn test() -> Result<()> {
                    Ok(())
                }
                taos::helpers::tests::Common::default()
                    .log_level(())
                    .init()?;
                test().await?;
                Ok(())
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }
    #[test]
    fn attr_from_iter() {
        let attr = quote! {
            databases = 10, rt = "tokio", naming = "random", dropping = "always"
        };

        let attr = super::Attr::from_iter(attr);

        assert_eq!(
            attr,
            Attr {
                databases: Some(10),
                rt: Some("\"tokio\"".to_string()),
                naming: Some(Literal::string("random")),
                precision: None,
                dropping: Some(Literal::string("always")),
                log_level: None,
            }
        );

        let fn_item = quote! {
            /// Comment.
            ///
            /// Long comment.
            async fn test_a(taos: &Taos, database: &str) -> anyhow::Result<()> {
                Ok(())
            }
        };
        let params = super::Params::from_tokens(fn_item.clone());
        dbg!(params);
        let tokens = attr.with_params(fn_item);

        let expect = quote! {
            #[tokio::test]
            async fn test_a() -> anyhow::Result<()> {
                /// Comment.
                ///
                /// Long comment.
                async fn test_a(taos: &Taos, database: &str) -> anyhow::Result<()> {
                    Ok(())
                }
                taos::helpers::tests::Common::default()
                    .log_level(())
                    .init()?;
                let __taos = taos::helpers::tests::Builder::default()
                       .naming("random")
                       .precision(())
                       .dropping("always")
                       .databases(1usize)
                       .build()?;
                let _taos = __taos.taos();
                let _database = __taos.default_database();
                test_a(_taos, _database).await?;
                Ok(())
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
        dbg!(tokens.to_string());
    }
}
