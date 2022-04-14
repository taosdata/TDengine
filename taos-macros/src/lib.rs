use proc_macro2::*;
use quote::*;

extern crate proc_macro;

mod cfg;
mod test;

fn fn_name(tokens: &[TokenTree]) -> String {
    tokens[tokens
        .iter()
        .position(|s| match s {
            TokenTree::Ident(ident) if ident.to_string() == "fn" => true,
            _ => false,
        })
        .expect("fn")
        + 1]
    .to_string()
}

#[proc_macro_attribute]
pub fn taos_cfg(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let cfg = TokenStream::from(attr)
        .into_iter()
        .next()
        .expect("condition is required");
    let mut item: Vec<_> = TokenStream::from(item).into_iter().collect();

    let i = item
        .iter()
        .position(|t| matches!(t, TokenTree::Group(_)))
        .expect("has group");
    assert!(i > 0);

    let (prefix, group) = item.split_at(i);

    let mut c_tokens = Vec::new();
    let mut r_tokens = Vec::new();
    if prefix[0].to_string() == "extern" {
        assert!(group.len() == 1);
        let group = &group[0];

        if let TokenTree::Group(group) = group {
            let tokens: Vec<_> = group.stream().into_iter().collect();
            // panic!("len: {}, {tokens:?}", tokens.len());

            let mut fn_tokens = Vec::new();

            for token in tokens {
                match &token {
                    TokenTree::Punct(p) => {
                        let fn_tokens: Vec<_> = fn_tokens.drain(..).collect();
                        let fn_name = fn_name(&fn_tokens);
                        let fn_item = TokenStream::from_iter(fn_tokens);
                        let fn_c = quote! {
                            #fn_item;
                        };
                        let fn_rs = quote! {

                            #[cfg(not(#cfg))]
                            #[no_mangle]
                            #fn_item {
                                panic!("function {} is not supported in this build", stringify!(#fn_name));
                            }
                        };
                        c_tokens.extend(fn_c);
                        r_tokens.extend(fn_rs);
                    }
                    _ => {
                        fn_tokens.push(token);
                    }
                }
            }
        }
    } else {
        let fn_name = fn_name(&item);
        c_tokens.extend(item.clone());
        item.pop();
        // r_tokens.extend(item);

        let fn_item = TokenStream::from_iter(item);

        let g = quote! {
            #[cfg(not(#cfg))]
            #[no_mangle]
            #fn_item {
                panic!("function {} is not supported in this build", stringify!(#fn_name));
            }


        };
        r_tokens.extend(g.into_iter())
    }

    let c_tokens = TokenStream::from_iter(c_tokens);
    let no_mangles = TokenStream::from_iter(r_tokens);

    quote! {
        #[cfg(#cfg)]
        extern "C" {
            #c_tokens
        }

        #no_mangles
    }
    .into()
}

/// A powerful test macro for taos.
///
/// ```rust
/// #[test(databases = true, naming = "uuid-v1", dropping = "always")]
/// async fn show_databases(taos: &Taos, database: &str) -> Result<()> {
///     let _ = taos.databases().await;
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    test::test(attr, item)
}
