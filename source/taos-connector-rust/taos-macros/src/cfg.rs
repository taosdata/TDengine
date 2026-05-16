use proc_macro2::*;
use quote::*;
use syn::{ForeignItemFn, ItemFn};

/// Replace official `[cfg]` macro to add empty backport functions after the foreign mod block.
pub fn cfg<T: Into<TokenStream>>(attr: T, item: T) -> TokenStream {
    let cfg = attr.into();

    let item = item.into();
    // taos cfg only focus on foreign mod.
    let syn: syn::ItemForeignMod = match syn::parse2(item.clone()) {
        Ok(it) => it,
        Err(_) => {
            return quote! {
              #[cfg(#cfg)]
              #item
            }
        }
    };

    let mut backport = TokenStream::new();

    for item_fn in syn.items {
        assert!(matches!(item_fn, syn::ForeignItem::Fn(_)));
        if let syn::ForeignItem::Fn(item_fn) = item_fn {
            let item_fn = foreign_fn_to_item_fn(item_fn);
            backport.extend(quote! {
                #[cfg(not(#cfg))]
                #[no_mangle]
                #item_fn
            });
        }
    }
    quote! {
        #[cfg(#cfg)]
        #item

        #backport
    }
}

/// Convert a extern "C" foreign function to a no_mangle Rust fn.
fn foreign_fn_to_item_fn(item: ForeignItemFn) -> ItemFn {
    let mut sig = item.sig;
    // use extern "C"
    let abi: syn::Abi = syn::parse_quote!(extern "C");
    sig.abi = Some(abi);
    let un: syn::token::Unsafe = syn::parse_quote!(unsafe);
    sig.unsafety = Some(un);

    // add panic block
    let err = Literal::string(&format!(
        "C function {} is not supported in this build",
        sig.ident
    ));
    let block: syn::Block = syn::parse_quote! {
        {
            panic!(#err);
        }
    };
    ItemFn {
        attrs: item.attrs,
        vis: item.vis,
        sig,
        block: Box::new(block),
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::{ForeignItemFn, ItemFn};

    use super::cfg;

    #[test]
    fn nothing() {
        let attr = quote!(tmq);
        let fn_item = quote! {
            #[doc = "doc"]
            fn abc() {}
        };

        let tokens = cfg(attr, fn_item);

        let expect = quote! {
            #[cfg(tmq)]
            #[doc = "doc"]
            fn abc() {}
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }

    #[test]
    fn simple() {
        let attr = quote!(tmq);
        let fn_item = quote! {
            // doc
            extern "C" {
                pub fn a();
            }
        };

        let tokens = cfg(attr, fn_item);

        let expect = quote! {
            #[cfg(tmq)]
            extern "C" {
                pub fn a();
            }
            #[cfg(not(tmq))]
            #[no_mangle]
            pub unsafe extern "C" fn a () {
              panic!("C function a is not supported in this build");
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }

    #[test]
    fn test_foreign_to_item() {
        let c: ForeignItemFn = syn::parse_quote! {

            pub fn tmq_list_new() -> *mut tmq_list_t;
        };
        let r = super::foreign_fn_to_item_fn(c);

        let e: ItemFn = syn::parse_quote!(
            pub unsafe extern "C" fn tmq_list_new() -> *mut tmq_list_t {
                panic!("C function tmq_list_new is not supported in this build");
            }
        );
        assert_eq!(r, e);
        dbg!(&r);
    }

    #[test]
    fn tmq_demo() {
        let attr = quote!(tmq);
        let fn_item = quote! {
            // doc
            extern "C" {
                pub fn tmq_list_new() -> *mut tmq_list_t;
                pub fn tmq_list_append(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32;
            }
        };

        let tokens = cfg(attr, fn_item);

        let expect = quote! {
            #[cfg(tmq)]
            extern "C" {
                pub fn tmq_list_new() -> *mut tmq_list_t;
                pub fn tmq_list_append(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32;
            }

            #[cfg(not(tmq))]
            #[no_mangle]
            pub unsafe extern "C" fn tmq_list_new() -> *mut tmq_list_t {
                panic!("C function tmq_list_new is not supported in this build");
            }
            #[cfg(not(tmq))]
            #[no_mangle]
            pub unsafe extern "C" fn tmq_list_append(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32 {
                panic!("C function tmq_list_append is not supported in this build");
            }
        };
        assert_eq!(tokens.to_string(), expect.to_string());
    }
}
