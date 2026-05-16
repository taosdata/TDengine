use std::error::Error;
use std::path::Path;
use std::{env, fs};

use syn::visit::Visit;
use syn::{ItemConst, ItemFn, Visibility};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-env-changed=TD_VERSION");

    let td_ver = env::var("TD_VERSION")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());

    println!("cargo:rustc-env=TD_VERSION={td_ver}");

    write_header("taos.h", "src/ws", "cbindgen_ws.toml")?;
    write_header("taosws.h", "src/old_ws", "cbindgen_old_ws.toml")?;
    Ok(())
}

fn write_header(header: &str, dir: &str, cfg: &str) -> Result<(), Box<dyn Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR")?;
    let out_dir = env::var("OUT_DIR")?;

    let target_path = Path::new(&out_dir)
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap();

    let bindings = target_path.join(header);
    let src_path = Path::new(&crate_dir).join("src");
    let exclude_path = Path::new(&crate_dir).join(dir);

    let mut config = cbindgen::Config::from_file(cfg)?;
    config.export.exclude = exclude_extern_c_fns(&src_path, &exclude_path)?;

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .unwrap_or_else(|err| panic!("Failed to generate {header}, err: {err}"))
        .write_to_file(bindings);

    Ok(())
}

fn exclude_extern_c_fns(path: &Path, exclude_path: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    let mut visitor = ExternCVisitor {
        extern_c_fns: Vec::new(),
        pub_consts: Vec::new(),
    };
    parse_rs_files(path, exclude_path, &mut visitor)?;
    Ok([visitor.extern_c_fns, visitor.pub_consts].concat())
}

fn parse_rs_files(
    path: &Path,
    exclude_path: &Path,
    visitor: &mut ExternCVisitor,
) -> Result<(), Box<dyn Error>> {
    if path == exclude_path {
        return Ok(());
    }

    if path.is_file() {
        if path.extension().is_some_and(|ext| ext == "rs") {
            let content = fs::read_to_string(path)?;
            let file = syn::parse_file(&content)?;
            visitor.visit_file(&file);
        }
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let path = entry?.path();
            parse_rs_files(&path, exclude_path, visitor)?;
        }
    }

    Ok(())
}

struct ExternCVisitor {
    extern_c_fns: Vec<String>,
    pub_consts: Vec<String>,
}

impl<'ast> Visit<'ast> for ExternCVisitor {
    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if let Some(abi) = &node.sig.abi {
            if abi.name.as_ref().map(|s| s.value()) == Some("C".to_owned()) {
                self.extern_c_fns.push(node.sig.ident.to_string());
            }
        }
        syn::visit::visit_item_fn(self, node);
    }

    fn visit_item_const(&mut self, node: &'ast ItemConst) {
        if let Visibility::Public(_) = &node.vis {
            self.pub_consts.push(node.ident.to_string());
        }
        syn::visit::visit_item_const(self, node);
    }
}
