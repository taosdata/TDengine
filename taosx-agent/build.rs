fn shadow_build() {
    use std::{fs::File, io::prelude::*, path::Path};

    use shadow_rs::SdResult;

    const DEFAULT_CUS_NAME: &str = "TDengine";
    const DEFAULT_CUS_PROMPT: &str = "taos";

    fn labeling(mut file: &File) -> SdResult<()> {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let manifest_dir = Path::new(&manifest_dir);
        let readme = manifest_dir.join("src").join("CLI.md");
        let target_dir = manifest_dir.parent().unwrap().join("target");

        let cus_name = std::env::var("CUS_NAME").unwrap_or(DEFAULT_CUS_NAME.to_string());
        let cus_prompt = std::env::var("CUS_PROMPT").unwrap_or(DEFAULT_CUS_PROMPT.to_string());
        let td_version = std::env::var("VER_NUMBER").ok();
        let cus_name = if cus_name.trim().is_empty() {
            DEFAULT_CUS_NAME
        } else {
            cus_name.trim()
        };
        let cus_prompt = if cus_prompt.trim().is_empty() {
            DEFAULT_CUS_PROMPT
        } else {
            cus_prompt.trim()
        };
        let content = std::fs::read_to_string(&readme)
            .unwrap()
            .replace("taos", cus_prompt)
            .replace("TDengine", cus_name);

        let service_template = manifest_dir.join("src").join("systemd.service");
        let service = std::fs::read_to_string(&service_template)
            .unwrap_or_else(|_| panic!("{}", service_template.display()))
            .replace(DEFAULT_CUS_PROMPT, cus_prompt)
            .replace(DEFAULT_CUS_NAME, cus_name);
        if !target_dir.exists() {
            std::fs::create_dir_all(&target_dir).unwrap();
        }
        std::fs::write(
            target_dir.join(format!("{cus_prompt}x-agent.service")),
            service,
        )
        .unwrap();

        writeln!(file, r#"pub const CUS_NAME: &str = "{}";"#, cus_name)?;
        writeln!(file, r#"pub const CUS_PROMPT: &str = "{}";"#, cus_prompt)?;
        writeln!(
            file,
            r#"pub const CUS_CLI_NAME: &str = "{}x-agent";"#,
            cus_prompt
        )?;
        writeln!(
            file,
            r#"pub const CUS_APP_NAME: &str = "{}X Agent";"#,
            cus_prompt
        )?;
        writeln!(file, r#"pub const CUS_CLI_ABOUT: &str = "{}";"#, content)?;
        if let Some(version) = td_version {
            writeln!(file, r#"pub const TD_VERSION: &str = "{}";"#, version)?;
        } else {
            writeln!(file, r#"pub const TD_VERSION: &str = PKG_VERSION;"#)?;
        }
        println!("cargo:rerun-if-env-changed=PKG_TIME");
        #[cfg(debug_assertions)]
        {
            writeln!(file, r#"pub const IS_DEBUG: bool = true;"#)?;
        }
        #[cfg(not(debug_assertions))]
        {
            writeln!(file, r#"pub const IS_DEBUG: bool = false;"#)?;
        }
        Ok(())
    }

    shadow_rs::new_hook(labeling).unwrap();
}

fn main() {
    dotenv::dotenv().ok();
    shadow_build();
    println!("cargo:rerun-if-changed=../.git");
}
