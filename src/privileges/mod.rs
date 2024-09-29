use std::{fmt::Debug, path::PathBuf};

use anyhow::{bail, Result};
use clap::Args;
use taos::Dsn;

/// Active-StandBy replication management commands
#[derive(Debug, Args)]
pub struct Cli {
    /// The source endpoint to replicate from.
    #[clap(short = 'f', long)]
    from: Option<Dsn>,
    /// The endpoint to replicate to.
    #[clap(short = 'i', long)]
    input: Option<PathBuf>,
    /// The endpoint to migrate to.
    #[clap(short = 't', long)]
    to: Option<Dsn>,
    /// Export data to a file.
    #[clap(short = 'o', long)]
    output: Option<PathBuf>,

    /// Scope
    #[clap(flatten)]
    scope: Option<Scope>,
}

#[derive(Debug, Args)]
struct Scope {
    /// Contains users but without whitelist.
    #[clap(short = 'u', long)]
    users: bool,
    /// Contains privileges.
    #[clap(short = 'p', long)]
    privileges: bool,
    /// Contains whitelist in users.
    #[clap(short = 'w', long)]
    whitelist: bool,
}

impl Scope {
    fn into_options(self) -> taosx_core::migrations::Options {
        taosx_core::migrations::Options::new(self.users, self.privileges, self.whitelist)
    }
}

impl Cli {
    #[tracing::instrument(skip_all, name = "privileges")]
    pub async fn run(self, _: super::OptArgs) -> Result<()> {
        let options = self.scope.map(Scope::into_options).unwrap_or_default();
        match (self.from, self.to, self.input, self.output) {
            (Some(from), Some(to), None, None) => {
                let results = taosx_core::migrations::migrate(&from, &to, &options).await?;
                println!("{}", results);
            }
            (None, Some(to), Some(input), None) => {
                let results = taosx_core::migrations::import(&input, &to, &options).await?;
                println!("{}", results);
            }
            (Some(from), None, None, Some(output)) => {
                taosx_core::migrations::export(&from, &output, &options).await?;
            }
            _ => bail!("Invalid arguments composition, use -f/-t, -f/-o, -i/-t options"),
        }
        Ok(())
    }
}
