use std::path::PathBuf;

use chrono_tz::Tz;
use clap::Args;
use url::Url;

#[derive(Debug, Args)]
pub struct TaosUri {
    pub uri: Url,
}
#[derive(Debug, Args)]
pub struct TaosOpts {
    /// TDengine host
    #[clap(short, long, env = "TAOS_HOST", group = "taos-opts")]
    pub host: Option<String>,
    /// TDengine port
    #[clap(short, long, env = "TAOS_PORT", group = "taos-opts")]
    pub port: Option<u16>,
    /// TDengine username
    #[clap(short, long, env = "TAOS_USERNAME", group = "taos-opts")]
    pub username: Option<String>,
    /// TDengine password for the user
    #[clap(short = 'P', long, env = "TAOS_PASSWORD", group = "taos-opts")]
    pub password: Option<String>,
    /// Choose database for the connection
    #[clap(short, long, env = "TAOS_DATABASE", group = "taos-opts")]
    pub database: Option<String>,
    #[clap(short, long, env = "TZ")]
    /// Timezone, example: Asia/Shanghai
    pub timezone: Option<Tz>,
    #[clap(short, long, env = "TAOS_CFG_DIR")]
    /// TDengine config directory
    pub cfg_dir: Option<PathBuf>,
}
