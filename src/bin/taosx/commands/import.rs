use clap::Args;
use taos::query::Dsn;
use taosx::TaosOpts;

#[derive(Debug, Args)]
/// Import external files to TDengine.
pub(crate) struct App {
    #[clap(short, long)]
    /// A DSN(database source name) format string for source TDengine: taos:///db1, for eg.
    from: Dsn,
    #[clap(short, long)]
    /// A DSN(database source name) format string for target TDengine: taos:///db2, for eg.
    to: Dsn,
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        todo!()
    }
}
