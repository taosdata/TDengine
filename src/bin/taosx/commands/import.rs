use clap::Args;
use taosx::TaosOpts;

#[derive(Debug, Args)]
/// Import external files to TDengine.
pub(crate) struct App {
    name: Option<String>,
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        todo!()
    }
}
