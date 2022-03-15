use clap::Args;
use taosx::TaosOpts;

#[derive(Debug)]
#[derive(Args)]
/// Restore from a backup output directory.
pub(crate) struct App {
    name: Option<String>,
}
impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        todo!()
    }
}
