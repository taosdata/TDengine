use clap::Args;
use taosx::TaosOpts;

#[derive(Debug)]
#[derive(Args)]
/// Backup database or tables to specific files.
/// 
/// Basically, an alternative command to `taosdump`.
pub(crate) struct App {
    name: Option<String>,
}
impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        todo!()
    }
}
