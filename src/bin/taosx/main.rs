mod commands;
use clap::{Parser, Subcommand};

use taosx::TaosOpts;
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(flatten)]
    options: TaosOpts,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Export(commands::export::App),
    Import(commands::import::App),
    Backup(commands::backup::App),
    Restore(commands::restore::App),
    #[clap(external_subcommand)]
    External(Vec<String>),
}

pub fn cli<'help>() -> clap::Command<'help> {
    clap::Command::new("taosx")
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    dbg!(&cli);

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Commands::Export(app) => {
            app.run_with_taos_opts(&cli.options);
        }
        Commands::Import(app) => {
            app.run_with_taos_opts(&cli.options);
        }
        Commands::Backup(app) => {
            app.run_with_taos_opts(&cli.options).await;
        }
        Commands::Restore(app) => {
            app.run_with_taos_opts(&cli.options);
        }

        Commands::External(args) => {
            let (name, args) = args.split_first().unwrap();
            println!("Call external plugin taosx-{name}: {args:?}");
            let cmd = format!("taosx-{name}");
            std::process::Command::new(&cmd)
                .args(args)
                .spawn()
                .expect(&format!("unable to run command {cmd}"));
        }
    }
}
