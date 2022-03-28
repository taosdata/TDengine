mod commands;
use clap::{Parser, Subcommand};
use log::LevelFilter;
use simple_logger::SimpleLogger;
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

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    let cli = Cli::parse();
    let command = cli.command;

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match command {
        Commands::Export(app) => {
            app.run_with_taos_opts(&cli.options);
        }
        Commands::Import(app) => {
            app.run_with_taos_opts(&cli.options);
        }
        Commands::Backup(app) => {
            app.run_with_taos_opts(&cli.options);
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
