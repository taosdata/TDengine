mod commands;
use clap::{Parser, Subcommand};
use log::Level;
use pretty_env_logger::env_logger::fmt::Color;
use std::io::prelude::*;
use taosx::TaosOpts;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Cli {
    #[clap(flatten)]
    options: TaosOpts,
    #[clap(short, long)]
    log_level: Option<log::LevelFilter>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Export(commands::export::App),
    Import(commands::import::App),
    Backup(commands::backup::App),
    Restore(commands::restore::App),
    Sync(commands::sync::App),
    #[clap(external_subcommand)]
    External(Vec<String>),
}

pub fn cli<'help>() -> clap::Command<'help> {
    clap::Command::new("taosx")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut builder = pretty_env_logger::formatted_timed_builder();
    if let Some(level) = cli.log_level {
        builder.filter_level(level);
    } else if let Ok(s) = ::std::env::var("RUST_LOG") {
        builder.parse_filters(&s);
    } else {
        builder.filter_level(log::LevelFilter::Info);
    }
    builder
        .format_module_path(true)
        .format(|buf, record| -> std::result::Result<(), std::io::Error> {
            fn colored_level<'a>(
                style: &'a mut pretty_env_logger::env_logger::fmt::Style,
                level: Level,
            ) -> pretty_env_logger::env_logger::fmt::StyledValue<'a, &'static str> {
                match level {
                    Level::Trace => style.set_color(Color::Magenta).value("TRACE"),
                    Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
                    Level::Info => style.set_color(Color::Green).value("INFO"),
                    Level::Warn => style.set_color(Color::Yellow).value("WARN "),
                    Level::Error => style.set_color(Color::Red).value("ERROR"),
                }
            }
            let mut style = buf.style();
            let level = colored_level(&mut style, record.level());
            let mut mod_path = buf.style();

            let mod_path = mod_path.set_bold(true).value(format!(
                "{}:{}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
            ));
            writeln!(
                buf,
                "[{:29} {: <5}] {} > {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.%f"),
                level,
                mod_path,
                record.args()
            )
        })
        // .is_test(true)
        .init();
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
        Commands::Sync(app) => {
            app.run_with_taos_opts(&cli.options).await?;
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

    Ok(())
}
