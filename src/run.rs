use std::{time::Duration, io::BufRead};

use anyhow::{bail, Context, Result};
use taos::*;

use taosx_core::{
    kafka_to_taos, influxdb_to_taos, legacy_to_taos, local_to_taos, mqtt_to_taos, opc_to_taos, pi_to_taos,
    query_to_csv, query_to_parquet, tmq_to_local, tmq_to_td, utils::{port_pool::PortPool, self}, Action,
};

use clap::Parser;

#[derive(Parser, Debug)]
pub(super) struct Cli {
    /// Input DSN(Data Source Name) string.
    ///
    /// Supported:
    ///
    /// ─ TMQ: TDengine message queue data stream, use as:
    ///  ** `tmq://host:port/topics?group.id=STR&client.id=STR&timeout`.
    ///
    /// ─ Legacy query, use as:
    ///
    /// └── a) database input: `taos://localhost:6030/database`, this will output stable schemas and child tables.
    ///
    /// └── b) table input: `taos://host:port/db?query=select c1,c2,c3 from stb1`, this will be queried by sql `select c1,c2,c3 from stb1` and output as a plain table.
    ///
    /// ─ Local backup, use as `local:./path`.
    ///
    /// ─ CSV: `csv:/path/to/file.csv`.
    ///
    /// ─ Parquet: `parquet:/path/to/*.parquet`.
    ///
    #[clap(short, long, value_parser)]
    from: Dsn,

    /// Output DSN.
    #[clap(short, long, value_parser)]
    to: Dsn,

    /// Parser.
    #[clap(short, long)]
    parser: Option<String>,
    // parser: Option<taosx_core::Parser>,

    /// Transformer actions.
    ///
    /// Supported action format:
    ///
    /// - 'add-tag:tag1=value1': add a tag named `tag1`, and valued `value1`.
    ///
    /// - 'rename-table:prefix:v1_': rename all tables as `v1_{{ name }}`
    ///
    /// - 'rename-super-table:suffix:_stb': rename all super tables as suffixed '_stb'
    ///
    /// - 'rename-child-table:template:prefix_{{ name }}_stb': rename all super tables with prefix 'prefix_' and suffix '_stb'
    #[clap(short = 'T', long)]
    transform: Vec<Action>,

    // /// Algorithm
    // #[clap(short, long, value_enum, default_value = "zstd")]
    // #[doc(hidden)]
    // algorithm: Algorithm,
    /// For verbosity print.
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0")]
    jobs: usize,

    /// When `endless` flag set, we'll re-write tmq timeout as `never` to wait messages
    /// without an ending, but it will still abort when there's error in the process.
    #[clap(short, long)]
    endless: bool,

    /// Override default TDengine connection protocol to websocket, both `from` and `to` will be affected.
    ///
    /// So that you don't need to append `+ws` in DSN.
    #[clap(short, long)]
    websocket: bool,
}

impl Cli {
    pub(super) async fn run_with(self, opts: super::GlobalOpts) -> Result<()> {
        let mut args = self;
        if args.websocket {
            if args.from.protocol.is_none() {
                args.from.protocol = Some("ws".to_string());
            }
            if args.to.protocol.is_none() {
                args.to.protocol = Some("ws".to_string());
            }
        }

        match (args.from.driver.as_str(), args.to.driver.as_str()) {
            ("tmq", "taos") => {
                let mut sleep = Duration::from_millis(1000);
                loop {
                    match tmq_to_td(
                        args.from.clone(),
                        args.transform.clone(),
                        args.to.clone(),
                        args.jobs,
                        Default::default(),
                        Default::default(),
                    ).await
                    {
                        Ok(_) => break,
                        Err(err) if err.to_string().contains("[0xE002]") => {
                            log::warn!("connection broken, retry after {sleep:?}.");
                            tokio::time::sleep(sleep).await;
                            sleep *= 2;
                            continue;
                        }
                        Err(err) => {
                            Err(err).with_context(|| format!("tmq to td task exec error"))?
                        }
                    }
                }
            }
            ("tmq", "local") => {
                let mut sleep = Duration::from_millis(1000);
                loop {
                    match tmq_to_local(
                        args.from.clone(),
                        args.to.clone(),
                        args.jobs,
                        opts.yes_i_really_mean_it,
                        Default::default(),
                        Default::default(),
                    ).await
                    {
                        Ok(_) => break,
                        Err(err) if err.to_string().contains("[0xE002]") => {
                            log::warn!("connection broken, retry after {sleep:?}.");
                            tokio::time::sleep(sleep).await;
                            sleep *= 2;
                            continue;
                        }
                        Err(err) => {
                            Err(err).with_context(|| format!("tmq to local task exec error"))?
                        }
                    }
                }
            }
            ("local", "taos" | "tmq") => {
                local_to_taos(args.from, args.to, args.jobs, opts.yes_i_really_mean_it).await?;
            }
            ("taos", "taos") => {
                legacy_to_taos(args.from, args.transform, args.to, args.jobs).await?;
            }
            ("taos", "csv") => {
                query_to_csv(args.from, args.to).await?;
            }
            ("taos", "parquet") => {
                query_to_parquet(args.from, args.to, opts.yes_i_really_mean_it).await?;
            }
            ("pi" | "pibackfill", "taos") => {
                let port_pool = PortPool::default();
                pi_to_taos(
                    args.from,
                    args.transform,
                    args.to,
                    args.jobs,
                    &port_pool,
                    Default::default(),
                    None,
                    None,
                ).await?;
                log::debug!("main scheduler done");
            }
            ("influxdb", "taos") => {
                let port_pool = PortPool::default();
                influxdb_to_taos(
                    args.from,
                    args.transform,
                    args.to,
                    args.jobs,
                    &port_pool,
                    Default::default(),
                    None,
                    None,
                ).await?;
                log::debug!("main scheduler done");
            }
            ("opc" | "opcua" | "opcda", "taos") => {
                let port_pool = PortPool::default();
                opc_to_taos(
                    args.from,
                    args.transform,
                    args.to,
                    args.jobs,
                    &port_pool,
                    Default::default(),
                    None,
                    None,
                ).await?;
                log::debug!("opc main scheduler done");
            }
            ("mqtt", "taos") => {
                let port_pool = PortPool::default();
                let parser = if args.parser.is_some() {
                    let file_content = utils::get_string_content_from_file_path(args.parser.unwrap().as_str());
                    if file_content.is_none() {
                        None
                    } else {
                        Some(serde_json::from_str(file_content.unwrap().as_str()).with_context(|| format!("file content deserialize error")).unwrap())
                    }
                } else {
                    None
                };
                if parser.is_none() {
                    anyhow::bail!("parser config error");
                }
                mqtt_to_taos(
                    args.from,
                    parser,
                    args.to,
                    args.jobs,
                    &port_pool,
                    Default::default(),
                    None,
                    None, // how to save the transferred number
                ).await?;
                log::debug!("opc main scheduler done");
            }
            ("kafka", "taos") => {
                let parser = if args.parser.is_some() {
                    let p = args.parser.unwrap();
                    println!("parser: {}", p);
                    let json = serde_json::from_str(&p).unwrap();
                    Some(json)
                } else {
                    None
                };

                kafka_to_taos(
                    args.from,
                    parser,
                    args.transform,
                    args.to,
                    args.jobs,
                    &PortPool::default(),
                    Default::default(),
                    None,
                    None,
                ).await?;
                log::debug!("kafka main scheduler done");
            }
            (_, _) => bail!(
                "unsupported source or dest: from `{}` to `{}`",
                args.from,
                args.to
            ),
        }

        Ok(())
    }
}
