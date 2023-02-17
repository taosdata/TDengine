use clap_verbosity_flag::{InfoLevel, Verbosity};
use log::{LevelFilter};
use std::{fs::File, io::Read};

use actix_embed::Embed;
use actix_web::{http::header::ContentType, web, App, HttpResponse, HttpServer, Responder};

use clap::Parser;
use rust_embed::RustEmbed;
use serde::Deserialize;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let file_path = "/etc/taos/explorer.toml";

    let args = if let Ok(mut file) = File::open(file_path) {
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let mut args: Args = toml::from_str(&content).unwrap();
        args.update_from(std::env::args());
        args
    } else {
        Args::parse()
    };
    pretty_env_logger::formatted_timed_builder()
        .filter_level(
            args.log_level
                .or(args.verbose.map(|v| v.log_level_filter()))
                .unwrap_or(log::LevelFilter::Info),
        )
        .init();
    HttpServer::new(|| {
        App::new()
            .route("/", web::get().to(index))
            .service(Embed::new("/", &Asset))
    })
    .bind(("0.0.0.0", args.port))?
    .run()
    .await
}

async fn index() -> impl Responder {
    let index_html = Asset::get("index.html").unwrap();
    HttpResponse::Ok().content_type(ContentType::html()).body(
        std::str::from_utf8(index_html.data.as_ref())
            .unwrap()
            .to_string(),
    )
}

#[derive(RustEmbed)]
#[folder = "../dist/"]
struct Asset;

#[derive(Parser, Debug, Clone, Deserialize)]
struct Args {
    /// Port
    #[clap(
        short,
        long,
        default_value = "6060",
        global = true,
        env = "EXPLORER_PORT"
    )]
    port: u16,
    /// For verbosity logging.
    #[clap(flatten)]
    #[serde(skip)]
    verbose: Option<Verbosity<InfoLevel>>,

    #[clap(skip)]
    log_level: Option<LevelFilter>,
}
