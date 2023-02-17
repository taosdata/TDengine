use std::{fs::File, io::Read, env};

use actix_embed::Embed;
use actix_web::{http::header::ContentType, web, App, HttpResponse, HttpServer, Responder};

use clap::Parser;
use rust_embed::RustEmbed;
use serde::Deserialize;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // HttpServer::new(|| App::new().service(fs::Files::new("/", "index.html").show_files_listing()))
    let mut port: u16 = 8080;
    let mut log_level = String::from("info");
    let mut command_options = false;
    let mut env_options = false;
    // 1.获取命令行参数
    let args = Args::parse();
    if args.port.is_some() {
        command_options = true;
        port = args.port.unwrap();
        println!("command port: {}", port);
        if args.log_level.is_some() {
            log_level = args.log_level.unwrap();
            println!("command log_level: {}", log_level);
        }
    }
    // 2.获取环境变量
    if !command_options {
        if env::var("TAOS_EXPLORER_PORT").is_ok() {
            port = env::var("TAOS_EXPLORER_PORT").unwrap().parse().expect("TAOS_EXPLORER_PORT should be  a number ");
            println!("env port: {}", port);
            env_options = true;
        }
        if env::var("TAOS_EXPLORER_LOG_LEVEL").is_ok() {
            log_level = env::var("TAOS_EXPLORER_LOG_LEVEL").unwrap();
            println!("env log_level: {}", log_level);
            env_options = true;
        }
    }
    // 3.读取配置文件
    if !command_options && !env_options {
        let file_path = "./config.toml";
        match File::open(file_path) {
            Ok(mut file) => {
                let mut str_val = String::new();
                match file.read_to_string(&mut str_val) {
                    Ok(_s) => {
                        let config: Config = toml::from_str(&str_val).unwrap();
                        if config.port.is_some() {
                            port = config.port.unwrap();
                            println!("config file port: {}", port);
                        }
                        if config.log_level.is_some() {
                            log_level = config.log_level.unwrap();
                            println!("config file log_level: {}", log_level);
                        }
                    }
                    Err(e) => println!("Error Reading file: {}", e),
                };
            }
            Err(_e) => println!("no config file {} ", file_path),
        };
    }

    HttpServer::new(|| {
        App::new()
            .route("/", web::get().to(index))
            .service(Embed::new("/", &Asset))
    })
    .bind(("0.0.0.0", port))?
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

extern crate toml;

#[derive(Deserialize, Debug)]
struct Config {
    port: Option<u16>,
    log_level: Option<String>,
}

#[derive(Parser, Debug)]
struct Args {
    port: Option<u16>,
    log_level: Option<String>,
}
