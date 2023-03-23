use taos::{Dsn, TBuilder, TaosBuilder};

use crate::{plugins::service::spawn_rest_service, Action};

mod config;
mod service;
mod sink;
mod source;
mod transform;

pub async fn pi_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
) -> anyhow::Result<()> {
    println!("# plugin: PI");
    let target_pool = TaosBuilder::from_dsn(to)?.pool()?;
    let server = spawn_rest_service(target_pool, 6050).await?;


    Ok(())
}
