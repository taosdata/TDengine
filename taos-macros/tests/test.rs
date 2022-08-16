// use anyhow::Result;
// use taos::prelude::*;
// use taos_macros::test;

// #[test()]
// fn sync() {}

// #[test]
// async fn async_unit() {}

// #[test]
// async fn async_with_taos(taos: &Taos) {
//     dbg!(taos);
// }

// #[test]
// async fn async_with_taos_db(_taos: &Taos, _database: &str) {}

// #[test]
// async fn show_databases(taos: &Taos, database: &str) -> Result<()> {
//     let databases = taos.databases().await?;
//     assert!(databases.iter().any(|db| db.name == database));
//     Ok(())
// }

// #[test(precision = "ns")]
// async fn with_precision(taos: &Taos, database: &str) -> Result<()> {
//     let databases = taos.databases().await?;
//     assert!(databases
//         .iter()
//         .any(|db| db.name == database && db.props.precision.as_ref().unwrap().as_str().eq("ns")));
//     Ok(())
// }

// #[test(naming = "abc1")]
// async fn custom_database(taos: &Taos, database: &str) -> Result<()> {
//     let databases = taos.databases().await?;
//     assert!(database == "abc1");
//     assert!(databases.iter().any(|db| db.name == database));
//     Ok(())
// }

// #[test(databases = 10)]
// async fn multi_databases(_taos: &Taos, databases: &[&str]) -> Result<()> {
//     assert!(databases.len() == 10);
//     Ok(())
// }
