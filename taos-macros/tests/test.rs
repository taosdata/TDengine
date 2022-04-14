use taos::Taos;
use taos_macros::test;

#[test()]
fn sync() {}

#[test]
async fn async_unit() {}

#[test]
async fn async_with_taos(taos: &Taos) {
    dbg!(taos);
}

#[test]
async fn async_with_taos_db(_taos: &Taos, _database: &str) {}
