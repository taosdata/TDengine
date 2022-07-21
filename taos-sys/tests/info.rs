#[test]
fn test_server_info() {
    use taos_query::prelude::sync::*;
    use taos_sys::Builder;
    let version = Builder::client_version();
    dbg!(version);

    let builder = Builder::from_dsn("taos://").unwrap();
    dbg!(builder.server_version());
}
