use taos_sys::TaosBuilder;

#[test]
fn ws_sync_json() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    use taos_query::prelude::sync::*;
    let client = TaosBuilder::from_dsn("taos://localhost:6030/")?.build()?;
    let db = "ws_sync_json";
    assert_eq!(client.exec(format!("drop database if exists {db}"))?, 0);
    assert_eq!(client.exec(format!("create database {db} keep 36500"))?, 0);
    assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            )?,
            0
        );
    assert_eq!(
        client.exec(format!(
            r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
        ))?,
        2
    );
    assert_eq!(
        client.exec(format!(
            r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
        ))?,
        2
    );

    // let mut rs = client.s_query("select * from wsabc.tb1").unwrap().unwrap();
    let mut rs = client.query(format!("select * from {db}.tb1 order by ts limit 1"))?;

    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    #[allow(dead_code)]
    struct A {
        ts: String,
        b1: bool,
        c8i1: i8,
        c16i1: i16,
        c32i1: i32,
        c64i1: i64,
        c8u1: u8,
        c16u1: u16,
        c32u1: u32,
        c64u1: u64,

        c8i2: i8,
        c16i2: i16,
        c32i2: i32,
        c64i2: i64,
        c8u2: u8,
        c16u2: u16,
        c32u2: u32,
        c64u2: u64,

        cb1: String,
        cb2: String,
        cn1: String,
        cn2: String,
    }

    use itertools::Itertools;
    let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

    dbg!(&values);

    assert_eq!(
        values[0],
        A {
            ts: "1970-01-01T00:00:00".to_string(),
            b1: true,
            c8i1: -1,
            c16i1: -2,
            c32i1: -3,
            c64i1: -4,
            c8u1: 1,
            c16u1: 2,
            c32u1: 3,
            c64u1: 4,
            c8i2: -5,
            c16i2: -6,
            c32i2: -7,
            c64i2: -8,
            c8u2: 5,
            c16u2: 6,
            c32u2: 7,
            c64u2: 8,
            cb1: "abc".to_string(),
            cb2: "def".to_string(),
            cn1: "涛思".to_string(),
            cn2: "数据".to_string(),
        }
    );

    assert_eq!(client.exec(format!("drop database {db}"))?, 0);
    Ok(())
}
