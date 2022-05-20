use crate::{impls::ResultSet, util::IntoCStr, Code, Error, Result, Taos};
use taos_query::common::{BorrowedColumn, Column};
use taos_sys::stmt::RawStmt;

use std::marker::PhantomData;

pub use taos_sys::{TaosBind, TaosMultiBind};
/// Stmt handler.
pub struct Stmt<'stmt>(RawStmt, PhantomData<&'stmt Taos>);

impl<'stmt> Stmt<'stmt> {
    pub fn execute(&self) -> Result<()> {
        self.0.execute()
    }

    pub fn result(&mut self) -> ResultSet {
        ResultSet::from_raw_res(self.0.use_result())
    }

    pub fn multi_bind(&mut self, params: &[TaosMultiBind]) -> Result<()> {
        self.0.bind_param_batch(params)?;
        self.0.add_batch()?;
        Ok(())
    }

    pub fn multi_bind_at(&mut self, bind: &TaosMultiBind, col: usize) -> Result<()> {
        self.0.bind_single_param_batch(bind, col as i32)
    }

    pub fn num_params(&self) -> Result<usize> {
        Ok(self.0.num_params()? as _)
    }

    pub fn set_tbname_tags(&mut self, name: impl AsRef<str>, tags: &[TaosBind]) -> Result<()> {
        self.0.set_tbname_tags_v3(name.as_ref(), tags)
    }
    pub fn set_tbname(&mut self, name: impl AsRef<str>) -> Result<()> {
        self.0.set_tbname(name.as_ref())
    }
    pub fn set_sub_tbname(&mut self, name: impl AsRef<str>) -> Result<()> {
        self.0.set_sub_tbname(name.as_ref())
    }

    pub fn is_insert(&self) -> bool {
        self.0.is_insert().unwrap_or(false)
    }

    pub fn affected_rows(&self) -> usize {
        self.0.affected_rows() as _
    }
}

// impl<'c> From<&'c Column> for MultiBind<'c> {
//     fn from(col: &'c Column) -> Self {
//         match col {
//             Column::Null(n) => MultiBind::nulls(*n),
//             Column::Bool(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::TinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::SmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::Int(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::BigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::UTinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::USmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::UInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::UBigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::Float(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::Double(nulls, values) => MultiBind::from_primitives(nulls, values),
//             Column::Timestamp(nulls, values) => MultiBind::from_raw_timestamps(nulls, values),
//             Column::Binary(values) => MultiBind::from_binary_vec(values),
//             Column::NChar(values) => MultiBind::from_string_vec(values),
//             _ => unreachable!(),
//         }
//     }
// }

// impl<'b> From<BorrowedColumn<'b>> for MultiBind<'b> {
//     fn from(col: BorrowedColumn<'b>) -> Self {
//         match col {
//             BorrowedColumn::Null(n) => MultiBind::nulls(n),
//             BorrowedColumn::Bool(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::TinyInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::SmallInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::Int(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::BigInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::UTinyInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::USmallInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::UInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::UBigInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::Float(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::Double(nulls, values) => MultiBind::from_primitives(&nulls, values),
//             BorrowedColumn::Timestamp(nulls, values) => {
//                 MultiBind::from_raw_timestamps(&nulls, values)
//             }
//             BorrowedColumn::Binary(values) => MultiBind::from_binary_vec(&values),
//             BorrowedColumn::NChar(values) => MultiBind::from_string_vec(&values),
//             _ => unreachable!(),
//         }
//     }
// }

// impl<'b, 'c> From<&'c BorrowedColumn<'b>> for MultiBind<'c> {
//     fn from(col: &'c BorrowedColumn<'b>) -> Self {
//         match col {
//             BorrowedColumn::Null(n) => MultiBind::nulls(*n),
//             BorrowedColumn::Bool(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::TinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::SmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Int(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::BigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UTinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::USmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UBigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Float(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Double(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Timestamp(nulls, values) => {
//                 MultiBind::from_raw_timestamps(nulls, values)
//             }
//             BorrowedColumn::Binary(values) => MultiBind::from_binary_vec(values),
//             BorrowedColumn::NChar(values) => MultiBind::from_string_vec(values),
//             _ => unreachable!(),
//         }
//     }
// }

impl Taos {
    /// Create stmt with sql
    pub fn stmt<'a, 'stmt>(&'stmt self, sql: impl AsRef<str>) -> Result<Stmt<'stmt>> {
        let mut stmt = RawStmt::from_raw_taos(&self.0);
        stmt.prepare(sql.as_ref())?;
        Ok(Stmt(stmt, PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use bitvec_simd::BitVec;
    use itertools::Itertools;
    use taos_query::common::itypes::ITimestamp;
    use taos_sys::TaosBind;
    use taos_sys::TaosMultiBind;

    use crate::prelude::common::*;
    use crate::prelude::sync::*;
    use crate::test;
    use anyhow::Result;

    // todo: stmt query support.
    // #[test]
    // fn test_stmt(taos: &Taos) -> Result<()> {
    //     let mut stmt = taos.stmt("show databases")?;
    //     stmt.execute()?;
    //     let mut rs = stmt.result();
    //     for block in rs {
    //         println!("{block:?}");
    //     }
    //     Ok(())
    // }

    #[test]
    fn test_multi_bind(taos: &Taos, _database: &str) -> Result<()> {
        taos.exec(concat!(
            "create table if not exists tb (ts timestamp,",
            "c1 bool, c2 tinyint, c3 smallint, c4 int,",
            "c5 bigint,",
            "c6 tinyint unsigned,",
            "c7 smallint unsigned,",
            "c8 int unsigned,",
            "c9 bigint unsigned,",
            "c10 float,",
            "c11 double,",
            "c12 timestamp,",
            "c13 binary(100),",
            "c14 nchar(100))",
        ))?;

        const N: usize = 100;
        let nulls = BitVec::zeros(N);

        let ts = Column::Timestamp(
            nulls.clone(),
            (0..N).map(|ts| ts as i64 + 1_500_000_000_000).collect(),
        );

        macro_rules! col {
            ($ty:ident) => {
                dbg!(Column::$ty(
                    nulls.clone(),
                    (0..N).map(|_| rand::random()).collect()
                ))
            };
        }
        let c1 = col!(Bool);
        let c2 = col!(TinyInt);
        let c3 = col!(SmallInt);
        let c4 = col!(Int);
        let c5 = col!(BigInt);
        let c6 = col!(UTinyInt);
        let c7 = col!(USmallInt);
        let c8 = col!(UInt);
        let c9 = col!(UBigInt);
        let c10 = col!(Float);
        let c11 = col!(Double);
        let c12 = col!(Timestamp);
        let c13 = Column::Binary(
            (0..N)
                .map(|_| Some(String::from("abc").into_bytes()))
                .collect(),
        );
        let c14 = Column::NChar((0..N).map(|_| None).collect());

        let block = vec![
            ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14,
        ];
        let binds: Vec<TaosMultiBind> = block.iter().map(|c| dbg!(c.into())).collect();
        let marks: String = std::iter::repeat("?").take(15).join(",");
        let mut stmt = taos
            .stmt(format!("insert into tb values({marks})"))
            .unwrap();
        stmt.multi_bind(&binds).unwrap();

        stmt.execute().unwrap();

        let rows = stmt.affected_rows();
        assert_eq!(N, rows as usize);

        let mut rs = taos.query("select * from tb")?;
        for row in rs.rows_iter() {
            for col in &row {
                println!("{:?}", col);
            }
        }

        Ok(())
    }
    #[crate::test(naming = "abc", dropping = "none")]
    fn test_multi_bind_tags(taos: &Taos, _database: &str) -> Result<()> {
        taos.exec(concat!(
            "create table if not exists tb (ts timestamp, v binary(100)) tags(",
            "c1 bool,",
            "c2 tinyint,",
            "c3 smallint,",
            "c4 int,",
            "c5 bigint,",
            "c6 tinyint unsigned,",
            "c7 smallint unsigned,",
            "c8 int unsigned,",
            "c9 bigint unsigned,",
            "c10 float,",
            "c11 double,",
            "c12 timestamp)",
        ))?;
        const N: usize = 5;
        let nulls = BitVec::zeros(N);
        let v: Vec<Option<String>> = (0..N).map(|_| Some("hello".to_string())).collect();
        let _ints = Column::NChar(v);
        let v: Vec<Option<Vec<u8>>> = (0..N)
            .map(|_| Some("hello".to_string().into_bytes()))
            .collect();
        let v = Column::Binary(v);

        let ts = Column::Timestamp(
            nulls.clone(),
            (0..N).map(|ts| ts as i64 + 1500000000000).collect(),
        );

        macro_rules! tag {
            ($v:expr) => {
                dbg!(TaosBind::from(&$v))
            };
        }
        let c1 = tag!(true);
        let c2 = tag!(1i8);
        let c3 = tag!(1i16);
        let c4 = tag!(1i32);
        let c5 = tag!(1i64);
        let c6 = tag!(1u8);
        let c7 = tag!(1u16);
        let c8 = tag!(1u32);
        let c9 = tag!(1u64);
        let c10 = tag!(0.0f32);
        let c11 = tag!(0.0f64);
        let c12 = tag!(ITimestamp(1500000000000));
        // let c13 = tag!();
        // let c14 = Column::NChar((0..N).map(|_| None).collect());

        let tags = vec![c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12];
        let marks: String = std::iter::repeat("?").take(12).join(",");

        let block = vec![ts, v];
        let values: Vec<TaosMultiBind> = block.iter().map(|c| dbg!(c.into())).collect();

        let mut stmt = taos
            .stmt(format!("insert into ? using tb tags({marks}) values(?, ?)"))
            .unwrap();
        stmt.set_tbname_tags("tb1", &tags)?;
        println!("bind parmas: {values:#?}");
        stmt.multi_bind(&values).unwrap();
        stmt.execute().unwrap();

        let rows = stmt.affected_rows();
        assert_eq!(N, rows as usize);

        let mut res = taos.query("select * from tb").unwrap();

        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct Row {
            ts: i64,
            v: String,
            c1: bool,
            c2: i8,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: f32,
            c11: f64,
            c12: i64,
        }
        let data: Row = res
            .deserialize()
            .next()
            .expect("there's no database")
            .expect("");
        dbg!(&data);
        assert!(
            data == Row {
                ts: 1500000000000,
                v: "hello".to_string(),
                c1: true,
                c2: 1,
                c3: 1,
                c4: 1,
                c5: 1,
                c6: 1,
                c7: 1,
                c8: 1,
                c9: 1,
                c10: 0.0,
                c11: 0.0,
                c12: 1500000000000
            }
        );
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//         taos.exec(format!("create database if not exists {} keep 36500", db))
//             .await?;
//         taos.exec(format!("use {}", db)).await?;
//         taos.exec(format!(
//         .await?;
//         let mut stmt = taos.stmt("insert into ? values(?,?)")?;
//         stmt.set_tbname("tb0")?;
//         assert!(stmt.is_insert());
//         assert_eq!(stmt.num_params(), 2);
//         let ts = Field::Timestamp(Timestamp::now());
//         stmt.bind(vec![ts, value.clone()].iter())?;
//         let _ = stmt.execute()?;
//         let res = taos.query("select n from tb0").await?;
//         assert_eq!(value, res.rows[0][0]);
//         taos.exec(format!("drop database {}", db)).await?;
//         Ok(())
//     }

//     macro_rules! _test_column_null {
//         ($ty:ty, $v:expr) => {
//             paste::paste! {
//                 #[tokio::test]
//                 #[test_catalogue()]
//                 #[doc = "Test bind null to type " $ty]
//                 async fn [<null_ $ty:snake>]() -> Result<(), Error> {
//                     let db = stdext::function_name!()
//                         .replace("::{{closure}}", "")
//                         .replace("::", "_");
//                     stmt_test(&db, $v, Field::Null).await
//                 }
//             }
//         };
//     }
//     _test_column_null!(bool, "bool");
//     _test_column_null!(tinyint, "tinyint");
//     _test_column_null!(smallint, "smallint");
//     _test_column_null!(int, "int");
//     _test_column_null!(bigint, "bigint");
//     _test_column_null!(utinyint, "tinyint unsigned");
//     _test_column_null!(usmallint, "smallint unsigned");
//     _test_column_null!(uint, "int unsigned");
//     _test_column_null!(ubigint, "bigint unsigned");
//     _test_column_null!(timestamp, "timestamp");
//     _test_column_null!(float, "float");
//     _test_column_null!(double, "double");
//     _test_column_null!(binary, "binary(10)");
//     _test_column_null!(nchar, "nchar(10)");
//     #[should_panic]
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test bind null to json tag, should panic because json is not supported in cols
//     async fn null_json() -> () {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "json", Field::Null).await.unwrap()
//     }

//     async fn stmt_tag(db: &str, ty: &str, tag: Field) -> Result<(), Error> {
//         let taos = taos()?;
//         println!("test {} using {}", ty, db);
//         taos.exec(format!("drop database if exists {}", db)).await?;
//         taos.exec(format!("create database if not exists {} keep 36500", db))
//             .await?;
//         taos.exec(format!("use {}", db)).await?;
//         taos.exec(format!(
//             "create table if not exists stb0 (ts timestamp, v int) tags (n {})",
//             ty
//         ))
//         .await?;
//         println!("start stmt");
//         let mut stmt = taos.stmt("insert into ? using stb0 tags(?) values(?,?)")?;
//         println!("set tags");
//         stmt.set_tbname_tags("tb0", [&tag])?;
//         assert!(stmt.is_insert());
//         assert_eq!(stmt.num_params(), 2);
//         let ts = Field::Timestamp(Timestamp::now());
//         println!("bind stmt");
//         stmt.bind(vec![ts, Field::Null].iter())?;
//         println!("execute");
//         stmt.execute()?;
//         println!("execute stmt done");
//         let res = taos.query("select n from tb0").await?;
//         assert_eq!(tag, res.rows[0][0]);
//         taos.exec(format!("drop database {}", db)).await?;
//         Ok(())
//     }
//     macro_rules! _test_tag_null {
//         ($ty:ty, $v:expr) => {
//             paste::paste! {
//                 #[tokio::test]
//                 #[test_catalogue()]
//                 #[doc = "Test bind null to type " $ty]
//                 async fn [<null_tag_ $ty:snake>]() -> Result<(), Error> {
//                     let db = stdext::function_name!()
//                         .replace("::{{closure}}", "")
//                         .replace("::", "_")
//                         .replace("libtaos_", "");
//                     stmt_tag(&db, $v, Field::Null).await
//                 }
//             }
//         };
//     }
//     _test_tag_null!(bool, "bool");
//     _test_tag_null!(tinyint, "tinyint");
//     _test_tag_null!(smallint, "smallint");
//     _test_tag_null!(int, "int");
//     _test_tag_null!(bigint, "bigint");
//     _test_tag_null!(utinyint, "tinyint unsigned");
//     _test_tag_null!(usmallint, "smallint unsigned");
//     _test_tag_null!(uint, "int unsigned");
//     _test_tag_null!(ubigint, "bigint unsigned");
//     _test_tag_null!(timestamp, "timestamp");
//     _test_tag_null!(float, "float");
//     _test_tag_null!(double, "double");
//     _test_tag_null!(binary, "binary(10)");
//     _test_tag_null!(nchar, "nchar(10)");
//     // set null in json tag is currently abort taosd. see TD-12452.
//     // TD-12452 is fixed by https://github.com/taosdata/TDengine/pull/9317
//     _test_tag_null!(json, "json");
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with bool values.
//     async fn bool() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "bool", Field::Bool(true)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with tiny int values.
//     async fn tinyint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "tinyint", Field::TinyInt(-0x7f)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with small int values.
//     async fn smallint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "smallint", Field::SmallInt(0x7fff)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with int values.
//     async fn int() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "int", Field::Int(0x7fffffff)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with bigint values.
//     async fn bigint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "bigint", Field::BigInt(0x7fffffff_ffffffff)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with unsigned tinyint values.
//     async fn utinyint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "tinyint unsigned", Field::UTinyInt(0)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with unsigned smallint values.
//     async fn usmallint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "smallint unsigned", Field::USmallInt(1)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with unsigned int values.
//     async fn uint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "int unsigned", Field::UInt(1)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with unsigned bigint values.
//     async fn ubigint() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "bigint unsigned", Field::UBigInt(1)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with float values.
//     async fn float() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "float", Field::Float(1.0)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with double values.
//     async fn double() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         stmt_test(&db, "double", Field::Double(1.0)).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with binary values.
//     async fn binary() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         let v = Field::Binary("0123456789".into());
//         stmt_test(&db, "binary(10)", v).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with nchar(unicode) values.
//     async fn nchar() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         let v = Field::NChar("一二三四五六七八九十".into());
//         stmt_test(&db, "nchar(10)", v).await
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with json values.
//     async fn json() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         let v = Field::Json(serde_json::from_str("{\"tag1\":\"一二三四五六七八九十\"}").unwrap());

//         let taos = taos()?;
//         println!("test json using {}", db);
//         taos.exec(format!("drop database if exists {}", db)).await?;
//         taos.exec(format!("create database if not exists {} keep 36500", db))
//             .await?;
//         taos.exec(format!("use {}", db)).await?;
//         taos.exec("create stable if not exists stb0 (ts timestamp, n int) tags(j json)")
//             .await?;
//         let mut stmt = taos.stmt("insert into ? using stb0 tags(?) values(?,?)")?;
//         println!("set tbname with tags");
//         stmt.set_tbname_tags("tb0", [&v])?;
//         println!("bind values");
//         assert!(stmt.is_insert());
//         assert_eq!(stmt.num_params(), 2);
//         let ts = Field::Timestamp(Timestamp::now());
//         stmt.bind(vec![ts, Field::Int(3)].iter())?;
//         let _ = stmt.execute()?;
//         let res = taos.query("select j from stb0").await?;
//         let row = res.rows.iter().next().unwrap();
//         assert_eq!(&v, row.iter().next().unwrap());
//         taos.exec(format!("drop database {}", db)).await?;
//         Ok(())
//     }

//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT inserting with all types of values.
//     async fn all_types() -> Result<(), Error> {
//         let taos = taos()?;
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         println!("{}", db);
//         taos.exec(format!("drop database if exists {}", db)).await?;
//         taos.exec(format!("create database if not exists {} keep 36500", db))
//             .await?;
//         taos.exec(format!("use {}", db)).await?;
//         taos.exec(
//             "create table if not exists tb0 (ts timestamp,
//              c1 tinyint, c2 smallint, c3 int, c4 bigint,
//              c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned,
//              c9 float, c10 double, c11 binary(10), c12 nchar(10))",
//         )
//         .await?;
//         let mut stmt = taos.stmt("insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?)")?;
//         stmt.set_tbname("tb0")?;
//         assert!(stmt.is_insert());

//         assert_eq!(stmt.num_params(), 13);
//         let ts = Field::Timestamp(Timestamp::now());
//         let c1 = Field::TinyInt(1);
//         let c2 = Field::SmallInt(2);
//         let c3 = Field::Int(3);
//         let c4 = Field::BigInt(4);
//         let c5 = Field::UTinyInt(5);
//         let c6 = Field::USmallInt(6);
//         let c7 = Field::UInt(7);
//         let c8 = Field::UBigInt(8);
//         let c9 = Field::Float(9.0);
//         let c10 = Field::Double(9.0);
//         let c11 = Field::Binary("binary".into());
//         let c12 = Field::NChar("nchar".into());
//         stmt.bind(vec![ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12].iter())?;
//         let _ = stmt.execute()?;
//         let res = taos.query("select count(*) as count from tb0").await?;
//         println!("{:?}", res);
//         taos.exec(format!("drop database {}", db)).await?;
//         Ok(())
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT set tbname with upper-case stable, see jira TD-12977
//     async fn test_uppercase_tbname() -> Result<(), Error> {
//         let db = "uppercase_test";
//         let taos = taos()?;
//         taos.exec(format!("drop database if exists {db}")).await?;
//         taos.exec(format!("create database {db}")).await?;
//         taos.exec(format!("use {db}")).await?;
//         taos.exec(format!("create stable STB(ts timestamp, n int) tags(b int)")).await?;
//         let mut stmt = taos.stmt("insert into ? using STB tags(?) values(?, ?)")?;

//         stmt.set_tbname_tags("tb0", [0i32])?;
//         // stmt.bind(&[0i32])?;
//         let values = vec![Field::Timestamp(Timestamp::now()), Field::Int(10)];
//         stmt.bind(&values)?;

//         assert!(stmt.is_insert());
//         assert_eq!(stmt.num_params(), 2);

//         let _ = stmt.execute()?;
//         const LIMIT: i64 = 100;

//         for i in 1..LIMIT {
//             stmt.set_tbname_tags(format!("tb{}", i), &[2i32])?;
//             stmt.bind(&values)?;
//         }
//         let _ = stmt.execute()?;
//         let res = taos.query("select count(*) as count from stb").await?;
//         assert_eq!(res.rows[0][0], Field::BigInt(LIMIT));
//         taos.exec(format!("drop database {}", db)).await?;

//         Ok(())
//     }
//     #[tokio::test]
//     #[test_catalogue()]
//     /// Test STMT API insertion with tags
//     async fn test_stmt_tags() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         println!("{:?}", db);
//         let taos = taos()?;
//         taos.exec(format!("drop database if exists {}", db)).await?;
//         taos.exec(format!("create database if not exists {} keep 36500", db))
//             .await?;
//         taos.exec(format!("use {}", db)).await?;
//         taos.exec("create table if not exists stb (ts timestamp, n int) tags(b int)")
//             .await?;

//         let mut stmt = taos.stmt("insert into ? using stb tags(?) values(?, ?)")?;

//         stmt.set_tbname_tags("tb0", [0i32])?;
//         // stmt.bind(&[0i32])?;
//         let values = vec![Field::Timestamp(Timestamp::now()), Field::Int(10)];
//         stmt.bind(&values)?;

//         assert!(stmt.is_insert());
//         assert_eq!(stmt.num_params(), 2);

//         let _ = stmt.execute()?;
//         const LIMIT: i64 = 100;

//         for i in 1..LIMIT {
//             stmt.set_tbname_tags(format!("tb{}", i), &[2i32])?;
//             stmt.bind(&values)?;
//         }
//         let _ = stmt.execute()?;
//         let res = taos.query("select count(*) as count from stb").await?;
//         assert_eq!(res.rows[0][0], Field::BigInt(LIMIT));
//         taos.exec(format!("drop database {}", db)).await?;
//         Ok(())
//     }
// }
