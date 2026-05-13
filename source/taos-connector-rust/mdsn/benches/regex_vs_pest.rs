#![feature(test)]

extern crate test;

use std::str::FromStr;

use mdsn::Dsn;
use test::Bencher;

#[bench]
fn bench_regex(b: &mut Bencher) {
    b.iter(|| {
        for _ in 0..100 {
            let _ = Dsn::from_regex(
                "taos://root:taosdata@h1:6030,h2:6030/database?name=abc&name2=abc2",
            )
            .unwrap();
        }
    });
}

#[bench]
fn bench_pest(b: &mut Bencher) {
    b.iter(|| {
        for _ in 0..100 {
            let _ =
                Dsn::from_str("taos://root:taosdata@h1:6030,h2:6030/database?name=abc&name2=abc2")
                    .unwrap();
        }
    });
}
