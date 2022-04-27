use super::{Error, Result, Taos, TaosOptions};

/// Connection pool with r2d2.
pub type TaosPool = r2d2::Pool<TaosOptions>;

impl r2d2::ManageConnection for TaosOptions {
    type Connection = Taos;
    type Error = Error;

    fn connect(&self) -> Result<Self::Connection> {
        self.build()
    }

    fn is_valid(&self, _: &mut Self::Connection) -> Result<()> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[test]
fn test_r2d2() {
    use std::sync::mpsc::channel;
    use std::thread;

    let opts = TaosOptions::new();
    let pool = TaosPool::builder().build(opts).expect("");
    let (tx, rx) = channel();
    for _ in 0..4 {
        let tx = tx.clone();
        let pool = pool.clone();
        thread::spawn(move || {
            let taos = pool.get().unwrap();
            let _ = taos.query_sync("show connections").unwrap().affected_rows();
            tx.send(10).unwrap();
        });
    }
    for _ in 0..4 {
        let j = rx.recv().unwrap();
        println!("res: {j}");
    }
}
