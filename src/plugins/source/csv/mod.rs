use taos::query::Dsn;

pub struct Csv {
    dsn: Dsn,
}

impl Csv {
    fn from_dsn(dsn: impl Into<Dsn>) -> Result<Self, anyhow::Error> {
        let dsn = dsn.into();
        Ok(Self { dsn })
    }
}

