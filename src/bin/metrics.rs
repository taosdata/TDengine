use std::{thread, time};

use anyhow::Result;
use taosx::metrics::*;

fn main() -> Result<()> {
    Metrics::default().init().unwrap();

    loop {
        thread::sleep(time::Duration::from_secs(1));
    }
}
