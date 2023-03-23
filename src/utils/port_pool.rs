use bitvec::prelude::*;
use std::{
    ops::Range,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone)]
pub struct PortPool {
    range: Range<u16>,
    bitmap: Arc<Mutex<BitVec>>,
}
impl Default for PortPool {
    fn default() -> Self {
        let range = 6051..16050;
        let bitmap = Arc::new(Mutex::new(bitvec!(0; range.len())));
        Self { range, bitmap }
    }
}

impl PortPool {
    pub fn get(&self) -> Option<u16> {
        let mut bitmap = self.bitmap.lock().unwrap();
        loop {
            if let Some(index) = bitmap.first_zero() {
                let port = self.range.start + index as u16;
                if port_selector::is_free_tcp(port) {
                    return Some(port);
                } else {
                    bitmap.set(index, true);
                    continue;
                }
            }
            return None;
        }
    }

    pub fn put(&self, port: u16) {
        let mut bitmap = self.bitmap.lock().unwrap();
        let index = port - self.range.start;
        bitmap.set(index as _, false);
    }
}
