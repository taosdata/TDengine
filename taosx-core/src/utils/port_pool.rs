use bitvec::prelude::*;
use tokio::sync::Mutex;
// use port_selector::Port;
use std::{
    fmt::{Debug, Formatter},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, ToSocketAddrs},
    ops::Range,
    sync::Arc,
};

#[derive(Clone)]
pub struct PortPool {
    range: Range<u16>,
    bitmap: Arc<Mutex<BitVec>>,
}

impl Debug for PortPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortPool")
            .field("range", &self.range)
            .field(
                "in_use",
                &self.bitmap.try_lock().map(|bitmap| bitmap.count_ones()),
            )
            .finish()
    }
}

impl Default for PortPool {
    fn default() -> Self {
        let range = 6051..16050;
        let bitmap = Arc::new(Mutex::new(bitvec!(0; range.len())));
        Self { range, bitmap }
    }
}

impl PortPool {
    pub async fn get(&self) -> Option<u16> {
        let mut bitmap = self.bitmap.lock().await;
        loop {
            if let Some(index) = bitmap.first_zero() {
                let port = self.range.start + index as u16;
                if is_free_tcp(port) {
                    bitmap.set(index, true);
                    return Some(port);
                } else {
                    bitmap.set(index, true);
                    continue;
                }
            }
            return None;
        }
    }

    pub async fn put(&self, port: u16) {
        let mut bitmap = self.bitmap.lock().await;
        let index = port - self.range.start;
        bitmap.set(index as _, false);
    }
}

fn is_free_tcp(port: u16) -> bool {
    let ipv4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    test_bind_tcp(ipv4).is_some()
}

// Try to bind to a socket using TCP
fn test_bind_tcp<A: ToSocketAddrs>(addr: A) -> Option<u16> {
    Some(TcpListener::bind(addr).ok()?.local_addr().ok()?.port())
}
