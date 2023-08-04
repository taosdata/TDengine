use bitvec::prelude::*;
// use port_selector::Port;
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6, TcpListener, ToSocketAddrs},
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

    pub fn put(&self, port: u16) {
        let mut bitmap = self.bitmap.lock().unwrap();
        let index = port - self.range.start;
        bitmap.set(index as _, false);
    }
}

fn is_free_tcp(port: u16) -> bool {
    let ipv4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let ipv6 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

    test_bind_tcp(ipv6).is_some() && test_bind_tcp(ipv4).is_some()
}

// Try to bind to a socket using TCP
fn test_bind_tcp<A: ToSocketAddrs>(addr: A) -> Option<u16> {
    Some(TcpListener::bind(addr).ok()?.local_addr().ok()?.port())
}
