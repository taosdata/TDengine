use bitvec::prelude::*;
use tokio::sync::Mutex;
// use port_selector::Port;
use std::{
    fmt::{Debug, Display, Formatter},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, ToSocketAddrs},
    ops::{Deref, Range},
    sync::Arc,
};

#[derive(Clone)]
pub struct Port {
    port: Arc<u16>,
    bitmap: Arc<Mutex<BitVec>>,
}

impl Display for Port {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.port)
    }
}
impl AsRef<u16> for Port {
    fn as_ref(&self) -> &u16 {
        &self.port
    }
}

impl Deref for Port {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.port
    }
}

impl PartialEq<u16> for Port {
    fn eq(&self, other: &u16) -> bool {
        *self.port.deref() == *other
    }
}

impl PartialEq<Port> for u16 {
    fn eq(&self, other: &Port) -> bool {
        self == &other.get()
    }
}

impl Debug for Port {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Port")
            .field("port", &self.port)
            .field(
                "in_use",
                &self.bitmap.try_lock().map(|bitmap| bitmap.count_ones()),
            )
            .finish()
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        let bitmap = self.bitmap.clone();
        tracing::info!("Dropping port: {}", &self.port);
        if Arc::strong_count(&self.port) > 1 {
            return;
        }
        let port = self.get();
        let index = port - 6051;

        // TD-32208: use sync lock to avoid use tokio::spawn, which may run with no tokio runtime and cause panic.
        let mut bitmap = futures::executor::block_on(bitmap.lock());
        bitmap.set(index as _, false);
    }
}

impl Port {
    /// Get the port number
    pub fn get(&self) -> u16 {
        *self.port.deref()
    }
}
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
    pub async fn get(&self) -> Option<Port> {
        let mut bitmap = self.bitmap.lock().await;
        loop {
            if let Some(index) = bitmap.first_zero() {
                let port = self.range.start + index as u16;
                if matches!(port, 6055 | 6060 | 7070) {
                    bitmap.set(index, true);
                    continue;
                }
                if is_free_tcp(port) {
                    bitmap.set(index, true);
                    return Some(Port {
                        port: Arc::new(port),
                        bitmap: self.bitmap.clone(),
                    });
                } else {
                    bitmap.set(index, true);
                    continue;
                }
            }
            return None;
        }
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
