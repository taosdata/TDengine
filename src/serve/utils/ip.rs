use std::net::{Ipv6Addr, SocketAddr, TcpListener, ToSocketAddrs};
use std::vec::IntoIter;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseAddrError {
    #[error("invalid address format: {0}")]
    InvalidAddressFormat(String),
    #[error("port cannot be 0, addr is: {0}")]
    PortCannotBeZero(String),
    #[error("all ports must be the same, addresses are: {0}")]
    AllPortsMustBeTheSame(String),
    #[error("no valid addresses provided")]
    NoValidAddressesProvided,
}

pub fn is_support_ipv6() -> bool {
    let addr = (Ipv6Addr::UNSPECIFIED, 0);
    TcpListener::bind(addr).is_ok()
}

pub fn str_to_socket_addr(addrs: &str) -> anyhow::Result<Vec<SocketAddr>, ParseAddrError> {
    let rs: anyhow::Result<Vec<IntoIter<SocketAddr>>, ParseAddrError> = addrs
        .split(',')
        .filter_map(|addr| {
            let addr = addr.trim();
            if addr.is_empty() {
                return None;
            }
            Some(addr.to_socket_addrs().map_err(|e| {
                ParseAddrError::InvalidAddressFormat(format!("{addr} detail error: {e}"))
            }))
        })
        .collect();
    Ok(rs?.into_iter().flatten().collect())
}

pub fn check_address_format(addrs: &str) -> anyhow::Result<(), ParseAddrError> {
    let mut ports: Vec<u16> = vec![];
    addrs.split(',').try_for_each(|addr| {
        let addr = addr.trim();
        if addr.is_empty() {
            return Ok(());
        }
        let rs = addr.to_socket_addrs();
        if rs.is_err() {
            return Err(ParseAddrError::InvalidAddressFormat(format!(
                "{addr}, detail error: {rs:?}"
            )));
        }
        for socket_addr in rs.unwrap() {
            if socket_addr.port() == 0 {
                return Err(ParseAddrError::PortCannotBeZero(addr.to_string()));
            }
            ports.push(socket_addr.port());
        }
        Ok(())
    })?;
    if ports.is_empty() {
        return Err(ParseAddrError::NoValidAddressesProvided);
    }
    let port = ports.first().unwrap();
    if ports.iter().any(|p| p != port) {
        return Err(ParseAddrError::AllPortsMustBeTheSame(addrs.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_str_to_socket_addr() {
        let addrs = "127.0.0.1:6050,,0.0.0.0:6050,[::1]:6050";
        let rs = str_to_socket_addr(addrs).unwrap();
        assert_eq!(rs.len(), 3);
    }

    #[tokio::test]
    async fn str_to_socket_addr_rejects_invalid_entry() {
        let addrs = "127.0.0.1:6050,invalid-addr";
        assert!(str_to_socket_addr(addrs).is_err());
    }

    #[tokio::test]
    async fn str_to_socket_addr_accepts_empty_input_as_empty_list() {
        let addrs = " , , ";
        let rs = str_to_socket_addr(addrs).unwrap();
        assert!(rs.is_empty());
    }

    #[tokio::test]
    async fn test_check_address_format() {
        let addrs = "127.0.0.1:6050,,0.0.0.0:6050,[::1]:6050";
        assert!(check_address_format(addrs).is_ok());
        let addrs = "127.0.0.1:6050,  , 0.0.0.0:6050 ,[::1]:6050 ";
        assert!(check_address_format(addrs).is_ok());
        let addrs = "127.0.0.1:6050,,0.0.0.0:6050,[::1]:0";
        assert!(check_address_format(addrs).is_err());
        let addrs = "127.0.0.1:6050,,0.0.0.0:6050,[::1]:0";
        assert!(check_address_format(addrs).is_err());
        let addrs = "127.0.0.1:6050,,0.0.0.0:6051";
        assert!(check_address_format(addrs).is_err());
    }

    #[tokio::test]
    async fn check_address_format_rejects_all_empty() {
        let addrs = " , , ";
        assert!(matches!(
            check_address_format(addrs),
            Err(ParseAddrError::NoValidAddressesProvided)
        ));
    }

    #[tokio::test]
    async fn check_address_format_rejects_invalid_host() {
        let addrs = "127.0.0.1:6050,not-a-host:1234";
        assert!(matches!(
            check_address_format(addrs),
            Err(ParseAddrError::InvalidAddressFormat(_))
        ));
    }
}
