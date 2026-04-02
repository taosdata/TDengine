use taos::Dsn;

pub fn get_datasource_failover_config(from: Dsn, to: Dsn) -> anyhow::Result<Vec<(Dsn, Dsn)>> {
    if from.addresses.is_empty() {
        return Ok(vec![(from, to)]);
    }
    let mut res = Vec::with_capacity(from.addresses.len());
    for address in &from.addresses {
        let mut addr_dsn = from.clone();
        addr_dsn.addresses = vec![address.clone()];
        res.push((addr_dsn, to.clone()));
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_address_returns_one_pair() {
        let from: Dsn = "mqtt://127.0.0.1:1883?client_id=test".parse().unwrap();
        let to: Dsn = "taos://127.0.0.1:6041".parse().unwrap();
        let result = get_datasource_failover_config(from, to.clone()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.addresses.len(), 1);
        assert_eq!(result[0].0.addresses[0].host.as_deref(), Some("127.0.0.1"));
        assert_eq!(result[0].0.addresses[0].port, Some(1883));
        assert_eq!(
            result[0].0.get("client_id").map(|s| s.as_str()),
            Some("test")
        );
    }

    #[test]
    fn multiple_addresses_returns_multiple_pairs() {
        let from: Dsn = "mqtt://192.168.1.1:1883,192.168.1.2:1884?client_id=test&version=5.0"
            .parse()
            .unwrap();
        let to: Dsn = "taos://127.0.0.1:6041".parse().unwrap();
        let result = get_datasource_failover_config(from, to).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].0.addresses[0].host.as_deref(),
            Some("192.168.1.1")
        );
        assert_eq!(result[0].0.addresses[0].port, Some(1883));
        assert_eq!(
            result[0].0.get("client_id").map(|s| s.as_str()),
            Some("test")
        );
        assert_eq!(result[0].0.get("version").map(|s| s.as_str()), Some("5.0"));
        assert_eq!(
            result[1].0.addresses[0].host.as_deref(),
            Some("192.168.1.2")
        );
        assert_eq!(result[1].0.addresses[0].port, Some(1884));
        assert_eq!(
            result[1].0.get("client_id").map(|s| s.as_str()),
            Some("test")
        );
    }

    #[test]
    fn empty_addresses_returns_original_pair() {
        let mut from: Dsn = "mqtt://?client_id=test".parse().unwrap();
        from.addresses.clear();
        let to: Dsn = "taos://127.0.0.1:6041".parse().unwrap();
        let result = get_datasource_failover_config(from.clone(), to).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, from);
    }
}
