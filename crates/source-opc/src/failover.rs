use std::str::FromStr;

use anyhow::Context;
use taos::Dsn;
use taosx_core::runners::opc::config::connect::ConnectConfig;
use taosx_utils::dsn::parse_multiple_value;

pub fn failover_config(from: Dsn, to: Dsn) -> anyhow::Result<Vec<(Dsn, Dsn)>> {
    let mut res = Vec::new();
    let config = ConnectConfig::from_dsn(&from).context("parse opc connect config error")?;
    if config.ua.is_some() || config.da.is_some() {
        res.push((from.clone(), to.clone()));
    }
    if let Some(failover_addrs) = parse_multiple_value::<String>(&from, "failover_endpoints")? {
        for addr in failover_addrs {
            let ep = format!(
                "opcua://{}",
                addr.trim_start_matches("opcua://")
                    .trim_start_matches("opcda://")
            );
            let mut addr = Dsn::from_str(&ep).context("opc failover addr parse to dsn error")?;
            addr.driver = from.driver.clone();
            addr.params = from.params.clone();
            res.push((addr, to.clone()))
        }
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use taosx_utils::dsn::json_to_dsn;

    use super::*;

    #[test]
    fn failover_config_no_failover_test() {
        let from = json_to_dsn(&serde_json::json!({
            "type": "opcua",
            "data": {
                "endpoint": "opcua://opc:50000/a/b",
                "security_mode": "None",
                "security_policy": "a"
            }
        }))
        .unwrap();

        let failover_addrs =
            failover_config(from, "taos://127.0.0.1:6041".parse().unwrap()).unwrap();
        assert_eq!(
            failover_addrs,
            vec![(
                "opcua://opc:50000/a/b?security_mode=None&security_policy=a"
                    .parse()
                    .unwrap(),
                "taos://127.0.0.1:6041".parse().unwrap()
            )]
        );
    }

    #[test]
    fn failover_config_single_failover_test() {
        let from = json_to_dsn(&serde_json::json!({
            "type": "opcda",
            "data": {
                "endpoint": "opcda://opc:50000/a/b",
                "failover_endpoints": "opcda://opc:60000/a2/b2",
                "security_mode": "None",
                "security_policy": "a"
            }
        }))
        .unwrap();

        let failover_addrs =
            failover_config(from, "taos://127.0.0.1:6041".parse().unwrap()).unwrap();
        assert_eq!(
            failover_addrs,
            vec![
                (
                    "opcda://opc:50000/a/b?security_mode=None&security_policy=a&failover_endpoints=opcda://opc:60000/a2/b2"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc:60000/a2/b2?security_mode=None&security_policy=a&failover_endpoints=opcda://opc:60000/a2/b2"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                )
            ]
        );

        let from = json_to_dsn(&serde_json::json!({
            "type": "opcda",
            "data": {
                "endpoint": "opcda://opc:50000/a/b",
                "failover_endpoints": "opc:60000/a2/b2",
                "security_mode": "None",
                "security_policy": "a"
            }
        }))
        .unwrap();

        let failover_addrs =
            failover_config(from, "taos://127.0.0.1:6041".parse().unwrap()).unwrap();
        assert_eq!(
            failover_addrs,
            vec![
                (
                    "opcda://opc:50000/a/b?security_mode=None&security_policy=a&failover_endpoints=opc:60000/a2/b2"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc:60000/a2/b2?security_mode=None&security_policy=a&failover_endpoints=opc:60000/a2/b2"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                )
            ]
        );
    }

    #[test]
    fn failover_config_multi_failover_test() {
        let from = json_to_dsn(&serde_json::json!({
            "type": "opcda",
            "data": {
                "endpoint": "opcda://opc:50000/a/b",
                "failover_endpoints": "opcda://opc1:60000/a2/b2,opcda://opc2:40000/a3/b3",
                "security_mode": "None",
                "security_policy": "a"
            }
        }))
        .unwrap();

        let failover_addrs =
            failover_config(from, "taos://127.0.0.1:6041".parse().unwrap()).unwrap();
        assert_eq!(
            failover_addrs,
            vec![
                (
                    "opcda://opc:50000/a/b?security_mode=None&security_policy=a&failover_endpoints=opcda://opc1:60000/a2/b2,opcda://opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc1:60000/a2/b2?security_mode=None&security_policy=a&failover_endpoints=opcda://opc1:60000/a2/b2,opcda://opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc2:40000/a3/b3?security_mode=None&security_policy=a&failover_endpoints=opcda://opc1:60000/a2/b2,opcda://opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                )
            ]
        );

        let from = json_to_dsn(&serde_json::json!({
            "type": "opcda",
            "data": {
                "endpoint": "opcda://opc:50000/a/b",
                "failover_endpoints": "opc1:60000/a2/b2,opc2:40000/a3/b3",
                "security_mode": "None",
                "security_policy": "a"
            }
        }))
        .unwrap();

        let failover_addrs =
            failover_config(from, "taos://127.0.0.1:6041".parse().unwrap()).unwrap();
        assert_eq!(
            failover_addrs,
            vec![
                (
                    "opcda://opc:50000/a/b?security_mode=None&security_policy=a&failover_endpoints=opc1:60000/a2/b2,opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc1:60000/a2/b2?security_mode=None&security_policy=a&failover_endpoints=opc1:60000/a2/b2,opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                ),
                (
                    "opcda://opc2:40000/a3/b3?security_mode=None&security_policy=a&failover_endpoints=opc1:60000/a2/b2,opc2:40000/a3/b3"
                        .parse()
                        .unwrap(),
                    "taos://127.0.0.1:6041".parse().unwrap()
                )
            ]
        );
    }
}
