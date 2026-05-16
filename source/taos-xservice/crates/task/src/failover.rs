use taos::Dsn;

pub fn get_datasource_failover_config(from: Dsn, to: Dsn) -> anyhow::Result<Vec<(Dsn, Dsn)>> {
    match (from.driver.as_str(), to.driver.as_str()) {
        ("opc" | "opcda" | "opcua", "taos") => source_opc::failover::failover_config(from, to),
        ("mqtt", "taos") => source_mqtt::failover::get_datasource_failover_config(from, to),
        _ => Ok(vec![(from, to)]),
    }
}
