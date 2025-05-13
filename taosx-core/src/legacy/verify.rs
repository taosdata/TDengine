use std::collections::HashSet;

pub const CLIENT_OPTIONS: [&str; 63] = [
    // taos.cfg options
    "shellActivityTimer",
    "firstEp",
    "fqdn",
    "serverPort",
    "maxShellConns",
    "logDir",
    "dataDir",
    "tempDir",
    "configDir",
    "libraryPath",
    "telemetryReporting",
    "crashReporting",
    "supportVnodes",
    "statusInterval",
    "minSlidingTime",
    "minIntervalTime",
    "queryBufferSize",
    "compressMsgSize",
    "compressColData",
    "timezone",
    "locale",
    "charset",
    "minimalLogDirGB",
    "minimalTmpDirGB",
    "minimalDataDirGB",
    "monitor",
    "numOfLogLines",
    "asyncLog",
    "logKeepDays",
    "debugFlag",
    "tmrDebugFlag",
    "uDebugFlag",
    "rpcDebugFlag",
    "jniDebugFlag",
    "qDebugFlag",
    "cDebugFlag",
    "dDebugFlag",
    "vDebugFlag",
    "mDebugFlag",
    "wDebugFlag",
    "sDebugFlag",
    "tsdbDebugFlag",
    "tqDebugFlag",
    "fsDebugFlag",
    "udfDebugFlag",
    "smaDebugFlag",
    "idxDebugFlag",
    "tdbDebugFlag",
    "metaDebugFlag",
    "enableCoreFile",
    // rust connection extras
    "timeout",
    "connectionRetries",
    "token",
    "sparse",
    "compression",
    "minimal",
    // health check options
    "health_check_window_in_second",
    "health_check_window_in_second_type",
    "busy_threshold",
    "busy_threshold_type",
    "max_queue_length",
    "max_errors_in_window",
    "excursion",
];

lazy_static::lazy_static! {
    static ref TAOS_PARAMS: HashSet<&'static str> = CLIENT_OPTIONS.into_iter().collect();
}

#[allow(unused)]
pub fn verify_dsn(dsn: &taos::Dsn) -> anyhow::Result<()> {
    for (k, v) in &dsn.params {
        if k.trim().is_empty() {
            // "?&" will be parsed as empty key, so skip it to avoid raise error.
            continue;
        }
        if !TAOS_PARAMS.contains(k.as_str()) {
            anyhow::bail!("Unknown parameters: {k}={v}");
        }
    }
    Ok(())
}

/// Verify the dsn params, remove the unknown params
pub fn verify_dsn_and_retain(dsn: &mut taos::Dsn) {
    dsn.params.retain(|k, _| TAOS_PARAMS.contains(k.as_str()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::IntoDsn;

    #[test]
    fn test_verify_dsn() {
        let dsn = taos::Dsn::from_str(
            "taos+ws://root:taosdata@localhost:6041/test?busy_threshold=30%&busy_threshold_type=%&compression=false&excursion=500ms&health_check_window_in_second_type=s&max_errors_in_window=10&max_queue_length=1000",
        )
            .unwrap();
        verify_dsn(&dsn).unwrap();
    }

    #[test]
    fn test_verify_dsn_return() {
        let mut dsn = taos::Dsn::from_str(
            "taos+ws://root:taosdata@localhost:6041/test?busy_threshold=30%&busy_threshold_type=%&compression=false&excursion=500ms&health_check_window_in_second_type=s&max_errors_in_window=10&max_queue_length=1000",
        )
            .unwrap();
        verify_dsn_and_retain(&mut dsn);
        assert_eq!(dsn.params.len(), 7);

        let mut dsn = "taos+ws://?&".into_dsn().unwrap();
        verify_dsn_and_retain(&mut dsn);
        assert_eq!(dsn.params.len(), 0);
    }
}
