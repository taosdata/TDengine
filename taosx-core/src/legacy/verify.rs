use std::collections::HashSet;

pub const CLIENT_OPTIONS: [&str; 50] = [
    // taos.cfg options
    "shellActivityTimer",
    "firstEp",
    "fqdn",
    "serverPort",
    "maxShellConns",
    "logDir",
    "dataDir",
    "tempDir",
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
];

lazy_static::lazy_static! {
    static ref TAOS_PARAMS: HashSet<&'static str> = CLIENT_OPTIONS.into_iter().collect();
}

pub fn verify_dsn(dsn: &taos::Dsn) -> anyhow::Result<()> {
    for (k, v) in &dsn.params {
        if !TAOS_PARAMS.contains(k.as_str()) {
            anyhow::bail!("Unknown parameters: {k}={v}");
        }
    }
    Ok(())
}
