use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::Context;
use faststr::FastStr;
use itertools::Itertools;
use rdkafka::{ClientConfig, Offset, config::RDKafkaLogLevel, consumer::Consumer};
use taos::Dsn;
use taosx_core::{
    config::AdvancedOptions,
    core_metrics::CoreMetrics,
    utils::{self, codec::StringDecoder},
};

use crate::{
    LoggingConsumer, METRIC_CONSUMERS, config::connect::KafkaConnectConfig, context::CustomContext,
};

#[derive(Debug, Clone)]
pub struct KafkaTaskConfig {
    pub connect: KafkaConnectConfig,

    pub timeout: i64,
    pub group: String,
    pub topics: Vec<String>,
    pub seek_to: Option<Offset>,

    pub fallback_offset: String,
    pub fetch_max_wait_time: Option<Duration>,
    pub fetch_min_bytes: Option<i32>,
    pub fetch_max_bytes_per_partition: Option<i32>,
    pub fetch_crc_validation: Option<bool>,
    pub connection_idle_timeout: Option<Duration>,
    pub client_id: Option<String>,
    pub commit_interval: Option<Duration>,
    pub enable_group_instance_id: bool,

    pub codec_processor: Option<StringDecoder>,

    pub advanced_options: AdvancedOptions,

    pub extras: Option<HashMap<FastStr, FastStr>>,
}

impl KafkaTaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let config = KafkaTaskConfig {
            connect: KafkaConnectConfig::from_dsn(dsn)?,
            timeout: Self::parse_timeout(dsn)?,
            group: Self::parse_group(dsn),
            topics: Self::parse_topics(dsn)?,
            seek_to: utils::parse_key_in_dsn::<bool>(dsn, "seek_to_end")?
                .filter(|&b| b)
                .map(|_| Offset::End),
            fallback_offset: Self::parse_fallback_offset(dsn)?,
            fetch_max_wait_time: Self::parse_fetch_max_wait_time(dsn)?,
            fetch_min_bytes: Self::parse_fetch_min_bytes(dsn)?,
            fetch_max_bytes_per_partition: Self::parse_fetch_max_bytes_per_partition(dsn)?,
            fetch_crc_validation: Self::parse_fetch_crc_validation(dsn)?,
            connection_idle_timeout: Self::parse_connection_idle_timeout(dsn)?,
            commit_interval: Self::parse_commit_interval(dsn)?,
            client_id: Self::parse_client_id(dsn)?,
            enable_group_instance_id: Self::parse_enable_group_instance_id(dsn),
            advanced_options: AdvancedOptions::from_dsn(dsn)?,
            extras: Self::parse_extras(dsn)?,
            codec_processor: Self::parse_codec_processor(dsn)?,
        };
        Ok(config)
    }

    pub fn parse_codec_processor(dsn: &Dsn) -> anyhow::Result<Option<StringDecoder>> {
        dsn.params
            .get("char_encoding")
            .map(|s| {
                s.parse()
                    .with_context(|| format!("invalid char_encoding: {s}"))
            })
            .transpose()
    }

    fn parse_group(dsn: &Dsn) -> String {
        dsn.params
            .get("group")
            .unwrap_or(&"".to_string())
            .to_string()
    }

    pub fn parse_topics(dsn: &Dsn) -> anyhow::Result<Vec<String>> {
        dsn.get("topics")
            .map(|s| s.split(",").map(|s| s.to_string()).collect::<Vec<String>>())
            .ok_or(anyhow::anyhow!("topics is required"))
    }

    pub fn parse_fallback_offset(dsn: &Dsn) -> anyhow::Result<String> {
        let fallback_offset = dsn.params.get("fallback_offset").map(String::as_str);

        match fallback_offset {
            Some("Smallest") => Ok(String::from("smallest")),
            Some("Earliest") => Ok(String::from("earliest")),
            Some("Beginning") => Ok(String::from("beginning")),
            Some("Largest") => Ok(String::from("largest")),
            Some("Latest") => Ok(String::from("latest")),
            Some("End") => Ok(String::from("end")),
            Some("Error") => Ok(String::from("error")),
            Some(_) | None => Ok(String::from("largest")),
        }
    }

    fn parse_fetch_max_wait_time(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("fetch_max_wait_time")
            .map(String::as_str)
            .map(|s| {
                let result = utils::parse_duration(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_max_wait_time: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_fetch_min_bytes(dsn: &Dsn) -> anyhow::Result<Option<i32>> {
        dsn.params
            .get("fetch_min_bytes")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<i32>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_min_bytes: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_fetch_max_bytes_per_partition(dsn: &Dsn) -> anyhow::Result<Option<i32>> {
        dsn.params
            .get("fetch_max_bytes_per_partition")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<i32>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_max_bytes_per_partition: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(Some(1024 * 1024)))
    }

    fn parse_fetch_crc_validation(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("fetch_crc_validation")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<bool>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_crc_validation: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_connection_idle_timeout(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("connection_idle_timeout")
            .map(String::as_str)
            .map(|s| {
                let result = utils::parse_duration(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid connection_idle_timeout: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_commit_interval(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("commit_interval")
            .map(String::as_str)
            .map(|s| {
                let result = utils::parse_duration(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid commit_interval: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_client_id(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let client_id = dsn.params.get("client_id").and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        Ok(client_id)
    }

    fn parse_enable_group_instance_id(dsn: &Dsn) -> bool {
        dsn.params
            .get("enable_group_instance_id")
            .map(String::as_str)
            .map(|s| s.parse::<bool>().unwrap_or(false))
            .unwrap_or(false)
    }

    fn parse_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        let timeout = dsn
            .params
            .get("timeout")
            .map(String::as_str)
            .unwrap_or("0ms");
        if timeout.eq("never") || timeout.starts_with("0") || timeout.starts_with("-1") {
            return Ok(-1);
        }

        let result = utils::parse_duration(timeout);
        match result {
            Ok(d) => Ok(d.as_millis() as i64),
            Err(e) => Err(anyhow::anyhow!(
                "invalid timeout: {}, cause: {}",
                timeout,
                e
            )),
        }
    }

    fn parse_extras(dsn: &Dsn) -> anyhow::Result<Option<HashMap<FastStr, FastStr>>> {
        let mut extras = HashMap::new();
        for (k, v) in dsn
            .params
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .filter(|(k, _)| k.contains('.') && !utils::contains_uppercase(k))
            .map(|(k, v)| (k.trim(), v.trim()))
        {
            extras.insert(FastStr::from_str(k)?, FastStr::from_str(v)?);
        }
        if let Ok(str) = std::env::var("KAFKA_CONSUMER_EXTRAS") {
            tracing::debug!("use env KAFKA_CONSUMER_EXTRAS: {}", str);
            for (k, v) in str
                .split(',')
                .flat_map(|s| s.split('=').collect_tuple::<(_, _)>())
                .map(|(k, v)| (k.trim(), v.trim()))
            {
                extras.insert(FastStr::from_str(k)?, FastStr::from_str(v)?);
            }
        }
        Ok(if extras.is_empty() {
            None
        } else {
            Some(extras)
        })
    }

    pub async fn build_consumer(
        &self,
        instance: Option<&str>,
        topics: &[&str],
        metrics: &Arc<CoreMetrics>,
    ) -> anyhow::Result<LoggingConsumer> {
        self.build_consumer_with_context(instance, topics, CustomContext::new(metrics.clone()))
            .await
    }

    pub async fn build_consumer_with_context(
        &self,
        instance: Option<&str>,
        topics: &[&str],
        mut context: CustomContext,
    ) -> anyhow::Result<LoggingConsumer> {
        let mut client = build_client_config(self.connect.clone())?;
        // Client identifier, default "rdkafka".
        if let Some(client_id) = &self.client_id {
            client.set("client.id", client_id);
        }
        // All clients sharing the same group.id belong to the same group.
        client.set("group.id", &self.group);
        // Action to take when there is no initial offset in offset store or the desired offset is out of range.
        // smallest, earliest, beginning, largest, latest, end, error
        client.set("auto.offset.reset", &self.fallback_offset);

        // Refer to [rdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
        // > Note: It is recommended to set `enable.auto.offset.store=false`
        // >  for long-time processing applications and then explicitly store offsets
        // >  (using offsets_store()) after message processing, to make sure
        // >  offsets are not auto-committed prior to processing has finished.
        client.set("enable.auto.offset.store", "false");
        client.set("enable.auto.commit", "true");

        client.set(
            "auto.commit.interval.ms",
            self.commit_interval
                .as_ref()
                .map_or(5000, |d| d.as_millis())
                .to_string(),
        );

        client.set("queued.max.messages.kbytes", "262144");

        // Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages, default 500ms.
        client.set(
            "fetch.wait.max.ms",
            self.fetch_max_wait_time
                .map(|v| v.as_millis())
                .unwrap_or(500)
                .to_string(),
        );

        // Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        // for high-level consumers. If this interval is exceeded the consumer is considered failed
        // and the group will rebalance in order to reassign the partitions to another consumer group member.
        // Warning: Offset commits may be not possible at this point.
        client.set("max.poll.interval.ms", "3600000");

        if let Some(instance) = instance {
            // client.set("enable.idempotence", "true");
            client.set("group.instance.id", instance);
        }

        // A larger value allows the consumer to fetch more messages in one request.
        // client.set("queued.min.messages", "1000000");

        // Minimum number of bytes the broker responds with, default is 1.
        client.set(
            "fetch.min.bytes",
            self.fetch_min_bytes.unwrap_or(1).to_string(),
        );

        // Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.
        if let Some(v) = self.fetch_max_bytes_per_partition {
            client.set("fetch.message.max.bytes", v.to_string());
        }
        // Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred
        if let Some(v) = self.fetch_crc_validation {
            client.set("check.crcs", v.to_string());
        }
        // Close broker connections after the specified time of inactivity.
        if let Some(v) = self.connection_idle_timeout {
            client.set("connections.max.idle.ms", v.as_millis().to_string());
        }

        client.set("partition.assignment.strategy", "cooperative-sticky");
        client.set("socket.keepalive.enable", "true");
        client.set("socket.timeout.ms", "300000");

        if let Some(extras) = &self.extras {
            for (k, v) in extras.iter() {
                client.set(k.as_str(), v.as_str());
                tracing::info!("Set extra config: {}={}", k, v);
            }
        }
        // Set log level and create consumer
        let joins = context.fetch_add_joins();
        match instance {
            Some(instance) => {
                tracing::info!(joins, "Consumer {instance} begin join");
            }
            None => {
                tracing::info!(joins, "Consumer begin join");
            }
        }

        if let Some(offset) = &self.seek_to {
            context.seek_to = Some(*offset);
        }

        let consumer: LoggingConsumer = client
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(context)
            .context("Consumer creation failed")?;

        consumer
            .subscribe(topics)
            .context("Kafka subscribe consumer error")?;

        let subscription = consumer
            .subscription()
            .context("Kafka get consumer subscription metadata error")?;
        consumer
            .context()
            .metrics()
            .add_extra_metric(&METRIC_CONSUMERS, 1);
        for t in subscription.elements() {
            tracing::info!(
                kafka.consumed.partions = subscription.count(),
                "Consumer subscribed to topic: {}:{}:{:?}",
                t.topic(),
                t.partition(),
                t.offset()
            );
        }

        if subscription.count() > 0 {
            let _ = consumer.store_offsets(&subscription);
        } else {
            tracing::info!("No subscription found");
        }

        Ok(consumer)
    }
}

pub fn build_client_config(config: KafkaConnectConfig) -> anyhow::Result<ClientConfig> {
    let mut client_config = ClientConfig::new();

    // set bootstrap servers
    client_config.set("bootstrap.servers", config.bootstrap_servers.join(","));

    // security.protocol: plaintext, ssl, sasl_plaintext, sasl_ssl
    match (config.use_ssl, config.use_sasl) {
        (true, true) => client_config.set("security.protocol", "sasl_ssl"),
        (true, false) => client_config.set("security.protocol", "ssl"),
        (false, true) => client_config.set("security.protocol", "sasl_plaintext"),
        (false, false) => client_config.set("security.protocol", "plaintext"),
    };

    // ssl settings
    if config.use_ssl {
        if let Some(ca_cert) = config.ca_cert {
            client_config.set("ssl.ca.pem", ca_cert);
        }
        if let Some(ca_password) = config.ca_cert_password {
            client_config.set("ssl.key.password", ca_password);
        }
        if let Some(client_cert) = config.client_cert {
            client_config.set("ssl.certificate.pem", client_cert);
        }
        if let Some(client_key) = config.client_key {
            client_config.set("ssl.key.pem", client_key);
        }
        // ref: https://karafka.io/docs/FAQ/#why-am-i-getting-error0a000086ssl-routinescertificate-verify-failed-after-upgrading-karafka
        client_config.set("ssl.endpoint.identification.algorithm", "none");
    }

    // sasl settings
    if config.use_sasl {
        if let Some(sasl_mechanism) = config.sasl_mechanism {
            if sasl_mechanism == "GSSAPI" {
                client_config.set("sasl.mechanisms", "GSSAPI");
                // get config or use default
                let sasl_kerberos_service_name =
                    if let Some(val) = config.sasl_kerberos_service_name {
                        val
                    } else {
                        "".to_string()
                    };
                let sasl_kerberos_principal = if let Some(val) = config.sasl_kerberos_principal {
                    val
                } else {
                    "".to_string()
                };
                let sasl_kerberos_kinit_cmd = if let Some(val) = config.sasl_kerberos_kinit_cmd {
                    val
                } else {
                    "kinit -R -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal} || kinit -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal}".to_string()
                };
                let sasl_kerberos_keytab = if let Some(val) = config.sasl_kerberos_keytab {
                    val
                } else {
                    "".to_string()
                };
                // verify the broker's kinit.cmd, keytab and principal
                let init_cmd = sasl_kerberos_kinit_cmd
                    .replace("%{sasl.kerberos.keytab}", sasl_kerberos_keytab.as_str())
                    .replace(
                        "%{sasl.kerberos.principal}",
                        sasl_kerberos_principal.as_str(),
                    );
                let output = std::process::Command::new("bash")
                    .arg("-c")
                    .arg(init_cmd)
                    .output();
                if let Ok(output) = output {
                    if !output.status.success() {
                        let stderr = std::str::from_utf8(&output.stderr)
                            .expect("Output should always be UTF-8");
                        tracing::error!("{stderr}");
                        anyhow::bail!("{}", stderr.lines().next().unwrap_or("EMPTY STDERR"));
                    }
                }
                // set to client
                client_config.set("sasl.kerberos.service.name", sasl_kerberos_service_name);
                client_config.set("sasl.kerberos.principal", sasl_kerberos_principal);
                client_config.set("sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
                client_config.set("sasl.kerberos.keytab", sasl_kerberos_keytab);
                // each entry will be resolved and expanded into a list of canonical names
                // client_config.set(
                //     "client.dns.lookup",
                //     "resolve_canonical_bootstrap_servers_only",
                // );
            } else {
                client_config.set("sasl.mechanisms", sasl_mechanism);
                if let Some(sasl_username) = config.sasl_username {
                    client_config.set("sasl.username", sasl_username);
                }
                if let Some(sasl_password) = config.sasl_password {
                    client_config.set("sasl.password", sasl_password);
                }
            }
        }
    }

    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_seek_to() {
        let dsn = Dsn::from_str("kafka://:?topics=t&group=g&seek_to_end=true").unwrap();
        let seek_to = KafkaTaskConfig::from_dsn(&dsn).unwrap().seek_to.unwrap();
        assert_eq!(Offset::End, seek_to);

        let dsn = Dsn::from_str("kafka://:?topics=t&group=g").unwrap();
        let seek_to = KafkaTaskConfig::from_dsn(&dsn).unwrap().seek_to;
        assert!(seek_to.is_none());

        let dsn = Dsn::from_str("kafka://:?topics=t&group=g&seek_to_end=false").unwrap();
        let seek_to = KafkaTaskConfig::from_dsn(&dsn).unwrap().seek_to;
        assert!(seek_to.is_none());
    }

    #[test]
    fn test_parse_group() {
        let dsn = Dsn::from_str("kafka://:?group=group1").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("group1", group);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("", group);

        let dsn = Dsn::from_str("kafka://:?group=&topics=tp1").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("", group);
    }

    #[test]
    fn test_parse_topics() {
        let dsn = Dsn::from_str("kafka://:?topics=tp1,tp2").unwrap();
        let topics = KafkaTaskConfig::parse_topics(&dsn);
        assert_eq!("tp1", topics.as_ref().unwrap()[0]);
        assert_eq!("tp2", topics.as_ref().unwrap()[1]);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let topics = KafkaTaskConfig::parse_topics(&dsn);
        assert!(topics.is_err());
        assert_eq!("topics is required", topics.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_fallback_offset() {
        // Smallest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Smallest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("smallest", result);

        // Earliest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Earliest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("earliest", result);

        // Beginning
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Beginning").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("beginning", result);

        // Largest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Largest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);

        // Latest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Latest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("latest", result);

        // End
        let dsn = Dsn::from_str("kafka://:?fallback_offset=End").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("end", result);

        // Error
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Error").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("error", result);

        // default 1
        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);

        // default 2
        let dsn = Dsn::from_str("kafka://:?fallback_offset=xx").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);
    }

    #[test]
    fn test_parse_fetch_max_wait_time() {
        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=1h").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(3600, result.unwrap().as_secs());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_fetch_min_bytes() {
        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=100").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(100, result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_min_bytes: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_max_bytes_per_partition() {
        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=100").unwrap();
        let config = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(100, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(1024 * 1024, config.unwrap());

        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_max_bytes_per_partition: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_crc_validation() {
        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=true").unwrap();
        let config = KafkaTaskConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_some());
        assert!(config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = KafkaTaskConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_crc_validation(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_crc_validation: invalid, cause: provided string was not `true` or `false`",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_connection_idle_timeout() {
        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=1h").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_some());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=invalid").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_client_id() {
        let dsn = Dsn::from_str("kafka://?client_id=client1").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!("client1", result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?client_id=").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("kafka://?timeout=5s").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5000, result);

        let dsn = Dsn::from_str("kafka://?timeout=30s").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(30 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=5m").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5 * 60 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=6h").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(6 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=1d").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(24 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("kafka://?timeout=never").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("kafka://?timeout=invalid").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn);
        assert!(result.is_err());
    }
}
