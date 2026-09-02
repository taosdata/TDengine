---
sidebar_label: Subscription Test Tool
title: Subscription Test Tool
toc_max_heading_level: 4
---

`taossubscribe` verifies that an existing TDengine topic can be consumed and helps diagnose subscription delivery errors.

## Local Deployment

```text
usage: taossubscribe local [-h] [--config CONFIG] [--backend] --ip IP
                           [--show-data] [--log-level {debug,info}]
                           [--port PORT]
```

`--ip` identifies the TDengine host, `--port` is the taosAdapter port and defaults to 6041, and `--show-data` prints consumed records and saves them to the log.

## Cloud Deployment

```text
usage: taossubscribe cloud [-h] [--config CONFIG] [--backend] --ip IP
                           [--show-data] [--log-level {debug,info}]
                           --token TOKEN
```

Cloud mode uses `--token` for authentication.

## Configuration

The configuration sets TMQ consumer properties and one or more existing topics:

```ini
[parameters]
td.connect.websocket.scheme=ws
group.id=test_group_01
client.id=test_consumer_01
enable.auto.commit=true
auto.commit.interval.ms=1000
auto.offset.reset=earliest
msg.with.table.name=true
td.connect.user=root
td.connect.pass=taosdata1

[topics]
t1=test_topic1
```

## Output

The tool writes `delivery.log`, containing the subscription responses and, when enabled, consumed records.

## Examples

```bash
# Consume from a self-hosted deployment
./taossubscribe local -i 192.168.0.1 -p 6041 -s

# Consume from TDengine Cloud
./taossubscribe cloud -i <cloud-host> -t <token> -s
```
