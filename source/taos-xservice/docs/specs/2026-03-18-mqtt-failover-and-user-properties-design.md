# MQTT Multi-Address Failover with User Properties — Design Spec

## Overview

Three capabilities added to the MQTT data source:

1. **Multi-address failover**: Configure multiple MQTT broker addresses. The system splits them via `get_datasource_failover_config()`, spawning a separate task per address.
2. **Custom user properties (MQTT v5 only)**: Two independent key-value pair parameters — `connect_user_properties` (CONNECT packet) and `subscribe_user_properties` (SUBSCRIBE packet).
3. **Sub-offset parameter (MQTT v5 only)**: A "Start From" dropdown (earliest/latest) merged into `subscribe_user_properties` at submission time.

## Data Flow

```
Frontend                          DSN                                    Backend
──────────────────────────────────────────────────────────────────────────────────
Broker Addresses (grouping)       endpoint=h1:p1,h2:p2                  get_datasource_failover_config()
  [{host,port}, ...]                                                      → Vec<(Dsn, Dsn)>
                                                                          → spawn mqtt_to_taos per pair

Connect User Properties (v5)      connect_user_properties=k1=v1,k2=v2   MqttConnectConfig.connect_user_properties
                                                                          → ConnectProperties.user_properties

Subscribe User Properties (v5)    subscribe_user_properties=...          MqttConnectConfig.subscribe_user_properties
                                                                          → SubscribeProperties.user_properties

Sub-offset dropdown (v5)          (merged into subscribe_user_properties  included in subscribe_user_properties
  earliest | latest                at submission time)                      → SubscribeProperties.user_properties
```

When neither user properties nor sub-offset is set, the parameters are omitted from the DSN entirely.

## DSN Format

Single address, no user properties:
```
mqtt://192.168.1.1:1883?client_id=test
```

Multiple addresses with connect user properties:
```
mqtt://192.168.1.1:1883,192.168.1.2:1883?connect_user_properties=client-type=sensor,env=prod
```

With subscribe user properties and sub-offset:
```
mqtt://192.168.1.1:1883?subscribe_user_properties=sub-offset=earliest,priority=high
```

Full example:
```
mqtt://192.168.1.1:1883,192.168.1.2:1883?connect_user_properties=client-type=sensor&subscribe_user_properties=sub-offset=earliest,priority=high
```

## Frontend Changes

### UI Section Layout

The MQTT config UI is organized into these top-level sections:

#### 1. Broker Addresses (`broker_addresses`, type: `grouping`)

Dynamic host+port list using the `HostPort` component:
- Each entry: host input + port input + delete button
- "Add" button to append a new entry
- Minimum 1 address required; order preserved (determines failover sequence)
- Serialized to the `endpoint` DSN parameter as comma-separated string

#### 2. Connection Configuration (`connection_options`)

Fields in order:
1. **MQTT protocol version** (`version`) — select: 3.1 / 3.1.1 / 5.0
2. **Client ID** (`client_id`) — customId type
3. **Keep Alive** (`keep_alive`) — number, default 60
4. **Clean Session** (`clean_session`) — switch, default true
5. **Connect User Properties** (`connect_user_properties`) — input, v5 only, `displayDependsOn: ['connection_options/version']`

#### 3. Authentication Configuration (`auth_options`) — NEW

Fields in order:
1. **Username** (`username`) — input
2. **Password** (`password`) — password
3. **TLS Verification** (`tsl_verify`) — select: none / single / both
4. **CA** (`ca`) — file, conditional on `auth_options/tsl_verify`
5. **Client Certificate** (`cert`) — file, conditional on `auth_options/tsl_verify`
6. **Client Key** (`cert_key`) — file, conditional on `auth_options/tsl_verify`

#### 4. Collect (`groups_before/collect`, hidden group)

Fields in order:
1. **Start From** (`sub-offset`) — select: earliest / latest, v5 only, `displayDependsOn: ['connection_options/version']`
2. **Subscribe User Properties** (`subscribe_user_properties`) — input, v5 only, `displayDependsOn: ['connection_options/version']`
3. **Topics QoS Config** (`topics`) — required input
4. **Topic Analysis** (`topic_pattern`) — input
5. **Compression** (`compression`) — select
6. **Char Encoding** (`char_encoding`) — select

### displayDependsOn Path Summary

| Field | Path |
|-------|------|
| `connect_user_properties` | `connection_options/version` |
| `sub-offset` | `connection_options/version` |
| `subscribe_user_properties` | `connection_options/version` |
| `ca` | `auth_options/tsl_verify` |
| `cert` | `auth_options/tsl_verify` |
| `cert_key` | `auth_options/tsl_verify` |

### DSN Serialization (`util.ts`)

At submission time for MQTT:
1. If `sub-offset` has a value, append `sub-offset=<value>` to `subscribe_user_properties`
2. Remove `sub-offset` from the data object (it's not a standalone DSN parameter)
3. If `subscribe_user_properties` is empty after merge, omit it
4. `connect_user_properties` is passed through as-is

### Task Edit/Recover (`sourceConfig.vue`)

When loading an existing task:
1. Extract `sub-offset` from `subscribe_user_properties` string if present
2. Set the dropdown value
3. Remove `sub-offset` from the displayed `subscribe_user_properties` input

### Files Modified

- `explorer/taos-ui/components/dataIn/config/en/09-mqtt.ts`
- `explorer/taos-ui/components/dataIn/config/zh/09-mqtt.ts`
- `explorer/taos-ui/components/dataIn/model/util.ts`
- `explorer/taos-ui/components/dataIn/views/sourceConfig.vue`

## Backend Changes

### Shared Utility: `crates/utils/src/dsn.rs`

General-purpose DSN key-value parser in `taosx-utils`:

```rust
/// Parse a comma-separated key=value string from a DSN parameter.
/// Returns `None` if the parameter is absent or empty.
pub fn parse_kv_pairs(dsn: &Dsn, key: &str) -> anyhow::Result<Option<Vec<(String, String)>>> {
    let Some(props_str) = dsn.get(key) else {
        return Ok(None);
    };
    if props_str.is_empty() {
        return Ok(None);
    }
    let mut props = Vec::new();
    for pair in props_str.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let Some(k) = parts.next().map(str::trim).filter(|s| !s.is_empty()) else {
            anyhow::bail!("{key}: property key cannot be empty");
        };
        let Some(v) = parts.next().map(str::trim).filter(|s| !s.is_empty()) else {
            anyhow::bail!("{key}: property value cannot be empty, key: {k}");
        };
        props.push((k.to_string(), v.to_string()));
    }
    Ok(Some(props))
}
```

### Failover Module: `crates/source-mqtt/src/failover.rs`

Splits multi-address DSNs into per-address task pairs:

```rust
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
```

Registered in `crates/task/src/failover.rs`:
```rust
("mqtt", "taos") => source_mqtt::failover::get_datasource_failover_config(from, to),
```

### Config: `crates/source-mqtt/src/config.rs`

```rust
pub struct MqttConnectConfig {
    pub host: String,
    pub port: u16,
    pub version: Version,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub certificates: Option<Certificates>,
    pub connect_user_properties: Option<Vec<(String, String)>>,
    pub subscribe_user_properties: Option<Vec<(String, String)>>,
}
```

Two thin wrappers call the shared utility:
```rust
fn parse_connect_user_properties(dsn: &Dsn) -> anyhow::Result<Option<Vec<(String, String)>>> {
    taosx_utils::dsn::parse_kv_pairs(dsn, "connect_user_properties")
}

fn parse_subscribe_user_properties(dsn: &Dsn) -> anyhow::Result<Option<Vec<(String, String)>>> {
    taosx_utils::dsn::parse_kv_pairs(dsn, "subscribe_user_properties")
}
```

### MQTT v5 Client: `crates/source-mqtt/src/client/v5.rs`

#### ConnectProperties

`build_options()` sets `ConnectProperties` with `session_expiry_interval` and `connect_user_properties` independently:

```rust
let needs_session_expiry = !config.clean_session;
let has_connect_props = config.connect_user_properties.as_ref().is_some_and(|p| !p.is_empty());

if needs_session_expiry || has_connect_props {
    let mut props = ConnectProperties::new();
    if needs_session_expiry {
        props.session_expiry_interval = Some(60);
    }
    if let Some(user_props) = &config.connect_user_properties {
        props.user_properties = user_props.clone();
    }
    options.set_connect_properties(props);
}
```

#### SubscribeProperties

`build_subscribe_properties()` creates subscribe properties from config:

```rust
fn build_subscribe_properties(config: &MqttConnectConfig) -> Option<SubscribeProperties> {
    let props = config.subscribe_user_properties.as_ref()?;
    if props.is_empty() {
        return None;
    }
    Some(SubscribeProperties {
        id: None,
        user_properties: props.clone(),
    })
}
```

Uses `subscribe_many_with_properties` when properties exist:
```rust
match &self.subscribe_properties {
    Some(props) => client.subscribe_many_with_properties(filters.clone(), props.clone()).await,
    None => client.subscribe_many(filters.clone()).await,
}
```

### MQTT v3 Client

No changes. Both `connect_user_properties` and `subscribe_user_properties` are ignored for v3.

### Module Export: `crates/source-mqtt/src/lib.rs`

Added `pub mod failover;`.

## Error Handling & Logging

- Log each failover connection attempt with address info
- Log which address successfully connected
- Log warning when an address fails and the system moves to the next
- When all addresses fail, the last error is returned

## Validation

### Frontend
- Host must not be empty; port must be 0–65535
- At least 1 broker address required
- Key-value format: `key=value` pairs separated by commas
- Keys and values must not be empty

### Backend
- `parse_kv_pairs()` rejects empty keys and empty values
- Returns `None` when parameter is absent or empty string

## Testing

### Unit Tests

- `parse_kv_pairs()`: valid pairs, single pair, empty, absent, empty key rejected, empty value rejected, no equals rejected, whitespace trimmed
- `get_datasource_failover_config()`: single address, multiple addresses, params preserved, empty addresses fallback
- `parse_connect_user_properties()`: valid pairs, empty, absent, invalid format
- `parse_subscribe_user_properties()`: same test matrix
- `build_subscribe_properties()`: with/without subscribe user properties

### Integration Tests

- Failover: first address unreachable, second succeeds
- Connect user properties sent in CONNECT packet
- Subscribe user properties sent in SUBSCRIBE packet
- Sub-offset included in subscribe user properties
- clean_session=false with connect_user_properties: both set independently
- clean_session=true with connect_user_properties: only user_properties set, no session_expiry
- MQTT v3: both parameters ignored gracefully

## Backward Compatibility

- Old DSNs with `user_properties=...` are no longer parsed (field name changed). If backward compatibility is needed, the parser can fall back: if `user_properties` exists and neither `connect_user_properties` nor `subscribe_user_properties` exists, treat `user_properties` as `connect_user_properties`.
- MQTT v3 tasks are unaffected — both new parameters are ignored.
- Existing v5 tasks without user properties are unaffected.
