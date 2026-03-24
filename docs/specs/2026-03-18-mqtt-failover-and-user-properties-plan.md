# MQTT Multi-Address Failover with User Properties — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-address failover, split MQTT v5 user properties into connect/subscribe, add sub-offset parameter, and reorganize the MQTT config UI into connection/auth sections.

**Architecture:** Backend adds a failover module that splits multi-address DSNs into per-address task pairs, a shared `parse_kv_pairs` utility in `taosx-utils`, split `connect_user_properties`/`subscribe_user_properties` fields in config, ConnectProperties/SubscribeProperties integration in the v5 client. Frontend uses `broker_addresses` grouping for dynamic address management, reorganizes fields into `connection_options` (protocol settings + connect_user_properties), `auth_options` (username/password/TLS), and `groups_before/collect` (sub-offset + subscribe_user_properties + topics).

**Tech Stack:** Rust (taosx-utils, source-mqtt, taosx-task crates), TypeScript/Vue (explorer UI config files), rumqttc 0.25.x

---

## File Structure

### Files to Create
- `crates/source-mqtt/src/failover.rs` — failover config function splitting DSN addresses

### Files to Modify
- `crates/utils/src/dsn.rs` — add `parse_kv_pairs()` shared utility
- `crates/source-mqtt/src/lib.rs` — export failover module
- `crates/source-mqtt/src/config.rs` — add `connect_user_properties` and `subscribe_user_properties` fields, parser, and `TryFrom` wiring
- `crates/source-mqtt/src/client/v5.rs` — ConnectProperties for connect props, SubscribeProperties for subscribe props, `subscribe_many_with_properties`
- `crates/task/src/failover.rs` — add MQTT case to dispatcher
- `explorer/taos-ui/components/dataIn/config/en/09-mqtt.ts` — broker_addresses, connection/auth reorganization, user properties split
- `explorer/taos-ui/components/dataIn/config/zh/09-mqtt.ts` — same changes in Chinese
- `explorer/taos-ui/components/dataIn/model/util.ts` — DSN serialization for subscribe_user_properties + sub-offset merge
- `explorer/taos-ui/components/dataIn/views/sourceConfig.vue` — recovery logic for sub-offset extraction

---

## Task 1: Backend — Shared Utility `parse_kv_pairs`

**Files:**
- Modify: `crates/utils/src/dsn.rs`

- [x] **Step 1: Add `parse_kv_pairs` function** — Generic comma-separated key=value parser
- [x] **Step 2: Add unit tests** — 8 tests: valid pairs, single pair, absent, empty, empty key rejected, empty value rejected, no equals rejected, whitespace trimmed
- [x] **Step 3: Run tests** — `cargo nextest run -p taosx-utils parse_kv_pairs`
- [x] **Step 4: Format and clippy** — `cargo fmt --all && cargo clippy -p taosx-utils --tests --no-deps`
- [x] **Step 5: Commit** — `feat(utils): add parse_kv_pairs DSN utility for comma-separated key=value parsing`

---

## Task 2: Backend — Failover Module

**Files:**
- Create: `crates/source-mqtt/src/failover.rs`
- Modify: `crates/source-mqtt/src/lib.rs`
- Modify: `crates/task/src/failover.rs`

- [x] **Step 1: Create failover module with tests** — `get_datasource_failover_config()` splits `dsn.addresses` into per-address `(Dsn, Dsn)` pairs
- [x] **Step 2: Implement the function** — Iterate addresses, clone DSN with single address per pair, fallback to original if empty
- [x] **Step 3: Export module** — `pub mod failover;` in `lib.rs`
- [x] **Step 4: Register in task dispatcher** — `("mqtt", "taos") => source_mqtt::failover::get_datasource_failover_config(from, to)`
- [x] **Step 5: Run tests** — `cargo nextest run -p source-mqtt failover`
- [x] **Step 6: Format and clippy**
- [x] **Step 7: Commit** — `feat(mqtt): add failover module for multi-address support`

---

## Task 3: Backend — Split user_properties in MqttConnectConfig

**Files:**
- Modify: `crates/source-mqtt/src/config.rs`

- [x] **Step 1: Replace `user_properties` with two fields** — `connect_user_properties` and `subscribe_user_properties`
- [x] **Step 2: Add thin wrapper parsers** — `parse_connect_user_properties()` and `parse_subscribe_user_properties()` calling `parse_kv_pairs`
- [x] **Step 3: Wire into `TryFrom<&Dsn>`** — Both fields parsed from DSN
- [x] **Step 4: Update tests** — Replace old `parse_user_properties` tests with split tests
- [x] **Step 5: Run tests** — `cargo nextest run -p source-mqtt`
- [x] **Step 6: Format and clippy**
- [x] **Step 7: Commit** — `feat(mqtt): split user_properties into connect and subscribe fields`

---

## Task 4: Backend — v5.rs ConnectProperties and SubscribeProperties

**Files:**
- Modify: `crates/source-mqtt/src/client/v5.rs`

- [x] **Step 1: Add `SubscribeProperties` import and error variant**
- [x] **Step 2: Update `build_options`** — Decouple `clean_session` from `connect_user_properties`; `session_expiry_interval` and connect user props set independently
- [x] **Step 3: Add `build_subscribe_properties` helper** — Creates `SubscribeProperties` from `subscribe_user_properties`
- [x] **Step 4: Store `subscribe_properties` in `MessagePoller`**
- [x] **Step 5: Use `subscribe_many_with_properties`** — In `from_config`, `poll` resubscribe, and retry paths
- [x] **Step 6: Run tests** — `cargo nextest run -p source-mqtt`
- [x] **Step 7: Format and clippy**
- [x] **Step 8: Commit** — `feat(mqtt): use SubscribeProperties for subscribe_user_properties in v5 client`

---

## Task 5: Frontend — MQTT Config Reorganization (EN + ZH)

**Files:**
- Modify: `explorer/taos-ui/components/dataIn/config/en/09-mqtt.ts`
- Modify: `explorer/taos-ui/components/dataIn/config/zh/09-mqtt.ts`

- [x] **Step 1: Add `broker_addresses` section** — `type: 'grouping'` with HostPort component, `host_0`/`port_0` fields
- [x] **Step 2: Reorganize `connection_options`** — Move version, client_id, keep_alive, clean_session FROM `groups_before/collect`. Add `connect_user_properties` (v5 only, `displayDependsOn: ['connection_options/version']`)
- [x] **Step 3: Create `auth_options` section** — Move username, password (flattened from old tabs). Move TLS fields (tsl_verify, ca, cert, cert_key). Update TLS `displayDependsOn` paths to `auth_options/tsl_verify`
- [x] **Step 4: Remove old `authentication` tabs section**
- [x] **Step 5: Add `sub-offset` and `subscribe_user_properties` to `groups_before/collect`** — Both v5 only, `displayDependsOn: ['connection_options/version']`
- [x] **Step 6: Commit** — `feat(explorer): reorganize MQTT config sections, split user_properties into connect/subscribe`

---

## Task 6: Frontend — DSN Serialization Update

**Files:**
- Modify: `explorer/taos-ui/components/dataIn/model/util.ts`
- Modify: `explorer/taos-ui/components/dataIn/views/sourceConfig.vue`

- [x] **Step 1: Update `formatFromData` in util.ts** — Merge `sub-offset` into `subscribe_user_properties` at submission time; clean up empty properties
- [x] **Step 2: Update recovery logic in sourceConfig.vue** — Extract `sub-offset` from `subscribe_user_properties` string when loading existing task
- [x] **Step 3: Commit** — `feat(explorer): update DSN serialization for subscribe_user_properties and sub-offset`

---

## Task 7: Frontend — Config Section Reorganization (connection/auth split)

**Files:**
- Modify: `explorer/taos-ui/components/dataIn/config/en/09-mqtt.ts`
- Modify: `explorer/taos-ui/components/dataIn/config/zh/09-mqtt.ts`

- [x] **Step 1: Move version/client_id/keep_alive/clean_session into `connection_options`** — Keep `connect_user_properties` with updated `displayDependsOn: ['connection_options/version']`
- [x] **Step 2: Create `auth_options` section** — username, password, TLS fields. Update TLS cert `displayDependsOn` to `auth_options/tsl_verify`
- [x] **Step 3: Update `sub-offset` and `subscribe_user_properties` paths** — `displayDependsOn: ['connection_options/version']`
- [x] **Step 4: Commit** — `refactor(explorer): reorganize MQTT config into connection/auth sections`

---

## Task 8: Verification and Final Cleanup

- [x] **Step 1: Run all backend tests** — `cargo nextest run -p taosx-utils && cargo nextest run -p source-mqtt`
- [x] **Step 2: Run clippy** — `cargo fmt --all && cargo clippy -p taosx-utils -p source-mqtt --tests --no-deps`
- [ ] **Step 3: Manual verification checklist**

Verify in the explorer UI:
1. [ ] "Broker Addresses" section with dynamic host+port list
2. [ ] "Connection Configuration" shows: version, client_id, keep_alive, clean_session, connect_user_properties (v5 only)
3. [ ] "Authentication Configuration" shows: username, password, TLS fields
4. [ ] connect_user_properties only shows when version=5.0
5. [ ] TLS cert fields show/hide correctly based on tsl_verify selection
6. [ ] sub-offset and subscribe_user_properties still show when version=5.0
7. [ ] Submitting with multiple addresses creates correct DSN with comma-separated endpoint
8. [ ] Submitting with connect_user_properties creates correct DSN param
9. [ ] Submitting with subscribe_user_properties + sub-offset merges correctly
10. [ ] Editing an existing task correctly recovers all fields
11. [ ] MQTT v3 tasks work without user properties (no regression)
12. [ ] clean_session=false works independently of connect_user_properties
