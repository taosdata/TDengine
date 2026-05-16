---
name: taosgen-config
version: 0.1.0
description: Generate taosgen configuration files for time-series database performance testing. Use this skill when users want to create taosgen configs, benchmark TDengine/MQTT/Kafka, generate test data schemas, set up performance tests, or need help with taosgen YAML configurations. Trigger for phrases like "create taosgen config", "generate benchmark config", "set up TDengine test", "MQTT performance test config", "Kafka load test", or when users mention taosgen, time-series benchmarking, or data generation for testing.
---

# taosgen Configuration Generator

Generate taosgen configuration files for performance testing time-series databases and message brokers.

## Overview

taosgen is a benchmarking tool that generates data and tests write performance for:
- **TDengine** - Time-series database
- **MQTT** - Message broker
- **Kafka** - Distributed streaming platform

Configurations use YAML format with job-based workflows that can form DAG execution flows.

## Documentation Sources

**Primary source (always try first):**
- **Official documentation:** https://docs.tdengine.com/tdengine-reference/tools/taosgen/
  - Use WebFetch to get the latest documentation
  - This has the most up-to-date parameters, features, and examples
  - If WebFetch fails or returns insufficient information, fall back to local references

**Fallback references (use if web fetch fails):**

This skill includes comprehensive reference documentation organized by target system:

- **references/tdengine.md** - TDengine configurations, parameters, actions, and examples
- **references/mqtt.md** - MQTT configurations, parameters, actions, and examples
- **references/kafka.md** - Kafka configurations, parameters, actions, and examples
- **references/schema.md** - Schema definition, data generation methods, and CSV import
- **references/common.md** - Common concepts, job structure, and best practices

**Documentation strategy:**
1. Before generating any configuration, try WebFetch on https://docs.tdengine.com/tdengine-reference/tools/taosgen/
2. If WebFetch succeeds, use that information as primary source
3. If WebFetch fails or returns incomplete info, read the relevant local reference files:
   - For TDengine: references/tdengine.md
   - For MQTT: references/mqtt.md
   - For Kafka: references/kafka.md
   - For schema/data: references/schema.md
   - For job structure: references/common.md
4. Always verify parameter names and syntax from documentation rather than guessing

## Workflow

When a user requests a taosgen configuration:

1. **Understand requirements** - Ask clarifying questions:
   - Target system: TDengine, MQTT, or Kafka?
   - Data characteristics: tables/devices count, columns/metrics, value ranges
   - Generation strategy: random, expressions, or CSV import?
   - Performance goals: concurrency, batch sizes, special features

2. **Fetch latest documentation** - Get up-to-date information:
   - Use WebFetch on https://docs.tdengine.com/tdengine-reference/tools/taosgen/
   - Ask for comprehensive parameter details, examples, and configuration options
   - If WebFetch fails or returns insufficient information, read local reference files:
     - references/tdengine.md (for TDengine)
     - references/mqtt.md (for MQTT)
     - references/kafka.md (for Kafka)
     - references/schema.md (for data generation)
     - references/common.md (for job structure)

3. **Generate configuration** - Create complete YAML based on requirements and documentation

4. **Validate configuration**:
   - Since taosgen doesn't have dry-run mode, test by running with minimal data:
   - **For TDengine target**:
     - Parse DSN to extract host, port, user, password, database name
     - Check if target database already exists: `taos -h {host} -P {port} -u {user} -p{password} -e "show databases" | grep {database_name}`
     - If database exists, warn user and recommend using a different database name or manually dropping the existing database
     - If database does not exist, proceed with validation:
       - Create a temporary test config with 10 tables, 10 rows
       - Run: `timeout 10s taosgen -h {host} -c {test_config} 2>&1 | head -50`
       - If no "Config validation failed" errors, the config is valid
   - **For MQTT/Kafka targets**:
     - Create a temporary test config with 10 tables, 10 rows
     - Run: `timeout 10s taosgen -h {host} -c {test_config} 2>&1 | head -50`
     - If no "Config validation failed" errors, the config is valid
   - Clean up temporary files

5. **Present and explain**:
   - Brief summary of what the config does
   - Complete YAML in a code block
   - Key highlights explaining important settings
   - Usage: `taosgen -c config.yaml`
   - Ask for feedback

6. **Iterate** - Adjust based on user feedback

## Key Principles

- **Don't guess parameters** - Always verify from references
- **Ask naturally** - Don't interrogate; have a conversation and infer from context
- **Make reasonable assumptions** - Explain your choices when filling gaps
- **Focus on clarity** - Explain the "why" behind configuration choices
- **Be concise** - Keep explanations informative but brief
- **Validate when possible** - Test config with minimal data before presenting
- **Checkpoint handling** - Only include checkpoint configuration when the user explicitly requests it. Do NOT generate checkpoint settings for `tdengine/create-database` or `tdengine/insert` actions unless specifically asked. Checkpoint is an optional feature for write interruption/recovery and should not be enabled by default.
- **Drop if exists warning** - When a configuration includes `drop_if_exists: true`, you MUST explicitly warn the user about the risk of data loss. Use warning language like: "⚠️ Warning: This configuration contains `drop_if_exists: true`, which will delete existing databases/tables and cause data loss. Please confirm you understand this risk." Ask for user confirmation before proceeding with such configurations.
- **Auto-create table handling** - Only include `auto_create_table: true` when the user explicitly requests it for convenience. Do NOT generate `auto_create_table` settings unless specifically asked. The default is `false` (no auto-creation), which provides better performance. When `auto_create_table` is `false` (or omitted), you should generate a separate `tdengine/create-child-table` step before `tdengine/insert` for optimal performance. Auto-creating tables during insert is slower than pre-creating tables.

## Configuration Structure

All taosgen configs follow this basic structure and include a header comment with generation metadata:

```yaml
# Generated by taosgen-config Skill v0.1.0
# Generation time: 2025-03-06T14:30:00Z
# Target system: TDengine

# Target system configuration (choose one)
tdengine:
  dsn: connection_string
  # ... system-specific parameters

mqtt:
  uri: broker_address
  # ... system-specific parameters

kafka:
  bootstrap_servers: broker_list
  # ... system-specific parameters

# Data schema definition
schema:
  name: table_name
  tbname:
    prefix: prefix
    count: number
    from: start_index  # integer starting index for numbering, e.g., 0 or 1
  columns:
    - name: column_name
      type: data_type
      # ... generation parameters (gen_type is optional, defaults to 'random',
      #    auto-inferred as 'expression' if 'expr' attribute is present)
  tags:
    - name: tag_name
      type: data_type
      # ... generation parameters
  generation:
    interlace: 1
    rows_per_table: count
    rows_per_batch: size

# Job workflow
jobs:
  job-name:
    needs: [dependencies]
    steps:
      - uses: action-name
        with:
          parameter: value
```

For detailed parameters, actions, and examples, consult the reference files.

## Common Validation Errors

If validation fails, check for these common mistakes:

| Error | Cause | Fix |
|-------|-------|-----|
| Unknown key: broker | Using wrong param name | Use `uri` instead of `broker` (MQTT) |
| Unknown key: username | Using wrong param name | Use `user` instead of `username` (MQTT) |
| precision without quotes | Missing quotes in props | Use `precision 'ms'` with single quotes |
| gen_type not allowed | Invalid gen_type value or configuration conflict | Valid gen_type values: `random`, `order`, `expression`. Note: gen_type can often be omitted as it will be auto-inferred: `random` (default), `expression` (if `expr` attribute is present) |
| rows_per_table not found | Wrong parameter name | Use `rows_per_table` not `per_table_rows` |

## Common Use Cases

**TDengine performance test:**
- Fetch latest docs from https://docs.tdengine.com/tdengine-reference/tools/taosgen/ or read references/tdengine.md
- Create super table, insert data with high concurrency
- Use stmt format for best performance

**CSV data import:**
- Fetch latest docs or read references/schema.md for CSV configuration
- Read references/tdengine.md for TDengine-specific setup
- Configure timestamp offsets and column mapping

**MQTT real-time simulation:**
- Fetch latest docs or read references/mqtt.md
- Configure dynamic topics and QoS levels
- Batch messages for throughput

**Kafka load testing:**
- Fetch latest docs or read references/kafka.md
- Enable compression and batching
- Configure acknowledgment levels

## Output Format

Present configurations in this structure:

1. **Summary** - One sentence describing what this config does
2. **Complete YAML** - Full configuration in a code block (includes header comment with Skill version v0.1.0 and generation timestamp)
3. **Validation** - Brief note if config was tested (e.g., "✅ Config validated with taosgen")
4. **Key highlights** - 2-4 bullet points explaining important settings and why
5. **Usage** - Command to run: `taosgen -c config.yaml`
6. **Feedback** - "Does this match your requirements? Any adjustments needed?"

**Generated Configuration Header**:
Every generated configuration file includes a standard header comment:
```yaml
# Generated by taosgen-config Skill v0.1.0
# Generation time: {ISO8601 timestamp}
# Target system: {TDengine|MQTT|Kafka}
# Description: {brief description}
```

## References

- **Official docs:** https://docs.tdengine.com/tdengine-reference/tools/taosgen/
- **Local refs:** references/{tdengine,mqtt,kafka,schema,common}.md
