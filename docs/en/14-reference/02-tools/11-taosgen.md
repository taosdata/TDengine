---
title: taosgen Reference Manual
sidebar_label: taosgen
slug: /tdengine-reference/tools/taosgen
toc_max_heading_level: 4
---

taosgen is a performance benchmarking tool for time-series data products, supporting data generation, write performance testing, and more. taosgen uses "jobs" as the basic unit, where a job is user-defined and consists of a set of operations to accomplish a specific task. Each job contains one or more steps and can be connected to other jobs via dependencies, forming a Directed Acyclic Graph (DAG) execution flow for flexible and efficient task orchestration.

taosgen currently supports Linux and macOS systems.

## Comparison of taosBenchmark and taosgen Features

Compared to taosBenchmark, taosgen offers the following advantages and improvements:

- Provides job orchestration capabilities, with jobs supporting DAG dependencies to simulate real business processes.
- Supports multiple targets/protocols (TDengine, MQTT), enabling scenarios such as database writing and message publishing.
- More diverse data generation methods, including support for Lua expressions to easily simulate real business data.
- Supports real-time data generation, eliminating the need to pre-generate large data files, saving preparation time and better simulating real scenarios.
- Supports various time interval strategies to control data writing operations, such as "playing" data according to its actual generation time.
- Automatically detects unknown or incorrect configuration items, promptly identifying and reporting spelling errors or invalid parameters in the configuration file to enhance configuration safety and usability.
- Supports TDengine database connection pooling for efficient management and reuse of database connections.

taosgen solves the problems of inflexible configuration, limited data generation methods, and poor extensibility in taosBenchmark, making it more suitable for modern IoT and industrial internet big data testing needs.

## Getting the Tool

Download the [taosgen](https://github.com/taosdata/taosgen/releases) tool as needed.
Download the binary release package locally and extract it. For convenient access, you can create a symbolic link in the system executable directory. For example, on Linux:

```shell
tar zxvf tsgen-v0.3.0-linux-amd64.tar.gz
cd tsgen
ln -sf `pwd`/taosgen /usr/bin/taosgen
```

## Running

taosgen supports parameter configuration via command line or configuration file. For parameters specified both ways, command line options take precedence.

:::tip
Before running taosgen, ensure that all target TDengine TSDB clusters are running normally.
:::

Example startup:

```shell
taosgen -h 127.0.0.1 -c config.yaml
```

## Command Line Parameters

| Parameter              | Description                                         |
| ---------------------- | -------------------------------------------------- |
| -h/--host              | Hostname or IP address of the server to connect to, default is localhost |
| -P/--port              | Port number of the server to connect to, default is 6030 |
| -u/--user              | Username for connecting to the server, default is root |
| -p/--password          | Password for connecting to the server, default is taosdata |
| -c/--yaml-config-file  | Path to the YAML configuration file |
| -?/--help              | Show help information and exit |
| -V/--version           | Show version information and exit. Cannot be used with other parameters |

Tip: If no parameters are specified when running taosgen, taosgen will create the TDengine database tsbench, the super table meters, 10,000 child tables, and batch write 10,000 rows of data to each child table.

## Configuration File Parameters

### Overall Structure
The configuration file is divided into five parts: "tdengine", "mqtt", "schema", "concurrency", and "jobs".
- tdengine: Describes configuration parameters related to the TDengine database.
- mqtt: Describes configuration parameters for the MQTT Broker.
- schema: Describes configuration parameters for data definition and generation.
- concurrency: Describes job execution concurrency.
- jobs: List structure, describes specific parameters for all jobs.

#### Job Format
A job is user-defined and contains an ordered set of steps. Each job has a unique identifier (key name) and can specify dependencies (needs) to control execution order with other jobs. Job attributes include:
- Job Key: String, unique key name in the jobs list, used for internal reference and dependency management.
- name: String, display name for the job, used in logs or UI.
- needs: List, identifiers of other jobs this job depends on. Empty list if no dependencies.
- steps: List, one or more steps executed in order, defining the job's operation flow.
Jobs inherit global configuration (such as tdengine, schema, etc.) by default.

#### Step Format
A step is the basic operation unit in a job, representing the execution process of a specific operation type. Each step runs in order and can reference a predefined Action to perform a specific function. Step attributes include:
- name: String, display name for the step, used in logs and UI.
- uses: String, points to the Action path or identifier to indicate which operation module to call.
- with: Map (dictionary), parameters passed to the Action. Parameter content varies by Action type and supports flexible configuration.
By combining multiple steps, jobs can implement complex logic flows, such as TDengine creating super tables & child tables, writing data, etc.

### Global Configuration Parameters

#### TDengine Parameters
- tdengine: Describes configuration parameters for the TDengine database, including:
  - dsn (string): DSN address for connecting to TDengine, default: taos+ws://root:taosdata@localhost:6041/tsbench.
  - drop_if_exists (bool): Whether to delete the database if it already exists, default: true.
  - props (string): Database creation property information.
    For example, precision ms vgroups 20 replica 3 keep 3650 sets virtual group count, replica count, and data retention period.
    - precision: Time precision, options: "ms", "us", "ns".
    - vgroups: Number of virtual groups.
    - replica: Replica format.
  - pool: Connection pool configuration:
    - enabled (bool): Whether to enable connection pool, default: true.
    - max_size (int): Maximum pool size, default: 100.
    - min_size (int): Minimum pool size, default: 2.
    - timeout (int): Connection timeout in milliseconds, default: 1000.

#### MQTT Parameters
- mqtt: Describes configuration parameters for the MQTT Broker, including:
  - uri (string): MQTT Broker URI, default: tcp://localhost:1883.
  - username (string): Username for Broker login.
  - password (string): Password for Broker login.
  - topic (string): MQTT Topic to publish messages to, default: tsbench/`{table}`. Supports dynamic topics via placeholder syntax:
    - `{table}`: Table name data
    - `{column}`: Column data, where column is the column field name
  - client_id (string): Client identifier prefix, default: taosgen.
  - qos (int): QoS level, values: 0, 1, 2, default: 0.
  - keep_alive (int): Heartbeat interval in seconds, default: 5.
  - clean_session (bool): Whether to clear session state, default: true.
  - retain (bool): Whether MQTT Broker retains the last message, default: false.
  - max_buffered_messages (int): Maximum buffered messages for the client, default: 10000.

#### schema Parameters
- schema: Describes configuration parameters for data definition and generation patterns.
  - name (string): Name of the schema.
  - from_csv: Configuration for using CSV files as data sources.
    - tags: Tag configuration.
      - file_path (string): Path to tag data CSV file.
      - has_header (bool): Whether the file contains a header row, default: true.
      - tbname_index (int): Column index for table name (starting from 0), default: -1 (inactive).
      - exclude_indices (string): Indices of unused tag columns to exclude (comma-separated), default: empty (no exclusion).
    - columns: Configuration for time-series data columns.
      - file_path (string): Path to time-series data CSV file.
      - has_header (bool): Whether the file contains a header row, default: true.
      - repeat_read (bool): Whether to read data repeatedly, default: false.
      - tbname_index (int): Column index for child table name (starting from 0), default: -1 (inactive).
      - timestamp_index (int): Column index for timestamp (starting from 0), default: -1 (inactive).
      - timestamp_precision (string): Timestamp precision, options: "s", "ms", "us", "ns".
      - timestamp_offset: Timestamp offset configuration.
        - offset_type (string): Offset type, options: "relative", "absolute".
        - value (string/int): Offset value (relative) or starting timestamp (absolute):
          - For "relative": string format ±[value][unit] (e.g., "+1d3h" means add 1 day 3 hours), units: y (year), m (month), d (day), s (second).
          - For "absolute": int or string, either timestamp value (precision as per timestamp_precision) or ISO 8601 string ("YYYY-MM-DD HH:mm:ss").
  - tbname: Table name generation configuration:
    - prefix (string): Table name prefix, default: "d".
    - count (int): Number of tables to create, default: 10000.
    - from (int): Starting index for table names (inclusive), default: 0.
  - tags (list): Pattern definition for the table's tag columns. The default configuration is: groupid INT, location VARCHAR(24).
  - columns (list): Pattern definition for the table's normal columns. The default configuration is: ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT.
  - generation: Data generation behavior configuration.
    - interlace (int): Number of rows for interlaced data generation, default: 0 (disabled).
    - concurrency (int): Number of threads for data generation, default: same as write threads.
    - rows_per_table (int): Number of rows to write per table, default: 10000, -1 means unlimited.
    - rows_per_batch (int): Maximum number of rows per batch request, default: 10,000.
    - num_cached_batches (int): Number of batches to pre-generate and cache, 0 means caching is disabled, default is 10000.
    - tables_reuse_data (bool): Whether multiple tables reuse the same data, default is true.

##### Column Configuration Attributes
Each column includes:
- name (string): Column name. If count > 1, name is the prefix (e.g., name: current, count: 3 yields current1, current2, current3).
- type (string): Data type, supports TDengine-compatible types (case-insensitive):
  - Integer: timestamp, bool, tinyint, tinyint unsigned, smallint, smallint unsigned, int, int unsigned, bigint, bigint unsigned.
  - Float: float, double, decimal.
  - String: nchar, varchar (binary).

  Currently, the following data types are not supported: json, geometry, varbinary, decimal, blob.
- count (int): Number of consecutive columns of this type (e.g., count: 4096 creates 4096 columns).
- props (string): Column property info for TDengine, may include:
  - encode: First-level encoding algorithm for two-level compression.
  - compress: Second-level encryption algorithm for two-level compression.
  - level: Compression rate for second-level encryption.
- gen_type (string): Data generation method for this column, default: random. Supported types:
  - random: Random generation.
  - order: Sequential natural number growth (integer only).
  - expression: Generated by expression (supports integer, float, double, string).

##### Data Generation Methods
- random: Random generation
  - distribution (string): Random model, currently only "uniform" supported, default: "uniform".
  - min (float): Minimum value (integer/float only), generated value ≥ min.
  - max (float): Maximum value (integer/float only), generated value < max.
  - values (list): Range of values to randomly select from.

- order: Sequential natural number growth (integer only), wraps to min after reaching max
  - min (int): Minimum value, generated value ≥ min.
  - max (int): Maximum value, generated value < max.

- expression: Generated by expression (supports integer, float, and string types). If gen_type is not explicitly specified but the expr attribute is detected, gen_type will be automatically set to expression.
  - expr (string): Expression content, syntax uses Lua language, built-in variables:
    - `_i`: Call index, starts from 0, e.g., "2 + math.sin(_i/10)";
    - `_table`: Table for which the expression builds data;
    - `_last`: Last returned value for this expression, only for numeric types, initial value 0.0;

    Example of a complex expression:

    ```lua
    (math.sin(_i / 7) * math.cos(_i / 13) + 0.5 * (math.random(80, 120) / 100)) * ((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7) + 10 * (math.floor(_i / 100) % 2)
    ```
    This combines various math functions, conditional logic, periodic behavior, and random noise to simulate a nonlinear, noisy, segmented dynamic data generation process, structured as (A + B) × C + D. Breakdown:

    | Part | Content                                                               | Type           | Function                                                      |
    |------|-----------------------------------------------------------------------|----------------|---------------------------------------------------------------|
    | A    | `math.sin(_i / 7) * math.cos(_i / 13)`                               | Base signal    | Dual-frequency modulation, generates complex waveform         |
    | B    | `0.5 * (math.random(80, 120) / 100)`                                 | Noise          | Adds 80%~120% random noise                                    |
    | C    | `((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7)`           | Dynamic gain   | Switches gain every 50 calls (first 25 high, next 25 low)     |
    | D    | `10 * (math.floor(_i / 100) % 2)`                                    | Baseline step  | Switches baseline every 100 calls (0 or 10), simulates peaks  |

### Action Types
Actions are encapsulated reusable operation units for specific functions. Each action represents an independent logic and can be called and executed in different steps. By abstracting common operations into standardized action modules, the system achieves extensibility and flexible configuration.
The same action type can be used in multiple steps in parallel or repeatedly, supporting diverse workflow orchestration. For example: creating databases, defining super tables, generating child tables, writing data, etc., can all be scheduled via corresponding actions.
Currently supported built-in actions:
- `tdengine/create-database`: Create TDengine database
- `tdengine/create-super-table`: Create TDengine super table
- `tdengine/create-child-table`: Create child tables for TDengine super table
- `tdengine/insert-data`: Write data to TDengine database
- `mqtt/publish-data`: Publish data to MQTT Broker
Each action can receive parameters via the with field, with content varying by action type.

### Format for Creating TDengine Database Action
The `tdengine/create-database` action creates a new database on the specified TDengine server. With connection info and database parameters, users can easily define database properties such as name, whether to drop if exists, time precision, etc.

- checkpoint: Configuration for write interruption/recovery:
  - enabled: Whether to enable write interruption/recovery.
  - interval_sec: Interval for storing write progress, in seconds.

### Format for Creating TDengine Super Table Action
The `tdengine/create-super-table` action creates a new super table in the specified database. With necessary connection info and super table parameters, users can define properties such as table name, normal columns, tag columns, etc.

- schema: Uses global schema configuration by default; can be overridden for this action.

### Format for Creating TDengine Child Table Action
The `tdengine/create-child-table` action creates multiple child tables in the target database based on the specified super table. Each child table can have different names and tag data, enabling effective classification and management of time-series data. Supports defining child table names and tag data from generator or CSV file sources.

- schema: Uses global schema configuration by default; can be overridden for this action.
- batch: Controls batch creation behavior:
  - size (int): Number of child tables per batch, default: 1000.
  - concurrency (int): Number of concurrent batches, improves creation efficiency, default: 10.

### Format for Writing Data to TDengine Action
The `tdengine/insert-data` action writes data to specified child tables. Supports obtaining child table names and normal column data from generator or CSV file sources, and allows users to control timestamp attributes via various strategies. Also provides rich write control strategies for optimization.

- schema: Uses global schema configuration by default; can be overridden for this action.
- format (string): Format for writing data, options: sql, stmt, default: stmt.
- concurrency (int): Number of threads for concurrent data writing, default: 8.
- failure_handling (optional): Failure handling strategy:
  - max_retries (int): Maximum retries, default: 0.
  - retry_interval_ms (int): Retry interval in ms, default: 1000 (effective if max_retries > 0).
  - on_failure (string): Default: exit. Options:
    - exit: Exit program on failure
    - skip: Warn and skip to continue execution on failure
- time_interval: Controls time interval distribution during writing.
  - enabled (bool): Whether to enable interval control, default: false.
  - interval_strategy (string): Interval strategy type, default: fixed. Options:
    - fixed: Fixed interval.
    - first_to_first: First row time of this batch - first row time of last batch.
    - last_to_first: First row time of this batch - last row time of last batch.
    - literal: Send based on time value of first row, simulating real-time data.
  - fixed_interval: Effective only if interval_strategy = fixed:
    - base_interval (int): Fixed interval value in milliseconds, default is 1000.
  - dynamic_interval: Effective only if interval_strategy = first_to_first / last_to_first:
    - min_interval (int): Minimum interval threshold in milliseconds, default is -1 (inactive).
    - max_interval (int): Maximum interval threshold in milliseconds, default is -1 (inactive).
  - wait_strategy (string): Wait strategy between requests when interval control is enabled, default: sleep. Options:
    - sleep: Sleep, yield thread to OS.
    - busy_wait: Busy wait, keep thread active.
- checkpoint: Configuration for write interruption/recovery (currently only supports stmt format, generator data source):
  - enabled: Whether to enable write interruption/recovery.
  - interval_sec: Interval for storing write progress, in seconds.

### Format for Publishing MQTT Data Action
The `mqtt/publish-data` action publishes data to the specified topic. Supports obtaining data from generator or CSV file sources, and allows users to control timestamp attributes via various strategies. Also provides rich publish control strategies for optimization.

- schema: Uses global schema configuration by default; can be overridden for this action.
- format (string): Format for publishing data, currently only supports json, default: json.
- concurrency (int): Number of threads for concurrent publishing, default: 8.
- failure_handling (optional): Same as in "Writing Data to TDengine Action".
- time_interval: Same as in "Writing Data to TDengine Action".

## Configuration File Examples

### Generator-Based Data Generation, STMT Write to TDengine Example

This example demonstrates how to use taosgen to simulate 10,000 smart meters, each collecting current, voltage, and phase. Each meter generates a record every 5 minutes, with current data generated randomly and voltage simulated using a sine wave. The generated data is written to the meters super table in the tsbench database of TDengine TSDB via WebSocket.

Configuration details:
- TDengine configuration
  - Connection info: Defined via DSN.
  - Database properties: Whether to recreate the database, time precision set to ms, 4 vgroups.
- schema configuration
  - Name: Specifies the super table name.
  - Child table names: Rule for generating 10,000 child table names, d0 to d9999.
  - Super table structure: 3 normal columns (current, voltage, phase), 2 tag columns (group ID, location).
    - Timestamp: Generation strategy starts from 1700000000000 (2023-11-14 22:13:20 UTC), increments by 5 minutes.
    - Time-series data: current and phase use random values, voltage uses sine wave simulation.
    - Tag data: groupid and location use random values.
  - Data generation: Interlace mode, 10,000 rows per child table, max 10,000 rows per batch.
- Child table creation: 10 threads concurrently, 1,000 child tables per batch.
- Data writing: Default stmt (parameterized write) format, 8 threads for concurrent writing, greatly improving batch performance.

Scenario description:

This configuration is designed for TDengine database performance benchmarking. It is suitable for simulating large-scale IoT devices (such as meters, sensors) continuously generating high-frequency data, and can be used for:
- Testing and evaluating TDengine cluster throughput, latency, and stability under massive time-series data write pressure.
- Verifying database schema design, resource planning, and performance under different hardware configurations.
- Providing data support for capacity planning in industrial IoT and related fields.

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_gen_stmt_insert_config}}
```

The parameters tdengine, schema::name, schema::tbname, schema::tags, tdengine/create-child-table::batch, and tdengine/insert-data::concurrency can use their default values to further simplify the configuration.

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_gen_stmt_insert_simple}}
```

### CSV-Based Data Generation, STMT Write to TDengine Example

This example demonstrates how to use taosgen to simulate 10,000 smart meters, each collecting current, voltage, and phase. Each meter generates a record every 5 minutes, with measurement data read from a CSV file and written to the meters super table in the tsbench database of TDengine TSDB via WebSocket.

Configuration details:
- TDengine configuration
  - Connection info: Defined via DSN.
  - Database properties: Whether to recreate the database, time precision set to ms, 4 vgroups.
- schema configuration
  - Name: Specifies the super table name.
  - from_csv configuration defines sources for child table names, tag columns, and time-series columns.
    - Child table names: Uses column index 2 from ctb-tags.csv.
    - Tag data: Uses all columns from ctb-tags.csv except the child table name column.
    - Timestamp: Uses column index 1 from ctb-data.csv, adds 10 seconds to original data.
    - Time-series data: Uses data from ctb-data.csv, with column 0 as child table name for association.
  - Super table structure: 3 normal columns (current, voltage, phase), 2 tag columns (group ID, location).
  - Data generation: Interlace mode, 10,000 rows per child table, max 10,000 rows per batch.
- Child table creation: 10 threads concurrently, 1,000 child tables per batch.
- Data writing: Default stmt (parameterized write) format, 8 threads for concurrent writing, greatly improving batch performance.

Scenario description:

This configuration is designed for importing device metadata and historical data from existing CSV files into TDengine. It is suitable for:
- Data migration: Migrating device metadata (tags) and historical monitoring data stored in CSV files to TDengine.
- System initialization: Initializing a batch of devices and historical data for a new monitoring system, for testing, demonstration, or retrospective analysis.
- Data replay: Simulating real-time data streams by reinjecting historical data, for testing system processing or reproducing specific historical scenarios.

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_csv_stmt_insert_config}}
```

Where:
- `ctb-tags.csv` file format:

```csv
groupid,location,tbname
1,California.Campbell,d1
2,Texas.Austin,d2
3,NewYork.NewYorkCity,d3
```

- `ctb-data.csv` file format:

```csv
tbname,ts,current,voltage,phase
d1,1700000010000,5.23,221.5,146.2
d3,1700000010000,8.76,219.8,148.7
d2,1700000010000,12.45,223.1,147.3
d3,1700000310000,9.12,220.3,149.1
d2,1700000310000,11.87,222.7,145.8
d1,1700000310000,4.98,220.9,147.9
```

### Generator-Based Data Generation and Publishing to MQTT Broker Example

This example demonstrates how to use taosgen to simulate 10,000 smart meters, each collecting current, voltage, phase, and location. Each meter generates a record every 5 minutes, with current data generated randomly, voltage simulated using a sine wave, and the generated data published via MQTT.

Configuration details:
- MQTT configuration
  - Connection info: URI for MQTT Broker.
  - Topic configuration: Uses dynamic topic factory/`{table}`/`{location}`, where:
    - `{table}` placeholder is replaced with the generated child table name.
    - `{location}` placeholder is replaced with the generated location column value, enabling publishing to different topics by device location.
  - qos: Quality of Service level set to 1 (at least once delivery).
- schema configuration
  - Name: Specifies the schema name.
  - Table names: Rule for generating 10,000 table names, d0 to d9999. Tables are logical concepts for organizing data.
  - Table structure: 4 normal columns (current, voltage, phase, device location).
    - Timestamp: Generation strategy starts from 1700000000000 (2023-11-14 22:13:20 UTC), increments by 5 minutes.
    - Time-series data: current, phase, and location use random values; voltage uses sine wave simulation.
  - Data generation: Interlace mode, 10,000 rows per table, max 1,000 rows per batch.
- Data publishing: 8 threads concurrently publishing to MQTT Broker for higher throughput.

Scenario description:

This configuration is designed for publishing simulated device data to an MQTT message broker. It is suitable for:
- MQTT consumer testing: Simulate a large number of devices publishing data to MQTT Broker to test consumer processing, load balancing, and stability.
- IoT platform demonstration: Quickly build a simulated IoT environment to show how device data is ingested via MQTT.
- Rule engine testing: Test MQTT topic subscription and message routing rules using dynamic topics (e.g., routing by device location).
- Real-time data stream simulation: Simulate real-time device data streams for testing stream processing frameworks.

```yaml
{{#include docs/doxgen/taosgen_config.md:mqtt_publish_config}}
```