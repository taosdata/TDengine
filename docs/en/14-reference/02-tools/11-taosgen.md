---
title: taosgen Reference
sidebar_label: taosgen
slug: /tdengine-reference/tools/taosgen
toc_max_heading_level: 4
---

The taosgen is a performance benchmarking tool for products in the time-series data domain, supporting functions such as data generation and write performance testing. taosgen is based on "jobs" as the basic unit. A job is a set of operations defined by the user to complete a specific task. Each job contains one or more steps and can be connected with other jobs through dependencies to form a directed acyclic graph (DAG) - style execution flow, enabling flexible and efficient task scheduling.

taosgen currently only supports the Linux system.

## Get

Download the [taosgen](https://github.com/taosdata/taosgen/releases) tool as needed.

Compile and install taosgen separately, for details please refer to the [taosgen](https://github.com/taosdata/taosgen/blob/main/README-CN.md)  repository.

## Running

The taosgen supports specifying parameter configurations via the command line and configuration files. For identical parameter configurations, the command line takes higher priority than the configuration file.

:::tip
Before running taosgen, ensure that all target TDengine TSDB clusters to be written to are operating normally.
:::

## Command-line options

| Option                       | Description                                                          |
| ---------------------------- | -------------------------------------------------------------------- |
| -h/--host             | Hostname or IP address to connect to. Default: localhost             |
| -P/--port             | Server port. Default: 6030                                           |
| -u/--user             | Username. Default: root                                              |
| -p/--password         | Password. Default: taosdata                                          |
| -c/--yaml-config-file | Path to the YAML configuration file                                  |
| -?/--help             | Show help and exit                                                   |
| -V/--version          | Displays version information and exits. Cannot be used with other parameters.|

## Configuration file parameters

**Overall structure**

The configuration file consists of: "global", "concurrency", and "jobs".
- global: Describes globally effective configuration parameters.
- concurrency: Describes the concurrency of job execution.
- jobs: List structure that describes the detailed parameters of all jobs.

**Job format**

A job is user-defined and consists of an ordered set of steps. Each job has a unique identifier (the key) and can declare dependencies (needs) to control its execution order relative to other jobs. A job includes the following attributes:
- Job Key: String. The unique key of the job in the jobs map, used for internal references and dependency management.
- name: String. The display name of the job, used for logs or UI.
- needs: List. Identifiers of other jobs that this job depends on. Use an empty list if there are no dependencies.
- steps: List. One or more Steps executed in sequence, defining the concrete workflow of the job. Jobs can reuse global configuration (e.g., database connection info) and leverage YAML anchors and aliases to reduce duplication, improving readability and maintainability.

**Step format**

A step is the fundamental unit of work within a job and represents the execution of a specific kind of operation. Each step runs in sequence and can reference predefined Actions to accomplish specific tasks. A step consists of the following attributes:
- name: String. The display name of the step, used for logs and UI.
- uses: String. The path or identifier of the Action to invoke, indicating which operation module executes this step.
- with: Map (dictionary). The set of parameters passed to the Action. The parameters vary by Action type and are flexibly configurable. By composing multiple steps, a job can implement complex workflows, such as creating databases or writing data.

Example configuration:

```yaml
{{#include docs/doxgen/taosgen_config.md:configuration_instructions}}
```

Key points:
- Creating the database, super table, and child tables is done by separate jobs with dependencies specified via the needs attribute.
- The insert-second-data job must wait for create-database, create-super-table, and create-second-child-table to complete before starting concurrent writes; the insert-minute-data job works the same way.

### Global configuration parameters

#### Connection parameters
- connection_info: Defines server connection settings, including:
  - host (string, optional): Hostname or IP address to connect to. Default: localhost.
  - port (integer, optional): Server port. Default: 6030.
  - user (string, optional): Username for the connection. Default: root.
  - password (string, optional): Password for the connection. Default: taosdata.
  - pool: Connection pool settings, including:
    - enabled (boolean, optional): Whether to enable the connection pool. Default: true.
    - max_pool_size (integer, optional): Maximum pool size. Default: 100.
    - min_pool_size (integer, optional): Minimum pool size. Default: 2.
    - connection_timeout (integer, optional): Timeout for acquiring a connection, in milliseconds. Default: 1000.

#### Data formatting parameters
- data_format: Defines the output data format and related settings, i.e., how data is formatted before being written to the storage medium. It includes:
  - format_type (string, optional): Data formatting type. Default: sql. Valid values:
    - sql: Format data as SQL statements.
    - stmt: Format data using the STMT interface.

  - Details differ by format_type:
    - When format_type: sql, no additional options.
    - When format_type: stmt:
      - version (string, optional): STMT interface version. Currently only v2 is supported.

#### Data channel parameters
- data_channel: Defines the communication channel or destination used for data transmission.
  - channel_type (string, optional): Channel type. Valid values:
    - native: Interact with the database using the native interface.
    - websocket: Interact with the database via the WebSocket protocol.

  Note: Mixing native and websocket is not supported at the moment!

#### Database parameters
- database_info: Defines a TDengine database instance, including:
  - name (string, required): Database name.
  - drop_if_exists (boolean, optional): Whether to drop the database if it already exists. Default: true.
  - properties (string, optional): Supported CREATE DATABASE properties.
    For example, precision ms vgroups 20 replica 3 keep 3650 sets time precision, number of vgroups, replica count, and data retention period.
    - precision:
      Time precision. Valid values: "ms", "us", "ns". Default: "ms".
    - vgroups:
      Number of virtual groups. Not set by default.
    - replica:
      Replica count. Not set by default.

#### Super table parameters
- super_table_info: Mapping that defines a TDengine super table, including:
  - name (string): Super table name.
  - columns (list): Schema definition for regular columns.
  - tags (list): Schema definition for tag columns.

**Column configuration attributes**

Each column includes:
- name (string, required): Column name. When count > 1, name is treated as a prefix. For example, name: current and count: 3 produce current1, current2, and current3.
- type (string, required): Data type (case-insensitive, compatible with TDengine), including:
  - Integer types: timestamp, bool, tinyint, tinyint unsigned, smallint, smallint unsigned, int, int unsigned, bigint, bigint unsigned.
  - Floating-point types: float, double, decimal.
  - Character types: nchar, varchar (binary).
- count (integer, optional): Number of consecutive columns of this type. For example, count: 4096 generates 4096 columns of the specified type.
- properties (string, optional): Column-level properties supported by TDengine, which may include:
  - encode: First-stage encoding algorithm in the two-level compression.
  - compress: Second-stage compression algorithm in the two-level compression.
  - level: Compression level for the second stage.
- gen_type (string, optional): How to generate values for this column. Default: random. Supported values:
  - random: Generate randomly.
  - order: Monotonically increasing natural numbers; integers only.
  - expression: Generate from an expression. Supported for integer types, floating-point types (float, double), and character types.

**Details of data generation methods**

- random: Random generation
  - distribution (string, optional): Random distribution model. Currently only uniform is supported; default is "uniform".
  - min (float, optional): Minimum value; applies to integer and floating-point types. Generated values are >= min.
  - max (float, optional): Maximum value; applies to integer and floating-point types. Generated values are < max.
  - values (list, optional): Candidate set from which values are randomly selected.

- order: Monotonically increasing natural numbers; integers only. After reaching the maximum, it wraps to the minimum.
  - min (integer, optional): Minimum value; generated values are >= min.
  - max (integer, optional): Maximum value; generated values are < max.

- expression: Generate based on an expression. Supports integer types, float/double, and character types.
  - formula (string, required): The expression written in Lua. Built-in variable `_i` is the call index starting from 0, e.g., "2 + math.sin(_i/10)".

  To illustrate the expressiveness of the expression-based approach, here is a more complex example:

   ```lua
    (math.sin(_i / 7) * math.cos(_i / 13) + 0.5 * (math.random(80, 120) / 100)) * ((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7) + 10 * (math.floor(_i / 100) % 2)
    ```
It combines multiple mathematical functions, conditional logic, periodic behavior, and random perturbations to simulate a nonlinear, noisy, piecewise-changing dynamic data generation process, structured as (A + B) × C + D. Breakdown:

| Part | Content                                                        | Category              | Role                                                                 |
|------|----------------------------------------------------------------|-----------------------|----------------------------------------------------------------------|
| A    | `math.sin(_i / 7) * math.cos(_i / 13)`                         | Base signal           | Dual-frequency modulation producing complex waveforms (beating)      |
| B    | `0.5 * (math.random(80, 120) / 100)`                           | Noise                 | Adds 80%–120% random disturbance (simulated noise)                   |
| C    | `((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7)`     | Dynamic gain modulation | Switches gain every 50 calls (first 25 high, next 25 low)           |
| D    | `10 * (math.floor(_i / 100) % 2)`                              | Baseline step change  | Toggles baseline every 100 calls (0 or 10), simulating peaks/valleys |

## Types of Actions

Actions are encapsulated, reusable units used to accomplish specific functions. Each action represents an independent logic block that can be invoked and executed in different steps. By abstracting common operations into standardized action modules, the system achieves strong extensibility and configuration flexibility.

The same type of action can be executed in parallel or reused across multiple steps, enabling diverse workflow orchestration. Core operations such as creating databases, defining super tables, generating child tables, and inserting data are uniformly orchestrated via the corresponding actions.

Currently supported built-in actions:
- `actions/create-database`: Create a database.
- `actions/create-super-table`: Create a super table.
- `actions/create-child-table`: Create child tables based on a super table.
- `actions/insert-data`: Insert data into specified tables.

Each action accepts parameters via the with field when invoked. The specific parameters vary by action type.

### Create Database action configuration

The `actions/create-database` creates a new database on the specified TDengine server. By passing the required connection info and database parameters, you can define properties such as database name, whether to drop an existing database, time precision, etc.

#### connection_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_format (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_channel (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### database_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters), containing all details required to create the database. If omitted, the global settings are used.

### Create Super Table action configuration
The `actions/create-super-table` creates a new Super Table in the specified database. By passing the required connection info and super table parameters, you can define properties such as table name, regular columns, and tag columns.

#### connection_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_format (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_channel (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### database_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters), specifying which database to create the Super Table in. If omitted, the global settings are used.

#### super_table_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters), containing all details required to create the Super Table. If omitted, the global settings are used.

### Create Child Table action configuration
The `actions/create-child-table` creates multiple child tables in bulk based on a specified Super Table in the target database. Each child table can have different names and tag values, enabling effective categorization and management of time-series data. This action supports defining child table names and tag columns from either a Generator or a CSV file for high flexibility and configurability.

#### connection_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_format (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### data_channel (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

#### database_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters), specifying which database to create the child tables in. If omitted, the global settings are used.

#### super_table_info (optional)
Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters), specifying which Super Table the child tables are based on. If omitted, the global settings are used.

#### child_table_info (required)
Contains the core information needed to create child tables, including the source and detailed configuration of child table names and tag column values.

**table_name (child table name)**

- source_type (string, required):
  Data source for child table names. Supported values: generator, csv.
- generator: Effective only when source_type="generator". Includes:
  Use a generator to dynamically produce the list of child table names. Provide:
  - prefix (string):
    Child table name prefix. Default: "d".
  - count (integer):
    Number of child tables to create. Default: 10000.
  - from (integer):
    Starting index (inclusive) for child table names. Default: 0.
- csv: Effective only when source_type="csv". Includes:
  Read child table names from a CSV file. Provide:
  - file_path (string):
    CSV file path.
  - has_header (boolean):
    Whether the file contains a header row. Default: true.
  - tbname_index (integer):
    Zero-based column index where the child table name is located. Default: 0.

**tags (tag columns)**

- source_type (string, required):
  Data source for tag columns. Supported values: generator, csv.
- generator: Effective only when source_type="generator". Includes:
  Use a generator to dynamically produce tag column values. Provide:
  - schema (list, optional):
    Schema definition for tag columns. Each element represents a tag column with name, type, and generation rules (e.g., random). If omitted, the predefined tag schema from the global scope is used.
- csv: Effective only when source_type="csv". Includes:
  Read tag column values from a CSV file. Provide:
  - schema (list, optional): Schema for tag columns. Each element defines a tag column with name, type, etc.
  - file_path (string):
    CSV file path.
  - has_header (boolean):
    Whether the file contains a header row. Default: true.
  - exclude_indices (string):
    If the file contains both the child table name column and tag columns, or only a subset of tag columns is desired, specify the zero-based indices to exclude (e.g., the child table name column or unused tag columns). Indices are separated by commas. Default: empty (exclude none).

#### batch (optional)
Controls behavior when creating child tables in batches:
- size (integer):
  Number of child tables per batch. Default: 1000.
- concurrency (integer):
  Number of batches executed in parallel to improve efficiency. Default: 10.

### Insert Data action configuration
The `actions/insert-data` action inserts data into specified child tables. It supports obtaining child table names and regular column data from either a generator or a CSV file, and allows multiple timestamp strategies to control time attributes. It also provides rich write-control strategies to optimize the insertion process.

#### source (required)
Contains all information related to the data to be inserted:

**table_name (child table name)**

Same as the table_name setting in [Create Child Table action configuration](#create-child-table-action-configuration).

**columns (regular columns)**

- source_type (string, required):
  Data source for regular columns. Supported values: generator, csv.
- generator:
  Effective only when source_type="generator". Use a generator to produce regular column data. Provide:
  - schema (list, required):
    Schema for regular columns. Each element represents a column with name, type, and generation rules (e.g., random).
  - timestamp_strategy (timestamp column strategy, optional):
    For generator sources, only the generator-based strategy is available. Includes:
    - start_timestamp (integer or "now", optional): Starting timestamp value for the table. Default: "now".
    - timestamp_precision (string, optional):
      Timestamp precision. Valid values: "ms", "us", "ns". Defaults to the target’s timestamp precision.
    - timestamp_step (integer, optional): Step between consecutive timestamps. Unit matches the precision. Default: 1.
- csv
  Effective only when source_type="csv". Read regular column data from CSV. Provide:
  - schema (list, optional):
    Schema for regular columns. Each element defines name and type.
  - file_path (string, required):
    CSV file path. Supports a single file or a directory.
  - has_header (boolean, optional):
    Whether the file contains a header row. Default: true.
  - tbname_index (integer, optional):
    Zero-based column index for the child table name.
  - timestamp_strategy (timestamp column strategy, required): Controls timestamp generation.
    - strategy_type (string, required): Strategy type. Default: original. Valid values:
      - original: Use the original time column in the file as the timestamp.
      - generator: Generate timestamps based on user rules start_timestamp and timestamp_step.
    1. original (object, optional): Effective only when strategy_type="original". Includes:
      - timestamp_index (integer, optional): Zero-based index of the original time column. Default: 0.
      - timestamp_precision (string, optional): Precision of the original time column. Defaults to the target’s precision. Valid values: "s", "ms", "us", "ns".
      - offset_config (optional):
        - offset_type (string): Timestamp offset type. Valid values: "relative", "absolute".
        - value (string or integer): Offset amount (relative) or start timestamp (absolute):
          - For "relative": String in ±[number][unit] format (e.g., "+1d3h" means +1 day and 3 hours). Supported units:
            - y: years
            - m: months
            - d: days
            - s: seconds
          - For "absolute": Integer or string in:
            - Numeric timestamp (precision determined by timestamp_precision)
            - ISO 8601 format ("YYYY-MM-DD HH:mm:ss")
    2. generator (object, optional): Effective only when strategy_type="generator". Includes:
      - start_timestamp (integer or string, optional): Starting timestamp value. Default: "now".
      - timestamp_precision (string, optional):
        Precision for the timestamp. Valid values: "ms", "us", "ns". Defaults to the target’s precision.
      - timestamp_step (integer, optional): Step between consecutive timestamps. Unit matches the precision. Default: 1.

#### target (required)
Describes the target database or other storage destination:

**timestamp_precision (optional)**

String: Timestamp precision. Valid values: "ms", "us", "ns". When target is tdengine, defaults to the database precision; otherwise defaults to "ms".

**target_type (required)**

String. Supported targets:
- tdengine: TDengine database.
- mqtt: The core server for message forwarding in the MQTT protocol.

**tdengine**

Effective only when target_type="tdengine". Includes:
- connection_info (required): Database connection info.
- database_info (object, required): Target database info:
  - name (string, required): Database name.
  - precision (string, optional): Database time precision; should match timestamp_precision above.
- super_table_info (required): Super table info:
  - name (string, required): Super table name.
  - columns (optional): Reference to predefined regular column schema.
  - tags (optional): Reference to predefined tag schema.

**mqtt**

Effective only when target_type="mqtt". Includes:
- host (string, optional): MQTT broker host. Default: localhost.
- port (integer, optional): MQTT broker port. Default: 1883.
- username (string, optional): Username for the broker.
- password (string, optional): Password for the broker.
- client_id (string, optional): Client identifier. Auto-generated if omitted.
- topic (string, required): MQTT topic to publish to. Supports placeholders:
  - `{table}`: Table name value
  - `{column}`: Column value, where column is the column name
- timestamp_precision (string, optional): Precision of the message timestamp. Valid values: "ms", "us", "ns". Default: "ms".
- qos (integer, optional): QoS level. Valid values: 0, 1, 2. Default: 0.
- keep_alive (integer, optional): Heartbeat interval in seconds when idle. Default: 5.
- clean_session (boolean, optional): Whether to clear previous session state. Default: true.
- retain (boolean, optional): Whether the broker retains the last message. Default: false.

#### control (required)
Defines behavior during data writing, including data_format, data_channel, data_generation, insert_control, and time_interval.

**data_format (optional)**

Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

**data_channel (optional)**

Same as the parameter with the same name in [Global configuration parameters](#global-configuration-parameters). If omitted, the global settings are used.

**data_generation (optional)**

Settings related to data generation:
- interlace_mode (optional): Controls interleaved generation across child tables.
  - enabled (boolean, optional): Whether to enable interleaving. Default: false.
  - rows (integer, optional): Rows generated per child table per pass. Default: 1.
- generate_threads (integer, optional): Number of data generation threads. Default: 1.
- per_table_rows (integer, optional): Rows to insert per child table. Default: 10000.
- queue_capacity (integer, optional): Capacity of the data queue. Default: 100.
- queue_warmup_ratio (float, optional): Pre-warm ratio of the queue. Default: 0.5 (pre-generate 50% of capacity).

**insert_control (optional)**

Controls how data is actually written to the destination:
- per_request_rows (integer, optional): Max rows per request. Default: 10000.
- insert_threads (integer, optional): Number of concurrent writer threads. Default: 8.

**time_interval (optional)**

Controls the distribution of time intervals during writing.
- enabled (boolean, optional): Whether to enable interval control. Default: false.
- interval_strategy (string, optional): Interval strategy type. Default: fixed. Valid values:
  - fixed: Fixed interval.
  - first_to_first: First row time of this batch minus first row time of the previous batch.
  - last_to_first: First row time of this batch minus last row time of the previous batch.
  - literal: Send according to the timestamp of the first row of this batch, simulating real-time data production.
- fixed_interval: Effective only when interval_strategy = fixed:
  - base_interval (integer, required): Fixed interval value in milliseconds.
- dynamic_interval: Effective only when interval_strategy = first_to_first / last_to_first:
  - min_interval (integer, optional): Minimum interval threshold. Default: -1.
  - max_interval (integer, optional): Maximum interval threshold. Default: -1.
- wait_strategy (string, optional): Wait strategy between send operations when interval control is enabled. Default: sleep. Valid values:
  - sleep: Sleep, yielding CPU to the OS scheduler.
  - busy_wait: Busy-wait, retaining CPU.

## Configuration examples

### Generator-based data generation, STMT v2 write to TDengine example

```yaml
{{#include docs/doxgen/taosgen_config.md:stmt_v2_write_config}}
```

### CSV-based data generation, STMT v2 write to TDengine example

```yaml
{{#include docs/doxgen/taosgen_config.md:csv_stmt_v2_write_config}}
```

csv file format:
- `ctb-tags.csv` file contents are:

```csv
groupid,location,tbname
1,loc1,d1
2,loc2,d2
3,loc3,d3
```

- `ctb-data.csv` file contents are:

```csv
tbname,current,voltage,phase
d1,11,200,1001
d3,13,201,3001
d2,12,202,2001
d3,23,203,3002
d2,22,204,2002
d1,21,205,1002
```

### Generator-based data generation and write to MQTT example

```yaml
{{#include docs/doxgen/taosgen_config.md:write_mqtt_config}}
```
