# Single process connector for taosX

This document describes how taosX calls an external data source connector to fill up a data-in task.

taosX service starts a new task that uses eg. PI connector reads data from PI server to TDengine.

- taosX is a service.
- taosX has a plugins directory to load connectors at runtime.
- A plugin is an executable binary which can connect to a data source.
- The plugin works on specified directories for individual tasks.

## 1st, taosx can validate DSN for a connector

For eg. a connector with the name tmq.

taosX will call tmq --check <dsn> to validate a DSN for quick fail.

If the status code is 0,that means OK.
For else cases, taosx will read the reason text from the stdout output.

## 2nd, taosx runs the connector process to start a data-in task

taosX will run the plugin process with some useful environment variables.

taosX information part:

- TAOS_X_EXE: taosx executable file path.
- TAOS_X_PLUGINS_DIR: taosx plugins base path.
- TAOS_X_LOG_LEVEL: taosx log level, ERROR, WARN, INFO, DEBUG, TRACE is valid.
- TAOS_X_LOG_DIR: taosx log directory.
- TAOS_X_SOCKET: the global socket for meta data communication. eg. connector could call a sql on the TAOS_X_TASK_OUT_DSN to retrieve data, especially for PI.

Task information part:

- TAOS_X_TASK_ID: a unique task id (uuid format) for the running task.
- TAOS_X_TASK_CREATED_AT: task created time.
- TAOS_X_TASK_IN_DSN: data in dsn for this plugin.
- TAOS_X_TASK_OUT_DSN: data out dsn to target.
- TAOS_X_TASK_TRANSFORMERS: a json string of transformers.
- TAOS_X_TASK_DATA_DIR: the data directory for this task.
- TAOS_X_TASK_TMP_DIR: the task specific temporary directory.
- TAOS_X_TASK_SOCKET: the task data in socket for arrow IPC data-only purpose.

When connector starts, it should print the initial transformers json to stdout, usually a schema definition:

```json
[
  { "schema": {"tbname": "field1", "using": "field2", "tags": ["field3", "field4"]} }
]
```

Wait for the stdin "Ok" message for taosX preparation (taosX will use the transformers to initialize the task socket).

Then connector could use TAOS_X_TASK_SOCKET for multi-threading writing.

For each connection at connector side, both reader and writer stream are in arrow IPC format (one schema, many record batches).

The reader stream is used to check if the previous operation is successful or not. The schema is

- code: 0 for success, others for error code. Int32.
- message: response message, or reason string for the error code. String.
- affected_rows: affected rows. Int32;

1. The writer writes schema first and only once.
2. Check the response in reader.
3. If successful, start writing data batch by batch.
4. For each message (a record batch in arrow), connector should check the response in reader stream (or skip if you don't care if the writing is successful or not).
