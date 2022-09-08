# taosX User Manual

taosX is an easy-to-use, high-performance, feature-rich TDengine data integration tool. It works like a streaming data platform that supports offline data import/export and real-time data synchronization from or to TDengine. It's built for performance, reliability, productivity, observability and ergonomics.

## Features

- Easy to use command line interface.
- Simple but flexible configuration(s).
- High-performance with best effort.
- High-throughout with massive data.
- Modular and plugin system easy to extend, for different data sources/sinks.
- Streaming data aggregation.
- Fearless service running for long term.
- Helpful metrics for monitoring.

## Use scenarios

1st, for TDengine database replication(or synchronization).

- Synchronize database or stable from one to another TDengine cluster.

2nd, for TDengine logical backup and restore.

- TDengine database/tables full backup.
- TDengine database/tables incremental backup.
- TDengine database/tables restore from backups.

3rd, for streaming data integration.

- Subscription (with aggregations) from TDengine.
- Synchronization from different data sources to TDengine(will support soon).

3rd, for offline data integration.

- Export from TDengine to CSV/Parquet files.
- Import CSV/Parquet file to TDengine tables(will support soon).

4th, for older version data migration(will support soon).

- TDengine data migration from 2.x to 3.x

## Installation

`taosx` binary will be included in TDengine release packages.

### Build from source

taosX use Rust to benefit from the awesome Rust community. You need to install Rust first to build from source. Better start it from [rustup](https://rustup.rs/)(the installer for Rust).

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then you can build it with [cargo].

```bash
git clone --depth 1 https://github.com/taosdata/taosx.git
cd taosx
cargo build --release
```

Copy the `target/release/taosx` to your `PATH`, or install `taosx` binary with `cargo install`:

```bash
cargo install --path .
```

## Quick start

### Database replication

It will synchronize all data and meta changes from database `db1` in local cluster to target database `db2` in `another` cluster.

```bash
taosx run \
  -f 'tmq:///db1?group.id=another' \
  -t 'taos://root:taosdata@another:6030/db2'
```

By default, it will stop when there's no new data in some time(`500ms` eg.). Use `timeout=0` to run it forever.

```bash
taosx run \
  -f 'tmq:///db1?group.id=another&timeout=0' \
  -t 'taos://root:taosdata@another:6030/db2'
```

It's able to synchronize a table to another cluster database.

```bash
taosx run \
  -f 'tmq://root@taosdata@localhost:6030/db1.table_name' \
  -t 'taos://root:taosdata@another:6030/db2'
```

Note that `table_name` could be super table, or child table, or normal table.

### Full backup

It will backup whole database `db1` in `this` cluster to directory `/path/to/backups/of/one`.

```bash
taosx run \
  -f 'tmq://this/db1' \
  -t 'local:/path/to/backups/of/one'
```

Like replication, it will stop when there's no new data in some time(`500ms` eg.). Use `timeout=0` to run it forever or customize the timeout parameter by a human-readable duration: `timeout=5s`.

With local backup directory, you can restore it to any database in any cluster at anytime:

```bash
taosx run \
  -f 'local:/path/to/backups/of/one' \
  -t 'taos://root:taosdata@another:6030/db1'
```

Single (s)table backup is like:

```bash
taosx run \
  -f 'tmq://this/db1.table1' \
  -t 'local:/path/to/backups/of/one'
```

### Incremental backup

Incremental backup could be performed on an existing full backup and generate the incremental backup files after the current backup version. With existing directory `/path/to/backups/of/one`, the command is evenly equal to a fully backup:

```bash
taosx run \
  -f 'tmq://this/db1' \
  -t 'local:/path/to/backups/of/one'
```

Users could easily schedule a daily or timely backup with systemd timer or crontab.

### Streaming data subscription

Subscribe a time-series data stream with TMQ topic to another cluster.

Suppose there's a topic created from a STable:

```sql
create topic meters with meta as stable meters;
```

Subscribe the topic to destination database `db1` in `another` cluster with `taosx`:

```bash
taosx run \
  -f 'tmq://this/meters?group.id=another-db1' \
  -t 'taos://another/db1'
```

### Data import/export

Export TDengine table data to a specific file format. Here only support [CSV] and [Parquet](https://parquet.apache.org/). For example,

Select from table `meters` of database `test` in local cluster to a single `meters.csv` file:

```bash
taosx run \
  -f 'taos:///test?query=select * from meters' \
  -t 'csv:./meters.csv'
```

Or to a single parquet file:

```bash
taosx run \
  -f 'taos:///test?query=select * from meters' \
  -t 'parquet:./meters.parquet'
```

## Advanced Usage

taosX use DSN to express a data source or target. DSN is short for **D**ata **S**ource **N**ame representation string, a data structure used to describe a connection to a data source.

A common DSN is basically constructed as this:

```text
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |

# or path-like
<driver>[+protocol]:<path>[?<p1>=<v1>]
|------|-----------|------|----------|
|driver|  protocol | path | params   |
```

For different parts:

- **driver**: is the main entrypoint to a processer. **Required**. Here is a incomplete driver list in taosX:
  - **taos**: the legacy TDengine connection data source.
  - **tmq**: subscription data source from TDengine.
  - **local**: for backup/restore.
  - **csv**: for read or write [CSV] file.
  - **parquet**: for read or write [Parquet](https://parquet.apache.org/) file.
- **protocol**: the additional information appended to driver, which can be be used to support different kind of data sources. For TDengine connection, taosX use this to choose the low-level API to connect to the TDengine server: empty for native dirver, and `ws` for websocket driver. **Optional**.
- **username**: as its definition, is the username to the connection. **Optional**.
- **password**: the password of the username. **Optional**.
- **host**: address host to the datasource. **Optional**.
- **port**: address port to the datasource. **Optional**.
- **database**: database name or collection name in the datasource. **Optional**.
- **params**: a key-value map for any other informations to the datasource. **Optional**.
- **path**: a local file or directory path, or a path-like string for the data source.

For example, a legacy TDengine connection DSN with default protocol is:

```text
taos://root:taosdata@localhost:6030/test
```

A TDengine subscription DSN with websocket protocol is:

```text
tmq+ws://root:taosdata@localhost:6041/topic1?group.id=name1
```

A CSV file with path:

```text
csv:./path/to/file.csv
```

taosX use path-like DSN to configure a backup location with some options:

```text
local:./directory/to/backup/?max-file-size=1G
```

There're three kinds of DSN you should know before using taosX.

### 1. TDengine DSN for query and write

A TDengine DSN string could be written as:

```text
taos[+ws]://<username>:<password>@<host>:<port>/<database>[?<params>]
```

In a TDengine query connection DSN, the **driver** will always be `taos`, the **protocol** is optional (but only websocket protocol is supported, you can choose to use `ws` (for `http`) or `wss`(for `https`)). The **username** will be default to `root`, and the **password** default to `taosdata`. The **host** and **port** is default to `localhost:6030` for native protocol, and `localhost:80` or `localhost:443` for websocket based on the **protocol** previously selected (`80` for `ws` and `443` for `wss`). When **database** is declared, it will be the default database for every connection built from the DSN, if the database not exist, it will cause error: `Database not specified or not avaliable`. The **params** key-value pairs contain more complex options:

- **configDir**: path to TDengine client configuration file, it will use `/etc/taos/` as default. For eg. `taos://?configDir=/custom/path/to/taos/`.
- **token**: use `token` in the **params** part for TDengine cloud service instead of **username** and **password** authentication, be careful to use this along with specified **protocol**(`ws` or `wss`).

### 2. TDengine DSN for subscription

Subscription is a bit different, you can use a subscription like this:

```text
tmq[+ws]://<username>:<password>@<host>:<port>/<topics>[?<params>]
```

In this kind of DSN, the **driver** must be set as `tmq`. Instead of **database**, subscription DSN contains **topics** part, which is a comma(`,`)-separated **topic** string list. A **topic** could be created by sql: `CREATE TOPIC topic1 AS SELECT * FROM db1.tb1`. Other parts is nearly the same to previous secion - **TDengine DSN for query**. Additional subscription parameters could be set in **params** part:

- **`group.id`**: the group id string of a subscription, which is **required**.
- **`client.id`**: the client id of a subscription, which can be used to track a subscription client.
- **`enable.auto.commit`**: you can set this to `true` when you want to auto commit the consumer.
- **`auto.commit.interval.ms`**: set this value to a integer along with **enable.auto.commit** topion.
- **`auto.offset.reset`**: possible values: `none`, `earliest`, `latest` to control the initial subscription posiiton, that means the parameter is not work when the **group.id** has been subscribed once. Default is `none`, usually means `earliest`.

So if you want to subscribe a topic, the DSN may be:

```text
'tmq://root:taosdata@localhost:6030/topic1?group.id=gid1&client.id=any-string'
```

### 3. TDengine backup location DSN

taosX use path-like DSN for backup/restore, and use **local** as driver name:

```text
local:/path/to/backups?max-file-size=1G
```

You can use `max-file-size` option to control a single backup file size, by default it's 1G.

In a backup location directory, the files structure will be:

```text
test-dump-abc2
├── abc2-0-1661847087.z
├── abc2-0-1661847159.z
├── abc2-0.sql
├── abc2-1-1661847087.z
├── abc2-1-1661847159.z
└── local.toml
```

## Service mode

taosx provides a builtin service mode, to automatically monitor a configuration directory, expose an OpenAPI with workflow control support and enable OpenMetrics exporter by default. We have a schedule to add more useful functionalities, include a monitor web dashboard to manage configurations and display the status and metrics view in later release channel.

```bash
taosx serve -l 0.0.0.0:6050
```

taosx follows the [OpenAPI Specification 3.x][oas3] and provides a [SwaggerUI] interface at <http://localhost:6050/swagger-ui/>.

- **GET /tasks**: list tasks in current processor.
- **POST /tasks**: create new task with from/to DSN and return `id` of the task.
- **POST /tasks/replicate**: create a replication task with explicit options.
- **POST /tasks/subscribe**: create a subscription task with explicit options.
- **GET /tasks/{id}**: get task status by `id`.
- **DELETE /tasks/{id}**: delete task by `id`, will cancel unfinished tasks and delete the task(taosx will mark the task as deleted indeed, and can't list it in *GET /tasks* action).

A task schema might be:

```json
{
  "id": 1,
  "stream_type": "replicate",
  "created_at": "2022-02-02T02:02:02+08:00",
  "last_modified_at": "2022-02-02T02:02:02+08:00",
  "completed": false,
  "from": "tmq:///test",
  "to": "local:/path/to/backup/test",
  "finished_at": "2022-02-02T02:02:02+08:00",
  "status": "cancelled",
  "reason": "will not use"
}
```

- *`id`*: a unique 64 bit integer.
- *`stream_type`*: possible `replicate`, `subscribe`, `backup`.
- *`created_at`*: created datetime in nanoseconds with RFC3339 format.
- *`last_modified_at`*: datetime that the task has last been modified.
- *`completed`*: check if the status is `completed`.
- *`from`*: DSN for source.
- *`to`*: DSN for target.
- *`status`*: possible values: `created`, `failed`, `cancelled`, `deleted`, `completed`.
- *`reason`*: an nullable field for the reason of current status.

To create a new task, use the schema:

```json
{
  "stream_type": "backup",
  "from": "tmq:///test",
  "to": "local:./backups-test"
}
```

And the POST response body is:

```json
{
  "id": 3,
  "stream_type": "backup",
  "from": "tmq:///test",
  "to": "local:./backups-test",
  "created_at": "2022-08-30T20:45:10.815742654+08:00",
  "status": "created"
}
```

When there's error, the response body is:

```json
{
  "code": 65535,
  "message": "foo"
}
```

[cargo]: https://doc.rust-lang.org/cargo/guide/
[CSV]: https://www.ietf.org/rfc/rfc4180.txt
[SwaggerUI]: https://swagger.io/tools/swagger-ui/
[oas3]: https://spec.openapis.org/oas/v3.1.0
