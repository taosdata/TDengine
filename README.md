# taosX User Manual

taosX is an easy-to-use, feature-rich TDengine data pipeline tool. It's a bridge between a data source and data sink. It supports offline data import/export and real-time data replication from or to a TDengine instance. It's built for performance, reliability, productivity, observability and ergonomics.

## Highlights

- Easy to use command line interface.
- Simple but flexible configuration(s).
- High-performance with best effort.
- High-throughout with massive data.
- Modular and plugin system easy to extend, for different data sources/sinks.
- Streaming data aggregation.
- Resilient service running for long term.
- Helpful metrics for monitoring.

## Features and Use scenarios

1st, for TDengine database replication.

- Replicate database or (s)tables from one to another TDengine instance.

2nd, for TDengine logical backup and restore.

- TDengine database/(s)tables full backup.
- TDengine database/(s)tables incremental backup.
- TDengine database/(s)tables restore from backups.

3rd, for data subscription.

- Subscribe topics from one or multiple TDengine instances
- Write all the subscribed data into one TDengine instance

3rd, for offline data backup/restore.

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

It will replicate all data and meta changes from database `db1` in local cluster to target database `db2` in `another` cluster.

```bash
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```

By default, it will stop when there's no new data in some period of time(`500ms` eg.). Use `timeout=never` to run it forever.

```bash
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1?timeout=never' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```

It's able to replicate a table to another cluster database.

```bash
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1.table_name' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```

Note that `table_name` could be super table, or child table, or normal table.

### Full backup

It will backup whole database `db1` in `this` cluster to directory `/path/to/backups/of/one`.

```bash
taosx run \
  -f 'tmq://this/db1' \
  -t 'local:/path/to/backup/directory'
```

Like replication, it will stop when there's no new data in some time(`500ms` eg.). Use `timeout=0` to run it forever or customize the timeout parameter by a human-readable duration: `timeout=5s`.

With local backup directory, you can restore it to any database in any cluster at anytime:

```bash
taosx run \
  -f 'local:/path/to/backups/of/one' \
  -t 'taos://root:taosdata@another.com:6030/db1'
```

Single (s)table backup is like:

```bash
taosx run \
  -f 'tmq://this.com/db1.table1' \
  -t 'local:/path/to/backups/of/one'
```

### Incremental backup

Incremental backup could be performed on an existing full backup and generate the incremental backup files after the current backup version. With existing directory `/path/to/backups/of/one`, the command is evenly equal to a fully backup:

```bash
taosx run \
  -f 'tmq://this.com/db1' \
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
  -f 'taos://root:taosdata@localhost:6030/test?query=select * from meters' \
  -t 'csv:./meters.csv'
```

Or to a single parquet file:

```bash
taosx run \
  -f 'taos://root:taosdata@localhost:6030/test?query=select * from meters' \
  -t 'parquet:./meters.parquet'
```

The parameter `query` only works in these two scenarios, and should be a fetchable SQL(like `SELECT`).

### Data Migration

If you want to migrate a **2.6** version (or any version) instance to another version (2.6 or 3.0), you need to install with feature `optin` instead of default.

```bash
cargo install --path /taosx/source/ --no-default-features --features optin
```

`optin` feature will let user to choose which library should be used in a connection.

Then you can use following commands to migrate from 2.6 to 3.0 database:

```bash
taosx run \
  -f 'taos://td2:6030/db1?libraryPath=./libtaos.so.2.6.0.30' \
  -t 'taos://td3:6030/db2?libraryPath=./libtaos.so.3.0.1.8'\
  -vv
```

For the lack of support of TMQ in 2.x instances, you can use `realtime` or `all` mode for data replication like 3.0 TMQ. The difference between `realtime` and `all` mode is `all` mode it will synchronize historical data also, and then monitoring the latest changes.

```bash
taosx run \
  -f 'taos://td2:6030/db1?libraryPath=./libtaos.so.2.6.0.30&mode=realtime' \
  -t 'taos://td3:6030/db2?libraryPath=./libtaos.so.3.0.1.8'\
  -vv
```

You can use this feature to synchronize data in a time range:

```bash
taosx run \
  -f 'taos://td2:6030/db1?start=2022-10-10T00:00:00Z&end=2023-10-10T00:00:00Z' \
  -t 'taos://td3:6030/db2'\
  -vv
```

Both `start` and `end` parameters should be RFC3339 with timezone.

For some use cases, you can synchronize one or more stables with
`stable=name1,name2` or child tables with `tables=stable_1.sub_1,ordinary_table_2`.

## Advanced Usage

taosX use DSN to express a data source or target. DSN is short for **D**ata **S**ource **N**ame representation string, a data structure used to describe a connection to a data source.

A common DSN is basically constructed as this:

```text
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

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
- **object**: Possibly one of: 1) database name(s), 2) data [subscription] topic name(s) 3) table expression: `database.table`. **Optional**.
- **params**: a key-value map for any other information to the datasource. **Optional**. The available parameters may depend on drivers or source / target positions.
- **path**: a file or directory path, or a path-like string for the data source.

For example, a legacy TDengine connection DSN with default protocol is:

```text
taos://root:taosdata@localhost:6030/test
```

A TDengine subscription DSN with websocket protocol is:

```text
tmq+ws://root:taosdata@localhost:6041/topic1
```

A CSV file with path:

```text
csv:./path/to/file.csv
```

taosX use path-like DSN to configure a backup location with some options:

```text
local:./path/relative/to/current/work/directory
local:~/path/under/home
local:/absolute/path
```

### Supported drivers

#### 1. **taos**: query or write with TDengine

A TDengine DSN string could be written as:

```text
taos[+ws]://<username>:<password>@<host>:<port>/<database>[?<params>]
```

In a TDengine query connection DSN, the **driver** will always be `taos`, the **protocol** is optional (but only websocket protocol is supported, you can choose to use `ws` (for `http`) or `wss`(for `https`)). The **username** will be default to `root`, and the **password** default to `taosdata`. The **host** and **port** is default to `localhost:6030` for native protocol, and `localhost:80` or `localhost:443` for websocket based on the **protocol** previously selected (`80` for `ws` and `443` for `wss`). When **database** is declared, it will be the default database for every connection built from the DSN, if the database not exist, it will cause error: `Database not specified or not avaliable`. The **params** key-value pairs contain more complex options, users could check the documentations of each scenario:

- **configDir**: path to TDengine client configuration file, it will use `/etc/taos/` as default. For eg. `taos://?configDir=/custom/path/to/taos/`.
- **token**: use `token` in the **params** part for TDengine cloud service instead of **username** and **password** authentication, be careful to use this along with specified **protocol**(`ws` or `wss`).
- **query**: a query SQL string, such as `SELECT * FROM meters`. This will be used as a table data source, eg. `taos://localhost:6030/test?query=SELECT * FROM meters` will use the query results as table data source, which can be exported as CSV or Parquet.

#### 2. **tmq**: subscription with TDengine

It's recommended to read [Data subscription] documentation first on the official website.

Subscription is a bit different from legacy TDengine connection, you can use a subscription like this:

```text
tmq[+ws]://<username>:<password>@<host>:<port>/<topics>[?<params>]
```

In this kind of DSN, the **driver** must be set as `tmq`. Subscription DSN use **topics** part, which is a comma(`,`)-separated **topic** string list - so that you can subscribe multiple topics in a single subscription. Other parts is nearly the same to previous section - **TDengine DSN for query**. Additional subscription parameters could be set in **params** part:

- **`group.id`**: the group id string of a subscription, which is **required**.
- **`client.id`**: the client id of a subscription, which can be used to track a subscription client.
- **`enable.auto.commit`**: you can set this to `true` when you want to auto commit the consumer.
- **`auto.commit.interval.ms`**: set this value to a integer along with **enable.auto.commit** option.
- **`auto.offset.reset`**: possible values: `none`, `earliest`, `latest` to control the initial subscription position, that means the parameter is not work when the **group.id** has been subscribed once. Default is `none`, usually means `earliest`.
- **`timeout`**: possible values: `never`, `none` or human-readable time duration string like `1d` for one day, `24h` for 24 hours, `5m` for 5 minutes. By default, the timeout value is `500ms`. When use `timeout=never`, taosx will wait for a usable message forever and never stop the subscription until any error caused.

So if you want to subscribe a topic, the DSN may be:

```text
'tmq://root:taosdata@localhost:6030/topic1?group.id=gid1&client.id=any-string'
```

#### 3. **local**: backup to or restore from local directory

taosX use path-like DSN for backup/restore, and use **local** as driver name:

```text
local:/path/to/backups?max-file-size=1G
```

You can use `max-file-size` option to control a single backup file size, by default it's 1G.

More useful parameters will be added in next release.

After backup done, for a backup location directory, the files structure will be:

```text
test-dump-abc2
├── abc2-0-1661847159.z
├── abc2-1-1661847159.z
└── local.toml
```

#### 4. **csv**: export as CSV file

taosX could export query result into single CSV file. The DSN format is:

```text
csv:./local/path/to/file.csv
csv:~/home/nested/path/to/any/file
csv:/absolute/path/to/file.ext
```

Combine query DSN with this, such as:

```bash
taosx run \
  -f 'taos://root:taosdata@localhost:6030/test?query=SELECT * FROM meters' \
  -t 'csv:./meters.csv'
```

#### 5. **parquet**: export as Parquet file

taosX could export query result into single Parquet file. The DSN format is:

```text
parquet:./local/path/to/file.parquet
```

Combine query DSN with this, such as:

```bash
taosx run \
  -f 'taos://root:taosdata@localhost:6030/test?query=SELECT * FROM meters' \
  -t 'parquet:./meters.parquet'
```

We strongly recommend to use Parquet for time-series data sharing and storing purpose, which has better reading performance and much smaller size.

#### 6. **kafka**: import from Kafka and export to kafka

Import data from kafka：

```bash
taosx run \
  -f 'kafka://localhost:9092/?topic=test' \
  -t 'taos://root:taosdata@localhost:6030/test?'
```

### Transformation

taosX support two kind of transformation actions in data replication:

- **Add tags**

    For M:1 data collection scenario (for example, many edge nodes push data to a central node), taosX could automatically add one or more identity tags while data transferring.

    The syntax is:

    ```text
    add-tag:<name>[(<len>)]=<value>
    ```

    For example, to add tag `area` with value `A1`:

    ```text
    add-tag:area=A1
    ```

    The tag data type is `VARCHAR` with default length `100`. Customize the length like this:

    ```text
    add-tag:area(2)=A1
    ```

- **Rename tables**

    taosX could rename the table names before write to target TDengine. The syntax is:

    ```text
    <rename-table-kind>:<rename-type>:<rename-item>
    ```

    Supported *rename-table-kind* list:

    - **rename-table**: rename all three kinds of tables: super table, child table or normal table.
    - **rename-super-table**: rename super table only.
    - **rename-child-table**: rename child table only.

    Supported *rename-type* and *rename-item* expression:

    - **`template:words_{name}_surrounded`**: new table name will use the template `words_{name}_surrounded` and replace `{name}` as real table name.
    - **`prefix:some_prefix_`**: is a short wrapper on template `some_prefix_{name}`.
    - **`suffix:_some_suffix`**: is a short wrapper on template `{name}_some_suffix`.

Here's a example shows how to use transformation.

Suppose we have three TDengine cluster in three location: A, B, C. Each location has the same stable `devices`, with same or not child tables, in database `test`:

```sql
CREATE STABLE `devices` (ts TIMESTAMP, val INT) TAGS (id VARCHAR(16));

# in A, B, and C
CREATE TABLE `d0` using `devices` TAGS ("d0");
# in B
CREATE TABLE `d1` using `devices` TAGS ("d1");
```

Run taosX for each location, replicate all three databases into central TDengine cluster:

```bash
# A
taosX run \
  -f 'tmq://root:taosdata@hostA:6030/test' \
  -t 'taos://root:taosdata@center:6030/test' \
  -T 'add-tag:location:A' \
  -T 'rename-child-table:prefix:A'
# B
taosX run \
  -f 'tmq://root:taosdata@hostB:6030/test' \
  -t 'taos://root:taosdata@center:6030/test' \
  -T 'add-tag:location:B' \
  -T 'rename-child-table:prefix:B'
# C
taosX run \
  -f 'tmq://root:taosdata@hostC:6030/test' \
  -t 'taos://root:taosdata@center:6030/test' \
  -T 'add-tag:location:C' \
  -T 'rename-child-table:prefix:C'
```

Then in central cluster database `test`, will have stable `meters` like this:

```sql
taos> show stables;
          stable_name           |
=================================
 devices                        |
Query OK, 1 rows in database (0.003971s)

taos> show tables;
           table_name           |
=================================
 Ad0                            |
 Bd0                            |
 Bd1                            |
 Cd0                            |
Query OK, 4 rows in database (0.005446s)

taos> desc `devices`;
             field              |         type         |   length    |   note   |
=================================================================================
 ts                             | TIMESTAMP            |           8 |          |
 val                            | INT                  |           4 |          |
 id                             | VARCHAR              |          16 | TAG      |
 location                       | VARCHAR              |           1 | TAG      |
Query OK, 4 rows in database (0.002262s)

taos> select distinct tbname, id, location from devices order by
tbname;
             tbname             |        id        | location |
===============================================================
 Ad0                            | d0               | A        |
 Bd0                            | d0               | B        |
 Bd0                            | d1               | B        |
 Cd0                            | d0               | C        |
Query OK, 4 rows in database (0.007300s)
```

## Service mode

taosx provides a builtin service mode, to automatically monitor a configuration directory, expose an OpenAPI with workflow control support and enable OpenMetrics exporter by default. We have a schedule to add more useful functionalities, include a monitor web dashboard to manage configurations and display the status and metrics view in later release channel.

```bash
taosx serve -l 0.0.0.0:6050
```

taosx follows the [OpenAPI Specification 3.x][oas3] and provides a [SwaggerUI] interface at <http://localhost:6050/swagger-ui/>.

- **GET /tasks**: list tasks in current processor. You can use these query filters here for search:
    - `from_cluster`/`to_cluster`: filter exact matches of cluster id.
    - `status`: filter exact matches for specific status.
    - `start_create_time`/`end_create_time`: filter `created` filed by a time range.
    - `deleted`: include deleted tasks too.
- **GET /tasks/count**: get tasks count only, all the filters above will work in this api.
- **POST /tasks**: create new task with from/to DSN and return `id` of the task.
- **GET /tasks/{id}**: get task status by `id`.
- **POST /tasks/{id}/start**: start a task by `id` if not running.
- **POST /tasks/{id}/stop**: stop a running tasks by `id`, do nothing if not running.
- **DELETE /tasks/{id}**: delete task by `id`, will cancel unfinished tasks and delete the task(taosx will mark the task as deleted indeed, and can't list it in *GET /tasks* action).

A task schema might be:

```json
{
  "id": 1,
  "from": "tmq://root:taosdata@localhost:6030/test",
  "to": "local:/path/to/backup/test",
  "created_at": "2022-02-02T02:02:02+08:00",
  "last_modified_at": "2022-02-02T02:02:02+08:00",
  "finished_at": "2022-02-02T02:02:02+08:00",
  "status": "completed",
  "labels": []
}
```

- *`id`*: a unique 64 bit integer.
- *`created_at`*: created datetime in nanoseconds with RFC3339 format.
- *`last_modified_at`*: datetime that the task has last been modified.
- *`completed`*: check if the status is `completed`.
- *`from`*: DSN for source.
- *`to`*: DSN for target.
- *`status`*: possible values: `created`, `failed`, `cancelled`, `deleted`, `completed`, `interrupted`, `stopped`.
- *`reason`*: an nullable field for the reason of current status (currently, for `failed` only).

To create a new task, use the schema:

```json
{
  "from": "tmq://root:taosdata@localhost:6030/test",
  "to": "local:./backups-test"
}
```

And the POST response body is:

```json
{
  "id": 3,
  "from": "tmq://root:taosdata@localhost:6030/test",
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
[TMQ]: https://docs.taosdata.com/develop/tmq/
