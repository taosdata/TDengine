# Nodejs 连接器-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-06 | 2025-01-09 | 1.0 | 门世斌 | 创建 |
| 2026-01-20 | 2026-01-20 | 1.1 | 郭振伟 | 更新文档至 TDengine v3.4.0.0 版本。 |
| 2026-01-23 | 2026-01-23 | 1.2 | 霍琳贺 | 添加安全部分 |

## 2. 背景

JavaScript 是前端开发的主流语言，随着技术的发展，越来越多的前端开发者开始涉足后端和全栈开发。使用 Node.js 实现 TDengine 连接器，可以让 JavaScript 开发者轻松与 TDengine 进行交互。这不仅减少了开发者在不同语言之间切换所需的时间和精力，还避免了因语言差异可能导致的错误和理解障碍，从而降低了开发门槛，提高了开发效率。

## 3. 定义

**无模式写入：**是指在写入数据时无需预先定义表结构，系统会根据数据自动创建或调整表结构的特性
**数据订阅：**允许客户端实时接收数据库中的数据变化，适用于实时监控场景；
**参数绑定：**是在 SQL 语句中使用占位符替代具体值的技术，可以防止 SQL 注入并提高性能
**WebSocket：** 是一种在单个 TCP 连接上提供全双工通信的协议。它允许客户端和服务器之间的双向实时通信，避免了 HTTP 请求-响应模型的限制。WebSocket协议的定义和规范是由 IETF（Internet Engineering Task Force）标准化的，其主要文档是 [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)。
**FQDN：**全限定域名是互联网域名系统（DNS）中的一种完整的、唯一的域名表示形式，用于明确标识互联网上的特定主机或服务器。
**RFC3339：**RFC 3339 是一种时间和日期格式的标准，旨在规范化地表示时间和日期，以便在计算机系统之间传递和处理。

## 4. 行为说明

Node.js 连接器（`@tdengine/websocket`）, 其通过 taosAdapter 提供的 WebSocket 接口连接 TDengine 实例。

### 4.1 数据类型映射

下表为 TDengine DataType 和 Node.js DataType 之间的映射关系
| TDengine DataType | Node.js DataType |
| --- | --- |
| TIMESTAMP | bigint |
| TINYINT | number |
| SMALLINT | number |
| INT | number |
| BIGINT | bigint |
| TINYINT UNSIGNED | number |
| SMALLINT UNSIGNED | number |
| INT UNSIGNED | number |
| BIGINT UNSIGNED | bigint |
| FLOAT | number |
| DOUBLE | number |
| BOOL | boolean |
| BINARY | string |
| NCHAR | string |
| JSON | string |
| VARBINARY | ArrayBuffer |
| GEOMETRY | ArrayBuffer |

**注意**：JSON 类型仅在 tag 中支持。

### 4.2 URL 规范

1. [+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------------|---|-----------|-----------|------|------|------------|-----------------------|
|   protocol |   | username  | password  | host | port |  database  |  params               |
1. **protocol**: 使用 websocket 协议建立连接。例如`ws://``localhost:6041`
2. **username/password**: 数据库的用户名和密码。
3. **host/port**: 主机地址和端口号。例如`localhost:6041`
4. **database**: 数据库名称。
5. **params**: 其他参数。 例如token。
完整 URL 示例：
```sql
ws://root:taosdata@localhost:6041
```

### 4.3 连接功能

WSConfig 中的配置如下：
```plaintext
setUrl(url string) 设置 taosAdapter 连接地址 url，详见上文 URL 规范。
setUser(user: string) 设置数据库用户名。
setDb(db: string) 设置数据库名称。
setTimeOut(ms : number) 设置连接超时，单位毫秒。
setToken(token: string) 设置 taosAdapter 认证token。
setTimezone(timezone: string) 设置时区
```

- `static async open(wsConfig:WSConfig):Promise<WsSql>`
  - **接口说明**：建立 taosAdapter 连接。
  - **参数说明**：
    - `wsConfig`：连接配置，详见上文 WSConfig 。
  - **返回值**：连接对象。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `async close():Promise<void>`
  - **接口说明**：关闭连接。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `destroyed()`
  - **接口说明**：释放销毁资源。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
连接示例：
```typescript
    let dsn = 'ws://localhost:6041';
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conf.setDb('power');
        conn = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        return conn;
    } catch (err) {
        console.log("Failed to connect to " + dsn + ", ErrCode: " + err.code + ", ErrMessage: " + err.message);
        throw err;
    }
```

SSL 连接示例
```typescript
    let dsn = 'wss://localhost:6041';
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conf.setDb('power');
        conn = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        return conn;
    } catch (err) {
        console.log("Failed to connect to " + dsn + ", ErrCode: " + err.code + ", ErrMessage: " + err.message);
        throw err;
    }
```

云服务实例：
```typescript
const TDENGINE_CLOUD_URL =
  'wss://gw.pre.cloud.taosdata.com?token=215f1e77f81abxssssxxx9';

try {
    let conf = new taos.WSConfig(TDENGINE_CLOUD_URL);
    conn = await taos.sqlConnect(conf);
    console.log("Connected to " + dsn + " successfully.");
    return conn;
} catch (err) {
    console.log("Failed to connect to " + dsn + ", ErrCode: " + err.code + ", ErrMessage: " + err.message);
    throw err;
}
  
```

### 4.4 获取 taosc 版本号

- `async version(): Promise<string>`
  - **接口说明**：获取 taosc 客户端版本。
  - **返回值**：TDengine 版本号。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。

### 4.5 执行 SQL

- `async exec(sql: string, reqId?: number): Promise<TaosResult>`
  - **接口说明**：执行非查询 SQL 语句。
  - **参数说明**：
    - `sql`：待执行的 SQL 语句。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：执行结果
    ```javascript
    TaosResult {
        affectRows: number,   影响的条数
        timing: number,       执行时长
        totalTime: number,    响应总时长
    }    
    ```

  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async query(sql: string, reqId?:number): Promise<WSRows>`
  - **接口说明**：查询数据。
  - **参数说明**：
    - `sql`：待执行的查询 SQL 语句。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：WSRows 数据集对象。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。

### 4.6 数据集

- `getMeta():Array<TDengineMeta> | null`
  - **接口说明**：获取查询结果的列的数量、类型和长度。
  - **返回值**：TDengineMeta 数据对象数组。
    ```javascript
    export interface TDengineMeta {
        name: string,
        type: string,
        length: number,
    }
    ```

- `async next(): Promise<boolean>`
  - **接口说明**：将游标从当前位置向后移动一行。用于遍历查询结果集。
  - **返回值**：如果新的当前行有效，则返回 true；如果结果集中没有更多行，则返回 false。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `getData(): Array<any>`
  - **接口说明**：返回查询的一行数据。
  - **返回值**：返回查询的一行数据，此接口需要搭配 next 接口一起使用。
- `async close():Promise<void>`
  - **接口说明**：数据读取完成后，释放结果集。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
完整用例：
```typescript
// ANCHOR: createConnect
const taos = require("@tdengine/websocket");

let dsn = 'ws://localhost:6041';
async function createConnect() {

    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conf.setDb('power');
        conn = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        return conn;
    } catch (err) {
        console.log("Failed to connect to " + dsn + ", ErrCode: " + err.code + ", ErrMessage: " + err.message);
        throw err;
    }

}

async function createDbAndTable() {
    let wsSql = null;
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conf.setDb('power');
        wsSql = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        // create database
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS power');
        console.log("Create database power successfully.");
        // create table
        await wsSql.exec('CREATE STABLE IF NOT EXISTS power.meters ' +
            '(ts timestamp, current float, voltage int, phase float) ' +
            'TAGS (location binary(64), groupId int);');

        console.log("Create stable power.meters successfully");
    } catch (err) {
        console.error(`Failed to create database power or stable meters, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}

async function insertData() {
    let wsSql = null
    let insertQuery = "INSERT INTO " +
            "power.d1001 USING power.meters (location, groupId) TAGS('California', 2) " +
            "VALUES " +
            "(NOW + 1a, 10.30000, 219, 0.31000) " +
            "(NOW + 2a, 12.60000, 218, 0.33000) " +
            "(NOW + 3a, 12.30000, 221, 0.31000) " +
            "power.d1002 USING power.meters TAGS('California', 3) " +
            "VALUES " +
            "(NOW + 1a, 10.30000, 218, 0.25000) ";
    try {
        wsSql = await createConnect();
        
        taosResult = await wsSql.exec(insertQuery);
        console.log("Successfully inserted " + taosResult.getAffectRows() + " rows to power.meters.");
    } catch (err) {
        console.error(`Failed to insert data to power.meters, sql: ${insertQuery}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }
}

async function queryData() {
    let wsRows = null;
    let wsSql = null;
    let sql = 'SELECT ts, current, location FROM power.meters limit 100';
    try {
        wsSql = await createConnect();
        wsRows = await wsSql.query(sql);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', current: ' + row[1] + ', location:  ' + row[2]);
        }
    }
    catch (err) {
        console.error(`Failed to query data from power.meters, sql: ${sql}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
    }
}

async function sqlWithReqid() {
    let wsRows = null;
    let wsSql = null;
    let reqId = 1;
    try {
        wsSql = await createConnect();
        wsRows = await wsSql.query('SELECT ts, current, location FROM power.meters limit 100', reqId);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', current: ' + row[1] + ', location:  ' + row[2]);
        }
    }
    catch (err) {
        console.error(`Failed to query data from power.meters, reqId: ${reqId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
    }
}

async function test() {
    try {
        taos.setLevel("debug");
        await createDbAndTable();
        await insertData();
        await queryData();
        await sqlWithReqid();
        taos.destroy();        
    } catch(e) {
        console.error(e);
        process.exitCode = 1;
    }

}

test()

```

### 4.7 无模式写入

- `async schemalessInsert(lines: Array<string>, protocol: SchemalessProto,                  precision: Precision, ttl: number, reqId?: number): Promise<void>`
  - **接口说明**：无模式写入。
  - **参数说明**：
    - `lines`：待写入的数据数组，无模式具体的数据格式可参考 `Schemaless 写入`。
    - `protocol`: 协议类型
      - SchemalessProto.InfluxDBLineProtocol：InfluxDB 行协议（Line Protocol)。
SchemalessProto.OpenTSDBTelnetLineProtocol：OpenTSDB 文本行协议。
SchemalessProto.OpenTSDBJsonFormatProtocol：JSON 协议格式。
    - `precision`: 时间精度
      - Precision.HOURS： 小时
Precision.MINUTES：分钟
Precision.SECONDS：秒
Precision.MILLI_SECONDS：毫秒
Precision.MICRO_SECONDS：微秒
Precision.NANO_SECONDS： 纳秒
    - `ttl`：表过期时间，单位天。
    - `reqId`: 用于问题追踪，可选。
  - **异常**：失败抛出 `TaosResultError` 异常。
完整示例：
```typescript
const taos = require("@tdengine/websocket");

if(host == null){
    console.log("Usage: node nodejsChecker.js host=<hostname> port=<port>");
    process.exit(0);
  }

let influxdbData = ["meters1,location=California.LosAngeles,groupId=2 current=11.8,voltage=221,phase=0.28 1648432611249",
    "meters1,location=California.LosAngeles,groupId=2 current=13.4,voltage=223,phase=0.29 1648432611250",
    "meters1,location=California.LosAngeles,groupId=3 current=10.8,voltage=223,phase=0.29 1648432611249"];

let jsonData = ["{\"metric\": \"meter_current\",\"timestamp\": 1626846402,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}",
    "{\"metric\": \"meter_current\",\"timestamp\": 1626846403,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1002\"}}",
    "{\"metric\": \"meter_current\",\"timestamp\": 1626846404,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1003\"}}"]

let telnetData = ["meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3"];

async function createConnect() {
    let dsn = 'ws://localhost:6041'
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    conf.setDb('power')
    return await (0, taos.sqlConnect)(conf);
}

async function test() {
    let wsSql = null;
    let wsRows = null;
    try {
        wsSql = await createConnect()
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;');
        await wsSql.schemalessInsert(influxdbData, taos.SchemalessProto.InfluxDBLineProtocol, taos.Precision.MILLI_SECONDS, 0);
        await wsSql.schemalessInsert(telnetData, taos.SchemalessProto.OpenTSDBTelnetLineProtocol, taos.Precision.MILLI_SECONDS, 0);
        await wsSql.schemalessInsert(jsonData, taos.SchemalessProto.OpenTSDBJsonFormatProtocol, taos.Precision.SECONDS, 0);
    }
    catch (e) {
        let err = e;
        console.error(err);
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
        taos.destroy();
    }
}
test()
```

### 4.8 参数绑定

- `async stmtInit(reqId?:number): Promise<WsStmt>`
  - **接口说明** 使用 WsSql 对象创建 stmt 对象。
  - **参数说明**：
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：stmt 对象。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async prepare(sql: string): Promise<void>`
  - **接口说明** 绑定预编译 sql 语句。
  - **参数说明**：
    - `sql`: 预编译的 SQL 语句。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `async setTableName(tableName: string): Promise<void>`
  - **接口说明** 设置将要写入数据的表名。
  - **参数说明**：
    - `tableName`: 表名，如果需要指定数据库, 例如： `db_name.table_name` 即可。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。 通过 StmtBindParams 对象设置绑定数据。
- `setBoolean(params :any[])`
  - **接口说明** 设置布尔值。
  - **参数说明**：
    - `params`: 布尔类型列表。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- 下面接口除了要设置的值类型不同外，其余同 setBoolean：
  ```sql
  setTinyInt(params :any[])
  setUTinyInt(params :any[])
  setSmallInt(params :any[])
  setUSmallInt(params :any[])
  setInt(params :any[])
  setUInt(params :any[])
  setBigint(params :any[])
  setUBigint(params :any[])
  setFloat(params :any[])
  setDouble(params :any[])
  setVarchar(params :any[])
  setBinary(params :any[])
  setNchar(params :any[])
  setJson(params :any[])
  setVarBinary(params :any[])
  setGeometry(params :any[])
  setTimestamp(params :any[])
  ```

- `async setTags(paramsArray:StmtBindParams): Promise<void>`
  - **接口说明** 设置表 Tags 数据，用于自动建表。
  - **参数说明**：
    - `paramsArray`: Tags 数据。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `async bind(paramsArray:StmtBindParams): Promise<void>`
  - **接口说明** 绑定数据。
  - **参数说明**：
    - `paramsArray`: 绑定数据。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `async batch(): Promise<void>`
  - **接口说明** 提交绑定数据。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `async exec(): Promise<void>`
  - **接口说明** 执行将绑定的数据全部写入。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
- `getLastAffected()`
  - **接口说明** 获取写入条数。
  - **返回值**：写入条数。
- `async close(): Promise<void>`
  - **接口说明** 关闭 stmt 对象。
  - **异常：失败抛出** `TDWebSocketClientError` 异常。
完整示例：
```typescript
const taos = require("@tdengine/websocket");

let db = 'power';
let stable = 'meters';
let numOfSubTable = 10;
let numOfRow = 10;
function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function Prepare() {
    let dsn = `ws://localhost:6041`
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    conf.setDb(db)
    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(`CREATE DATABASE IF NOT EXISTS ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
    await wsSql.exec(`CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`);
    return wsSql
}

(async () => {
    let stmt = null;
    let connector = null;
    try {
        await Prepare();
        let wsConf = new config.WSConfig(dsn);
        wsConf.setDb(db);
        connector = await (0, src_1.sqlConnect)(wsConf);
        stmt = await connector.stmtInit();
        await stmt.prepare(`INSERT INTO ? USING ${db}.${stable} (location, groupId) TAGS (?, ?) VALUES (?, ?, ?, ?)`);
        for (let i = 0; i < numOfSubTable; i++) {
            await stmt.setTableName(`d_bind_${i}`);
            let tagParams = stmt.newStmtParam();
            tagParams.setVarchar([`location_${i}`]);
            tagParams.setInt([i]);
            await stmt.setTags(tagParams);
            let timestampParams = [];
            let currentParams = [];
            let voltageParams = [];
            let phaseParams = [];
            const currentMillis = new Date().getTime();
            for (let j = 0; j < numOfRow; j++) {
                timestampParams.push(currentMillis + j);
                currentParams.push(Math.random() * 30);
                voltageParams.push(getRandomInt(100, 300));
                phaseParams.push(Math.random());
            }
            let bindParams = stmt.newStmtParam();
            bindParams.setTimestamp(timestampParams);
            bindParams.setFloat(currentParams);
            bindParams.setInt(voltageParams);
            bindParams.setFloat(phaseParams);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();
            console.log(`d_bind_${i} insert ` + stmt.getLastAffected() + " rows.");
        }
    }
    catch (e) {
        console.error(e);
    }
    finally {
        if (stmt) {
            await stmt.close();
        }
        if (connector) {
            await connector.close();
        }
        (0, src_1.destroy)();
    }
})();
```

### 4.9 数据订阅

- **创建消费者支持属性列表**：
  ```sql
  taos.TMQConstants.CONNECT_USER: 用户名。
  taos.TMQConstants.CONNECT_PASS: 密码。
  taos.TMQConstants.GROUP_ID: 所在的 group。
  taos.TMQConstants.CLIENT_ID: 客户端id。
  taos.TMQConstants.WS_URL: taosAdapter 的url地址。
  taos.TMQConstants.AUTO_OFFSET_RESET: 来确定消费位置为最新数据（latest）还是包含旧数据（earliest）。
  taos.TMQConstants.ENABLE_AUTO_COMMIT: 是否允许自动提交。
  taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS: 自动提交间隔。
  taos.TMQConstants.CONNECT_MESSAGE_TIMEOUT: 数据传输超时参数，单位 ms，默认为 10000 ms。
  ```

- `static async newConsumer(wsConfig:Map<string, any>):Promise<WsConsumer>`
  - **接口说明** 消费者构造函数。
  - **参数说明**：
    - `wsConfig`: 创建消费者属性配置。
  - **返回值**：WsConsumer 消费者对象。
  - **异常**：如果在执行过程中出现异常，抛出 `TDWebSocketClientError` 错误。
- `async subscribe(topics: Array<string>, reqId?:number): Promise<void>`
  - **接口说明** 订阅一组主题。
  - **参数说明**：
    - `topics`: 订阅的主题列表。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async unsubscribe(reqId?:number): Promise<void>`
  - **接口说明** 取消订阅。
  - **参数说明**：
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async poll(timeoutMs: number, reqId?:number):Promise<Map<string, TaosResult>>`
  - **接口说明** 轮询消息。
  - **参数说明**：
    - `timeoutMs`: 表示轮询的超时时间，单位毫秒。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：`Map<string, TaosResult>` 每个主题对应的数据。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async subscription(reqId?:number):Promise<Array<string>>`
  - **接口说明** 获取当前订阅的所有主题。
  - **参数说明**：
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：`Array<string>` 主题列表。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async commit(reqId?:number):Promise<Array<TopicPartition>>`
  - **接口说明** 提交当前处理的消息的偏移量。
  - **参数说明**：
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：`Array<TopicPartition>` 每个主题的消费进度。
  - **异常**：失败抛出 `TDWebSocketClientError` 异常。
- `async committed(partitions:Array<TopicPartition>, reqId?:number):Promise<Array<TopicPartition>>`
  - **接口说明**：获取一组分区最后提交的偏移量。
  - **参数说明**：
    - `partitions`：一个 `Array<TopicPartition>` 类型的参数，表示要查询的分区集合。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：`Array<TopicPartition>`，即一组分区最后提交的偏移量。
  - **异常**：如果在获取提交的偏移量过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async seek(partition:TopicPartition, reqId?:number):Promise<void>`
  - **接口说明**：将给定分区的偏移量设置到指定的位置。
  - **参数说明**：
    - `partition`：一个 `TopicPartition` 类型的参数，表示要操作的分区和要设置的偏移量。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **异常**：如果在设置偏移量过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async positions(partitions:Array<TopicPartition>, reqId?:number):                                                           Promise<Array<TopicPartition>>`
  - **接口说明**：获取给定分区当前的偏移量。
  - **参数说明**：
    - `partitions`：一个 `TopicPartition` 类型的参数，表示要查询的分区。
    - `reqId`: 请求 id 非必填，用于问题追踪。
  - **返回值**：`Array<TopicPartition>`，即一组分区最后提交的偏移量。
  - **异常**：如果在获取偏移量过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async seekToBeginning(partitions:Array<TopicPartition>):Promise<void>`
  - **接口说明**：将一组分区的偏移量设置到最早的偏移量。
  - **参数说明**：
    - `partitions`：一个 `Array<TopicPartition>` 类型的参数，表示要操作的分区集合。
  - **异常**：如果在设置偏移量过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async seekToEnd(partitions:Array<TopicPartition>):Promise<void>`
  - **接口说明**：将一组分区的偏移量设置到最新的偏移量。
  - **参数说明**：
    - `partitions`：一个 `Array<TopicPartition>` 类型的参数，表示要操作的分区集合。
  - **异常**：如果在设置偏移量过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async assignment(topics?:string[]):Promise<Array<TopicPartition>>`
  - **接口说明**：获取消费者当前分配的指定的分区或所有分区。
  - **参数说明**：
    - `topics`：需要获取的分区（非必填），不填表示获取全部的分区
  - **返回值**：返回值类型为 `Array<TopicPartition>`，即消费者当前分配的所有分区。
  - **异常**：如果在获取分配的分区过程中发生错误，将抛出 `TDWebSocketClientError` 异常。
- `async close():Promise<void>`
  - **接口说明**：关闭 tmq 连接。
  - **异常**：操作失败抛出 `TDWebSocketClientError` 异常。
完整示例：
```typescript
const { sleep } = require("@tdengine/websocket");
const taos = require("@tdengine/websocket");

const db = 'power';
const stable = 'meters';
const url = 'ws://localhost:6041';
const topic = 'topic_meters'
const topics = [topic];
const groupId = "group1";
const clientId = "client1";

async function createConsumer() {
    let groupId = "group1";
    let clientId = "client1";
    let configMap = new Map([
        [taos.TMQConstants.GROUP_ID, groupId],
        [taos.TMQConstants.CLIENT_ID, clientId],
        [taos.TMQConstants.CONNECT_USER, "root"],
        [taos.TMQConstants.CONNECT_PASS, "taosdata"],
        [taos.TMQConstants.AUTO_OFFSET_RESET, "latest"],
        [taos.TMQConstants.WS_URL, url],
        [taos.TMQConstants.ENABLE_AUTO_COMMIT, 'true'],
        [taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS, '1000']
    ]);
    try {
        conn = await taos.tmqConnect(configMap);
        console.log(`Create consumer successfully, host: ${url}, groupId: ${groupId}, clientId: ${clientId}`)
        return conn;
    } catch (err) {
        console.error(`Failed to create websocket consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }

}

async function prepare() {
    let conf = new taos.WSConfig('ws://localhost:6041');
    conf.setUser('root');
    conf.setPwd('taosdata');

    const createDB = `CREATE DATABASE IF NOT EXISTS ${db}`;
    const createStable = `CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`;

    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(createDB);
    await wsSql.exec(createStable);

    let createTopic = `CREATE TOPIC IF NOT EXISTS ${topics[0]} AS SELECT * FROM ${db}.${stable}`;
    await wsSql.exec(createTopic);
    await wsSql.close();
}

async function insert() {
    let conf = new taos.WSConfig('ws://localhost:6041');
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    let wsSql = await taos.sqlConnect(conf);
    for (let i = 0; i < 1; i++) {
        await wsSql.exec(`INSERT INTO d1001 USING ${stable} (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW, ${10 + i}, ${200 + i}, ${0.32 + i})`);
    }
    await wsSql.close();
}

async function subscribe(consumer) {
    try {
        await consumer.subscribe(['topic_meters']);
        let res = new Map();
        while (res.size == 0) {
            res = await consumer.poll(100);
            await consumer.commit();
        }
        let assignment = await consumer.assignment();
        await consumer.seekToBeginning(assignment);
        console.log("Assignment seek to beginning successfully");
    } catch (err) {
        console.error(`Failed to seek offset, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}

async function test() {
    let consumer = null;
    try {
        await prepare();
        consumer = await createConsumer();
        const allPromises = [];
        allPromises.push(subscribe(consumer));
        allPromises.push(insert());
        await Promise.all(allPromises);
        await consumer.unsubscribe();
        console.log("Consumer unsubscribed successfully.");
    }
    catch (err) {
        console.error(`Failed to consumer, topic: ${topic}, groupId: ${groupId}, clientId: ${clientId}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (consumer) {
            await consumer.close();
            console.log("Consumer closed successfully.");
        }
        taos.destroy();
    }
}

test()
```

## 5. 安全

1. 日志/错误输出脱敏（隐藏密码、token、路径）
2. 支持 TLS 认证
3. SQL 防注入：文档与示例必须优先使用 `PreparedStatement` 参数绑定，明确其为防 SQL 注入的要求。
4. 支持请求超时，避免资源耗尽。

## 6. 性能

1. [Node.js](https://m.baidu.com/s?word=Node.js&sa=re_dqa_zy)[**](https://m.baidu.com/s?word=Node.js&sa=re_dqa_zy) 是一种流行的网络服务器平台，它以其在处理 I/O 密集型任务时的性能而闻名。Node.js 使用事件驱动和非阻塞 I/O 模型，这使得它能够高效地处理大量并发请求，而不会因为线程阻塞而影响性能。在面对短时任务时，Node.js 的性能表现尤为优秀。
2. Node.js 在处理计算密集型任务时性能较弱，因为它是主线程是单线程调度事件执行的，这意味着它无法充分利用多核 CPU 的优势。此外，[V8 引擎](https://m.baidu.com/s?word=V8%20%E5%BC%95%E6%93%8E&sa=re_dqa_zy)[**](https://m.baidu.com/s?word=V8%20%E5%BC%95%E6%93%8E&sa=re_dqa_zy)的内存限制也可能导致在处理大量数据时性能下降。
3. Node.js 的另一个局限性是它不太适合编写大型复杂的应用程序，如游戏引擎或图形处理等领域。这主要是因为 Node.js 的单线程模型和事件驱动模型可能不适合长时间运行的进程，这可能导致事件循环阻塞，影响整个系统的性能。
4. 尽管存在这些局限性，Node.js 仍然是一个受欢迎的选择，尤其是在处理高并发场景时，因为它能够支持多个实例并通过 cluster 模块充分利用多核 CPU 的优势，节约服务器资源。

## 7. 兼容性

兼容 TDengine 3.3.2.0 及更高版本

## 8. 运维

无

## 9. 使用场景

1. 被以 nodejs 编写的后端服务使用，与 TDengine 进行交互。
2. 被浏览器直接调用，与 TDengine 进行交互。
3. 连接支持普通连接、SSL 连接、云服务的 token 方式。

## 10. 约束和限制

1. 支持 node.js 14.x.x 及以上版本
2. 浏览器支持 ES2020
  - Chrome：51 版起便可以支持 97% 的 ES2020 新特性。
  - Firefox：53 版起便可以支持 97% 的 ES2020 新特性。
  - Safari：10 版起便可以支持 99% 的 ES2020 新特性。
  - IE：Edge 15可以支持 96% 的 ES2020新特性。
  - Edge 14 可以支持 93% 的 ES2020 新特性。（IE7~11 基本不支持 ES6）

## 11. 常见错误和排查

在调用连接器 api 报错后，通过 try catch 可以获取到错误的信息和错误码。
错误说明：Node.js 连接器错误码在 100 到 110 之间，之外的错误为 TDengine 其他功能模块的报错。
具体的连接器错误码请参考：
| Error Code | Description | Suggested Actions |
| --- | --- | --- |
| 100 | invalid variables | 参数不合法，请检查相应接口规范，调整参数类型及大小。 |
| 101 | invalid url | url 错误，请检查 url 是否填写正确。 |
| 102 | received server data but did not find a callback for processing | 接收到服务端数据但没有找到上层回调 |
| 103 | invalid message type | 接收到的消息类型无法识别，请检查服务端是否正常。 |
| 104 | connection creation failed | 连接创建失败，请检查网络是否正常。 |
| 105 | websocket request timeout | 请求超时 |
| 106 | authentication fail | 认证失败，请检查用户名，密码是否正确。 |
| 107 | unknown sql type in tdengine | 请检查 TDengine 支持的 Data Type 类型。 |
| 108 | connection has been closed | 连接已经关闭，请检查 Connection 是否关闭后再次使用，或是连接是否正常。 |
| 109 | fetch block data parse fail | 获取到的查询数据，解析失败 |
| 110 | websocket connection has reached its maximum limit | WebSocket 连接达到上限 |
| 111 | topic partitions and positions are not equal in length | 重新订阅 |
| 112 | version mismatch. The minimum required TDengine TSDB version is 3.3.2.0 | TDengine TSDB 的版本低于 3.3.2.0 连接器不支持，用户需要升级到 3.3.2.0 以上版本。 |

- [TDengine Node.js Connector Error Code](https://github.com/taosdata/taos-connector-node/blob/main/nodejs/src/common/wsError.ts)
- TDengine 其他功能模块的报错，请参考 [错误码](https://docs.taosdata.com/reference/error-code/)

## 12. 可观测性

如果应用使用扩展接口传递 reqId，则可以在后续模块如 taosc、taosAdapter 等日志中进行分析。

## 13. 安装和卸载

### 13.1 **安装前准备**

安装 Node.js 开发环境, 使用14以上版本。下载链接： [https://nodejs.org/en/download/](https://nodejs.org/en/download/)

### 13.2 **安装**

1. 使用 npm 安装 Node.js 连接器
2. npm install @tdengine/websocket

### 13.3 **安装验证**

1. 新建安装验证目录，例如：`~/tdengine-test`，下载 GitHub 上 [nodejsChecker.js 源代码](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/nodejsChecker.js)到本地。
2. 在命令行中执行以下命令：
  ```bash {wrap}
  npm init -y
  npm install @tdengine/websocket
  node nodejsChecker.js
  ```

1. 执行以上步骤后，在命令行会输出 nodeChecker.js 连接 TDengine 实例，并执行简单插入和查询的结果。

## 14. 文档

需要在官方文档中添加章节【TDengine Node.js Connector】。

## 15. 参考文档

Url path RFC：https://datatracker.ietf.org/doc/html/rfc3986#section-3.3

## 16. 附录

无
