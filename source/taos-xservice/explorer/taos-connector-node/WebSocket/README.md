# WebSocket APIs

## Bulk Pulling

### DSN

User can connect to the TDengine by passing DSN to WebSocket client. The description about the DSN like before.

```text
[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------------|---|-----------|-----------|------|------|------------|-----------------------|
|   protocol |   | username  | password  | host | port |  database  |  params               |
```

- **protocol**: Display using websocket protocol to establish connection. eg. `ws://localhost:6041`
- **username/password**: Database's username and password.
- **host/port**: Declare host and port. eg. `localhost:6041`
- **database**: Optional, use to specify database name.
- **params**: Other parameters. Like cloud Token.

A complete DSN string example：

```text
ws://localhost:6041/test
```

### Basic Usage

``` typescript
import {connect} from '@tdengine/websocket'
let dsn = "ws://host:port/rest/ws/db"
// create an instance of taoWS, while the returned websocket connection of the returned instance 'ws' may is not in 'OPEN' status
var ws = connect(dsn)
```

``` typescript
// build connect with tdengine
ws.connect().then(connectRes=>console.log(connectRes)).catch(e=>{/*do some thing to  handle error*/})
```

``` typescript
//query data with SQL
ws.query(sql).then(taosResult=>{console.log(taosResult)}).catch(e=>{/*do some thing to  handle error*/})
```

```typescript
// get client version
ws.version().then(version=>console.log(version)).catch(e=>{/*do some thing to  handle error*/})
```

``` typescript
// get current WebSocket connection status
let status:number = ws.status()
```

``` typescript
// close current WebSocket connection
ws.close();
```