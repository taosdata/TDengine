# TDengine RESTful

This is a TDengine's RESTful connector in TypeScript. It's depend on [node-fetch v2](https://github.com/node-fetch/node-fetch/tree/2.x). Using `fetch(url,options)` to send sql statement and receive response.

## Installation

```bash
npm i @tdengine/rest
```

## Usage

```TypeScript
import { options, connect } from '@tdengine/rest'
// From v3.0.0 path is '/rest/sql', and this path will return timestamp in RFC3339.
// This config step can be skipped.
options.path='/rest/sql';
// set host
options.host='localhost';
// set other options like user/passwd

let conn = connect(options);
let cursor = conn.cursor();
(async()=>{
    let result = await cursor.query('show databases');
    // optional:
    // let result = await cursor.query('show databases',pure=false);
    
    // Get Result object, return Result object.
    console.log(result.getResult());
    // Get status, return 'succ'|'error'.
    console.log(result.getStatus());
    // Get head,return response head (Array<any>|undefined,when execute failed this is undefined).
    console.log(result.getHead());
    // Get Meta data, return Meta[]|undefined(when execute failed this is undefined).
    console.log(result.getMeta());
    // Get data,return Array<Array<any>>|undefined(when execute failed this is undefined).
    console.log(result.getData());
    // Get affect rows,return number|undefined(when execute failed this is undefined).
    console.log(result.getAffectRows());
    // Get command,return SQL send to server(need to `query(sql,false)`,set 'pure=false',default true).
    console.log(result.getCommand());
    // Get error code ,return number|undefined(when execute failed this is undefined).
    console.log(result.getErrCode());
    // Get error string,return string|undefined(when execute failed this is undefined).
    console.log(result.getErrStr());
})()

```
