const { options, connect } = require("@tdengine/rest");
options.url = process.env.TDENGINE_CLOUD_URL;
options.query = { token: process.env.TDENGINE_CLOUD_TOKEN };

// ANCHOR: usage
let conn = connect(options);
let cursor = conn.cursor();
(async()=>{
    let result = await cursor.query('show databases');
    // print query result as taos shell
    result.toString();
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
// ANCHOR_END: usage
