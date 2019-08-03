/**
 * C Interface with TDengine Module
 * @module CTaosInterface
 */

const ref = require('ref');
const ffi = require('ffi');
const ArrayType = require('ref-array');
const Struct = require('ref-struct');
const FieldTypes = require('./constants');
const errors = require ('./error');
const TaosObjects = require('./taosobjects');

module.exports = CTaosInterface;

function convertMillisecondsToDatetime(time) {
  return new TaosObjects.TaosTimestamp(time);
}
function convertMicrosecondsToDatetime(time) {
  return new TaosObjects.TaosTimestamp(time * 0.001);
}

function convertTimestamp(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  timestampConverter = convertMillisecondsToDatetime;
  if (micro == true) {
    timestampConverter = convertMicrosecondsToDatetime;
  }
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let queue = [];
    let time = 0;
    for (let i = currOffset; i < currOffset + nbytes; i++) {
      queue.push(data[i]);
      if (data[i] == 0) {
        break;
      }
    }
    for (let i = queue.length - 1; i >= 0; i--) {
      time += queue[i] * Math.pow(16, i * 2);
    }
    currOffset += nbytes;
    res.push(timestampConverter(time));
  }
  return res;
}
function convertBool(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = new Array(data.length);
  for (let i = 0; i < data.length; i++) {
    if (data[i] == 0) {
      res[i] = false;
    }
    else {
      res[i] = true;
    }
  }
  return res;
}
function convertTinyint(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readIntLE(currOffset,1));
    currOffset += nbytes;
  }
  return res;
}
function convertSmallint(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readIntLE(currOffset,2));
    currOffset += nbytes;
  }
  return res;
}
function convertInt(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readInt32LE(currOffset));
    currOffset += nbytes;
  }
  return res;
}
function convertBigint(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(BigInt(data.readInt64LE(currOffset)));
    currOffset += nbytes;
  }
  return res;
}
function convertFloat(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(parseFloat(data.readFloatLE(currOffset).toFixed(7)));
    currOffset += nbytes;
  }
  return res;
}
function convertDouble(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(parseFloat(data.readDoubleLE(currOffset).toFixed(16)));
    currOffset += nbytes;
  }
  return res;
}
function convertBinary(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let dataEntry = data.slice(currOffset, currOffset + nbytes); //one entry in a row under a column;
    res.push(ref.readCString(dataEntry));
    currOffset += nbytes;
  }
  return res;
}
function convertNchar(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  //every 4;
  while (currOffset < data.length) {
    let dataEntry = data.slice(currOffset, currOffset + nbytes); //one entry in a row under a column;
    res.push(dataEntry.toString("utf16le").replace(/\u0000/g, ""));
    currOffset += nbytes;
  }
  return res;
}

// Object with all the relevant converters from pblock data to javascript readable data
let convertFunctions = {
    [FieldTypes.C_BOOL] : convertBool,
    [FieldTypes.C_TINYINT] : convertTinyint,
    [FieldTypes.C_SMALLINT] : convertSmallint,
    [FieldTypes.C_INT] : convertInt,
    [FieldTypes.C_BIGINT] : convertBigint,
    [FieldTypes.C_FLOAT] : convertFloat,
    [FieldTypes.C_DOUBLE] : convertDouble,
    [FieldTypes.C_BINARY] : convertBinary,
    [FieldTypes.C_TIMESTAMP] : convertTimestamp,
    [FieldTypes.C_NCHAR] : convertNchar
}

// Define TaosField structure
var char_arr = ArrayType(ref.types.char);
var TaosField = Struct({
                      'name': char_arr,
                      });
TaosField.fields.name.type.size = 64;
TaosField.defineProperty('bytes', ref.types.short);
TaosField.defineProperty('type', ref.types.char);

/**
 *
 * @param {Object} config - Configuration options for the interface
 * @return {CTaosInterface}
 * @class CTaosInterface
 * @classdesc The CTaosInterface is the interface through which Node.JS communicates data back and forth with TDengine. It is not advised to
 * access this class directly and use it unless you understand what these functions do.
 */
function CTaosInterface (config = null, pass = false) {
  ref.types.char_ptr = ref.refType(ref.types.char);
  ref.types.void_ptr = ref.refType(ref.types.void);
  ref.types.void_ptr2 = ref.refType(ref.types.void_ptr);
  /*Declare a bunch of functions first*/
  /* Note, pointers to TAOS_RES, TAOS, are ref.types.void_ptr. The connection._conn buffer is supplied for pointers to TAOS  */
  this.libtaos = ffi.Library('libtaos', {
    'taos_options': [ ref.types.int, [ ref.types.int , ref.types.void_ptr ] ],
    'taos_init': [ ref.types.void, [ ] ],
    //TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port)
    'taos_connect': [ ref.types.void_ptr, [ ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.int ] ],
    //void taos_close(TAOS *taos)
    'taos_close': [ ref.types.void, [ ref.types.void_ptr ] ],
    //TAOS_RES *taos_use_result(TAOS *taos);
    'taos_use_result': [ ref.types.void_ptr, [ ref.types.void_ptr ] ],
    //int taos_query(TAOS *taos, char *sqlstr)
    'taos_query': [ ref.types.int, [ ref.types.void_ptr, ref.types.char_ptr ] ],
    //int taos_affected_rows(TAOS *taos)
    'taos_affected_rows': [ ref.types.int, [ ref.types.void_ptr] ],
    //int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
    'taos_fetch_block': [ ref.types.int, [ ref.types.void_ptr, ref.types.void_ptr] ],
    //int taos_num_fields(TAOS_RES *res);
    'taos_num_fields': [ ref.types.int, [ ref.types.void_ptr] ],
    //TAOS_ROW taos_fetch_row(TAOS_RES *res)
    //TAOS_ROW is void **, but we set the return type as a reference instead to get the row
    'taos_fetch_row': [ ref.refType(ref.types.void_ptr2), [ ref.types.void_ptr ] ],
    //int taos_result_precision(TAOS_RES *res)
    'taos_result_precision': [ ref.types.int, [ ref.types.void_ptr ] ],
    //void taos_free_result(TAOS_RES *res)
    'taos_free_result': [ ref.types.void, [ ref.types.void_ptr] ],
    //int taos_field_count(TAOS *taos)
    'taos_field_count': [ ref.types.int, [ ref.types.void_ptr ] ],
    //TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)
    'taos_fetch_fields': [ ref.refType(TaosField),  [ ref.types.void_ptr ] ],
    //int taos_errno(TAOS *taos)
    'taos_errno': [ ref.types.int, [ ref.types.void_ptr] ],
    //char *taos_errstr(TAOS *taos)
    'taos_errstr': [ ref.types.char, [ ref.types.void_ptr] ],

    // ASYNC
    // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *, TAOS_RES *, int), void *param)
    'taos_query_a': [ ref.types.void, [ ref.types.void_ptr, ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr ] ],
    // void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
    'taos_fetch_rows_a': [ ref.types.void, [ ref.types.void_ptr, ref.types.void_ptr, ref.types.void_ptr ]]
  });
  if (pass == false) {
    if (config == null) {
      //check this buffer
      this._config = ref.alloc(ref.types.char_ptr, ref.NULL);
    }
    else {
      try {
        this._config = ref.allocCString(config);
      }
      catch(err){
        throw "Attribute Error: config is expected as a str";
      }
    }
    if (config != null) {
      this.libtaos.taos_options(3, this._config);
    }
    this.libtaos.taos_init();
  }
  return this;
}
CTaosInterface.prototype.config = function config() {
    return this._config;
  }
CTaosInterface.prototype.connect = function connect(host=null, user="root", password="taosdata", db=null, port=0) {
  let _host,_user,_password,_db,_port;
  try  {
    _host = host != null ? ref.allocCString(host) : ref.alloc(ref.types.char_ptr, ref.NULL);
  }
  catch(err) {
    throw "Attribute Error: host is expected as a str";
  }
  try {
    _user = ref.allocCString(user)
  }
  catch(err) {
    throw "Attribute Error: user is expected as a str";
  }
  try {
    _password = ref.allocCString(password);
  }
  catch(err) {
    throw "Attribute Error: password is expected as a str";
  }
  try {
    _db = db != null ? ref.allocCString(db) : ref.alloc(ref.types.char_ptr, ref.NULL);
  }
  catch(err) {
    throw "Attribute Error: db is expected as a str";
  }
  try {
    _port = ref.alloc(ref.types.int, port);
  }
  catch(err) {
    throw TypeError("port is expected as an int")
  }
  let connection = this.libtaos.taos_connect(_host, _user, _password, _db, _port);
  if (ref.isNull(connection)) {
    throw new errors.TDError('Failed to connect to TDengine');
  }
  else {
    console.log('Successfully connected to TDengine');
  }
  return connection;
}
CTaosInterface.prototype.close = function close(connection) {
  this.libtaos.taos_close(connection);
  console.log("Connection is closed");
}
CTaosInterface.prototype.query = function query(connection, sql) {
    return this.libtaos.taos_query(connection, ref.allocCString(sql));
}
CTaosInterface.prototype.affectedRows = function affectedRows(connection) {
  return this.libtaos.taos_affected_rows(connection);
}
CTaosInterface.prototype.useResult = function useResult(connection) {
  let result = this.libtaos.taos_use_result(connection);
  let fields = [];
  let pfields = this.fetchFields(result);
  if (ref.isNull(pfields) == false) {
    pfields = ref.reinterpret(pfields, this.fieldsCount(connection) * 68, 0);
    for (let i = 0; i < pfields.length; i += 68) {
      //0 - 63 = name //64 - 65 = bytes, 66 - 67 = type
      fields.push( {
        name: ref.readCString(ref.reinterpret(pfields,64,i)),
        bytes: pfields[i + 64],
        type: pfields[i + 66]
      })
    }
  }
  return {result:result, fields:fields}
}
CTaosInterface.prototype.fetchBlock = function fetchBlock(result, fields) {
  let pblock = ref.ref(ref.ref(ref.NULL)); // equal to our raw data
  let num_of_rows = this.libtaos.taos_fetch_block(result, pblock)
  if (num_of_rows == 0) {
    return {block:null, num_of_rows:0};
  }
  let isMicro = (this.libtaos.taos_result_precision(result) == FieldTypes.C_TIMESTAMP_MICRO)
  let blocks = new Array(fields.length);
  blocks.fill(null);
  num_of_rows = Math.abs(num_of_rows);
  let offset = 0;
  pblock = pblock.deref()
  for (let i = 0; i < fields.length; i++) {

    if (!convertFunctions[fields[i]['type']] ) {
      throw new errors.DatabaseError("Invalid data type returned from database");
    }
    blocks[i] = convertFunctions[fields[i]['type']](pblock, num_of_rows, fields[i]['bytes'], offset, isMicro);
    offset += fields[i]['bytes'] * num_of_rows;
  }
  return {blocks: blocks, num_of_rows:Math.abs(num_of_rows)}
}
CTaosInterface.prototype.fetchRow = function fetchRow(result, fields) {
  let row = this.libtaos.taos_fetch_row(result);
  return row;
}
CTaosInterface.prototype.freeResult = function freeResult(result) {
  this.libtaos.taos_free_result(result);
  result = null;
}
/** Number of fields returned in this result handle, must use with async */
CTaosInterface.prototype.numFields = function numFields(result) {
  return this.libtaos.taos_num_fields(result);
}
/** @deprecated */
CTaosInterface.prototype.fieldsCount = function fieldsCount(connection) {
  return this.libtaos.taos_field_count(connection);
}
CTaosInterface.prototype.fetchFields = function fetchFields(result) {
  return this.libtaos.taos_fetch_fields(result);
}
CTaosInterface.prototype.errno = function errno(connection) {
  return this.libtaos.taos_errno(connection);
}
CTaosInterface.prototype.errStr = function errStr(connection) {
  return this.libtaos.taos_errstr(connection);
}
// Async
CTaosInterface.prototype.query_a = function query_a(connection, sql, callback, param = ref.ref(ref.NULL)) {
  // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, int), void *param)
  callback = ffi.Callback(ref.types.void, [ ref.types.void_ptr, ref.types.void_ptr, ref.types.int ], callback);
  this.libtaos.taos_query_a(connection, ref.allocCString(sql), callback, param);
  return param;
}
/** Asynchrnously fetches the next block of rows. Wraps callback and transfers a 4th argument to the cursor, the row data as blocks in javascript form
 * Note: This isn't a recursive function, in order to fetch all data either use the TDengine cursor object, TaosQuery object, or implement a recrusive
 * function yourself using the libtaos.taos_fetch_rows_a function
 */
CTaosInterface.prototype.fetch_rows_a = function fetch_rows_a(result, callback, param = ref.ref(ref.NULL)) {
  // void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
  var cti = this;
  // wrap callback with a function so interface can access the numOfRows value, needed in order to properly process the binary data
  let asyncCallbackWrapper = function (param2, result2, numOfRows2) {
    // Data preparation to pass to cursor. Could be bottleneck in query execution callback times.
    let row = cti.libtaos.taos_fetch_row(result2);
    let fields = cti.fetchFields_a(result2);
    let isMicro = (cti.libtaos.taos_result_precision(result) == FieldTypes.C_TIMESTAMP_MICRO);
    let blocks = new Array(fields.length);
    blocks.fill(null);
    numOfRows2 = Math.abs(numOfRows2);
    let offset = 0;
    if (numOfRows2 > 0){
      for (let i = 0; i < fields.length; i++) {
        if (!convertFunctions[fields[i]['type']] ) {
          throw new errors.DatabaseError("Invalid data type returned from database");
        }
        blocks[i] = convertFunctions[fields[i]['type']](row, numOfRows2, fields[i]['bytes'], offset, isMicro);
        offset += fields[i]['bytes'] * numOfRows2;
      }
    }
    callback(param2, result2, numOfRows2, blocks);
  }
  asyncCallbackWrapper = ffi.Callback(ref.types.void, [ ref.types.void_ptr, ref.types.void_ptr, ref.types.int], asyncCallbackWrapper);
  this.libtaos.taos_fetch_rows_a(result, asyncCallbackWrapper, param);
  return param;
}
// Fetch field meta data by result handle
CTaosInterface.prototype.fetchFields_a = function fetchFields_a (result) {
  //
  let pfields = this.fetchFields(result);
  let pfieldscount = this.numFields(result);
  let fields = [];
  if (ref.isNull(pfields) == false) {
    pfields = ref.reinterpret(pfields, 68 * pfieldscount , 0);
    for (let i = 0; i < pfields.length; i += 68) {
      //0 - 63 = name //64 - 65 = bytes, 66 - 67 = type
      fields.push( {
        name: ref.readCString(ref.reinterpret(pfields,64,i)),
        bytes: pfields[i + 64],
        type: pfields[i + 66]
      })
    }
  }
  return fields;
}
