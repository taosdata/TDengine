/**
 * C Interface with TDengine Module
 * @module CTaosInterface
 */

const ref = require('ref-napi');
const os = require('os');
const ffi = require('ffi-napi');
const ArrayType = require('ref-array-di')(ref);
const Struct = require('ref-struct-di')(ref);
const FieldTypes = require('./constants');
const errors = require('./error');
const _ = require('lodash')
const TaosObjects = require('./taosobjects');

module.exports = CTaosInterface;
const TAOSFIELD = {
  NAME_LENGTH: 65,
  TYPE_OFFSET: 65,
  BYTES_OFFSET: 66,
  STRUCT_SIZE: 68,
}

function convertTimestamp(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let time = data.readInt64LE(currOffset);
    currOffset += nbytes;
    res.push(new TaosObjects.TaosTimestamp(time, precision));
  }
  return res;
}

function convertBool(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = new Array(data.length);
  for (let i = 0; i < data.length; i++) {
    if (data[i] == 0) {
      res[i] = false;
    }
    else if (data[i] == 1) {
      res[i] = true;
    }
    else if (data[i] == FieldTypes.C_BOOL_NULL) {
      res[i] = null;
    }
  }
  return res;
}

function convertTinyint(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readIntLE(currOffset, 1);
    res.push(d == FieldTypes.C_TINYINT_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertTinyintUnsigned(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readUIntLE(currOffset, 1);
    res.push(d == FieldTypes.C_TINYINT_UNSIGNED_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertSmallint(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readIntLE(currOffset, 2);
    res.push(d == FieldTypes.C_SMALLINT_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertSmallintUnsigned(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readUIntLE(currOffset, 2);
    res.push(d == FieldTypes.C_SMALLINT_UNSIGNED_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertInt(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readInt32LE(currOffset);
    res.push(d == FieldTypes.C_INT_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertIntUnsigned(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readUInt32LE(currOffset);
    res.push(d == FieldTypes.C_INT_UNSIGNED_NULL ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertBigint(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readInt64LE(currOffset);
    res.push(d == FieldTypes.C_BIGINT_NULL ? null : BigInt(d));
    currOffset += nbytes;
  }
  return res;
}

function convertBigintUnsigned(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = data.readUInt64LE(currOffset);
    res.push(d == FieldTypes.C_BIGINT_UNSIGNED_NULL ? null : BigInt(d));
    currOffset += nbytes;
  }
  return res;
}

function convertFloat(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = parseFloat(data.readFloatLE(currOffset).toFixed(5));
    res.push(isNaN(d) ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertDouble(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    let d = parseFloat(data.readDoubleLE(currOffset).toFixed(16));
    res.push(isNaN(d) ? null : d);
    currOffset += nbytes;
  }
  return res;
}

function convertBinary(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];

  let currOffset = 0;
  while (currOffset < data.length) {
    let len = data.readIntLE(currOffset, 2);
    let dataEntry = data.slice(currOffset + 2, currOffset + len + 2); //one entry in a row under a column;
    if (dataEntry[0] == 255) {
      res.push(null)
    } else {
      res.push(dataEntry.toString("utf-8"));
    }
    currOffset += nbytes;
  }
  return res;
}

function convertNchar(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];

  let currOffset = 0;
  while (currOffset < data.length) {
    let len = data.readIntLE(currOffset, 2);
    let dataEntry = data.slice(currOffset + 2, currOffset + len + 2); //one entry in a row under a column;
    if (dataEntry[0] == 255 && dataEntry[1] == 255) {
      res.push(null)
    } else {
      res.push(dataEntry.toString("utf-8"));
    }
    currOffset += nbytes;
  }
  return res;
}

function convertJsonTag(data, num_of_rows, nbytes = 0, offset = 0, precision = 0) {
  data = ref.reinterpret(data.deref(), nbytes * num_of_rows, offset);
  let res = [];

  let currOffset = 0;
  while (currOffset < data.length) {
    let len = data.readIntLE(currOffset, 2);
    let dataEntry = data.slice(currOffset + 2, currOffset + len + 2); //one entry in a row under a column;
    if (dataEntry[0] == 255 && dataEntry[1] == 255) {
      res.push(null)
    } else {
      res.push(dataEntry.toString("utf-8"));
    }
    currOffset += nbytes;
  }
  return res;
}

// Object with all the relevant converters from pblock data to javascript readable data
let convertFunctions = {
  [FieldTypes.C_BOOL]: convertBool,
  [FieldTypes.C_TINYINT]: convertTinyint,
  [FieldTypes.C_SMALLINT]: convertSmallint,
  [FieldTypes.C_INT]: convertInt,
  [FieldTypes.C_BIGINT]: convertBigint,
  [FieldTypes.C_FLOAT]: convertFloat,
  [FieldTypes.C_DOUBLE]: convertDouble,
  [FieldTypes.C_BINARY]: convertBinary,
  [FieldTypes.C_TIMESTAMP]: convertTimestamp,
  [FieldTypes.C_NCHAR]: convertNchar,
  [FieldTypes.C_TINYINT_UNSIGNED]: convertTinyintUnsigned,
  [FieldTypes.C_SMALLINT_UNSIGNED]: convertSmallintUnsigned,
  [FieldTypes.C_INT_UNSIGNED]: convertIntUnsigned,
  [FieldTypes.C_BIGINT_UNSIGNED]: convertBigintUnsigned,
  [FieldTypes.C_JSON_TAG]: convertJsonTag,
}

// Define TaosField structure
var char_arr = ArrayType(ref.types.char);
var TaosField = Struct({
  'name': char_arr,
});
TaosField.fields.name.type.size = 65;
TaosField.defineProperty('type', ref.types.uint8);
TaosField.defineProperty('bytes', ref.types.int16);

//define schemaless line array
var smlLine = ArrayType(ref.coerceType('char *'))

/**
 *
 * @param {Object} config - Configuration options for the interface
 * @return {CTaosInterface}
 * @class CTaosInterface
 * @classdesc The CTaosInterface is the interface through which Node.JS communicates data back and forth with TDengine. It is not advised to
 * access this class directly and use it unless you understand what these functions do.
 */
function CTaosInterface(config = null, pass = false) {
  ref.types.char_ptr = ref.refType(ref.types.char);
  ref.types.void_ptr = ref.refType(ref.types.void);
  ref.types.void_ptr2 = ref.refType(ref.types.void_ptr);
  /*Declare a bunch of functions first*/
  /* Note, pointers to TAOS_RES, TAOS, are ref.types.void_ptr. The connection._conn buffer is supplied for pointers to TAOS *  */
  if ('win32' == os.platform()) {
    taoslibname = 'taos';
  } else {
    taoslibname = 'libtaos';
  }
  this.libtaos = ffi.Library(taoslibname, {
    'taos_options': [ref.types.int, [ref.types.int, ref.types.void_ptr]],
    'taos_init': [ref.types.void, []],
    //TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port)
    'taos_connect': [ref.types.void_ptr, [ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.int]],
    //void taos_close(TAOS *taos)
    'taos_close': [ref.types.void, [ref.types.void_ptr]],
    //int *taos_fetch_lengths(TAOS_RES *res);
    'taos_fetch_lengths': [ref.types.void_ptr, [ref.types.void_ptr]],
    //int taos_query(TAOS *taos, char *sqlstr)
    'taos_query': [ref.types.void_ptr, [ref.types.void_ptr, ref.types.char_ptr]],
    //int taos_affected_rows(TAOS_RES *res)
    'taos_affected_rows': [ref.types.int, [ref.types.void_ptr]],
    //int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
    'taos_fetch_block': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr]],
    //int taos_num_fields(TAOS_RES *res);
    'taos_num_fields': [ref.types.int, [ref.types.void_ptr]],
    //TAOS_ROW taos_fetch_row(TAOS_RES *res)
    //TAOS_ROW is void **, but we set the return type as a reference instead to get the row
    'taos_fetch_row': [ref.refType(ref.types.void_ptr2), [ref.types.void_ptr]],
    'taos_print_row': [ref.types.int, [ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr, ref.types.int]],
    //int taos_result_precision(TAOS_RES *res)
    'taos_result_precision': [ref.types.int, [ref.types.void_ptr]],
    //void taos_free_result(TAOS_RES *res)
    'taos_free_result': [ref.types.void, [ref.types.void_ptr]],
    //int taos_field_count(TAOS *taos)
    'taos_field_count': [ref.types.int, [ref.types.void_ptr]],
    //TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)
    'taos_fetch_fields': [ref.refType(TaosField), [ref.types.void_ptr]],
    //int taos_errno(TAOS *taos)
    'taos_errno': [ref.types.int, [ref.types.void_ptr]],
    //char *taos_errstr(TAOS *taos)
    'taos_errstr': [ref.types.char_ptr, [ref.types.void_ptr]],
    //void taos_stop_query(TAOS_RES *res);
    'taos_stop_query': [ref.types.void, [ref.types.void_ptr]],
    //char *taos_get_server_info(TAOS *taos);
    'taos_get_server_info': [ref.types.char_ptr, [ref.types.void_ptr]],
    //char *taos_get_client_info();
    'taos_get_client_info': [ref.types.char_ptr, []],

    // ASYNC
    // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *, TAOS_RES *, int), void *param)
    'taos_query_a': [ref.types.void, [ref.types.void_ptr, ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr]],
    // void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
    'taos_fetch_rows_a': [ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.void_ptr]],

    // Subscription
    //TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval)
    'taos_subscribe': [ref.types.void_ptr, [ref.types.void_ptr, ref.types.int, ref.types.char_ptr, ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr, ref.types.int]],
    // TAOS_RES *taos_consume(TAOS_SUB *tsub)
    'taos_consume': [ref.types.void_ptr, [ref.types.void_ptr]],
    //void taos_unsubscribe(TAOS_SUB *tsub);
    'taos_unsubscribe': [ref.types.void, [ref.types.void_ptr]],

    //Schemaless insert
    //TAOS_RES* taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol，int precision)
    // 'taos_schemaless_insert': [ref.types.void_ptr, [ref.types.void_ptr, ref.types.char_ptr, ref.types.int, ref.types.int, ref.types.int]]
    'taos_schemaless_insert': [ref.types.void_ptr, [ref.types.void_ptr, smlLine, 'int', 'int', 'int']]

    //stmt APIs
    // TAOS_STMT* taos_stmt_init(TAOS *taos)
    , 'taos_stmt_init': [ref.types.void_ptr, [ref.types.void_ptr]]

    // int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)
    , 'taos_stmt_prepare': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr, ref.types.ulong]]

    // int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name)
    , 'taos_stmt_set_tbname': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    // int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND* tags)
    , 'taos_stmt_set_tbname_tags': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr, ref.types.void_ptr]]

    // int taos_stmt_set_sub_tbname(TAOS_STMT* stmt, const char* name)
    , 'taos_stmt_set_sub_tbname': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    // int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind)
    // , 'taos_stmt_bind_param': [ref.types.int, [ref.types.void_ptr, ref.refType(TAOS_BIND)]]
    , 'taos_stmt_bind_param': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr]]


    // int taos_stmt_bind_single_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind, int colIdx)  
    , 'taos_stmt_bind_single_param_batch': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int]]
    // int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind) 
    , 'taos_stmt_bind_param_batch': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr]]

    // int taos_stmt_add_batch(TAOS_STMT *stmt)
    , 'taos_stmt_add_batch': [ref.types.int, [ref.types.void_ptr]]

    // int taos_stmt_execute(TAOS_STMT *stmt) 
    , 'taos_stmt_execute': [ref.types.int, [ref.types.void_ptr]]

    // TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)  
    , 'taos_stmt_use_result': [ref.types.void_ptr, [ref.types.void_ptr]]

    // int taos_stmt_close(TAOS_STMT *stmt) 
    , 'taos_stmt_close': [ref.types.int, [ref.types.void_ptr]]

    // char * taos_stmt_errstr(TAOS_STMT *stmt)
    , 'taos_stmt_errstr': [ref.types.char_ptr, [ref.types.void_ptr]]

    // int taos_load_table_info(TAOS *taos, const char* tableNameList)
    , 'taos_load_table_info': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]
  });

  if (pass == false) {
    if (config == null) {
      this._config = ref.alloc(ref.types.char_ptr, ref.NULL);
    }
    else {
      try {
        this._config = ref.allocCString(config);
      }
      catch (err) {
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

CTaosInterface.prototype.connect = function connect(host = null, user = "root", password = "taosdata", db = null, port = 0) {
  let _host, _user, _password, _db, _port;
  try {
    _host = host != null ? ref.allocCString(host) : ref.NULL;
  }
  catch (err) {
    throw "Attribute Error: host is expected as a str";
  }
  try {
    _user = ref.allocCString(user)
  }
  catch (err) {
    throw "Attribute Error: user is expected as a str";
  }
  try {
    _password = ref.allocCString(password);
  }
  catch (err) {
    throw "Attribute Error: password is expected as a str";
  }
  try {
    _db = db != null ? ref.allocCString(db) : ref.NULL;
  }
  catch (err) {
    throw "Attribute Error: db is expected as a str";
  }
  try {
    _port = ref.alloc(ref.types.int, port);
  }
  catch (err) {
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

CTaosInterface.prototype.affectedRows = function affectedRows(result) {
  return this.libtaos.taos_affected_rows(result);
}

CTaosInterface.prototype.useResult = function useResult(result) {

  let fields = [];
  let pfields = this.fetchFields(result);

  if (ref.isNull(pfields) == false) {
    pfields = ref.reinterpret(pfields, this.fieldsCount(result) * TAOSFIELD.STRUCT_SIZE, 0);
    for (let i = 0; i < pfields.length; i += TAOSFIELD.STRUCT_SIZE) {
      fields.push({
        name: ref.readCString(ref.reinterpret(pfields, TAOSFIELD.NAME_LENGTH, i)),
        type: pfields[i + TAOSFIELD.TYPE_OFFSET],
        bytes: pfields[i + TAOSFIELD.BYTES_OFFSET] + pfields[i + TAOSFIELD.BYTES_OFFSET + 1] * 256
      })
    }
  }
  return fields;
}

CTaosInterface.prototype.fetchBlock = function fetchBlock(result, fields) {
  let pblock = ref.NULL_POINTER;
  let num_of_rows = this.libtaos.taos_fetch_block(result, pblock);
  if (ref.isNull(pblock.deref()) == true) {
    return { block: null, num_of_rows: 0 };
  }

  var fieldL = this.libtaos.taos_fetch_lengths(result);
  let precision = this.libtaos.taos_result_precision(result);

  var fieldlens = [];

  if (ref.isNull(fieldL) == false) {
    for (let i = 0; i < fields.length; i++) {
      let plen = ref.reinterpret(fieldL, 4, i * 4);
      let len = plen.readInt32LE(0);
      fieldlens.push(len);
    }
  }

  let blocks = new Array(fields.length);
  blocks.fill(null);
  num_of_rows = Math.abs(num_of_rows);
  let offset = 0;
  let ptr = pblock.deref();

  for (let i = 0; i < fields.length; i++) {
    pdata = ref.reinterpret(ptr, 8, i * 8);
    if (ref.isNull(pdata.readPointer())) {
      blocks[i] = new Array();
    } else {
      pdata = ref.ref(pdata.readPointer());
      if (!convertFunctions[fields[i]['type']]) {
        throw new errors.DatabaseError("Invalid data type returned from database");
      }
      blocks[i] = convertFunctions[fields[i]['type']](pdata, num_of_rows, fieldlens[i], offset, precision);
    }
  }
  return { blocks: blocks, num_of_rows }
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

// Fetch fields count by connection, the latest query
CTaosInterface.prototype.fieldsCount = function fieldsCount(result) {
  return this.libtaos.taos_field_count(result);
}

CTaosInterface.prototype.fetchFields = function fetchFields(result) {
  return this.libtaos.taos_fetch_fields(result);
}

CTaosInterface.prototype.errno = function errno(result) {
  return this.libtaos.taos_errno(result);
}

CTaosInterface.prototype.errStr = function errStr(result) {
  return ref.readCString(this.libtaos.taos_errstr(result));
}

// Async
CTaosInterface.prototype.query_a = function query_a(connection, sql, callback, param = ref.ref(ref.NULL)) {
  // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, int), void *param)
  callback = ffi.Callback(ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int], callback);
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

    let precision = cti.libtaos.taos_result_precision(result2);
    let blocks = new Array(fields.length);
    blocks.fill(null);
    numOfRows2 = Math.abs(numOfRows2);
    let offset = 0;
    var fieldL = cti.libtaos.taos_fetch_lengths(result);
    var fieldlens = [];
    if (ref.isNull(fieldL) == false) {

      for (let i = 0; i < fields.length; i++) {
        let plen = ref.reinterpret(fieldL, 8, i * 8);
        let len = ref.get(plen, 0, ref.types.int32);
        fieldlens.push(len);
      }
    }
    if (numOfRows2 > 0) {
      for (let i = 0; i < fields.length; i++) {
        if (ref.isNull(pdata.readPointer())) {
          blocks[i] = new Array();
        } else {
          if (!convertFunctions[fields[i]['type']]) {
            throw new errors.DatabaseError("Invalid data type returned from database");
          }
          let prow = ref.reinterpret(row, 8, i * 8);
          prow = prow.readPointer();
          prow = ref.ref(prow);
          blocks[i] = convertFunctions[fields[i]['type']](prow, 1, fieldlens[i], offset, precision);
          //offset += fields[i]['bytes'] * numOfRows2;
        }
      }
    }
    callback(param2, result2, numOfRows2, blocks);
  }
  asyncCallbackWrapper = ffi.Callback(ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int], asyncCallbackWrapper);
  this.libtaos.taos_fetch_rows_a(result, asyncCallbackWrapper, param);
  return param;
}

// Fetch field meta data by result handle
CTaosInterface.prototype.fetchFields_a = function fetchFields_a(result) {
  let pfields = this.fetchFields(result);
  let pfieldscount = this.numFields(result);
  let fields = [];
  if (ref.isNull(pfields) == false) {
    pfields = ref.reinterpret(pfields, pfieldscount * TAOSFIELD.STRUCT_SIZE, 0);
    for (let i = 0; i < pfields.length; i += TAOSFIELD.STRUCT_SIZE) {
      fields.push({
        name: ref.readCString(ref.reinterpret(pfields, TAOSFIELD.NAME_LENGTH, i)),
        type: pfields[i + TAOSFIELD.TYPE_OFFSET],
        bytes: pfields[i + TAOSFIELD.BYTES_OFFSET] + pfields[i + TAOSFIELD.BYTES_OFFSET + 1] * 256
      })
    }
  }
  return fields;
}

// Stop a query by result handle
CTaosInterface.prototype.stopQuery = function stopQuery(result) {
  if (result != null) {
    this.libtaos.taos_stop_query(result);
  }
  else {
    throw new errors.ProgrammingError("No result handle passed to stop query");
  }
}

CTaosInterface.prototype.getServerInfo = function getServerInfo(connection) {
  return ref.readCString(this.libtaos.taos_get_server_info(connection));
}

CTaosInterface.prototype.getClientInfo = function getClientInfo() {
  return ref.readCString(this.libtaos.taos_get_client_info());
}

// Subscription
CTaosInterface.prototype.subscribe = function subscribe(connection, restart, topic, sql, interval) {
  let topicOrig = topic;
  let sqlOrig = sql;
  try {
    sql = sql != null ? ref.allocCString(sql) : ref.alloc(ref.types.char_ptr, ref.NULL);
  }
  catch (err) {
    throw "Attribute Error: sql is expected as a str";
  }
  try {
    topic = topic != null ? ref.allocCString(topic) : ref.alloc(ref.types.char_ptr, ref.NULL);
  }
  catch (err) {
    throw TypeError("topic is expected as a str");
  }

  restart = ref.alloc(ref.types.int, restart);

  let subscription = this.libtaos.taos_subscribe(connection, restart, topic, sql, null, null, interval);
  if (ref.isNull(subscription)) {
    throw new errors.TDError('Failed to subscribe to TDengine | Database: ' + dbOrig + ', Table: ' + tableOrig);
  }
  else {
    console.log('Successfully subscribed to TDengine - Topic: ' + topicOrig);
  }
  return subscription;
}

CTaosInterface.prototype.consume = function consume(subscription) {
  let result = this.libtaos.taos_consume(subscription);
  let fields = [];
  let pfields = this.fetchFields(result);
  if (ref.isNull(pfields) == false) {
    pfields = ref.reinterpret(pfields, this.numFields(result) * TAOSFIELD.STRUCT_SIZE, 0);
    for (let i = 0; i < pfields.length; i += TAOSFIELD.STRUCT_SIZE) {
      fields.push({
        name: ref.readCString(ref.reinterpret(pfields, TAOSFIELD.NAME_LENGTH, i)),
        bytes: pfields[TAOSFIELD.TYPE_OFFSET],
        type: pfields[i + TAOSFIELD.BYTES_OFFSET] + pfields[i + TAOSFIELD.BYTES_OFFSET + 1] * 256
      })
    }
  }

  let data = [];
  while (true) {
    let { blocks, num_of_rows } = this.fetchBlock(result, fields);
    if (num_of_rows == 0) {
      break;
    }
    for (let i = 0; i < num_of_rows; i++) {
      data.push([]);
      let rowBlock = new Array(fields.length);
      for (let j = 0; j < fields.length; j++) {
        rowBlock[j] = blocks[j][i];
      }
      data[data.length - 1] = (rowBlock);
    }
  }
  return { data: data, fields: fields, result: result };
}

CTaosInterface.prototype.unsubscribe = function unsubscribe(subscription) {
  //void taos_unsubscribe(TAOS_SUB *tsub);
  this.libtaos.taos_unsubscribe(subscription);
}

//Schemaless insert API
/**
 * TAOS* taos, char* lines[], int numLines, int protocol，int precision)
 * using  taos_errstr get error info, taos_errno get error code. Remember
 * to release taos_res, otherwise will lead memory leak.
 * TAOS schemaless insert api
 * @param {*} connection a valid database connection
 * @param {*} lines string data, which satisfied with line protocol
 * @param {*} numLines number of rows in param lines.
 * @param {*} protocol Line protocol, enum type (0,1,2,3),indicate different line protocol
 * @param {*} precision timestamp precision in lines, enum type (0,1,2,3,4,5,6)
 * @returns TAOS_RES
 *
 */
CTaosInterface.prototype.schemalessInsert = function schemalessInsert(connection, lines, protocal, precision) {
  let _numLines = null;
  let _lines = null;

  if (_.isString(lines)) {
    _numLines = 1;
    _lines = Buffer.alloc(_numLines * ref.sizeof.pointer);
    ref.set(_lines, 0, ref.allocCString(lines), ref.types.char_ptr);
  }
  else if (_.isArray(lines)) {
    _numLines = lines.length;
    _lines = Buffer.alloc(_numLines * ref.sizeof.pointer);
    for (let i = 0; i < _numLines; i++) {
      ref.set(_lines, i * ref.sizeof.pointer, ref.allocCString(lines[i]), ref.types.char_ptr)
    }
  }
  else {
    throw new errors.InterfaceError("Unsupport lines input")
  }
  return this.libtaos.taos_schemaless_insert(connection, _lines, _numLines, protocal, precision);
}

//stmt APIs

/**
 * init a TAOS_STMT object for later use.it should be freed with stmtClose.
 * @param {*} connection valid taos connection 
 * @returns  Not NULL returned for success, and NULL for failure. 
 * 
 */
CTaosInterface.prototype.stmtInit = function stmtInit(connection) {
  return this.libtaos.taos_stmt_init(connection)
}

/**
 * prepare a sql statement,'sql' should be a valid INSERT/SELECT statement, 'length' is not used.
 * @param {*} stmt 
 * @param {string} sql  a valid INSERT/SELECT statement
 * @param {ulong} length not used
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtPrepare = function stmtPrepare(stmt, sql, length) {
  return this.libtaos.taos_stmt_prepare(stmt, ref.allocCString(sql), 0);
}

/**
 * For INSERT only. Used to bind table name as a parmeter for the input stmt object.
 * @param {*} stmt could be the value returned by 'stmtInit', 
 *            that may be a valid object or NULL.
 * @param {TaosBind} tableName target table name you want to  bind
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtSetTbname = function stmtSetTbname(stmt, tableName) {
  return this.libtaos.taos_stmt_set_tbname(stmt, ref.allocCString(tableName));
}

/**
 * For INSERT only. 
 * Set a table name for binding table name as parameter and tag values for all  tag parameters. 
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @param {*} tableName use to set target table name
 * @param {TaosMultiBind} tags use to set tag value for target table. 
 * @returns 
 */
CTaosInterface.prototype.stmtSetTbnameTags = function stmtSetTbnameTags(stmt, tableName, tags) {
  return this.libtaos.taos_stmt_set_tbname_tags(stmt, ref.allocCString(tableName), tags);
}

/**
 * For INSERT only. 
 * Set a table name for binding table name as parameter. Only used for binding all tables
 * in one stable, user application must call 'loadTableInfo' API to load all table
 * meta before calling this API. If the table meta is not cached locally, it will return error.
 * @param {*} stmt could be the value returned by 'StmtInit', that may be a valid object or NULL.
 * @param {*} subTableName table name which is belong to an stable
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtSetSubTbname = function stmtSetSubTbname(stmt, subTableName) {
  return this.libtaos.taos_stmt_set_sub_tbname(stmt, subTableName);
}

/**
 * bind a whole line data, for both INSERT and SELECT. The parameter 'bind' points to an array 
 * contains the whole line data. Each item in array represents a column's value, the item 
 * number and sequence should keep consistence with columns in sql statement. The usage of 
 * structure TAOS_BIND is the same with MYSQL_BIND in MySQL.
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @param {*} binds points to an array contains the whole line data.
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.bindParam = function bindParam(stmt, binds) {
  return this.libtaos.taos_stmt_bind_param(stmt, binds);
}

/**
 * Bind a single column's data, INTERNAL used and for INSERT only. 
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @param {TaosMultiBind} mbind points to a column's data which could be the one or more lines. 
 * @param {*} colIndex the column's index in prepared sql statement, it starts from 0.
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtBindSingleParamBatch = function stmtBindSingleParamBatch(stmt, mbind, colIndex) {
  return this.libtaos.taos_stmt_bind_single_param_batch(stmt, mbind.ref(), colIndex);
}

/**
 * For INSERT only.
 * Bind one or multiple lines data.
 * @param {*} stmt could be the value returned by 'stmtInit',
 *            that may be a valid object or NULL.
 * @param {*} mbinds Points to an array contains one or more lines data.The item 
 *            number and sequence should keep consistence with columns
 *            n sql statement. 
 * @returns  0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtBindParamBatch = function stmtBindParamBatch(stmt, mbinds) {
  return this.libtaos.taos_stmt_bind_param_batch(stmt, mbinds);
}
/**
 * add all current bound parameters to batch process, for INSERT only.
 * Must be called after each call to bindParam/bindSingleParamBatch, 
 * or all columns binds for one or more lines with bindSingleParamBatch. User 
 * application can call any bind parameter API again to bind more data lines after calling
 * to this API.
 * @param {*} stmt 
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.addBatch = function addBatch(stmt) {
  return this.libtaos.taos_stmt_add_batch(stmt);
}
/**
 * actually execute the INSERT/SELECT sql statement. User application can continue
 * to bind new data after calling to this API.
 * @param {*} stmt 
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtExecute = function stmtExecute(stmt) {
  return this.libtaos.taos_stmt_execute(stmt);
}

/**
 * For SELECT only,getting the query result. 
 * User application should free it with API 'FreeResult' at the end.
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @returns Not NULL for success, NULL for failure.
 */
CTaosInterface.prototype.stmtUseResult = function stmtUseResult(stmt) {
  return this.libtaos.taos_stmt_use_result(stmt);
}

/**
 * user application call this API to load all tables meta info.
 * This method must be called before stmtSetSubTbname(IntPtr stmt, string name);
 * @param {*} taos taos connection
 * @param {*} tableList tables need to load meta info are form in an array
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.loadTableInfo = function loadTableInfo(taos, tableList) {
  let _tableListBuf = Buffer.alloc(ref.sizeof.pointer);
  let _listStr = tableList.toString();

  if ((_.isString(tableList) )|| (_.isArray(tableList))) {
    ref.set(_tableListBuf, 0, ref.allocCString(_listStr), ref.types.char_ptr);
    return this.libtaos.taos_load_table_info(taos, _tableListBuf);
  } else {
    throw new errors.InterfaceError("Unspport tableLis input");
  }
}

/**
 * Close STMT object and free resources.
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.closeStmt = function closeStmt(stmt) {
  return this.libtaos.taos_stmt_close(stmt);
}

/**
 * Get detail error message when got failure for any stmt API call. 
 * If not failure, the result returned by this API is unknown.
 * @param {*} stmt Could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @returns error string
 */
CTaosInterface.prototype.stmtErrStr = function stmtErrStr(stmt) {
  return ref.readCString(this.libtaos.taos_stmt_errstr(stmt));
}