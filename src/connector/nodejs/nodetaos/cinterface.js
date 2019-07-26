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
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
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
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
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
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readIntLE(currOffset,1));
    currOffset += nbytes;
  }
  return res;
}
function convertSmallint(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readIntLE(currOffset,2));
    currOffset += nbytes;
  }
  return res;
}
function convertInt(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(data.readInt32LE(currOffset));
    currOffset += nbytes;
  }
  return res;
}
function readBigInt64LE(buffer, offset = 0) {
  const first = buffer[offset];
  const last = buffer[offset + 7];
  if (first === undefined || last === undefined)
    boundsError(offset, buffer.length - 8);

  const val = buffer[offset + 4] + buffer[offset + 5] * 2 ** 8 + buffer[offset + 6] * 2 ** 16 + (last << 24); // Overflow
  return ((BigInt(val) << 32n) + BigInt(first + buffer[++offset] * 2 ** 8 + buffer[++offset] * 2 ** 16 + buffer[++offset] * 2 ** 24));
}
function convertBigint(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(BigInt(data.readInt64LE(currOffset)));
    currOffset += nbytes;
  }
  return res;
}
function convertFloat(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(parseFloat(data.readFloatLE(currOffset).toFixed(7)));
    currOffset += nbytes;
  }
  return res;
}
function convertDouble(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
  let res = [];
  let currOffset = 0;
  while (currOffset < data.length) {
    res.push(parseFloat(data.readDoubleLE(currOffset).toFixed(16)));
    currOffset += nbytes;
  }
  return res;
}
function convertBinary(data, num_of_rows, nbytes = 0, offset = 0, micro=false) {
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
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
  data = ref.reinterpret(data.deref().deref(), nbytes * num_of_rows, offset);
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
  /*Declare a bunch of functions first*/
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
    'taos_errstr': [ ref.types.char, [ ref.types.void_ptr] ]
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
    let fullpfields = ref.reinterpret(pfields, this.fieldsCount(connection) * 68, 0);
    for (let i = 0; i < fullpfields.length; i += 68) {
      //0 - 63 = name //64 - 65 = bytes, 66 - 67 = type
      fields.push( {
        name: ref.readCString(ref.reinterpret(fullpfields,64,i)),
        bytes: fullpfields[i + 64],
        type: fullpfields[i + 66]
      })
    }
  }
  return {result:result, fields:fields}
}
CTaosInterface.prototype.fetchBlock = function fetchBlock(result, fields) {
  let pblock = ref.ref(ref.ref(ref.NULL));
  let num_of_rows = this.libtaos.taos_fetch_block(result, pblock)
  if (num_of_rows == 0) {
    return {block:null, num_of_rows:0};
  }
  let isMicro = (this.libtaos.taos_result_precision(result) == FieldTypes.C_TIMESTAMP_MICRO)
  let blocks = new Array(fields.length);
  blocks.fill(null);
  num_of_rows = Math.abs(num_of_rows);
  let offset = 0;
  for (let i = 0; i < fields.length; i++) {

    if (!convertFunctions[fields[i]['type']] ) {
      throw new errors.DatabaseError("Invalid data type returned from database");
    }
    let data = ref.reinterpret(pblock.deref().deref(), fields[i]['bytes'], offset);
    blocks[i] = convertFunctions[fields[i]['type']](pblock, num_of_rows, fields[i]['bytes'], offset, isMicro);
    offset += fields[i]['bytes'] * num_of_rows;
  }
  return {blocks: blocks, num_of_rows:Math.abs(num_of_rows)}
}
CTaosInterface.prototype.freeResult = function freeResult(result) {
  this.libtaos.taos_free_result(result);
  result = null;
}
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
  return (this.libtaos.taos_errstr(connection));
}
