/**
 * C Interface with TDengine Module
 * @module CTaosInterface
 */

const ref = require('ref-napi');
const os = require('os');
const ffi = require('ffi-napi');
const FieldTypes = require('./constants');
const errors = require('./error');
const _ = require('lodash')
const TaosObjects = require('./taosobjects');
const libtaos = require('./taoslib');
const AppendRun = require('./uft8');
const { TaosMultiBind } = require('./taosMultiBind');

module.exports = CTaosInterface;

const TAOSFIELD = {
  NAME_LENGTH: 65,
  TYPE_OFFSET: 65,
  BYTES_OFFSET: 68,
  STRUCT_SIZE: 72,
}

function convertTimestamp(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {

  let res = [];
  let bitMapSize = (numOfRows + ((1 << 3) - 1)) >> 3;
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt64LE();
      res.push(new TaosObjects.TaosTimestamp(data, precision));
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertBool(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt8();
      res.push(data == 1 ? true : false);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertTinyint(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {

  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)
    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt8();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertTinyintUnsigned(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readUInt8();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertSmallint(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt16LE();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertSmallintUnsigned(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;
  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readUInt16LE();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertInt(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readUInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt32LE();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertIntUnsigned(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readUInt32LE();
      res.push(data);
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertBigint(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readInt64LE();
      res.push(BigInt(data));
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertBigintUnsigned(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readUInt64LE();
      res.push(BigInt(data));
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertFloat(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;

  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readFloatLE().toFixed(5);
      res.push(parseFloat(data));
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertDouble(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let bitMapSize = Math.ceil(numOfRows / 8.0);
  let bitMapBuf = ref.reinterpret(colHeadPtr, bitMapSize, 0);

  offset = bitMapSize;
  for (let i = 0; i < numOfRows; i++) {
    let byteIndex = i >> 3
    let bitWiseOffset = 7 - (i & 7)

    let ifNullByte = bitMapBuf.readInt8(byteIndex);
    let ifNullFlag = (ifNullByte & (1 << bitWiseOffset)) >> bitWiseOffset;

    if (ifNullFlag == 1) {
      res.push(null);
    } else {
      let colDataBuf = ref.reinterpret(colHeadPtr, nBytes, offset);
      let data = colDataBuf.readDoubleLE().toFixed(16);
      // console.log("convertDouble buff:",JSON.stringify(colDataBuf))
      // console.log("convertDouble data:",data,i)
      res.push(parseFloat(data));
    }
    offset = offset + nBytes;
  }
  return res;
}

function convertBinary(colHeadPtr, numOfRows, nbytes = 0, offset = 0, precision = 0) {

  let res = [];
  let int16Size = ref.sizeof.int16;
  let intSize = ref.sizeof.int;

  let offsetArrLength = intSize * numOfRows;
  let offsetArrBuff = ref.reinterpret(colHeadPtr, offsetArrLength, 0);

  for (let i = 0; i < numOfRows; i++) {
    let offset = offsetArrBuff.readInt32LE(i * intSize);
    if (offset == -1) {
      res.push(null)
    } else {
      let dataLengthBuff = ref.reinterpret(colHeadPtr, int16Size, offsetArrLength + offset);
      let dataLength = dataLengthBuff.readInt16LE()

      let dataBuff = ref.reinterpret(colHeadPtr, dataLength, offsetArrLength + offset + int16Size);
      let data = dataBuff.toString('utf8')
      res.push(data)
    }
  }
  return res;
}

function convertNchar(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {

  let res = [];
  let int16Size = ref.sizeof.int16;
  let intSize = ref.sizeof.int32;

  let offsetArrLength = intSize * numOfRows;
  let offsetArrBuff = ref.reinterpret(colHeadPtr, offsetArrLength, 0);

  for (let i = 0; i < numOfRows; i++) {
    let offset = offsetArrBuff.readInt32LE(i * intSize);
    if (offset == -1) {
      res.push(null)
    } else {
      let dataLengthBuff = ref.reinterpret(colHeadPtr, int16Size, offsetArrLength + offset);
      let dataLength = dataLengthBuff.readInt16LE()

      let stringLength = dataLength / 4;
      let result = '';

      for (let j = 0; j < stringLength; j++) {
        let dataBuff = ref.reinterpret(colHeadPtr, 4, offsetArrLength + offset + int16Size + j * 4);
        let str = dataBuff.readUInt32LE(0);

        result += AppendRun(str)
      }
      res.push(result);
    }
  }
  return res;
}

function convertJsonTag(colHeadPtr, numOfRows, nBytes = 0, offset = 0, precision = 0) {
  let res = [];
  let int16Size = ref.sizeof.int16;
  let intSize = ref.sizeof.int;

  let offsetArrLength = intSize * numOfRows;
  let offsetArrBuff = ref.reinterpret(colHeadPtr, offsetArrLength, 0);

  for (let i = 0; i < numOfRows; i++) {
    let offset = offsetArrBuff.readInt32LE(i * intSize);
    if (offset == -1) {
      res.push(null)
    } else {
      let dataLengthBuff = ref.reinterpret(colHeadPtr, int16Size, offsetArrLength + offset);
      let dataLength = dataLengthBuff.readInt16LE()

      let dataBuff = ref.reinterpret(colHeadPtr, dataLength, offsetArrLength + offset + int16Size);
      let data = dataBuff.toString('utf8')
      res.push(data)
    }
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

/**
 *
 * @param {Object} config - Configuration options for the interface
 * @return {CTaosInterface}
 * @class CTaosInterface
 * @classdesc The CTaosInterface is the interface through which Node.JS communicates data back and forth with TDengine. It is not advised to
 * access this class directly and use it unless you understand what these functions do.
 */
function CTaosInterface(config = null, pass = false) {

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
      libtaos.taos_options(3, this._config);
    }
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
  let connection = libtaos.taos_connect(_host, _user, _password, _db, _port);
  if (ref.isNull(connection)) {
    throw new errors.TDError('Failed to connect to TDengine');
  }
  else {
    console.log('Successfully connected to TDengine');
  }
  return connection;
}

CTaosInterface.prototype.close = function close(connection) {
  libtaos.taos_close(connection);
  console.log("Connection is closed");
}

CTaosInterface.prototype.query = function query(connection, sql) {
  return libtaos.taos_query(connection, ref.allocCString(sql));
}

CTaosInterface.prototype.affectedRows = function affectedRows(result) {
  return libtaos.taos_affected_rows(result);
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

CTaosInterface.prototype.fetchRawBlock = function fetchRawBlock(taosRes) {
  var numOfRowPtr = ref.alloc('int *');
  var pDataPtr = ref.alloc('void **');

  let code = libtaos.taos_fetch_raw_block(taosRes, numOfRowPtr, pDataPtr);
   
  if (code == 0) {
    let numOfRows = numOfRowPtr.readInt32LE();
    let pData = ref.deref(pDataPtr);

    let numOfFields = libtaos.taos_field_count(taosRes);
    let precision = libtaos.taos_result_precision(taosRes);
    let fields = this.useResult(taosRes);

    let bitMapSize = Math.ceil(numOfRows / 8.0);
    let offsetArrLength = ref.sizeof.int32 * numOfRows;

    let blocks = new Array(numOfFields);
    blocks.fill(null);

    // print block raw
    // let blockLength = ref.reinterpret(pData, ref.sizeof.int,4).readInt32LE();
    // console.log(blockLength)
    // let content_str=''
    // for(let i = 0; i<blockLength;i++){
    //    let byteContent = ref.reinterpret(pData,ref.sizeof.byte,i).readUInt8()
    //    content_str += byteContent +"\t"
    //    if((i+1)%4 == 0){
    //     content_str +="\n"
    //    }
    // }
    // console.log(content_str)


    // offset pData
    let offsetBeforeLengthArr = (4 * 5) + 8 + (4 + 1) * numOfFields;
    var lengthArraySize = 4 * numOfFields;
    let offsetForColData = offsetBeforeLengthArr + lengthArraySize;
    // read column after column
    if (numOfRows > 0) {
      for (let i = 0; i < numOfFields; i++) {

        let lengthPtr = ref.reinterpret(pData, ref.sizeof.int, offsetBeforeLengthArr + (i * 4));
        let length = lengthPtr.readInt32LE();

        if (!convertFunctions[fields[i]['type']]) {
          throw new errors.DatabaseError("Invalid data type returned from database");
        } else if (fields[i]['type'] == 8 || fields[i]['type'] == 10 || fields[i]['type'] == 15) {
          let colHeadPtr = ref.reinterpret(pData, length + offsetArrLength, offsetForColData)
          blocks[i] = convertFunctions[fields[i]['type']](colHeadPtr, numOfRows, fields[i].bytes, 0, precision);
          offsetForColData = offsetForColData + offsetArrLength + length;
        } else {
          let colHeadPtr = ref.reinterpret(pData, length + bitMapSize, offsetForColData)
          blocks[i] = convertFunctions[fields[i]['type']](colHeadPtr, numOfRows, fields[i].bytes, 0, precision);
          offsetForColData = offsetForColData + bitMapSize + length;
        }
      }
    }

    return { blocks: blocks, num_of_rows: Math.abs(numOfRows) }
  } else {
    throw new OperationalError(`${libtaos.taos_errstr(taosRes)} ,${code}`);
  }

}

CTaosInterface.prototype.freeResult = function freeResult(result) {
  libtaos.taos_free_result(result);
  result = null;
}

/** Number of fields returned in this result handle, must use with async */
CTaosInterface.prototype.numFields = function numFields(result) {
  return libtaos.taos_num_fields(result);
}

// Fetch fields count by connection, the latest query
CTaosInterface.prototype.fieldsCount = function fieldsCount(result) {
  return libtaos.taos_field_count(result);
}

CTaosInterface.prototype.fetchFields = function fetchFields(result) {
  return libtaos.taos_fetch_fields(result);
}

CTaosInterface.prototype.errno = function errno(result) {
  return libtaos.taos_errno(result);
}

CTaosInterface.prototype.errStr = function errStr(result) {
  return ref.readCString(libtaos.taos_errstr(result));
}

// Async
CTaosInterface.prototype.query_a = function query_a(connection, sql, callback, param = ref.ref(ref.NULL)) {
  // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, int), void *param)
  callback = ffi.Callback(ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int], callback);
  libtaos.taos_query_a(connection, ref.allocCString(sql), callback, param);
  return param;
}

/** Asynchrnously fetches the next block of rows. Wraps callback and transfers a 4th argument to the cursor, the row data as blocks in javascript form
 * Note: This isn't a recursive function, in order to fetch all data either use the TDengine cursor object, TaosQuery object, or implement a recrusive
 * function yourself using the libtaos.taos_fetch_raw_block_a function
 */

CTaosInterface.prototype.fetch_raw_block_a = function fetch_raw_block_a(taosRes, callback, param = ref.ref(ref.NULL)) {
  var cti = this;
  var fetchRawBlock_a_callback = function (param2, taosRes2, numOfRows2) {
    let fields = cti.fetchFields_a(taosRes2);
    let precision = libtaos.taos_result_precision(taosRes2);

    let fieldLengthArr = [];
    let fieldLengthPtr = libtaos.taos_fetch_lengths(taosRes2);

    if (ref.isNull(fieldLengthPtr) == false) {
      for (let i = 0; i < fields.length; i++) {
        let fieldLengthBuf = ref.reinterpret(fieldLengthPtr, ref.sizeof.int32, i * ref.sizeof.int32);
        let fieldLength = ref.get(fieldLengthBuf, 0, ref.types.int32);
        fieldLengthArr.push(fieldLength);
      }
    }
    // =====logic same as fetch_raw_block
    // parse raw block from here.
    let pData = libtaos.taos_get_raw_block(taosRes2);
    let numOfRows = numOfRows2;
    let numOfFields = libtaos.taos_field_count(taosRes2);

    let bitMapSize = (numOfRows + ((1 << 3) - 1)) >> 3;
    let offsetArrLength = ref.sizeof.int32 * numOfRows;

    let blocks = new Array(numOfFields);
    blocks.fill(null);

    // offset pData
    let offsetBeforeLengthArr = (4 * 5) + 8 + (4 + 1) * numOfFields;
    var lengthArraySize = 4 * numOfFields;
    let offsetForColData = offsetBeforeLengthArr + lengthArraySize;
    // read column after column
    for (let i = 0; i < numOfFields; i++) {

      let lengthPtr = ref.reinterpret(pData, ref.sizeof.int, offsetBeforeLengthArr + (i * 4));
      let length = lengthPtr.readInt32LE();

      if (!convertFunctions[fields[i]['type']]) {
        throw new errors.DatabaseError("Invalid data type returned from database");
      } else if (fields[i]['type'] == 8 || fields[i]['type'] == 10 || fields[i]['type'] == 15) {
        let colHeadPtr = ref.reinterpret(pData, length + offsetArrLength, offsetForColData)
        blocks[i] = convertFunctions[fields[i]['type']](colHeadPtr, numOfRows, fields[i].bytes, 0, precision);
        offsetForColData = offsetForColData + offsetArrLength + length;
      } else {
        let colHeadPtr = ref.reinterpret(pData, length + bitMapSize, offsetForColData)
        blocks[i] = convertFunctions[fields[i]['type']](colHeadPtr, numOfRows, fields[i].bytes, 0, precision);
        offsetForColData = offsetForColData + bitMapSize + length;
      }
    }
    callback(param2, taosRes2, numOfRows2, blocks);
  }
  var fetchRawBlock_a_callback = ffi.Callback(ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int], fetchRawBlock_a_callback);
  libtaos.taos_fetch_raw_block_a(taosRes, fetchRawBlock_a_callback, param)
}
// Used to parse the TAO_RES retrieved in taos_fetch_row_a()'s callback
// block after block.
CTaosInterface.prototype.get_raw_block = function get_raw_block(taosRes) {
  return libtaos.get_raw_block(taosRes)
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
    libtaos.taos_stop_query(result);
  }
  else {
    throw new errors.ProgrammingError("No result handle passed to stop query");
  }
}

CTaosInterface.prototype.getServerInfo = function getServerInfo(connection) {
  return ref.readCString(libtaos.taos_get_server_info(connection));
}

CTaosInterface.prototype.getClientInfo = function getClientInfo() {
  return ref.readCString(libtaos.taos_get_client_info());
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
  return libtaos.taos_schemaless_insert(connection, _lines, _numLines, protocal, precision);
}

//stmt APIs

/**
 * init a TAOS_STMT object for later use.it should be freed with stmtClose.
 * @param {*} connection valid taos connection 
 * @returns  Not NULL returned for success, and NULL for failure. 
 * 
 */
CTaosInterface.prototype.stmtInit = function stmtInit(connection) {
  return libtaos.taos_stmt_init(connection)
}

/**
 * prepare a sql statement,'sql' should be a valid INSERT/SELECT statement, 'length' is not used.
 * @param {*} stmt 
 * @param {string} sql  a valid INSERT/SELECT statement
 * @param {ulong} length not used
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtPrepare = function stmtPrepare(stmt, sql, length) {
  return libtaos.taos_stmt_prepare(stmt, ref.allocCString(sql), 0);
}

/**
 * For INSERT only. Used to bind table name as a parmeter for the input stmt object.
 * @param {*} stmt could be the value returned by 'stmtInit', 
 *            that may be a valid object or NULL.
 * @param {TaosMultiBind} tableName target table name you want to  bind
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtSetTbname = function stmtSetTbname(stmt, tableName) {
  return libtaos.taos_stmt_set_tbname(stmt, ref.allocCString(tableName));
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
  return libtaos.taos_stmt_set_tbname_tags(stmt, ref.allocCString(tableName), tags);
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
  return libtaos.taos_stmt_set_sub_tbname(stmt, subTableName);
}


/**
 * Bind a single column's data, INTERNAL used and for INSERT only. 
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @param {TaosMultiBind} mbind points to a column's data which could be the one or more lines. 
 * @param {*} colIndex the column's index in prepared sql statement, it starts from 0.
 * @returns 0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtBindSingleParamBatch = function stmtBindSingleParamBatch(stmt, mbind, colIndex) {
  return libtaos.taos_stmt_bind_single_param_batch(stmt, mbind.ref(), colIndex);
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
  return libtaos.taos_stmt_bind_param_batch(stmt, mbinds);
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
  return libtaos.taos_stmt_add_batch(stmt);
}

/**
 * actually execute the INSERT/SELECT sql statement. User application can continue
 * to bind new data after calling to this API.
 * @param {*} stmt 
 * @returns 	0 for success, non-zero for failure.
 */
CTaosInterface.prototype.stmtExecute = function stmtExecute(stmt) {
  return libtaos.taos_stmt_execute(stmt);
}

/**
 * For SELECT only,getting the query result. 
 * User application should free it with API 'FreeResult' at the end.
 * @param {*} stmt could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @returns Not NULL for success, NULL for failure.
 */
CTaosInterface.prototype.stmtUseResult = function stmtUseResult(stmt) {
  return libtaos.taos_stmt_use_result(stmt);
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

  if ((_.isString(tableList)) || (_.isArray(tableList))) {
    ref.set(_tableListBuf, 0, ref.allocCString(_listStr), ref.types.char_ptr);
    return libtaos.taos_load_table_info(taos, _tableListBuf);
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
  return libtaos.taos_stmt_close(stmt);
}

/**
 * Get detail error message when got failure for any stmt API call. 
 * If not failure, the result returned by this API is unknown.
 * @param {*} stmt Could be the value returned by 'stmtInit', that may be a valid object or NULL.
 * @returns error string
 */
CTaosInterface.prototype.stmtErrStr = function stmtErrStr(stmt) {
  return ref.readCString(libtaos.taos_stmt_errstr(stmt));
}

// CTaosInterface.prototype.getServerStatus = function getServerStatus(fqdn,port){
//     const maxLen = 512;
//     let detailPtr = ref.ref(Buffer.alloc(maxLen));
//     let statusCode = libtaos.taos_check_server_status(fqdn,port,detailPtr,maxLen);
//     let detail = ref.readCString(detailPtr);
//     if (statusCode == 0){
//       return "[TSDB_SRV_STATUS_UNAVAILABLE]:"+detail;
//     }else if(statusCode == 1){
//       return "[TSDB_SRV_STATUS_NETWORK_OK]:"+detail;
//     }else if(statusCode == 2){
//       return "[TSDB_SRV_STATUS_NETWORK_OK]:"+detail;
//     }else if(statusCode == 3){
//       return "[TSDB_SRV_STATUS_SERVICE_DEGRADED]:"+detail;
//     }else if(statusCode == 4){
//       return "[TSDB_SRV_STATUS_EXTING]:"+detail;
//     }

// }
