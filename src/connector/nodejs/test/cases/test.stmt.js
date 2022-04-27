const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

// This is a taos connection
let conn;
// This is a Cursor
let c1;

// prepare data
let dbName = 'node_test_stmt_db';
let tsArr = [1642435200000, 1642435300000, 1642435400000, 1642435500000, 1642435600000];
let boolArr = [true, false, true, false, null];
let tinyIntArr = [-127, 3, 127, 0, null];
let smallIntArr = [-32767, 16, 32767, 0, null];
let intArr = [-2147483647, 17, 2147483647, 0, null];
let bigIntArr = [-9223372036854775807n, 9223372036854775807n, 18n, 0n, null];
let floatArr = [3.4028234663852886e+38, -3.4028234663852886e+38, 19, 0, null];
let doubleArr = [1.7976931348623157e+308, -1.7976931348623157e+308, 20, 0, null];
let binaryArr = ['TDengine_Binary', 'taosdata涛思数据', '~!@#$%^&*()', '', null];
let ncharArr = ['TDengine_Nchar', 'taosdata涛思数据', '~!@#$$%^&*()', '', null];
let uTinyIntArr = [0, 127, 254, 23, null];
let uSmallIntArr = [0, 256, 65534, 24, null];
let uIntArr = [0, 1233, 4294967294, 25, null];
let uBigIntArr = [0n, 36424354000001111n, 18446744073709551614n, 26n, null];

//prepare tag data.
let tagData1 = [true, 1, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967290 / 2, 164243520000011111n];
let tagData2 = [true, 2, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967290 / 2, 164243520000011111n];
let tagData3 = [true, 3, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967290 / 2, 164243520000011111n];

/**
 * Combine individual array of every tdengine type that 
 * has been declared and then return a new array.
 * @returns return data array.
 */
function getBindData() {
  let bindDataArr = [];
  for (let i = 0; i < 5; i++) {
    bindDataArr.push(tsArr[i]);
    bindDataArr.push(boolArr[i]);
    bindDataArr.push(tinyIntArr[i]);
    bindDataArr.push(smallIntArr[i]);
    bindDataArr.push(intArr[i]);
    bindDataArr.push(bigIntArr[i]);
    bindDataArr.push(floatArr[i]);
    bindDataArr.push(doubleArr[i]);
    bindDataArr.push(binaryArr[i]);
    bindDataArr.push(ncharArr[i]);
    bindDataArr.push(uTinyIntArr[i]);
    bindDataArr.push(uSmallIntArr[i]);
    bindDataArr.push(uIntArr[i]);
    bindDataArr.push(uBigIntArr[i]);
  }
  return bindDataArr;
}

function executeUpdate(sql) {
  console.log(sql);
  c1.execute(sql);
}

function executeQuery(sql) {
  c1.execute(sql, { quiet: true })
  var data = c1.fetchall();
  let fields = c1.fields;
  let resArr = [];

  data.forEach(row => {
    row.forEach(data => {
      if (data instanceof Date) {
        // console.log("date obejct:"+data.valueOf());
        resArr.push(data.taosTimestamp());
      } else {
        // console.log("not date:"+data);
        resArr.push(data);
      }
      // console.log(data instanceof Date)
    })
  })
  return { resData: resArr, resFeilds: fields };
}

beforeAll(() => {
  conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
  c1 = conn.cursor();
  executeUpdate(`create database if not exists ${dbName} keep 3650;`);
  executeUpdate(`use ${dbName};`);
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
  executeUpdate(`drop database if exists ${dbName};`);
  c1.close();
  conn.close();
});

describe("stmt_bind_single_param", () => {
  test(`name:bindSingleParamWithOneTable;` +
    `author:${author};` +
    `desc:Using stmtBindSingleParam() bind one table in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindsingleparambatch_121';
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), tagData1, 14);

      // prepare tag TAOS_BIND 
      let tagBind1 = new taos.TaosBind(14);
      tagBind1.bindBool(true);
      tagBind1.bindTinyInt(1);
      tagBind1.bindSmallInt(32767);
      tagBind1.bindInt(1234555);
      tagBind1.bindBigInt(-164243520000011111n);
      tagBind1.bindFloat(214.02);
      tagBind1.bindDouble(2.01);
      tagBind1.bindBinary('taosdata涛思数据');
      tagBind1.bindNchar('TDengine数据');
      tagBind1.bindUTinyInt(254);
      tagBind1.bindUSmallInt(65534);
      tagBind1.bindUInt(4294967290 / 2);
      tagBind1.bindUBigInt(164243520000011111n);

      //Prepare TAOS_MULTI_BIND data
      let mBind1 = new taos.TaosMultiBind();

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      c1.stmtSetTbnameTags(`${table}_s01`, tagBind1.getBind());
      c1.stmtBindSingleParamBatch(mBind1.multiBindTimestamp(tsArr), 0);
      c1.stmtBindSingleParamBatch(mBind1.multiBindBool(boolArr), 1);
      c1.stmtBindSingleParamBatch(mBind1.multiBindTinyInt(tinyIntArr), 2);
      c1.stmtBindSingleParamBatch(mBind1.multiBindSmallInt(smallIntArr), 3);
      c1.stmtBindSingleParamBatch(mBind1.multiBindInt(intArr), 4);
      c1.stmtBindSingleParamBatch(mBind1.multiBindBigInt(bigIntArr), 5);
      c1.stmtBindSingleParamBatch(mBind1.multiBindFloat(floatArr), 6);
      c1.stmtBindSingleParamBatch(mBind1.multiBindDouble(doubleArr), 7);
      c1.stmtBindSingleParamBatch(mBind1.multiBindBinary(binaryArr), 8);
      c1.stmtBindSingleParamBatch(mBind1.multiBindNchar(ncharArr), 9);
      c1.stmtBindSingleParamBatch(mBind1.multiBindUTinyInt(uTinyIntArr), 10);
      c1.stmtBindSingleParamBatch(mBind1.multiBindUSmallInt(uSmallIntArr), 11);
      c1.stmtBindSingleParamBatch(mBind1.multiBindUInt(uIntArr), 12);
      c1.stmtBindSingleParamBatch(mBind1.multiBindUBigInt(uBigIntArr), 13);
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:bindSingleParamWithMultiTable;` +
    `author:${author};` +
    `desc:Using stmtBindSingleParam() bind multiple tables in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindsingleparambatch_m21';//bind multiple table to one batch
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), tagData1, 14).concat(getResData(getBindData(), tagData2, 14)).concat(getResData(getBindData(), tagData3, 14));

      // prepare tag TAOS_BIND 
      let tagBind1 = new taos.TaosBind(14);
      tagBind1.bindBool(true);
      tagBind1.bindTinyInt(1);
      tagBind1.bindSmallInt(32767);
      tagBind1.bindInt(1234555);
      tagBind1.bindBigInt(-164243520000011111n);
      tagBind1.bindFloat(214.02);
      tagBind1.bindDouble(2.01);
      tagBind1.bindBinary('taosdata涛思数据');
      tagBind1.bindNchar('TDengine数据');
      tagBind1.bindUTinyInt(254);
      tagBind1.bindUSmallInt(65534);
      tagBind1.bindUInt(4294967290 / 2);
      tagBind1.bindUBigInt(164243520000011111n);

      let tagBind2 = new taos.TaosBind(14);
      tagBind2.bindBool(true);
      tagBind2.bindTinyInt(2);
      tagBind2.bindSmallInt(32767);
      tagBind2.bindInt(1234555);
      tagBind2.bindBigInt(-164243520000011111n);
      tagBind2.bindFloat(214.02);
      tagBind2.bindDouble(2.01);
      tagBind2.bindBinary('taosdata涛思数据');
      tagBind2.bindNchar('TDengine数据');
      tagBind2.bindUTinyInt(254);
      tagBind2.bindUSmallInt(65534);
      tagBind2.bindUInt(4294967290 / 2);
      tagBind2.bindUBigInt(164243520000011111n);

      let tagBind3 = new taos.TaosBind(14);
      tagBind3.bindBool(true);
      tagBind3.bindTinyInt(3);
      tagBind3.bindSmallInt(32767);
      tagBind3.bindInt(1234555);
      tagBind3.bindBigInt(-164243520000011111n);
      tagBind3.bindFloat(214.02);
      tagBind3.bindDouble(2.01);
      tagBind3.bindBinary('taosdata涛思数据');
      tagBind3.bindNchar('TDengine数据');
      tagBind3.bindUTinyInt(254);
      tagBind3.bindUSmallInt(65534);
      tagBind3.bindUInt(4294967290 / 2);
      tagBind3.bindUBigInt(164243520000011111n);

      //Prepare TAOS_MULTI_BIND data
      let mBind = new taos.TaosMultiBind();

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      // ========bind for 1st table =============
      c1.stmtSetTbnameTags(`${table}_s01`, tagBind1.getBind());
      c1.stmtBindSingleParamBatch(mBind.multiBindTimestamp(tsArr), 0);
      c1.stmtBindSingleParamBatch(mBind.multiBindBool(boolArr), 1);
      c1.stmtBindSingleParamBatch(mBind.multiBindTinyInt(tinyIntArr), 2);
      c1.stmtBindSingleParamBatch(mBind.multiBindSmallInt(smallIntArr), 3);
      c1.stmtBindSingleParamBatch(mBind.multiBindInt(intArr), 4);
      c1.stmtBindSingleParamBatch(mBind.multiBindBigInt(bigIntArr), 5);
      c1.stmtBindSingleParamBatch(mBind.multiBindFloat(floatArr), 6);
      c1.stmtBindSingleParamBatch(mBind.multiBindDouble(doubleArr), 7);
      c1.stmtBindSingleParamBatch(mBind.multiBindBinary(binaryArr), 8);
      c1.stmtBindSingleParamBatch(mBind.multiBindNchar(ncharArr), 9);
      c1.stmtBindSingleParamBatch(mBind.multiBindUTinyInt(uTinyIntArr), 10);
      c1.stmtBindSingleParamBatch(mBind.multiBindUSmallInt(uSmallIntArr), 11);
      c1.stmtBindSingleParamBatch(mBind.multiBindUInt(uIntArr), 12);
      c1.stmtBindSingleParamBatch(mBind.multiBindUBigInt(uBigIntArr), 13);
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ========bind for 2nd table =============
      c1.stmtSetTbnameTags(`${table}_s02`, tagBind2.getBind());
      c1.stmtBindSingleParamBatch(mBind.multiBindTimestamp(tsArr), 0);
      c1.stmtBindSingleParamBatch(mBind.multiBindBool(boolArr), 1);
      c1.stmtBindSingleParamBatch(mBind.multiBindTinyInt(tinyIntArr), 2);
      c1.stmtBindSingleParamBatch(mBind.multiBindSmallInt(smallIntArr), 3);
      c1.stmtBindSingleParamBatch(mBind.multiBindInt(intArr), 4);
      c1.stmtBindSingleParamBatch(mBind.multiBindBigInt(bigIntArr), 5);
      c1.stmtBindSingleParamBatch(mBind.multiBindFloat(floatArr), 6);
      c1.stmtBindSingleParamBatch(mBind.multiBindDouble(doubleArr), 7);
      c1.stmtBindSingleParamBatch(mBind.multiBindBinary(binaryArr), 8);
      c1.stmtBindSingleParamBatch(mBind.multiBindNchar(ncharArr), 9);
      c1.stmtBindSingleParamBatch(mBind.multiBindUTinyInt(uTinyIntArr), 10);
      c1.stmtBindSingleParamBatch(mBind.multiBindUSmallInt(uSmallIntArr), 11);
      c1.stmtBindSingleParamBatch(mBind.multiBindUInt(uIntArr), 12);
      c1.stmtBindSingleParamBatch(mBind.multiBindUBigInt(uBigIntArr), 13);
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ========bind for 3rd table =============
      c1.stmtSetTbnameTags(`${table}_s0`, tagBind3.getBind());
      c1.stmtBindSingleParamBatch(mBind.multiBindTimestamp(tsArr), 0);
      c1.stmtBindSingleParamBatch(mBind.multiBindBool(boolArr), 1);
      c1.stmtBindSingleParamBatch(mBind.multiBindTinyInt(tinyIntArr), 2);
      c1.stmtBindSingleParamBatch(mBind.multiBindSmallInt(smallIntArr), 3);
      c1.stmtBindSingleParamBatch(mBind.multiBindInt(intArr), 4);
      c1.stmtBindSingleParamBatch(mBind.multiBindBigInt(bigIntArr), 5);
      c1.stmtBindSingleParamBatch(mBind.multiBindFloat(floatArr), 6);
      c1.stmtBindSingleParamBatch(mBind.multiBindDouble(doubleArr), 7);
      c1.stmtBindSingleParamBatch(mBind.multiBindBinary(binaryArr), 8);
      c1.stmtBindSingleParamBatch(mBind.multiBindNchar(ncharArr), 9);
      c1.stmtBindSingleParamBatch(mBind.multiBindUTinyInt(uTinyIntArr), 10);
      c1.stmtBindSingleParamBatch(mBind.multiBindUSmallInt(uSmallIntArr), 11);
      c1.stmtBindSingleParamBatch(mBind.multiBindUInt(uIntArr), 12);
      c1.stmtBindSingleParamBatch(mBind.multiBindUBigInt(uBigIntArr), 13);
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });
      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });
})

describe("stmt_bind_para_batch", () => {
  test(`name:bindParamBatchWithOneTable;` +
    `author:${author};` +
    `desc:Using stmtBindParamBatch() bind one table in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindparambatch_121';//bind one table to one batch
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), tagData1, 14);

      //prepare tag TAO_BIND
      let tagBind = new taos.TaosBind(14);
      tagBind.bindBool(true);
      tagBind.bindTinyInt(1);
      tagBind.bindSmallInt(32767);
      tagBind.bindInt(1234555);
      tagBind.bindBigInt(-164243520000011111n);
      tagBind.bindFloat(214.02);
      tagBind.bindDouble(2.01);
      tagBind.bindBinary('taosdata涛思数据');
      tagBind.bindNchar('TDengine数据');
      tagBind.bindUTinyInt(254);
      tagBind.bindUSmallInt(65534);
      tagBind.bindUInt(4294967290 / 2);
      tagBind.bindUBigInt(164243520000011111n);

      //Prepare TAOS_MULTI_BIND data array
      let mBinds = new taos.TaosMultiBindArr(14);
      mBinds.multiBindTimestamp(tsArr);
      mBinds.multiBindBool(boolArr);
      mBinds.multiBindTinyInt(tinyIntArr);
      mBinds.multiBindSmallInt(smallIntArr);
      mBinds.multiBindInt(intArr);
      mBinds.multiBindBigInt(bigIntArr);
      mBinds.multiBindFloat(floatArr);
      mBinds.multiBindDouble(doubleArr);
      mBinds.multiBindBinary(binaryArr);
      mBinds.multiBindNchar(ncharArr);
      mBinds.multiBindUTinyInt(uTinyIntArr);
      mBinds.multiBindUSmallInt(uSmallIntArr);
      mBinds.multiBindUInt(uIntArr);
      mBinds.multiBindUBigInt(uBigIntArr);

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      c1.stmtSetTbnameTags(`${table}_s01`, tagBind.getBind());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:bindParamBatchWithMultiTable;` +
    `author:${author};` +
    `desc:Using stmtBindParamBatch() bind multiple tables in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindparambatch_m21';//bind multiple tables to one batch
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), tagData1, 14).concat(getResData(getBindData(), tagData2, 14)).concat(getResData(getBindData(), tagData3, 14));


      // prepare tag TAOS_BIND 
      let tagBind1 = new taos.TaosBind(14);
      tagBind1.bindBool(true);
      tagBind1.bindTinyInt(1);
      tagBind1.bindSmallInt(32767);
      tagBind1.bindInt(1234555);
      tagBind1.bindBigInt(-164243520000011111n);
      tagBind1.bindFloat(214.02);
      tagBind1.bindDouble(2.01);
      tagBind1.bindBinary('taosdata涛思数据');
      tagBind1.bindNchar('TDengine数据');
      tagBind1.bindUTinyInt(254);
      tagBind1.bindUSmallInt(65534);
      tagBind1.bindUInt(4294967290 / 2);
      tagBind1.bindUBigInt(164243520000011111n);

      let tagBind2 = new taos.TaosBind(14);
      tagBind2.bindBool(true);
      tagBind2.bindTinyInt(2);
      tagBind2.bindSmallInt(32767);
      tagBind2.bindInt(1234555);
      tagBind2.bindBigInt(-164243520000011111n);
      tagBind2.bindFloat(214.02);
      tagBind2.bindDouble(2.01);
      tagBind2.bindBinary('taosdata涛思数据');
      tagBind2.bindNchar('TDengine数据');
      tagBind2.bindUTinyInt(254);
      tagBind2.bindUSmallInt(65534);
      tagBind2.bindUInt(4294967290 / 2);
      tagBind2.bindUBigInt(164243520000011111n);

      let tagBind3 = new taos.TaosBind(14);
      tagBind3.bindBool(true);
      tagBind3.bindTinyInt(3);
      tagBind3.bindSmallInt(32767);
      tagBind3.bindInt(1234555);
      tagBind3.bindBigInt(-164243520000011111n);
      tagBind3.bindFloat(214.02);
      tagBind3.bindDouble(2.01);
      tagBind3.bindBinary('taosdata涛思数据');
      tagBind3.bindNchar('TDengine数据');
      tagBind3.bindUTinyInt(254);
      tagBind3.bindUSmallInt(65534);
      tagBind3.bindUInt(4294967290 / 2);
      tagBind3.bindUBigInt(164243520000011111n);

      //Prepare TAOS_MULTI_BIND data array
      let mBinds = new taos.TaosMultiBindArr(14);
      mBinds.multiBindTimestamp(tsArr);
      mBinds.multiBindBool(boolArr);
      mBinds.multiBindTinyInt(tinyIntArr);
      mBinds.multiBindSmallInt(smallIntArr);
      mBinds.multiBindInt(intArr);
      mBinds.multiBindBigInt(bigIntArr);
      mBinds.multiBindFloat(floatArr);
      mBinds.multiBindDouble(doubleArr);
      mBinds.multiBindBinary(binaryArr);
      mBinds.multiBindNchar(ncharArr);
      mBinds.multiBindUTinyInt(uTinyIntArr);
      mBinds.multiBindUSmallInt(uSmallIntArr);
      mBinds.multiBindUInt(uIntArr);
      mBinds.multiBindUBigInt(uBigIntArr);

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      // ===========bind for 1st table ==========
      c1.stmtSetTbnameTags(`${table}_s01`, tagBind1.getBind());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ===========bind for 2nd table ==========
      c1.stmtSetTbnameTags(`${table}_s02`, tagBind2.getBind());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ===========bind for 3rd table ==========
      c1.stmtSetTbnameTags(`${table}_s03`, tagBind3.getBind());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })


    });
})

describe("stmt_bind_param", () => {
  test(`name:bindParamWithOneTable;` +
    `author:${author};` +
    `desc:using stmtBindParam() bind one table in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindparam_121';//bind one table to one batch
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let data = getBindData();
      let expectResData = getResData(data, tagData1, 14);

      //prepare tag data
      let tags = new taos.TaosBind(13);
      tags.bindBool(true);
      tags.bindTinyInt(1);
      tags.bindSmallInt(32767);
      tags.bindInt(1234555);
      tags.bindBigInt(-164243520000011111n);
      tags.bindFloat(214.02);
      tags.bindDouble(2.01);
      tags.bindBinary('taosdata涛思数据');
      tags.bindNchar('TDengine数据');
      tags.bindUTinyInt(254);
      tags.bindUSmallInt(65534);
      tags.bindUInt(4294967290 / 2);
      tags.bindUBigInt(164243520000011111n);
      executeUpdate(createSql);

      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      c1.stmtSetTbnameTags(`${table}_s01`, tags.getBind());
      for (let i = 0; i < data.length - 14; i += 14) {
        let bind = new taos.TaosBind(14);
        bind.bindTimestamp(data[i]);
        bind.bindBool(data[i + 1]);
        bind.bindTinyInt(data[i + 2]);
        bind.bindSmallInt(data[i + 3]);
        bind.bindInt(data[i + 4]);
        bind.bindBigInt(data[i + 5]);
        bind.bindFloat(data[i + 6]);
        bind.bindDouble(data[i + 7]);
        bind.bindBinary(data[i + 8]);
        bind.bindNchar(data[i + 9]);
        bind.bindUTinyInt(data[i + 10]);
        bind.bindUSmallInt(data[i + 11]);
        bind.bindUInt(data[i + 12]);
        bind.bindUBigInt(data[i + 13]);
        c1.stmtBindParam(bind.getBind());
        c1.stmtAddBatch();
      }
      let bind2 = new taos.TaosBind(14);
      bind2.bindTimestamp(data[14 * 4]);
      for (let j = 0; j < 13; j++) {
        bind2.bindNil();
      }
      c1.stmtBindParam(bind2.getBind());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:bindParamWithMultiTable;` +
    `author:${author};` +
    `desc:using stmtBindParam() bind multiple tables in a batch;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindparam_m21';//bind one table to one batch
      let createSql = `create table if not exists ${table} ` +
        `(ts timestamp,` +
        `bl bool,` +
        `i8 tinyint,` +
        `i16 smallint,` +
        `i32 int,` +
        `i64 bigint,` +
        `f32 float,` +
        `d64 double,` +
        `bnr binary(20),` +
        `nchr nchar(20),` +
        `u8 tinyint unsigned,` +
        `u16 smallint unsigned,` +
        `u32 int unsigned,` +
        `u64 bigint unsigned` +
        `)tags(` +
        `t_bl bool,` +
        `t_i8 tinyint,` +
        `t_i16 smallint,` +
        `t_i32 int,` +
        `t_i64 bigint,` +
        `t_f32 float,` +
        `t_d64 double,` +
        `t_bnr binary(20),` +
        `t_nchr nchar(20),` +
        `t_u8 tinyint unsigned,` +
        `t_u16 smallint unsigned,` +
        `t_u32 int unsigned,` +
        `t_u64 bigint unsigned` +
        `);`;
      let insertSql = `insert into ? using ${table} tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let data = getBindData();
      let expectResData = getResData(data, tagData1, 14).concat(getResData(data, tagData2, 14)).concat(getResData(data, tagData3, 14));

      // prepare tag TAOS_BIND 
      let tagBind1 = new taos.TaosBind(14);
      tagBind1.bindBool(true);
      tagBind1.bindTinyInt(1);
      tagBind1.bindSmallInt(32767);
      tagBind1.bindInt(1234555);
      tagBind1.bindBigInt(-164243520000011111n);
      tagBind1.bindFloat(214.02);
      tagBind1.bindDouble(2.01);
      tagBind1.bindBinary('taosdata涛思数据');
      tagBind1.bindNchar('TDengine数据');
      tagBind1.bindUTinyInt(254);
      tagBind1.bindUSmallInt(65534);
      tagBind1.bindUInt(4294967290 / 2);
      tagBind1.bindUBigInt(164243520000011111n);

      let tagBind2 = new taos.TaosBind(14);
      tagBind2.bindBool(true);
      tagBind2.bindTinyInt(2);
      tagBind2.bindSmallInt(32767);
      tagBind2.bindInt(1234555);
      tagBind2.bindBigInt(-164243520000011111n);
      tagBind2.bindFloat(214.02);
      tagBind2.bindDouble(2.01);
      tagBind2.bindBinary('taosdata涛思数据');
      tagBind2.bindNchar('TDengine数据');
      tagBind2.bindUTinyInt(254);
      tagBind2.bindUSmallInt(65534);
      tagBind2.bindUInt(4294967290 / 2);
      tagBind2.bindUBigInt(164243520000011111n);

      let tagBind3 = new taos.TaosBind(14);
      tagBind3.bindBool(true);
      tagBind3.bindTinyInt(3);
      tagBind3.bindSmallInt(32767);
      tagBind3.bindInt(1234555);
      tagBind3.bindBigInt(-164243520000011111n);
      tagBind3.bindFloat(214.02);
      tagBind3.bindDouble(2.01);
      tagBind3.bindBinary('taosdata涛思数据');
      tagBind3.bindNchar('TDengine数据');
      tagBind3.bindUTinyInt(254);
      tagBind3.bindUSmallInt(65534);
      tagBind3.bindUInt(4294967290 / 2);
      tagBind3.bindUBigInt(164243520000011111n);

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      // ========= bind for 1st table =================
      c1.stmtSetTbnameTags(`${table}_s01`, tagBind1.getBind());
      for (let i = 0; i < data.length - 14; i += 14) {
        let bind = new taos.TaosBind(14);
        bind.bindTimestamp(data[i]);
        bind.bindBool(data[i + 1]);
        bind.bindTinyInt(data[i + 2]);
        bind.bindSmallInt(data[i + 3]);
        bind.bindInt(data[i + 4]);
        bind.bindBigInt(data[i + 5]);
        bind.bindFloat(data[i + 6]);
        bind.bindDouble(data[i + 7]);
        bind.bindBinary(data[i + 8]);
        bind.bindNchar(data[i + 9]);
        bind.bindUTinyInt(data[i + 10]);
        bind.bindUSmallInt(data[i + 11]);
        bind.bindUInt(data[i + 12]);
        bind.bindUBigInt(data[i + 13]);
        c1.stmtBindParam(bind.getBind());
        c1.stmtAddBatch();
      }
      let bind2 = new taos.TaosBind(14);
      bind2.bindTimestamp(data[14 * 4]);
      for (let j = 0; j < 13; j++) {
        bind2.bindNil();
      }
      c1.stmtBindParam(bind2.getBind());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ========= bind for 2nd table =================
      c1.stmtSetTbnameTags(`${table}_s02`, tagBind2.getBind());
      for (let i = 0; i < data.length - 14; i += 14) {
        let bind = new taos.TaosBind(14);
        bind.bindTimestamp(data[i]);
        bind.bindBool(data[i + 1]);
        bind.bindTinyInt(data[i + 2]);
        bind.bindSmallInt(data[i + 3]);
        bind.bindInt(data[i + 4]);
        bind.bindBigInt(data[i + 5]);
        bind.bindFloat(data[i + 6]);
        bind.bindDouble(data[i + 7]);
        bind.bindBinary(data[i + 8]);
        bind.bindNchar(data[i + 9]);
        bind.bindUTinyInt(data[i + 10]);
        bind.bindUSmallInt(data[i + 11]);
        bind.bindUInt(data[i + 12]);
        bind.bindUBigInt(data[i + 13]);
        c1.stmtBindParam(bind.getBind());
        c1.stmtAddBatch();
      }
      c1.stmtBindParam(bind2.getBind());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ========= bind for 3rd table =================
      c1.stmtSetTbnameTags(`${table}_s03`, tagBind3.getBind());
      for (let i = 0; i < data.length - 14; i += 14) {
        let bind = new taos.TaosBind(14);
        bind.bindTimestamp(data[i]);
        bind.bindBool(data[i + 1]);
        bind.bindTinyInt(data[i + 2]);
        bind.bindSmallInt(data[i + 3]);
        bind.bindInt(data[i + 4]);
        bind.bindBigInt(data[i + 5]);
        bind.bindFloat(data[i + 6]);
        bind.bindDouble(data[i + 7]);
        bind.bindBinary(data[i + 8]);
        bind.bindNchar(data[i + 9]);
        bind.bindUTinyInt(data[i + 10]);
        bind.bindUSmallInt(data[i + 11]);
        bind.bindUInt(data[i + 12]);
        bind.bindUBigInt(data[i + 13]);
        c1.stmtBindParam(bind.getBind());
        c1.stmtAddBatch();
      }
      c1.stmtBindParam(bind2.getBind());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });
})

