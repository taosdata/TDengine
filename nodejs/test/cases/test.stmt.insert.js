/**
 * @jest-environment node
 */
const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'stmt_db';

// This is a taos connection
let conn;
// This is a Cursor
let c1;

// prepare data

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
let tagData1 = [true, 1, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967295, 164243520000011111n];
let tagData2 = [true, 2, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967295, 164243520000011111n];
let tagData3 = [true, 3, 32767, 1234555, -164243520000011111n, 214.02, 2.01, 'taosdata涛思数据', 'TDengine数据', 254, 65534, 4294967295, 164243520000011111n];
let jsonTag = ['{\"tag1\":\"jtag_1\",\"tag2\":1,\"tag3\":true}']
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

function executeUpdate(sql, printFlag = false) {
  if (printFlag === true) {
    console.log(sql);
  }
  c1.execute(sql, { 'quiet': false });
}

function executeQuery(sql, printFlag = false) {
  if (printFlag === true) {
    console.log(sql);
  }

  c1.execute(sql, { quiet: true })
  var data = c1.fetchall({ 'quiet': false });
  let fields = c1.fields;
  let resArr = [];

  data.forEach(row => {
    row.forEach(data => {
      if (data instanceof Date) {
        resArr.push(data.taosTimestamp());
      } else {
        resArr.push(data);
      }
    })
  })
  return { resData: resArr, resFields: fields };
}

beforeAll(() => {
  conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
  c1 = conn.cursor();
  executeUpdate(`create database if not exists ${db} keep 36500;`);
  executeUpdate(`use ${db};`);
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
  executeUpdate(`drop database if exists ${db};`);
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

      // prepare tag
      let tags = new taos.TaosMultiBindArr(14);
      tags.multiBindBool([true]);
      tags.multiBindTinyInt([1]);
      tags.multiBindSmallInt([32767]);
      tags.multiBindInt([1234555])
      tags.multiBindBigInt([-164243520000011111n]);
      tags.multiBindFloat([214.02]);
      tags.multiBindDouble([2.01]);
      tags.multiBindBinary(['taosdata涛思数据']);
      tags.multiBindNchar(['TDengine数据']);
      tags.multiBindUTinyInt([254]);
      tags.multiBindUSmallInt([65534]);
      tags.multiBindUInt([4294967295]);
      tags.multiBindUBigInt([164243520000011111n]);

      //Prepare TAOS_MULTI_BIND data
      let mBind1 = new taos.TaosMultiBind();

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      c1.stmtSetTbnameTags(`${table}_s01`, tags.getMultiBindArr());
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
      let actualResFields = result.resFields;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(actualResData[index]).toEqual(item);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(actualResFields[index]).toEqual(item)
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
      let querySql1 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s01;`;
      let querySql2 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s02;`;
      let querySql3 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s03;`;

      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData1 = getResData(getBindData(), tagData1, 14)
      let expectResData2 = getResData(getBindData(), tagData2, 14)
      let expectResData3 = getResData(getBindData(), tagData3, 14);

      // prepare tag TAOS_BIND 
      let tag1 = new taos.TaosMultiBindArr(14);
      tag1.multiBindBool([true]);
      tag1.multiBindTinyInt([1]);
      tag1.multiBindSmallInt([32767]);
      tag1.multiBindInt([1234555]);
      tag1.multiBindBigInt([-164243520000011111n]);
      tag1.multiBindFloat([214.02]);
      tag1.multiBindDouble([2.01]);
      tag1.multiBindBinary(['taosdata涛思数据']);
      tag1.multiBindNchar(['TDengine数据']);
      tag1.multiBindUTinyInt([254]);
      tag1.multiBindUSmallInt([65534]);
      tag1.multiBindUInt([4294967295]);
      tag1.multiBindUBigInt([164243520000011111n]);

      let tag2 = new taos.TaosMultiBindArr(14);
      tag2.multiBindBool([true]);
      tag2.multiBindTinyInt([2]);
      tag2.multiBindSmallInt([32767]);
      tag2.multiBindInt([1234555]);
      tag2.multiBindBigInt([-164243520000011111n]);
      tag2.multiBindFloat([214.02]);
      tag2.multiBindDouble([2.01]);
      tag2.multiBindBinary(['taosdata涛思数据']);
      tag2.multiBindNchar(['TDengine数据']);
      tag2.multiBindUTinyInt([254]);
      tag2.multiBindUSmallInt([65534]);
      tag2.multiBindUInt([4294967295]);
      tag2.multiBindUBigInt([164243520000011111n]);

      let tag3 = new taos.TaosMultiBindArr(14);
      tag3.multiBindBool([true]);
      tag3.multiBindTinyInt([3]);
      tag3.multiBindSmallInt([32767]);
      tag3.multiBindInt([1234555]);
      tag3.multiBindBigInt([-164243520000011111n]);
      tag3.multiBindFloat([214.02]);
      tag3.multiBindDouble([2.01]);
      tag3.multiBindBinary(['taosdata涛思数据']);
      tag3.multiBindNchar(['TDengine数据']);
      tag3.multiBindUTinyInt([254]);
      tag3.multiBindUSmallInt([65534]);
      tag3.multiBindUInt([4294967295]);
      tag3.multiBindUBigInt([164243520000011111n]);

      //Prepare TAOS_MULTI_BIND data
      let mBind = new taos.TaosMultiBind();

      executeUpdate(createSql);
      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      // ========bind for 1st table =============
      c1.stmtSetTbnameTags(`${table}_s01`, tag1.getMultiBindArr());
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
      c1.stmtSetTbnameTags(`${table}_s02`, tag2.getMultiBindArr());
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
      c1.stmtSetTbnameTags(`${table}_s03`, tag3.getMultiBindArr());
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

      let result1 = executeQuery(querySql1);
      let actualResData1 = result1.resData;
      let actualResFields1 = result1.resFields;

      //assert result data length for table 1
      expect(expectResData1.length).toEqual(actualResData1.length);
      //assert result data for table 1
      expectResData1.forEach((item, index) => {
        expect(item).toEqual(actualResData1[index]);
      });
      //assert result meta data for table 1
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields1[index])
      })

      let result2 = executeQuery(querySql2);
      let actualResData2 = result2.resData;
      let actualResFields2 = result2.resFields;

      //assert result data length for table 2
      expect(expectResData2.length).toEqual(actualResData2.length);
      //assert result data for table 2
      expectResData2.forEach((item, index) => {
        expect(item).toEqual(actualResData2[index]);
      });
      //assert result meta data for table 2
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields2[index])
      })

      let result3 = executeQuery(querySql3);
      let actualResData3 = result3.resData;
      let actualResFields3 = result3.resFields;

      //assert result data length for table 3
      expect(expectResData3.length).toEqual(actualResData3.length);
      //assert result data for table 3
      expectResData3.forEach((item, index) => {
        expect(item).toEqual(actualResData3[index]);
      });
      //assert result meta data for table 3
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields3[index])
      })


    });

  test('name:bindSingleParamWithJson' +
    `author:${author};` +
    `desc:Using stmtBindSingleParam() bind one table in a batch and set;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'bindsingleparambatch_121_j';
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
        `json_tag json`+
        `);`;
      let insertSql = `insert into ? using ${table} tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), jsonTag, 14);

      // prepare json tag
      let  jTag = new taos.TaosMultiBindArr(1);
      jTag.multiBindJSON(jsonTag);
      
      //prepare TAOS_MULTI_BIND dat
      let mBind1 = new taos.TaosMultiBind();

      executeUpdate(createSql);

      c1.stmtInit();
      c1.stmtPrepare(insertSql);
      c1.stmtSetTbnameTags(`${table}_s01`, jTag.getMultiBindArr());
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
      let actualResFields = result.resFields;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(actualResData[index]).toEqual(item);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(actualResFields[index]).toEqual(item)
      })
      expect(2).toEqual(2) 
    })
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
      let tags = new taos.TaosMultiBindArr(14);
      tags.multiBindBool([true]);
      tags.multiBindTinyInt([1]);
      tags.multiBindSmallInt([32767]);
      tags.multiBindInt([1234555]);
      tags.multiBindBigInt([-164243520000011111n]);
      tags.multiBindFloat([214.02]);
      tags.multiBindDouble([2.01]);
      tags.multiBindBinary(['taosdata涛思数据']);
      tags.multiBindNchar(['TDengine数据']);
      tags.multiBindUTinyInt([254]);
      tags.multiBindUSmallInt([65534]);
      tags.multiBindUInt([4294967295]);
      tags.multiBindUBigInt([164243520000011111n]);

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
      c1.stmtSetTbnameTags(`${table}_s01`, tags.getMultiBindArr());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFields;

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

      let querySql1 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s01;`;
      let querySql2 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s02;`;
      let querySql3 = `select ts,bl,i8,i16,i32,i64,f32,d64,bnr,nchr,u8,u16,u32,u64,t_bl,t_i8,t_i16,t_i32,t_i64,t_f32,t_d64,t_bnr,t_nchr,t_u8,t_u16,t_u32,t_u64 from ${table}_s03;`;

      let expectResField = getFieldArr(getFeildsFromDll(createSql));

      let expectResData1 = getResData(getBindData(), tagData1, 14);
      let expectResData2 = getResData(getBindData(), tagData2, 14);
      let expectResData3 = getResData(getBindData(), tagData3, 14);;


      // prepare tag TAOS_BIND 
      let tag1 = new taos.TaosMultiBindArr(14);
      tag1.multiBindBool([true]);
      tag1.multiBindTinyInt([1]);
      tag1.multiBindSmallInt([32767]);
      tag1.multiBindInt([1234555]);
      tag1.multiBindBigInt([-164243520000011111n]);
      tag1.multiBindFloat([214.02]);
      tag1.multiBindDouble([2.01]);
      tag1.multiBindBinary(['taosdata涛思数据']);
      tag1.multiBindNchar(['TDengine数据']);
      tag1.multiBindUTinyInt([254]);
      tag1.multiBindUSmallInt([65534]);
      tag1.multiBindUInt([4294967295]);
      tag1.multiBindUBigInt([164243520000011111n]);

      let tag2 = new taos.TaosMultiBindArr(14);
      tag2.multiBindBool([true]);
      tag2.multiBindTinyInt([2]);
      tag2.multiBindSmallInt([32767]);
      tag2.multiBindInt([1234555]);
      tag2.multiBindBigInt([-164243520000011111n]);
      tag2.multiBindFloat([214.02]);
      tag2.multiBindDouble([2.01]);
      tag2.multiBindBinary(['taosdata涛思数据']);
      tag2.multiBindNchar(['TDengine数据']);
      tag2.multiBindUTinyInt([254]);
      tag2.multiBindUSmallInt([65534]);
      tag2.multiBindUInt([4294967295]);
      tag2.multiBindUBigInt([164243520000011111n]);

      let tag3 = new taos.TaosMultiBindArr(14);
      tag3.multiBindBool([true]);
      tag3.multiBindTinyInt([3]);
      tag3.multiBindSmallInt([32767]);
      tag3.multiBindInt([1234555]);
      tag3.multiBindBigInt([-164243520000011111n]);
      tag3.multiBindFloat([214.02]);
      tag3.multiBindDouble([2.01]);
      tag3.multiBindBinary(['taosdata涛思数据']);
      tag3.multiBindNchar(['TDengine数据']);
      tag3.multiBindUTinyInt([254]);
      tag3.multiBindUSmallInt([65534]);
      tag3.multiBindUInt([4294967295]);
      tag3.multiBindUBigInt([164243520000011111n]);

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
      c1.stmtSetTbnameTags(`${table}_s01`, tag1.getMultiBindArr());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ===========bind for 2nd table ==========
      c1.stmtSetTbnameTags(`${table}_s02`, tag2.getMultiBindArr());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      // c1.stmtExecute();

      // ===========bind for 3rd table ==========
      c1.stmtSetTbnameTags(`${table}_s03`, tag3.getMultiBindArr());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result1 = executeQuery(querySql1);
      let actualResData1 = result1.resData;
      let actualResFields1 = result1.resFields;

      //assert result data length 
      expect(expectResData1.length).toEqual(actualResData1.length);
      //assert result data
      expectResData1.forEach((item, index) => {
        expect(item).toEqual(actualResData1[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields1[index])
      })

      let result2 = executeQuery(querySql2);
      let actualResData2 = result2.resData;
      let actualResFields2 = result2.resFields;

      //assert result data length for table 2
      expect(expectResData2.length).toEqual(actualResData2.length);
      //assert result data for table 2
      expectResData2.forEach((item, index) => {
        expect(item).toEqual(actualResData2[index]);
      });
      //assert result meta data for table 2
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields2[index])
      })

      let result3 = executeQuery(querySql3);
      let actualResData3 = result3.resData;
      let actualResFields3 = result3.resFields;

      //assert result data length for table 3
      expect(expectResData3.length).toEqual(actualResData3.length);
      //assert result data for table 3
      expectResData3.forEach((item, index) => {
        expect(item).toEqual(actualResData3[index]);
      });
      //assert result meta data for table 3
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields3[index])
      })

    });

  test(`name:bindParamBatchWithJson`+
  `author:${author};` +
  `desc:Using stmtBindParamBatch() bind one json tag table in a batch;` +
  `filename:${fileName};` +
  `result:${result}`, () => {
    let table = 'bindparambatch_121_j';//bind one table to one batch
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
        `json_tag json`+
        `);`;

      let insertSql = `insert into ? using ${table} tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);`;
      let querySql = `select * from ${table}`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(getBindData(), jsonTag, 14);

      // prepare json tag
      let jTags = new taos.TaosMultiBindArr(1);
      jTags.multiBindJSON(jsonTag);

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
      c1.stmtSetTbnameTags(`${table}_s01`, jTags.getMultiBindArr());
      c1.stmtBindParamBatch(mBinds.getMultiBindArr());
      c1.stmtAddBatch();
      c1.stmtExecute();
      c1.stmtClose();

      let result = executeQuery(querySql);
      let actualResData = result.resData;
      let actualResFields = result.resFields;

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
  })
})

