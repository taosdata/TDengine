const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'stmt_q_db';

// This is a taos connection
let conn;
// This is a Cursor
let c1;

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
  //executeUpdate(`drop database if exists ${db};`);
  c1.close();
  conn.close();
});

describe("stmt_query", () => {
  test(`name:stmt query normal type;` +
    `author:${author};` +
    `desc:stmt query normal type;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'stmt_q_n';
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

      let colData = [1658286671000, true, -1, -2, -3, -4n, 3.1415, 3.141592654, 'binary_col_1', 'nchar_col_1', 1, 2, 3, 4n
        , 1658286672000, false, -2, -3, -4, -5n, 3.1415 * 2, 3.141592654 * 2, 'binary_col_2', 'nchar_col_2', 2, 3, 4, 5n
        , 1658286673000, true, -3, -4, -5, -6n, 3.1415 * 3, 3.141592654 * 3, 'binary_col_3', 'nchar_col_3', 3, 4, 5, 6n
        , 1658286674000, false, -4, -5, -6, -7n, 3.1415 * 4, 3.141592654 * 4, 'binary_col_4', 'nchar_col_4', 4, 5, 6, 7n
        , 1658286675000, true, -5, -6, -7, -8n, 3.1415 * 5, 3.141592654 * 5, 'binary_col_5', 'nchar_col_5', 5, 6, 7, 8n];
      let tagData = [true, 1, 2, 3, 4n, 3.1415, 3.141592654, 'binary_tag_1', 'nchar_tag_1', 1, 2, 3, 4n];
      let insertSql = buildInsertSql(table + "_s01", table, colData, tagData, 14);

      executeUpdate(createSql);
      executeUpdate(insertSql);

      let querySql = `select * from ${table} where ` +
        'ts<? and t_bl = ? and t_i8 = ? and t_i16 = ? ' +
        'and t_i32 = ? and t_i64 = ? and t_f32 = ?' +
        'and t_d64 = ? and t_bnr = ? and t_nchr= ?' +
        'and t_u8 = ? and t_u16 = ? and t_u32 = ? and t_u64 = ?'

      // stmt query steps

      let conditions = new taos.TaosMultiBindArr(15);
      conditions.multiBindTimestamp([1658286675000]);
      conditions.multiBindBool([true]);
      conditions.multiBindTinyInt([1]);
      conditions.multiBindSmallInt([2]);
      conditions.multiBindInt([3]);
      conditions.multiBindBigInt([4n]);
      conditions.multiBindFloat([3.1415]);
      conditions.multiBindDouble([3.141592654]);
      conditions.multiBindBinary(['binary_tag_1']);
      conditions.multiBindNchar(['nchar_tag_1']);
      conditions.multiBindUTinyInt([1]);
      conditions.multiBindUSmallInt([2]);
      conditions.multiBindUInt([3]);
      conditions.multiBindUBigInt([4n]);

      c1.stmtInit();
      c1.stmtPrepare(querySql);
      c1.stmtBindParamBatch(conditions.getMultiBindArr());
      c1.stmtExecute();
      c1.stmtUseResult();

      c1.fetchall();
      let actualResFields = c1.fields;
      let tmpData = c1.data;

      let expectResData = getResData(colData.slice(0, 56), tagData, 14);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));

      // assert feilds
      expectResField.forEach((item, index) => {
        expect(actualResFields[index]).toEqual(item);
      })

      // transfer data
      let actualResData = [];
      tmpData.forEach(row => {
        row.forEach(data => {
          if (data instanceof Date) {
            actualResData.push(data.taosTimestamp());
          } else {
            actualResData.push(data);
          }
        })
      })
      
      // assert data
      expectResData.forEach((item, index) => {
        expect(actualResData[index]).toEqual(item);
      })

      c1.stmtClose();
    })

  test(`name:stmt query JSON tag;` +
    `author:${author};` +
    `desc:stmt query JSON tag;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let table = 'stmt_q_json';
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

      let colData = [1658286671000, true, -1, -2, -3, -4n, 3.1415, 3.141592654, 'binary_col_1', 'nchar_col_1', 1, 2, 3, 4n
        , 1658286672000, false, -2, -3, -4, -5n, 3.1415 * 2, 3.141592654 * 2, 'binary_col_2', 'nchar_col_2', 2, 3, 4, 5n
        , 1658286673000, true, -3, -4, -5, -6n, 3.1415 * 3, 3.141592654 * 3, 'binary_col_3', 'nchar_col_3', 3, 4, 5, 6n
        , 1658286674000, false, -4, -5, -6, -7n, 3.1415 * 4, 3.141592654 * 4, 'binary_col_4', 'nchar_col_4', 4, 5, 6, 7n
        , 1658286675000, true, -5, -6, -7, -8n, 3.1415 * 5, 3.141592654 * 5, 'binary_col_5', 'nchar_col_5', 5, 6, 7, 8n];
      let tagData1 = ['{\"tag1\":false,\"tag2\":\"beijing\",\"tag3\":1}'];
      let tagData2 = ['{\"tag1\":false,\"tag2\":\"shanghai\",\"tag3\":2}'];
      let insertSql1 = buildInsertSql(table + "_s01", table, colData, tagData1, 14);
      let insertSql2 = buildInsertSql(table + "_s02", table, colData, tagData2, 14);

      executeUpdate(createSql);
      executeUpdate(insertSql1);
      executeUpdate(insertSql2);
      
      expect(2).toEqual(2);
      let querySql = `select * from ${table} where json_tag->'tag2'=? and json_tag->'tag3'<? and ts<? ;`
      //console.log(querySql);

      let conditions = new taos.TaosMultiBindArr(3);
      
      conditions.multiBindBinary(['beijing']);
      conditions.multiBindInt([2]);
      conditions.multiBindTimestamp([1658286675000]);

      c1.stmtInit();
      c1.stmtPrepare(querySql);
      c1.stmtBindParamBatch(conditions.getMultiBindArr());
      c1.stmtExecute();
      c1.stmtUseResult();

      c1.fetchall();
      let actualResFields = c1.fields;
      let tmpData = c1.data;

      let expectResData = getResData(colData.slice(0, 56), tagData1, 14);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));

      // assert feilds
      expectResField.forEach((item, index) => {
        expect(actualResFields[index]).toEqual(item);
      })

      // transfer data
      let actualResData = [];
      tmpData.forEach(row => {
        row.forEach(data => {
          if (data instanceof Date) {
            actualResData.push(data.taosTimestamp());
          } else {
            actualResData.push(data);
          }
        })
      })
      
      // assert data
      expectResData.forEach((item, index) => {
        expect(actualResData[index]).toEqual(item);
      })

      c1.stmtClose();
      
    })
}) 