const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

const db = "td_node_async";
const table = "async_query"
const createTable = `create table if not exists ${table} ` +
    `(ts timestamp,` +
    `bl bool,` +
    `i8 tinyint,` +
    `i16 smallint,` +
    `i32 int,` +
    `i64 bigint,` +
    `f32 float,` +
    `d64 double,` +
    `bnr binary(50),` +
    `nchr nchar(50),` +
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
    `t_bnr binary(50),` +
    `t_nchr nchar(50),` +
    `t_u8 tinyint unsigned,` +
    `t_u16 smallint unsigned,` +
    `t_u32 int unsigned,` +
    `t_u64 bigint unsigned` +
    `);`

// This is a taos connection
let conn;
// This is a Cursor
let c1;

function initData(numOfRows) {
    var data = [];
    var tag = [];
    const ts = 1654858800000;
    for (let i = 1; i <= numOfRows; i++) {
        data.push(ts + i);
        data.push(Boolean(Math.round(Math.random())))//bool
        data.push(-1 * i); //tinyint
        data.push(-10 * i);//smallint
        data.push(-100 * i); //int
        data.push(BigInt(-1000 * i)); //bigint
        data.push(parseFloat((Math.random() * (2 ** i)).toFixed(5))) //float
        data.push(parseFloat(((Math.random() * (2 ** i))).toFixed(5))) //double
        data.push(`taosdata${i}`); // binary
        data.push(`tdnegine涛思数据${i}`); // nchar
        data.push(1 * i); //tinyint
        data.push(10 * i);//smallint
        data.push(100 * i); //int
        data.push(BigInt(1000 * i)); //bigint
    }
    let i = 2;
    tag.push(Boolean(Math.round(Math.random())))//bool
    tag.push(-1 * i); //tinyint
    tag.push(-10 * i);//smallint
    tag.push(-100 * i); //int
    tag.push(BigInt(-1000 * i)); //bigint
    tag.push(parseFloat((Math.random() * (10 ** i)).toFixed(5))) //float
    tag.push(parseFloat((Math.random() * (10 ** i)).toFixed(5))) //double
    tag.push(`taosdata_tag`); // binary
    tag.push(`TDegine涛思数据_tag`); // nchar
    tag.push(1 * i); //tinyint
    tag.push(10 * i);//smallint
    tag.push(100 * i); //int
    tag.push(BigInt(1000 * i)); //bigint
    return { resData: data, resTag: tag }
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
    executeUpdate(`drop database if exists ${db};`);
    executeUpdate(`create database if not exists ${db} keep 3650;`);
    executeUpdate(`use ${db};`);
    executeUpdate(createTable);
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
    // executeUpdate(`drop database if exists ${db};`);
    c1.close();
    conn.close();
});


describe("test async query", () => {
    test(`name:test promise.execute_a()` +
        `author:${author};` +
        `desc:query data using TaosQuery's execute_a();` +
        `filename:${fileName};` +
        `result:${result}`, async () => {

            let expectResField = getFieldArr(getFeildsFromDll(createTable));
            let expectRes = initData(5);
            let colData = expectRes.resData;
            let tagData = expectRes.resTag;
            let insertSql = buildInsertSql(table + 'sb_1', table, colData, tagData, 14);
            let expectResData = getResData(colData, tagData, 14);

            executeUpdate(insertSql);
            //console.log(expectResData);

            let promise = c1.query(`select * from ${table};`);
            let resData = await promise.execute_a();

            var actualResData = [];;
            var actualResFields = resData.fields;

            // console.log(actualResData);
            resData.data.forEach(item => {
                // console.log(item.data);
                let tmpRow = item.data;
                tmpRow.forEach((item, index) => {
                    // console.log(item);
                    // console.log(actualResFields[index].type); //Timestamp
                    if (actualResFields[index].type == 'Timestamp') {
                        // console.log(item.valueOf());
                        actualResData.push(item.valueOf());
                    } else {
                        actualResData.push(item);
                    }
                })
            });

            // assert result data
            expectResData.forEach((item, index) => {
                // console.log("expectResData:" + item + " " + index);
                // console.log(actualResData[index]);
                expect(actualResData[index]).toBe(item);
            });

            // //assert result meta data
            expectResField.forEach((item, index) => {
                // console.log("expectResField:"+item);
                // console.log("actualResFields[index]._field:"+actualResFields[index]._field);
                expect(JSON.stringify(actualResFields[index]._field)).toBe(JSON.stringify(item))
            })
        })

})