import { connect } from "../../index"
// const TDWebSocket = require("../../dist/main/index.js") 
import { createSTable, createSTableJSON, createTable, expectStableData, insertNTable, insertStable, jsonMeta, tableMeta, tagMeta } from "../utils";
const DSN = 'ws://root:taosdata@127.0.0.1:6041/rest/ws'
// const DSN = 'ws://root:taosdata@182.92.127.131:6041/rest/ws'
var ws = connect(DSN)
const table = 'ws_q_n';
const stable = 'ws_q_s';
const tableCN = 'ws_q_n_cn';
const stableCN = 'ws_q_s_cn';
const db = 'ws_q_db'
const jsonTable = 'ws_q_j';
const jsonTableCN = 'ws_q_j_cn';

const createDB = `create database if not exists ${db} keep 3650`
const dropDB = `drop database if exists ${db}`
const useDB = `use ${db}`

const stableTags = [true, -1, -2, -3, BigInt(-4), 1, 2, 3, BigInt(4), parseFloat(3.1415.toFixed(5)), parseFloat(3.14159265.toFixed(15)), 'varchar_tag_1', 'nchar_tag_1']
const stableCNTags = [false, -1 * 2, -2 * 2, -3 * 2, BigInt(-4 * 2), 1 * 2, 2 * 2, 3 * 2, BigInt(4 * 2), parseFloat((3.1415 * 2).toFixed(5)), parseFloat((3.14159265 * 2).toFixed(15)), 'varchar_标签_壹', 'nchar_标签_贰']

const tableValues = [
    [BigInt(1656677710000), 0, -1, -2, BigInt(-3), 0, 1, 2, BigInt(3), parseFloat(3.1415.toFixed(5)), parseFloat(3.14159265.toFixed(15)), 'varchar_col_1', 'nchar_col_1', true, 'NULL'],
    [BigInt(1656677720000), -1, -2, -3, BigInt(-4), 1, 2, 3, BigInt(4), parseFloat((3.1415 * 2).toFixed(5)), parseFloat((3.14159265 * 2).toFixed(15)), 'varchar_col_2', 'nchar_col_2', false, 'NULL'],
    [BigInt(1656677730000), -2, -3, -4, BigInt(-5), 2, 3, 4, BigInt(5), parseFloat((3.1415 * 3).toFixed(5)), parseFloat((3.14159265 * 3).toFixed(15)), 'varchar_col_3', 'nchar_col_3', true, 'NULL'],
    [BigInt(1656677740000), -3, -4, -5, BigInt(-6), 3, 4, 5, BigInt(6), parseFloat((3.1415 * 4).toFixed(5)), parseFloat((3.14159265 * 4).toFixed(15)), 'varchar_col_4', 'nchar_col_4', false, 'NULL'],
    [BigInt(1656677750000), -4, -5, -6, BigInt(-7), 4, 5, 6, BigInt(7), parseFloat((3.1415 * 5).toFixed(5)), parseFloat((3.14159265 * 5).toFixed(15)), 'varchar_col_5', 'nchar_col_5', true, 'NULL'],
]

const tableCNValues = [
    [BigInt(1656677760000), 0, -1, -2, BigInt(-3), 0, 1, 2, BigInt(3), parseFloat(3.1415.toFixed(5)), parseFloat(3.14159265.toFixed(15)), 'varchar_列_壹', 'nchar_列_甲', true, 'NULL'],
    [BigInt(1656677770000), -1, -2, -3, BigInt(-4), 1, 2, 3, BigInt(4), parseFloat((3.1415 * 2).toFixed(5)), parseFloat((3.14159265 * 2).toFixed(15)), 'varchar_列_贰', 'nchar_列_乙', false, 'NULL'],
    [BigInt(1656677780000), -2, -3, -4, BigInt(-5), 2, 3, 4, BigInt(5), parseFloat((3.1415 * 3).toFixed(5)), parseFloat((3.14159265 * 3).toFixed(15)), 'varchar_列_叁', 'nchar_列_丙', true, 'NULL'],
    [BigInt(1656677790000), -3, -4, -5, BigInt(-6), 3, 4, 5, BigInt(6), parseFloat((3.1415 * 4).toFixed(5)), parseFloat((3.14159265 * 4).toFixed(15)), 'varchar_列_肆', 'nchar_列_丁', false, 'NULL'],
    [BigInt(1656677800000), -4, -5, -6, BigInt(-7), 4, 5, 6, BigInt(7), parseFloat((3.1415 * 5).toFixed(5)), parseFloat((3.14159265 * 5).toFixed(15)), 'varchar_列_伍', 'nchar_列_戊', true, 'NULL'],
]
const jsonTags = ["{\"key1\":\"taos\",\"key2\":null,\"key3\":\"TDengine\",\"key4\":0,\"key5\":false}"]
const jsonTagsCN = ["{\"key1\":\"taosdata\",\"key2\":null,\"key3\":\"TDengine涛思数据\",\"key4\":1,\"key5\":true}"]

const selectStable = `select * from ${stable}`
const selectStableCN = `select * from ${stableCN}`
const selectTable = `select * from ${table}`
const selectTableCN = `select * from ${tableCN}`
const selectJsonTable = `select * from ${jsonTable}`
const selectJsonTableCN = `select * from ${jsonTableCN}`

beforeAll(async () => {
    return await ws.connect()
        .then(() => ws.query(createDB))
        .then(() => ws.query(useDB))
        .then(() => ws.query(createSTable(stable)))
        .then(() => ws.query(createSTable(stableCN)))
        .then(() => ws.query(createTable(table)))
        .then(() => ws.query(createTable(tableCN)))
        .then(() => ws.query(createSTableJSON(jsonTable)))
        .then(() => ws.query(createSTableJSON(jsonTableCN)))
        .catch((e) => { throw new Error(e) })
})

describe('ws.query(stable)', () => {
    test('Insert query stable without CN character', async () => {
        let insert = insertStable(tableValues, stableTags, stable)
        let insertRes = await ws.query(insert)
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectStable)

        let expectMeta = tableMeta.concat(tagMeta)
        let expectData = expectStableData(tableValues, stableTags)
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                ////   console.log(meta);
                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    // //   console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])
                })
            }
        } else {
            throw new Error("retrieve empty result")
        }
    })

    test('query stable with CN character', async () => {
        let insertCN = insertStable(tableCNValues, stableCNTags, stableCN)
        // console.log(insertCN)
        let insertRes = await ws.query(insertCN)
        // console.log(insertRes)
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectStableCN)

        let expectMeta = tableMeta.concat(tagMeta)
        let expectData = expectStableData(tableCNValues, stableCNTags)
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
                ////   console.log(meta);
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    // //   console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])

                })
            }
        } else {
            throw new Error("retrieve empty result")
        }


    })


})

describe('ws.query(table)', () => {
    test('Insert query normal table without CN character', async () => {
        let insert = insertNTable(tableValues, table)
        // console.log(insert)
        let insertRes = await ws.query(insert)
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectTable)

        let expectMeta = tableMeta
        let expectData = tableValues
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                // console.log(meta,expectMeta[index]);
                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    // //   console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])
                })
            }
        } else {
            throw new Error("retrieve empty result")
        }
    })

    test('Insert query normal table with CN character', async () => {
        let insertCN = insertNTable(tableCNValues, tableCN)
        // console.log(insertCN)
        let insertRes = await ws.query(insertCN)
        // console.log(insertRes)
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectTableCN)

        let expectMeta = tableMeta
        let expectData = tableCNValues
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                // console.log(meta, expectMeta[index]);

                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    // console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])

                })
            }
        } else {
            throw new Error("retrieve empty result")
        }
    })
})

describe('ws.query(jsonTable)', () => {
    test('Insert and query json data from table without CN', async () => {
        let insert = insertStable(tableValues, jsonTags, jsonTable)
        // console.log(insert)

        let insertRes = await ws.query(insert);
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectJsonTable)
        let expectMeta = tableMeta.concat(jsonMeta)
        let expectData = expectStableData(tableValues, jsonTags)
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                //   console.log(meta);
                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    //   console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])
                })
            }
        } else {
            throw new Error("retrieve empty result")
        }

    })


    test('Insert and query json data from table with CN', async () => {
        let insert = insertStable(tableCNValues, jsonTagsCN, jsonTableCN)
        // console.log(insert)

        let insertRes = await ws.query(insert);
        expect(insertRes.affectRows).toBe(5)

        let queryRes = await ws.query(selectJsonTableCN)
        let expectMeta = tableMeta.concat(jsonMeta)
        let expectData = expectStableData(tableCNValues, jsonTagsCN)
        let actualMeta = queryRes.getTDengineMeta()
        let actualData = queryRes.data

        if (actualData && actualMeta) {
            actualMeta.forEach((meta, index) => {
                //   console.log(meta);
                expect(meta.name).toBe(expectMeta[index].name)
                expect(meta.type).toBe(expectMeta[index].type)
                expect(meta.length).toBe(expectMeta[index].length)
            })

            for (let i = 0; i < actualData.length; i++) {
                actualData[i].forEach((d, index) => {
                    //   console.log(i, index, d, expectData[i][index])
                    expect(d).toBe(expectData[i][index])
                })
            }
        } else {
            throw new Error("retrieve empty result")
        }
    })

})

afterAll(async () => {
    await ws.query(dropDB)
    ws.close()
})

//--detectOpenHandles --maxConcurrency=1 --forceExit