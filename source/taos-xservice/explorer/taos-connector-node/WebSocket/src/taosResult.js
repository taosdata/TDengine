"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaosResult = void 0;
exports.parseBlock = parseBlock;
const constant_1 = require("./constant");
const wsError_1 = require("./wsError");
const ut8Helper_1 = require("./ut8Helper");
const moment = require("moment-timezone");
class TaosResult {
    constructor(queryResponse) {
        this.precision = 0;
        this.affectRows = 0;
        if (queryResponse.is_update == true) {
            this.meta = null;
            this.data = null;
        }
        else {
            if (queryResponse.fields_count &&
                queryResponse.fields_names &&
                queryResponse.fields_types &&
                queryResponse.fields_lengths) {
                let _meta = [];
                for (let i = 0; i < queryResponse.fields_count; i++) {
                    _meta.push({
                        name: queryResponse.fields_names[i],
                        type: queryResponse.fields_types[i],
                        length: queryResponse.fields_lengths[i],
                    });
                }
                this.meta = _meta;
            }
            else {
                throw new wsError_1.TaosResultError(`fields_count,fields_names,fields_types,fields_lengths of the update query response should be null`);
            }
            this.data = [];
        }
        this.affectRows = queryResponse.affected_rows;
        this.timing = queryResponse.timing;
        this.precision = queryResponse.precision;
        // console.log(`typeof this.timing:${typeof this.timing}, typeof fetchResponse.timing:${typeof queryResponse.timing}`)
    }
    setRows(fetchResponse) {
        this.affectRows += fetchResponse.rows;
        // console.log(`typeof this.timing:${typeof this.timing}, typeof fetchResponse.timing:${typeof fetchResponse.timing}`)
        this.timing = this.timing + fetchResponse.timing;
    }
    setData(fetchBlockResponse) {
        if (this.data) {
            this.data.push([]);
        }
        else {
            throw new wsError_1.TaosResultError(`update query response cannot set data`);
        }
    }
    /**
     * Mapping the WebSocket response type code to TDengine's type name.
     */
    getTDengineMeta() {
        if (this.meta) {
            let _ = new Array();
            this.meta.forEach((m) => {
                _.push({
                    name: m.name,
                    type: constant_1.TDengineTypeName[m.type],
                    length: m.length,
                });
            });
            return _;
        }
        else {
            return null;
        }
    }
}
exports.TaosResult = TaosResult;
function parseBlock(fetchResponse, blocks, taosResult, tz) {
    if (taosResult.meta && taosResult.data) {
        let metaList = taosResult.meta;
        // console.log(typeof taosResult.timing)
        // console.log(typeof blocks.timing)
        // console.log(blocks.id)
        taosResult.timing = BigInt(taosResult.timing) + blocks.timing;
        const INT_32_SIZE = 4;
        // Offset num of bytes from rawBlockBuffer.
        let bufferOffset = 4 * 5 + 8 + (4 + 1) * metaList.length;
        let colLengthBlockSize = INT_32_SIZE * metaList.length;
        // console.log("===colLengthBlockSize:" + colLengthBlockSize)
        let bitMapSize = (fetchResponse.rows + (1 << 3) - 1) >> 3;
        // whole raw block ArrayBuffer
        let dataBuffer = blocks.data.slice(bufferOffset);
        // record the head of column in block
        let colBlockHead = 0;
        for (let i = 0; i < fetchResponse.rows; i++) {
            let row = [];
            // point to the head of the column in the block
            colBlockHead = 0 + colLengthBlockSize;
            // point to the head of columns's data in the block (include bitMap and offsetArray)
            let colDataHead = colBlockHead;
            // traverse row after row.
            for (let j = 0; j < metaList.length; j++) {
                let isVarType = _isVarTye(metaList[j]);
                // console.log("== dataBuffer Length:" + dataBuffer.byteLength)
                // console.log("== loop i:" + i + "J=" + j + "col:" + metaList[j].name + "type:" + metaList[j].type)
                // console.log("== loop isVarType:" + isVarType);
                if (isVarType == constant_1.ColumnsBlockType.SOLID) {
                    colDataHead = colBlockHead + bitMapSize + metaList[j].length * i;
                    let byteArrayIndex = i >> 3;
                    let bitwiseOffset = 7 - (i & 7);
                    let bitMapArr = dataBuffer.slice(colBlockHead, colBlockHead + bitMapSize);
                    // console.log("==i:" + i + "byteArrayIndex=" + byteArrayIndex)
                    // console.log("== loop colblockhead:" + colBlockHead)
                    // console.log("== loop bitmap:" + bitMapSize)
                    // console.log("== loop bitMap length=" + bitMapArr.byteLength)
                    // console.log("==loop bitMap bitwiseoffset:" + bitwiseOffset + "byteArrayIndex:" + byteArrayIndex)
                    let bitFlag = (new DataView(bitMapArr).getUint8(byteArrayIndex) &
                        (1 << bitwiseOffset)) >>
                        bitwiseOffset;
                    if (bitFlag == 1) {
                        row.push("NULL");
                    }
                    else {
                        row.push(readSolidData(dataBuffer, colDataHead, metaList[j], tz));
                    }
                    // console.log("=====(new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0))=" + (new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0, true)));
                    colBlockHead =
                        colBlockHead +
                            bitMapSize +
                            new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0, true);
                }
                else {
                    // if null check
                    let varOffset = new DataView(dataBuffer, colBlockHead + INT_32_SIZE * i, INT_32_SIZE).getInt32(0, true);
                    // console.log("== var type offset:" + varOffset)
                    if (varOffset == -1) {
                        row.push("NULL");
                        colBlockHead =
                            colBlockHead +
                                INT_32_SIZE * fetchResponse.rows +
                                new DataView(dataBuffer, j * INT_32_SIZE, INT_32_SIZE).getInt32(0, true);
                    }
                    else {
                        colDataHead =
                            colBlockHead + INT_32_SIZE * fetchResponse.rows + varOffset;
                        let dataLength = new DataView(dataBuffer, colDataHead, 2).getInt16(0, true);
                        // console.log("== loop var type length:" + dataLength)
                        if (isVarType == constant_1.ColumnsBlockType.VARCHAR) {
                            row.push(readVarchar(dataBuffer, colDataHead + 2, dataLength));
                        }
                        else {
                            row.push(readNchar(dataBuffer, colDataHead + 2, dataLength));
                        }
                        colBlockHead =
                            colBlockHead +
                                INT_32_SIZE * fetchResponse.rows +
                                new DataView(dataBuffer, j * INT_32_SIZE, INT_32_SIZE).getInt32(0, true);
                    }
                }
            }
            taosResult.data.push(row);
        }
        return taosResult;
    }
    else {
        throw new wsError_1.TaosResultError("cannot fetch block for an update query.");
    }
}
function _isVarTye(meta) {
    switch (meta.type) {
        case constant_1.TDengineTypeCode["NCHAR"]: {
            return constant_1.ColumnsBlockType["NCHAR"];
        }
        case constant_1.TDengineTypeCode["VARCHAR"]: {
            return constant_1.ColumnsBlockType["VARCHAR"];
        }
        case constant_1.TDengineTypeCode["BINARY"]: {
            return constant_1.ColumnsBlockType["VARCHAR"];
        }
        case constant_1.TDengineTypeCode["JSON"]: {
            return constant_1.ColumnsBlockType["VARCHAR"];
        }
        default: {
            return constant_1.ColumnsBlockType["SOLID"];
        }
    }
}
function readSolidData(dataBuffer, colDataHead, meta, tz) {
    switch (meta.type) {
        case constant_1.TDengineTypeCode["BOOL"]: {
            return Boolean(new DataView(dataBuffer, colDataHead, 1).getInt8(0));
        }
        case constant_1.TDengineTypeCode["TINYINT"]: {
            return new DataView(dataBuffer, colDataHead, 1).getInt8(0);
        }
        case constant_1.TDengineTypeCode["SMALLINT"]: {
            return new DataView(dataBuffer, colDataHead, 2).getInt16(0, true);
        }
        case constant_1.TDengineTypeCode["INT"]: {
            return new DataView(dataBuffer, colDataHead, 4).getInt32(0, true);
        }
        case constant_1.TDengineTypeCode["BIGINT"]: {
            return new DataView(dataBuffer, colDataHead, 8).getBigInt64(0, true);
        }
        case constant_1.TDengineTypeCode["TINYINT UNSIGNED"]: {
            return new DataView(dataBuffer, colDataHead, 1).getUint8(0);
        }
        case constant_1.TDengineTypeCode["SMALLINT UNSIGNED"]: {
            return new DataView(dataBuffer, colDataHead, 2).getUint16(0, true);
        }
        case constant_1.TDengineTypeCode["INT UNSIGNED"]: {
            return new DataView(dataBuffer, colDataHead, 4).getUint32(0, true);
        }
        case constant_1.TDengineTypeCode["BIGINT UNSIGNED"]: {
            return new DataView(dataBuffer, colDataHead, 8).getBigUint64(0, true);
        }
        case constant_1.TDengineTypeCode["FLOAT"]: {
            return parseFloat(new DataView(dataBuffer, colDataHead, 4).getFloat32(0, true).toFixed(5));
        }
        case constant_1.TDengineTypeCode["DOUBLE"]: {
            return parseFloat(new DataView(dataBuffer, colDataHead, 8).getFloat64(0, true).toFixed(15));
        }
        case constant_1.TDengineTypeCode["TIMESTAMP"]: {
            //1680481718906
            let ts = new DataView(dataBuffer, colDataHead, 8).getBigInt64(0, true);
            let tsN = Number(ts);
            // 毫秒
            if (tsN / 10 ** 13 < 1) {
                return _toISODateTime(Number(tsN), tz);
            }
            // 微妙
            if (tsN / 10 ** 16 < 1) {
                return _toISODateTime(Number(tsN / 1000), tz);
            }
            // 纳秒
            if (tsN / 10 ** 19 < 1) {
                return _toISODateTime(Number(tsN / 1000000), tz);
            }
        }
        default: {
            throw new wsError_1.WebSocketQueryInterFaceError(`unspported type ${meta.type} for column ${meta.name}`);
        }
    }
}
function _toISODateTime(tsN, tz) {
    let result;
    if (tz) {
        result = moment(tsN).tz(tz).format();
    }
    else {
        result = new Date(Number(tsN)).toISOString();
    }
    return result;
}
function readVarchar(dataBuffer, colDataHead, length) {
    let data = "";
    data += new TextDecoder().decode(dataBuffer.slice(colDataHead, colDataHead + length));
    return data;
}
function readNchar(dataBuffer, colDataHead, length) {
    let decoder = new TextDecoder();
    let data = "";
    let buff = dataBuffer.slice(colDataHead, colDataHead + length);
    for (let i = 0; i < length / 4; i++) {
        // console.log("== readNchar data:" + new DataView(buff, i * 4, 4).getUint32(0, true))
        data += (0, ut8Helper_1.AppendRune)(new DataView(buff, i * 4, 4).getUint32(0, true));
    }
    return data;
}
function iteratorBuff(arr) {
    let buf = Buffer.from(arr);
    for (const value of buf) {
        console.log(value.toString());
    }
}
