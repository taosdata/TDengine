import { WSFetchBlockResponse, WSFetchResponse, WSQueryResponse } from "./wsQueryResponse";
import { ColumnsBlockType, TDengineTypeCode, TDengineTypeName } from './constant'
import { TaosResultError, WebSocketQueryInterFaceError } from "./wsError";
import { AppendRune } from "./ut8Helper"

export class TaosResult {
    meta: Array<ResponseMeta> | null;
    data: Array<Array<any>> | null;
    precision: number = 0;
    affectRows: number = 0;
    /** unit nano seconds */
    timing: bigint;
    constructor(queryResponse: WSQueryResponse) {
        if (queryResponse.is_update == true) {
            this.meta = null
            this.data = null
        } else {
            if (queryResponse.fields_count && queryResponse.fields_names && queryResponse.fields_types && queryResponse.fields_lengths) {
                let _meta = [];
                for (let i = 0; i < queryResponse.fields_count; i++) {
                    _meta.push({
                        name: queryResponse.fields_names[i],
                        type: queryResponse.fields_types[i],
                        length: queryResponse.fields_lengths[i]
                    })
                }
                this.meta = _meta;
            } else {
                throw new TaosResultError(`fields_count,fields_names,fields_types,fields_lengths of the update query response should be null`)
            }
            this.data = [];
        }

        this.affectRows = queryResponse.affected_rows
        this.timing = queryResponse.timing
        this.precision = queryResponse.precision
        // console.log(`typeof this.timing:${typeof this.timing}, typeof fetchResponse.timing:${typeof queryResponse.timing}`)
    }

    setRows(fetchResponse: WSFetchResponse) {
        this.affectRows += fetchResponse.rows;
        // console.log(`typeof this.timing:${typeof this.timing}, typeof fetchResponse.timing:${typeof fetchResponse.timing}`)
        this.timing = this.timing + fetchResponse.timing
    }

    setData(fetchBlockResponse: WSFetchBlockResponse) {
        if (this.data) {
            this.data.push([])
        } else {
            throw new TaosResultError(`update query response cannot set data`)
        }

    }
    /**
     * Mapping the WebSocket response type code to TDengine's type name. 
     */
    getTDengineMeta(): Array<TDengineMeta> | null {
        if (this.meta) {
            let _ = new Array<TDengineMeta>()
            this.meta.forEach(m => {
                _.push({
                    name: m.name,
                    type: TDengineTypeName[m.type],
                    length: m.length
                })
            })
            return _;
        } else {
            return null;
        }
    }

}

interface TDengineMeta {
    name: string,
    type: string,
    length: number,
}
interface ResponseMeta {
    name: string,
    type: number,
    length: number,
}

export function parseBlock(fetchResponse: WSFetchResponse, blocks: WSFetchBlockResponse, taosResult: TaosResult): TaosResult {

    if (taosResult.meta && taosResult.data) {
        let metaList = taosResult.meta;

        // console.log(typeof taosResult.timing)
        // console.log(typeof blocks.timing)
        // console.log(blocks.id)

        taosResult.timing = BigInt(taosResult.timing) + blocks.timing

        const INT_32_SIZE = 4;

        // Offset num of bytes from rawBlockBuffer.
        let bufferOffset = (4 * 5) + 8 + (4 + 1) * metaList.length
        let colLengthBlockSize = INT_32_SIZE * metaList.length
        // console.log("===colLengthBlockSize:" + colLengthBlockSize)

        let bitMapSize = (fetchResponse.rows + (1 << 3) - 1) >> 3

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

                let isVarType = _isVarTye(metaList[j])
                // console.log("== dataBuffer Length:" + dataBuffer.byteLength)
                // console.log("== loop i:" + i + "J=" + j + "col:" + metaList[j].name + "type:" + metaList[j].type)
                // console.log("== loop isVarType:" + isVarType);
                if (isVarType == ColumnsBlockType.SOLID) {

                    colDataHead = colBlockHead + bitMapSize + metaList[j].length * i

                    let byteArrayIndex = i >> 3;
                    let bitwiseOffset = 7 - (i & 7)
                    let bitMapArr = dataBuffer.slice(colBlockHead, colBlockHead + bitMapSize)
                    // console.log("==i:" + i + "byteArrayIndex=" + byteArrayIndex)
                    // console.log("== loop colblockhead:" + colBlockHead)
                    // console.log("== loop bitmap:" + bitMapSize)
                    // console.log("== loop bitMap length=" + bitMapArr.byteLength)
                    // console.log("==loop bitMap bitwiseoffset:" + bitwiseOffset + "byteArrayIndex:" + byteArrayIndex)
                    let bitFlag = ((new DataView(bitMapArr).getUint8(byteArrayIndex)) & (1 << bitwiseOffset)) >> bitwiseOffset

                    if (bitFlag == 1) {
                        row.push("NULL")
                    } else {
                        row.push(readSolidData(dataBuffer, colDataHead, metaList[j]))
                    }
                    // console.log("=====(new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0))=" + (new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0, true)));
                    colBlockHead = colBlockHead + bitMapSize + (new DataView(dataBuffer, INT_32_SIZE * j, INT_32_SIZE).getInt32(0, true))

                } else {
                    // if null check
                    let varOffset = new DataView(dataBuffer, colBlockHead + (INT_32_SIZE * i), INT_32_SIZE).getInt32(0, true)
                    // console.log("== var type offset:" + varOffset)
                    if (varOffset == -1) {
                        row.push("NULL")
                        colBlockHead = colBlockHead + INT_32_SIZE * fetchResponse.rows + (new DataView(dataBuffer, j * INT_32_SIZE, INT_32_SIZE).getInt32(0, true))
                    } else {
                        colDataHead = colBlockHead + INT_32_SIZE * fetchResponse.rows + varOffset
                        let dataLength = (new DataView(dataBuffer, colDataHead, 2).getInt16(0, true))
                        // console.log("== loop var type length:" + dataLength)
                        if (isVarType == ColumnsBlockType.VARCHAR) {

                            row.push(readVarchar(dataBuffer, colDataHead + 2, dataLength))

                        } else {
                            row.push(readNchar(dataBuffer, colDataHead + 2, dataLength))
                        }
                        colBlockHead = colBlockHead + INT_32_SIZE * fetchResponse.rows + (new DataView(dataBuffer, j * INT_32_SIZE, INT_32_SIZE).getInt32(0, true))
                    }
                }
            }
            taosResult.data.push(row);
        }
        return taosResult;
    } else {
        throw new TaosResultError("cannot fetch block for an update query.")
    }

}

function _isVarTye(meta: ResponseMeta): Number {
    switch (meta.type) {
        case TDengineTypeCode['NCHAR']: {
            return ColumnsBlockType['NCHAR']
        }
        case TDengineTypeCode['VARCHAR']: {
            return ColumnsBlockType['VARCHAR']
        }
        case TDengineTypeCode['BINARY']: {
            return ColumnsBlockType['VARCHAR']
        }
        case TDengineTypeCode['JSON']: {
            return ColumnsBlockType['VARCHAR']
        }
        default: {
            return ColumnsBlockType['SOLID']
        }
    }
}

function readSolidData(dataBuffer: ArrayBuffer, colDataHead: number, meta: ResponseMeta): Number | Boolean | BigInt | String {

    switch (meta.type) {
        case TDengineTypeCode['BOOL']: {
            return (Boolean)(new DataView(dataBuffer, colDataHead, 1).getInt8(0))
        }
        case TDengineTypeCode['TINYINT']: {
            return (new DataView(dataBuffer, colDataHead, 1).getInt8(0))
        }
        case TDengineTypeCode['SMALLINT']: {
            return (new DataView(dataBuffer, colDataHead, 2).getInt16(0, true))
        }
        case TDengineTypeCode['INT']: {
            return (new DataView(dataBuffer, colDataHead, 4).getInt32(0, true))
        }
        case TDengineTypeCode['BIGINT']: {
            return (new DataView(dataBuffer, colDataHead, 8).getBigInt64(0, true))
        }
        case TDengineTypeCode['TINYINT UNSIGNED']: {
            return (new DataView(dataBuffer, colDataHead, 1).getUint8(0))
        }
        case TDengineTypeCode['SMALLINT UNSIGNED']: {
            return (new DataView(dataBuffer, colDataHead, 2).getUint16(0, true))
        }
        case TDengineTypeCode['INT UNSIGNED']: {
            return (new DataView(dataBuffer, colDataHead, 4).getUint32(0, true))
        }
        case TDengineTypeCode['BIGINT UNSIGNED']: {
            return (new DataView(dataBuffer, colDataHead, 8).getBigUint64(0, true))
        }
        case TDengineTypeCode['FLOAT']: {
            return (parseFloat(new DataView(dataBuffer, colDataHead, 4).getFloat32(0, true).toFixed(5)) )
        }
        case TDengineTypeCode['DOUBLE']: {
            return (parseFloat(new DataView(dataBuffer, colDataHead, 8).getFloat64(0, true).toFixed(15)))
        }
        case TDengineTypeCode['TIMESTAMP']: {
            //1680481718906
            let ts = new DataView(dataBuffer, colDataHead, 8).getBigInt64(0, true)
            let tsN = Number(ts)
            // 毫秒
            if (tsN / (10**13) < 1) {
                let d = new Date(Number(tsN))
                return d.toISOString()
            }
           // 微妙
           if (tsN / (10**16) < 1) {
                let d = new Date(Number(tsN/1000))
                return d.toISOString()
           }
           // 纳秒
           if (tsN / (10**19) < 1) {
            let d = new Date(Number(tsN/1000000))
            return d.toISOString()
       }

        }
        default: {
            throw new WebSocketQueryInterFaceError(`unspported type ${meta.type} for column ${meta.name}`)
        }
    }
}


function readVarchar(dataBuffer: ArrayBuffer, colDataHead: number, length: number): string {
    let data = "";
    data += new TextDecoder().decode(dataBuffer.slice(colDataHead, colDataHead + length))
    return data;
}

function readNchar(dataBuffer: ArrayBuffer, colDataHead: number, length: number): string {
    let decoder = new TextDecoder();
    let data = "";
    let buff: ArrayBuffer = dataBuffer.slice(colDataHead, colDataHead + length);
    for (let i = 0; i < length / 4; i++) {
        // console.log("== readNchar data:" + new DataView(buff, i * 4, 4).getUint32(0, true))
        data += AppendRune(new DataView(buff, i * 4, 4).getUint32(0, true))

    }
    return data;
}


function iteratorBuff(arr: ArrayBuffer) {
    let buf = Buffer.from(arr);
    for (const value of buf) {
        console.log(value.toString())
    }

}