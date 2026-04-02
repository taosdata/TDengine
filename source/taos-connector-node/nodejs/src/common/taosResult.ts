import { WSFetchBlockResponse, WSQueryResponse } from "../client/wsResponse";
import {
    ColumnsBlockType,
    TDengineTypeCode,
    TDengineTypeName,
} from "./constant";
import {
    ErrorCode,
    TaosResultError,
    WebSocketQueryInterFaceError,
} from "./wsError";
import { appendRune } from "./ut8Helper";
import logger from "./log";
import { decimalToString } from "./utils";

export interface TDengineMeta {
    name: string;
    type: string;
    length: number;
}

interface ResponseMeta {
    name: string;
    type: number;
    length: number;
}

export interface MessageResp {
    totalTime: number;
    msg: any;
}

export class TaosResult {
    private _topic?: string;
    private _meta: Array<ResponseMeta> | null;
    private _data: Array<Array<any>> | null;

    private _precision: number | null | undefined;
    protected _affectRows: number | null | undefined;
    private _totalTime = 0;
    private fields_precisions?: Array<bigint> | null;
    private fields_scales?: Array<bigint> | null;

    /** unit nano seconds */
    private _timing: bigint | null | undefined;
    constructor(queryResponse?: WSQueryResponse) {
        if (queryResponse == null) {
            this._meta = [];
            this._data = [];
            this._timing = BigInt(0);
            return;
        }
        if (queryResponse.is_update == true) {
            this._meta = null;
            this._data = null;
        } else {
            if (
                queryResponse.fields_count &&
                queryResponse.fields_names &&
                queryResponse.fields_types &&
                queryResponse.fields_lengths
            ) {
                let _meta = [];
                for (let i = 0; i < queryResponse.fields_count; i++) {
                    _meta.push({
                        name: queryResponse.fields_names[i],
                        type: queryResponse.fields_types[i],
                        length: queryResponse.fields_lengths[i],
                    });
                }
                this._meta = _meta;
            } else {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_FETCH_MESSAGE_DATA,
                    `fields_count,fields_names,fields_types,fields_lengths of the update query response should be null`
                );
            }
            this._data = [];
        }

        this._affectRows = queryResponse.affected_rows;
        this._timing = queryResponse.timing;
        this._precision = queryResponse.precision;
        this._totalTime = queryResponse.totalTime;
        this.fields_precisions = queryResponse.fields_precisions;
        this.fields_scales = queryResponse.fields_scales;
    }

    public setPrecision(precision: number) {
        this._precision = precision;
    }

    public setRowsAndTime(rows: number, timing?: bigint) {
        if (this._affectRows) {
            this._affectRows += rows;
        } else {
            this._affectRows = rows;
        }
        if (timing) {
            this.setTiming(timing);
        }
    }
    public getTopic(): string {
        if (this._topic) {
            return this._topic;
        }
        return "";
    }
    public setTopic(topic: string = "") {
        this._topic = topic;
    }
    public getMeta(): Array<TDengineMeta> | null {
        return this.getTDengineMeta();
    }

    public setMeta(metaData: ResponseMeta) {
        if (this._meta) {
            this._meta.push(metaData);
        }
    }

    public getData(): Array<Array<any>> | null {
        return this._data;
    }

    public setData(value: Array<Array<any>> | null) {
        this._data = value;
    }

    public getAffectRows(): number | null | undefined {
        return this._affectRows;
    }

    public getTaosMeta(): Array<ResponseMeta> | null {
        return this._meta;
    }

    public getPrecision(): number | null | undefined {
        return this._precision;
    }

    public getTotalTime() {
        return this._totalTime;
    }

    public addTotalTime(totalTime: number) {
        this._totalTime += totalTime;
    }

    public setTiming(timing?: bigint) {
        if (!this._timing) {
            this._timing = BigInt(0);
        }

        if (timing) {
            this._timing = this._timing + timing;
        }
    }

    public getFieldsScales(index: number): bigint | null {
        if (this.fields_scales) {
            return this.fields_scales[index];
        }
        return null;
    }

    /**
     * Mapping the WebSocket response type code to TDengine's type name.
     */
    private getTDengineMeta(): Array<TDengineMeta> | null {
        if (this._meta) {
            let tdMeta = new Array<TDengineMeta>();
            this._meta.forEach((m) => {
                tdMeta.push({
                    name: m.name,
                    type: TDengineTypeName[m.type],
                    length: m.length,
                });
            });
            return tdMeta;
        }
        return null;
    }
}

export function parseBlock(
    blocks: WSFetchBlockResponse,
    taosResult: TaosResult
): TaosResult {
    let metaList = taosResult.getTaosMeta();
    let dataList = taosResult.getData();
    let textDecoder = new TextDecoder();
    if (metaList && dataList && blocks && blocks.data) {
        let rows = blocks.data.getUint32(8, true);
        if (rows == 0) {
            return taosResult;
        }

        taosResult.setTiming(blocks.timing);
        const INT_32_SIZE = 4;
        // Offset num of bytes from rawBlockBuffer.
        let bufferOffset = 4 * 5 + 8 + (4 + 1) * metaList.length;
        let colLengthBlockSize = INT_32_SIZE * metaList.length;
        logger.debug("===colLengthBlockSize:" + colLengthBlockSize);

        let bitMapSize = (rows + (1 << 3) - 1) >> 3;

        // whole raw block ArrayBuffer
        // let dataBuffer = blocks.data.slice(bufferOffset);
        let headOffset = blocks.data.byteOffset + bufferOffset;
        let dataView = new DataView(blocks.data.buffer, headOffset);
        // record the head of column in block
        let colBlockHead = 0;
        for (let i = 0; i < rows; i++) {
            let row = [];
            // point to the head of the column in the block
            colBlockHead = 0 + colLengthBlockSize;
            // point to the head of columns's data in the block (include bitMap and offsetArray)
            let colDataHead = colBlockHead;
            // traverse row after row.
            for (let j = 0; j < metaList.length; j++) {
                let isVarType = _isVarType(metaList[j].type);
                if (isVarType == ColumnsBlockType.SOLID) {
                    colDataHead =
                        colBlockHead + bitMapSize + metaList[j].length * i;

                    let byteArrayIndex = i >> 3;
                    let bitwiseOffset = 7 - (i & 7);
                    // let bitMapArr = dataBuffer.slice(colBlockHead, colBlockHead + bitMapSize)
                    let bitMapArr = new DataView(
                        dataView.buffer,
                        dataView.byteOffset + colBlockHead,
                        bitMapSize
                    );
                    let bitFlag =
                        (bitMapArr.getUint8(byteArrayIndex) &
                            (1 << bitwiseOffset)) >>
                        bitwiseOffset;

                    if (bitFlag == 1) {
                        row.push("NULL");
                    } else {
                        row.push(
                            readSolidData(
                                dataView,
                                colDataHead,
                                metaList[j],
                                taosResult.getFieldsScales(j)
                            )
                        );
                    }

                    colBlockHead =
                        colBlockHead +
                        bitMapSize +
                        dataView.getInt32(INT_32_SIZE * j, true);
                } else {
                    // if null check
                    let varOffset = dataView.getInt32(
                        colBlockHead + INT_32_SIZE * i,
                        true
                    );
                    if (varOffset == -1) {
                        row.push("NULL");
                        colBlockHead =
                            colBlockHead +
                            INT_32_SIZE * rows +
                            dataView.getInt32(j * INT_32_SIZE, true);
                    } else {
                        colDataHead =
                            colBlockHead + INT_32_SIZE * rows + varOffset;
                        let dataLength = dataView.getInt16(colDataHead, true);
                        if (isVarType == ColumnsBlockType.VARCHAR) {
                            row.push(
                                readVarchar(
                                    dataView.buffer,
                                    dataView.byteOffset + colDataHead + 2,
                                    dataLength,
                                    textDecoder
                                )
                            );
                        } else if (
                            isVarType == ColumnsBlockType.GEOMETRY ||
                            isVarType == ColumnsBlockType.VARBINARY
                        ) {
                            row.push(
                                readBinary(
                                    dataView.buffer,
                                    dataView.byteOffset + colDataHead + 2,
                                    dataLength
                                )
                            );
                        } else {
                            row.push(
                                readNchar(
                                    dataView.buffer,
                                    dataView.byteOffset + colDataHead + 2,
                                    dataLength
                                )
                            );
                        }
                        colBlockHead =
                            colBlockHead +
                            INT_32_SIZE * rows +
                            dataView.getInt32(j * INT_32_SIZE, true);
                    }
                }
            }
            dataList.push(row);
        }
        return taosResult;
    } else {
        throw new TaosResultError(
            ErrorCode.ERR_INVALID_FETCH_MESSAGE_DATA,
            "cannot fetch block for an update query."
        );
    }
}

export function _isVarType(metaType: number): Number {
    switch (metaType) {
        case TDengineTypeCode.NCHAR: {
            return ColumnsBlockType["NCHAR"];
        }
        case TDengineTypeCode.VARCHAR: {
            return ColumnsBlockType["VARCHAR"];
        }
        case TDengineTypeCode.BINARY: {
            return ColumnsBlockType["VARCHAR"];
        }
        case TDengineTypeCode.JSON: {
            return ColumnsBlockType["VARCHAR"];
        }
        case TDengineTypeCode.GEOMETRY: {
            return ColumnsBlockType["GEOMETRY"];
        }
        case TDengineTypeCode.VARBINARY: {
            return ColumnsBlockType.VARBINARY;
        }
        default: {
            return ColumnsBlockType["SOLID"];
        }
    }
}

export function readSolidDataToArray(
    dataBuffer: DataView,
    colBlockHead: number,
    rows: number,
    metaType: number,
    bitMapArr: Uint8Array,
    startOffset: number,
    colIndex: number
): any[] {
    let result: any[] = [];
    switch (metaType) {
        case TDengineTypeCode.BOOL:
        case TDengineTypeCode.TINYINT:
        case TDengineTypeCode.TINYINT_UNSIGNED: {
            for (let i = 0; i < rows; i++, colBlockHead++) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getInt8(colBlockHead));
                }
            }
            break;
        }
        case TDengineTypeCode.SMALLINT: {
            for (let i = 0; i < rows; i++, colBlockHead += 2) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getInt16(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.INT: {
            for (let i = 0; i < rows; i++, colBlockHead += 4) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getInt32(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.BIGINT: {
            for (let i = 0; i < rows; i++, colBlockHead += 8) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getBigInt64(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.SMALLINT_UNSIGNED: {
            for (let i = 0; i < rows; i++, colBlockHead += 2) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getUint16(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.INT_UNSIGNED: {
            for (let i = 0; i < rows; i++, colBlockHead += 4) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getUint32(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.BIGINT_UNSIGNED: {
            for (let i = 0; i < rows; i++, colBlockHead += 8) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getBigUint64(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.FLOAT: {
            for (let i = 0; i < rows; i++, colBlockHead += 4) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(
                        parseFloat(
                            dataBuffer.getFloat32(colBlockHead, true).toFixed(5)
                        )
                    );
                }
            }
            break;
        }
        case TDengineTypeCode.DOUBLE: {
            for (let i = 0; i < rows; i++, colBlockHead += 8) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(
                        parseFloat(
                            dataBuffer
                                .getFloat64(colBlockHead, true)
                                .toFixed(15)
                        )
                    );
                }
            }
            break;
        }
        case TDengineTypeCode.TIMESTAMP: {
            for (let i = 0; i < rows; i++, colBlockHead += 8) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    result.push(dataBuffer.getBigInt64(colBlockHead, true));
                }
            }
            break;
        }
        case TDengineTypeCode.DECIMAL64: {
            let scale = getScaleFromRowBlock(dataBuffer, colIndex, startOffset);
            for (let i = 0; i < rows; i++, colBlockHead += 8) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    let decimalVal = dataBuffer.getBigInt64(colBlockHead, true);
                    result.push(
                        decimalToString(decimalVal.toString(), BigInt(scale))
                    );
                }
            }
            break;
        }
        case TDengineTypeCode.DECIMAL: {
            let scale = getScaleFromRowBlock(dataBuffer, colIndex, startOffset);
            for (let i = 0; i < rows; i++, colBlockHead += 16) {
                if (isNull(bitMapArr, i)) {
                    result.push(null);
                } else {
                    let decimalHighPart = dataBuffer.getBigInt64(
                        colBlockHead + 8,
                        true
                    );
                    const decimalLowPart = dataBuffer.getBigUint64(
                        colBlockHead,
                        true
                    );
                    const decimalCombined =
                        (decimalHighPart << 64n) | decimalLowPart;
                    result.push(
                        decimalToString(
                            decimalCombined.toString(),
                            BigInt(scale)
                        )
                    );
                }
            }
            break;
        }
        default: {
            throw new WebSocketQueryInterFaceError(
                ErrorCode.ERR_UNSUPPORTED_TDENGINE_TYPE,
                `unsupported type ${metaType}`
            );
        }
    }
    return result;
}

export function readSolidData(
    dataBuffer: DataView,
    colDataHead: number,
    meta: ResponseMeta,
    fields_scale: bigint | null
): Number | Boolean | BigInt | string {
    switch (meta.type) {
        case TDengineTypeCode.BOOL: {
            return Boolean(dataBuffer.getInt8(colDataHead));
        }
        case TDengineTypeCode.TINYINT: {
            return dataBuffer.getInt8(colDataHead);
        }
        case TDengineTypeCode.SMALLINT: {
            return dataBuffer.getInt16(colDataHead, true);
        }
        case TDengineTypeCode.INT: {
            return dataBuffer.getInt32(colDataHead, true);
        }
        case TDengineTypeCode.BIGINT: {
            return dataBuffer.getBigInt64(colDataHead, true);
        }
        case TDengineTypeCode.TINYINT_UNSIGNED: {
            return dataBuffer.getUint8(colDataHead);
        }
        case TDengineTypeCode.SMALLINT_UNSIGNED: {
            return dataBuffer.getUint16(colDataHead, true);
        }
        case TDengineTypeCode.INT_UNSIGNED: {
            return dataBuffer.getUint32(colDataHead, true);
        }
        case TDengineTypeCode.BIGINT_UNSIGNED: {
            return dataBuffer.getBigUint64(colDataHead, true);
        }
        case TDengineTypeCode.FLOAT: {
            return parseFloat(
                dataBuffer.getFloat32(colDataHead, true).toFixed(5)
            );
        }
        case TDengineTypeCode.DOUBLE: {
            return parseFloat(
                dataBuffer.getFloat64(colDataHead, true).toFixed(15)
            );
        }
        case TDengineTypeCode.TIMESTAMP: {
            return dataBuffer.getBigInt64(colDataHead, true);
        }
        case TDengineTypeCode.DECIMAL: {
            let decimalHighPart = dataBuffer.getBigInt64(colDataHead + 8, true);
            const decimalLowPart = dataBuffer.getBigUint64(colDataHead, true);
            const decimalCombined = (decimalHighPart << 64n) | decimalLowPart;
            return decimalToString(decimalCombined.toString(), fields_scale);
        }
        case TDengineTypeCode.DECIMAL64: {
            let decimalVal = dataBuffer.getBigInt64(colDataHead, true);
            return decimalToString(decimalVal.toString(), fields_scale);
        }
        default: {
            throw new WebSocketQueryInterFaceError(
                ErrorCode.ERR_UNSUPPORTED_TDENGINE_TYPE,
                `unsupported type ${meta.type} for column ${meta.name}`
            );
        }
    }
}

export function readBinary(
    dataBuffer: ArrayBuffer,
    colDataHead: number,
    length: number
): ArrayBuffer {
    let buff = dataBuffer.slice(colDataHead, colDataHead + length);
    return buff;
}

export function readVarchar(
    dataBuffer: ArrayBuffer,
    colDataHead: number,
    length: number,
    textDecoder: TextDecoder
): string {
    let dataView = new DataView(dataBuffer, colDataHead, length);
    return textDecoder.decode(dataView);
}

export function readNchar(
    dataBuffer: ArrayBuffer,
    colDataHead: number,
    length: number
): string {
    let data: string[] = [];
    let dataView = new DataView(dataBuffer, colDataHead, length);
    for (let i = 0; i < length / 4; i++) {
        data.push(appendRune(dataView.getUint32(i * 4, true)));
    }
    return data.join("");
}

export function getString(
    dataBuffer: DataView,
    colDataHead: number,
    length: number,
    textDecoder: TextDecoder
): string {
    let dataView = new Uint8Array(
        dataBuffer.buffer,
        dataBuffer.byteOffset + colDataHead,
        length - 1
    );
    return textDecoder.decode(dataView);
}

function iteratorBuff(arr: ArrayBuffer) {
    let buf = Buffer.from(arr);
    for (const value of buf) {
        logger.debug(value.toString());
    }
}

function isNull(bitMapArr: ArrayBuffer, n: number) {
    let c = new Uint8Array(bitMapArr);
    let position = n >>> 3;
    let index = n & 0x7;
    return (c[position] & (1 << (7 - index))) == 1 << (7 - index);
}

export function getCharOffset(n: number): number {
    return n >> 3;
}

export function setBitmapNull(c: number, n: number): number {
    return c + (1 << (7 - bitPos(n)));
}

function bitPos(n: number): number {
    return n & ((1 << 3) - 1);
}

export function bitmapLen(n: number): number {
    return (n + ((1 << 3) - 1)) >> 3;
}

function getScaleFromRowBlock(
    buffer: DataView,
    colIndex: number,
    startOffset: number
): number {
    let backupPos = buffer.byteOffset + startOffset + 28 + colIndex * 5 + 1;
    let scaleBuffer = new DataView(buffer.buffer, backupPos);
    let scale = scaleBuffer.getInt32(0, true);
    return scale & 0xff;
}
