import {
    PrecisionLength,
    TDengineTypeCode,
    TDengineTypeLength,
} from "../common/constant";
import { ErrorCode, TaosError } from "../common/wsError";
import { getCharOffset, setBitmapNull, bitmapLen } from "../common/taosResult";
import { isEmpty } from "../common/utils";
import { ColumnInfo } from "./wsColumnInfo";
import { IDataEncoder, StmtBindParams } from "./wsParamsBase";

export class Stmt1BindParams extends StmtBindParams implements IDataEncoder {
    constructor(precision?: number) {
        super(precision);
    }

    encode(): void {
        return;
    }

    mergeParams(bindParams: StmtBindParams): void {
        return;
    }

    addParams(
        params: any[],
        dataType: string,
        typeLen: number,
        columnType: number
    ): void {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "StmtBindParams params is invalid!"
            );
        }
        if (
            dataType === "number" ||
            dataType === "bigint" ||
            dataType === "boolean"
        ) {
            this._params.push(
                this.encodeDigitColumns(params, dataType, typeLen, columnType)
            );
        } else {
            if (columnType === TDengineTypeCode.NCHAR) {
                this._params.push(this.encodeNcharColumn(params));
            } else {
                this._params.push(
                    this.encodeVarLengthColumn(params, columnType)
                );
            }
        }
    }

    setTimestamp(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SeTimestampColumn params is invalid!"
            );
        }

        // computing bitmap length
        let bitMapLen: number = bitmapLen(params.length);
        // computing the length of data
        let arrayBuffer = new ArrayBuffer(
            bitMapLen +
            TDengineTypeLength[TDengineTypeCode.TIMESTAMP] * params.length
        );
        // bitmap get data range
        let bitmapBuffer = new DataView(arrayBuffer);
        // skip bitmap get data range
        let dataBuffer = new DataView(arrayBuffer, bitMapLen);
        if (this._rows > 0) {
            if (this._rows !== params.length) {
                throw new TaosError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "wrong row length!"
                );
            }
        } else {
            this._rows = params.length;
        }

        for (let i = 0; i < params.length; i++) {
            if (!isEmpty(params[i])) {
                if (params[i] instanceof Date) {
                    let date: Date = params[i];
                    // node only support milliseconds, need fill 0
                    if (this.precisionLength == PrecisionLength["us"]) {
                        let ms = date.getTime() * 1000;
                        dataBuffer.setBigInt64(i * 8, BigInt(ms), true);
                    } else if (this.precisionLength == PrecisionLength["ns"]) {
                        let ns = date.getTime() * 1000 * 1000;
                        dataBuffer.setBigInt64(i * 8, BigInt(ns), true);
                    } else {
                        dataBuffer.setBigInt64(
                            i * 8,
                            BigInt(date.getTime()),
                            true
                        );
                    }
                } else if (
                    typeof params[i] == "bigint" ||
                    typeof params[i] == "number"
                ) {
                    let data: bigint;
                    if (typeof params[i] == "number") {
                        data = BigInt(params[i]);
                    } else {
                        data = params[i];
                    }
                    // statistical bits of digit
                    let digit = this.countBigintDigits(data);
                    // check digit same table Precision
                    if (this.precisionLength == PrecisionLength["ns"]) {
                        if (this.precisionLength <= digit) {
                            dataBuffer.setBigInt64(i * 8, data, true);
                        } else {
                            throw new TaosError(
                                ErrorCode.ERR_INVALID_PARAMS,
                                "SeTimestampColumn params precisionLength is invalid! param:=" +
                                params[i]
                            );
                        }
                    } else if (this.precisionLength == digit) {
                        dataBuffer.setBigInt64(i * 8, data, true);
                    } else {
                        throw new TaosError(
                            ErrorCode.ERR_INVALID_PARAMS,
                            "SeTimestampColumn params is invalid! param:=" +
                            params[i]
                        );
                    }
                }
            } else {
                // set bitmap bit is null
                let charOffset = getCharOffset(i);
                let nullVal = setBitmapNull(dataBuffer.getInt8(charOffset), i);
                bitmapBuffer.setInt8(charOffset, nullVal);
            }
        }

        this._dataTotalLen += arrayBuffer.byteLength;
        this._params.push(
            new ColumnInfo(
                [
                    TDengineTypeLength[TDengineTypeCode.TIMESTAMP] *
                    params.length,
                    arrayBuffer,
                ],
                TDengineTypeCode.TIMESTAMP,
                TDengineTypeLength[TDengineTypeCode.TIMESTAMP],
                this._rows
            )
        );
    }

    private encodeDigitColumns(
        params: any[],
        dataType: string = "number",
        typeLen: number,
        columnType: number
    ): ColumnInfo {
        let bitMapLen: number = bitmapLen(params.length);
        let arrayBuffer = new ArrayBuffer(typeLen * params.length + bitMapLen);
        let bitmapBuffer = new DataView(arrayBuffer);
        let dataBuffer = new DataView(arrayBuffer, bitMapLen);
        if (this._rows > 0) {
            if (this._rows !== params.length) {
                throw new TaosError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "wrong row length!"
                );
            }
        } else {
            this._rows = params.length;
        }

        for (let i = 0; i < params.length; i++) {
            if (!isEmpty(params[i])) {
                this.writeDataToBuffer(
                    dataBuffer,
                    params[i],
                    dataType,
                    typeLen,
                    columnType,
                    i
                );
            } else {
                // set bitmap bit is null
                let charOffset = getCharOffset(i);
                let nullVal = setBitmapNull(
                    bitmapBuffer.getUint8(charOffset),
                    i
                );
                bitmapBuffer.setInt8(charOffset, nullVal);
            }
        }
        this._dataTotalLen += dataBuffer.buffer.byteLength;
        return new ColumnInfo(
            [typeLen * params.length, dataBuffer.buffer],
            columnType,
            typeLen,
            this._rows
        );
    }

    private encodeVarLengthColumn(
        params: any[],
        columnType: number
    ): ColumnInfo {
        let data: ArrayBuffer[] = [];
        let dataLength = 0;
        // create params length buffer
        let paramsLenBuffer = new ArrayBuffer(
            TDengineTypeLength[TDengineTypeCode.INT] * params.length
        );
        let paramsLenView = new DataView(paramsLenBuffer);
        if (this._rows > 0) {
            if (this._rows !== params.length) {
                throw new TaosError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "wrong row length!"
                );
            }
        } else {
            this._rows = params.length;
        }
        for (let i = 0; i < params.length; i++) {
            // get param length offset 4byte
            let offset = TDengineTypeLength[TDengineTypeCode.INT] * i;
            if (!isEmpty(params[i])) {
                // save param length offset 4byte
                paramsLenView.setInt32(offset, dataLength, true);
                if (typeof params[i] == "string") {
                    // string TextEncoder
                    let encode = new TextEncoder();
                    let value = encode.encode(params[i]).buffer;
                    data.push(value);
                    // add offset length
                    dataLength +=
                        value.byteLength +
                        TDengineTypeLength[TDengineTypeCode.SMALLINT];
                } else if (params[i] instanceof ArrayBuffer) {
                    // input arraybuffer, save not need encode
                    let value: ArrayBuffer = params[i];
                    dataLength +=
                        value.byteLength +
                        TDengineTypeLength[TDengineTypeCode.SMALLINT];
                    data.push(value);
                } else {
                    throw new TaosError(
                        ErrorCode.ERR_INVALID_PARAMS,
                        "getColumString params is invalid! param_type:=" +
                        typeof params[i]
                    );
                }
            } else {
                // set length -1, param is null
                for (
                    let j = 0;
                    j < TDengineTypeLength[TDengineTypeCode.INT];
                    j++
                ) {
                    paramsLenView.setInt8(offset + j, 255);
                }
            }
        }

        this._dataTotalLen += paramsLenBuffer.byteLength + dataLength;
        return new ColumnInfo(
            [
                dataLength,
                this.getBinaryColumnArrayBuffer(
                    data,
                    paramsLenView.buffer,
                    dataLength
                ),
            ],
            columnType,
            0,
            this._rows
        );
    }

    // splicing encode params to arraybuffer
    private getBinaryColumnArrayBuffer(
        data: ArrayBuffer[],
        paramsLenBuffer: ArrayBuffer,
        dataLength: number
    ): ArrayBuffer {
        // create arraybuffer
        let paramsBuffer = new ArrayBuffer(
            paramsLenBuffer.byteLength + dataLength
        );
        // get length data range
        const paramsUint8 = new Uint8Array(paramsBuffer);
        const paramsLenView = new Uint8Array(paramsLenBuffer);
        paramsUint8.set(paramsLenView, 0);
        // get data range
        const paramsView = new DataView(
            paramsBuffer,
            paramsLenBuffer.byteLength
        );

        let offset = 0;
        for (let i = 0; i < data.length; i++) {
            // save param field length
            paramsView.setInt16(offset, data[i].byteLength, true);
            const dataView = new DataView(data[i]);
            // save data
            for (let j = 0; j < data[i].byteLength; j++) {
                paramsView.setUint8(offset + 2 + j, dataView.getUint8(j));
            }
            offset += data[i].byteLength + 2;
        }

        return paramsBuffer;
    }

    // encode nchar type params
    private encodeNcharColumn(params: any[]): ColumnInfo {
        let data: ArrayBuffer[] = [];
        let dataLength = 0;
        let indexBuffer = new ArrayBuffer(
            TDengineTypeLength[TDengineTypeCode.INT] * params.length
        );
        let indexView = new DataView(indexBuffer);
        if (this._rows > 0) {
            if (this._rows !== params.length) {
                throw new TaosError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "wrong row length!"
                );
            }
        } else {
            this._rows = params.length;
        }

        for (let i = 0; i < params.length; i++) {
            let offset = TDengineTypeLength[TDengineTypeCode.INT] * i;
            if (!isEmpty(params[i])) {
                indexView.setInt32(offset, dataLength, true);
                if (typeof params[i] == "string") {
                    let codes: number[] = [];
                    let strNcharParams: string = params[i];
                    for (let j = 0; j < params[i].length; j++) {
                        // get char, cn char need 3~4 byte
                        codes.push(strNcharParams.charCodeAt(j));
                    }

                    let ncharBuffer: ArrayBuffer = new ArrayBuffer(
                        codes.length * 4
                    );
                    let ncharView = new DataView(ncharBuffer);
                    for (let j = 0; j < codes.length; j++) {
                        // char, save into uint32
                        ncharView.setUint32(j * 4, codes[j], true);
                    }
                    data.push(ncharBuffer);
                    dataLength +=
                        codes.length * 4 +
                        TDengineTypeLength[TDengineTypeCode.SMALLINT];
                } else if (params[i] instanceof ArrayBuffer) {
                    let value: ArrayBuffer = params[i];
                    dataLength +=
                        value.byteLength +
                        TDengineTypeLength[TDengineTypeCode.SMALLINT];
                    data.push(value);
                } else {
                    throw new TaosError(
                        ErrorCode.ERR_INVALID_PARAMS,
                        "getColumString params is invalid! param_type:=" +
                        typeof params[i]
                    );
                }
            } else {
                // set length -1, param is null
                for (
                    let j = 0;
                    j < TDengineTypeLength[TDengineTypeCode.INT];
                    j++
                ) {
                    indexView.setInt8(offset + j, 255);
                }
            }
        }

        this._dataTotalLen += indexBuffer.byteLength + dataLength;
        return new ColumnInfo(
            [
                dataLength,
                this.getBinaryColumnArrayBuffer(
                    data,
                    indexView.buffer,
                    dataLength
                ),
            ],
            TDengineTypeCode.NCHAR,
            0,
            this._rows
        );
    }

    private countBigintDigits(numeral: bigint): number {
        if (numeral === 0n) {
            return 1;
        }
        let count = 0;
        let temp = numeral;
        while (temp !== 0n) {
            temp /= 10n;
            count++;
        }
        return count;
    }
}
