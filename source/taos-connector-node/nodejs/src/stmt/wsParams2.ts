import {
    ColumnsBlockType,
    FieldBindType,
    PrecisionLength,
} from "../common/constant";
import { ErrorCode, TaosError } from "../common/wsError";
import { isEmpty } from "../common/utils";
import { ColumnInfo } from "./wsColumnInfo";
import { IDataEncoder, StmtBindParams } from "./wsParamsBase";
import { _isVarType } from "../common/taosResult";
import { FieldBindParams } from "./FieldBindParams";
import JSONBig from "json-bigint";
import { StmtFieldInfo } from "./wsProto";

export class Stmt2BindParams extends StmtBindParams implements IDataEncoder {
    private _fields: Array<StmtFieldInfo>;
    protected paramIndex: number = 0;

    constructor(
        paramsCount?: number,
        precision?: number,
        fields?: Array<StmtFieldInfo>
    ) {
        super(precision, paramsCount);
        this._fields = fields || [];
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
        if (this._fieldParams) {
            if (this.paramsCount > 0) {
                if (this._fieldParams[this.paramIndex]) {
                    if (
                        this._fieldParams[this.paramIndex].dataType !==
                        dataType ||
                        this._fieldParams[this.paramIndex].columnType !==
                        columnType
                    ) {
                        throw new TaosError(
                            ErrorCode.ERR_INVALID_PARAMS,
                            `StmtBindParams params type is not match! ${this.paramIndex
                            } ${this.paramsCount} ${JSONBig.stringify({
                                dataType,
                                columnType,
                            })} vs ${JSONBig.stringify({
                                dataType:
                                    this._fieldParams[this.paramIndex].dataType,
                                columnType:
                                    this._fieldParams[this.paramIndex]
                                        .columnType,
                            })}`
                        );
                    }
                    this._fieldParams[this.paramIndex].params.push(...params);
                } else {
                    let bindType = this._fields[this.paramIndex].bind_type || 0;
                    this._fieldParams[this.paramIndex] = new FieldBindParams(
                        params,
                        dataType,
                        typeLen,
                        columnType,
                        bindType
                    );
                    this._bindCount++;
                }
                this.paramIndex++;
                if (this.paramIndex >= this.paramsCount) {
                    this.paramIndex = 0;
                }
            } else {
                this._fieldParams.push(
                    new FieldBindParams(
                        params,
                        dataType,
                        typeLen,
                        columnType,
                        FieldBindType.TAOS_FIELD_COL
                    )
                );
            }
        }
    }

    mergeParams(bindParams: StmtBindParams): void {
        if (
            !bindParams ||
            !bindParams._fieldParams ||
            bindParams._fieldParams.length === 0 ||
            !this._fieldParams
        ) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "StmtBindParams params is invalid!"
            );
        }
        this.paramIndex = 0;
        for (let i = 0; i < bindParams._fieldParams.length; i++) {
            let fieldParam = bindParams._fieldParams[i];
            if (fieldParam) {
                this.addParams(
                    fieldParam.params,
                    fieldParam.dataType,
                    fieldParam.typeLen,
                    fieldParam.columnType
                );
            }
        }
    }

    encode(): void {
        this.paramIndex = 0;
        if (!this._fieldParams || this._fieldParams.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "StmtBindParams params is invalid!"
            );
        }

        if (this._rows > 0) {
            if (this._rows !== this._fieldParams[0].params.length) {
                throw new TaosError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "wrong row length!"
                );
            }
        } else {
            this._rows = this._fieldParams[0].params.length;
        }
        for (let i = 0; i < this._fieldParams.length; i++) {
            let fieldParam = this._fieldParams[i];
            if (!fieldParam) {
                continue;
            }

            let isVarType = _isVarType(fieldParam.columnType);
            if (isVarType == ColumnsBlockType.SOLID) {
                if (fieldParam.dataType === "TIMESTAMP") {
                    this._params.push(
                        this.encodeTimestampColumn(
                            fieldParam.params,
                            fieldParam.typeLen,
                            fieldParam.columnType
                        )
                    );
                } else {
                    this._params.push(
                        this.encodeDigitColumns(
                            fieldParam.params,
                            fieldParam.dataType,
                            fieldParam.typeLen,
                            fieldParam.columnType
                        )
                    );
                }
            } else {
                this._params.push(
                    this.encodeVarColumns(
                        fieldParam.params,
                        fieldParam.dataType,
                        fieldParam.typeLen,
                        fieldParam.columnType
                    )
                );
            }
        }
    }

    private encodeVarColumns(
        params: any[],
        dataType: string = "number",
        typeLen: number,
        columnType: number
    ): ColumnInfo {
        let isNull: number[] = [];
        let dataLengths: number[] = [];
        // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + 4 * v.length + totalLength
        // 17 + (5 * params.length) + totalLength;
        let totalLength = 17 + 5 * params.length;
        const bytes: number[] = [];
        for (let i = 0; i < params.length; i++) {
            if (!isEmpty(params[i])) {
                isNull.push(0);
                if (typeof params[i] == "string") {
                    let encoder = new TextEncoder().encode(params[i]);
                    let length = encoder.length;
                    totalLength += length;
                    dataLengths.push(length);
                    bytes.push(...encoder);
                } else if (params[i] instanceof ArrayBuffer) {
                    let value: ArrayBuffer = params[i];
                    totalLength += value.byteLength;
                    dataLengths.push(value.byteLength);
                    bytes.push(...new Uint8Array(value));
                } else {
                    throw new TaosError(
                        ErrorCode.ERR_INVALID_PARAMS,
                        "getColumString params is invalid! param_type:=" +
                        typeof params[i]
                    );
                }
            } else {
                isNull.push(1);
            }
        }
        this._dataTotalLen += totalLength;
        const dataBuffer = new Uint8Array(bytes).buffer;
        return new ColumnInfo(
            [totalLength, dataBuffer],
            columnType,
            typeLen,
            this._rows,
            isNull,
            dataLengths,
            1
        );
    }

    private encodeDigitColumns(
        params: any[],
        dataType: string = "number",
        typeLen: number,
        columnType: number
    ): ColumnInfo {
        let isNull: number[] = [];
        // TotalLength(4) + Type (4) + Num(4) + IsNull(1) * size + haveLength(1) + BufferLength(4) + size * dataLen
        let dataLength = 17 + (typeLen + 1) * params.length;
        let arrayBuffer = new ArrayBuffer(typeLen * params.length);
        let dataBuffer = new DataView(arrayBuffer);
        for (let i = 0; i < params.length; i++) {
            if (!isEmpty(params[i])) {
                isNull.push(0);
                this.writeDataToBuffer(
                    dataBuffer,
                    params[i],
                    dataType,
                    typeLen,
                    columnType,
                    i
                );
            } else {
                isNull.push(1);
                if (dataType === "bigint") {
                    this.writeDataToBuffer(
                        dataBuffer,
                        BigInt(0),
                        dataType,
                        typeLen,
                        columnType,
                        i
                    );
                } else {
                    this.writeDataToBuffer(
                        dataBuffer,
                        0,
                        dataType,
                        typeLen,
                        columnType,
                        i
                    );
                }
            }
        }

        this._dataTotalLen += dataLength;
        return new ColumnInfo(
            [dataLength, dataBuffer.buffer],
            columnType,
            typeLen,
            this._rows,
            isNull
        );
    }

    private encodeTimestampColumn(
        params: any[],
        typeLen: number,
        columnType: number
    ): ColumnInfo {
        let timeStamps = [];
        for (let i = 0; i < params.length; i++) {
            if (!isEmpty(params[i])) {
                let timeStamp: bigint = BigInt(0);
                if (params[i] instanceof Date) {
                    let date: Date = params[i];
                    if (this.precisionLength == PrecisionLength["us"]) {
                        timeStamp = BigInt(date.getTime() * 1000);
                    } else if (this.precisionLength == PrecisionLength["ns"]) {
                        timeStamp = BigInt(date.getTime() * 1000 * 1000);
                    } else {
                        timeStamp = BigInt(date.getTime());
                    }
                } else if (
                    typeof params[i] == "bigint" ||
                    typeof params[i] == "number"
                ) {
                    if (typeof params[i] == "number") {
                        timeStamp = BigInt(params[i]);
                    } else {
                        timeStamp = params[i];
                    }
                }
                timeStamps.push(timeStamp);
            } else {
                timeStamps.push(null);
            }
        }
        return this.encodeDigitColumns(
            timeStamps,
            "bigint",
            typeLen,
            columnType
        );
    }
}
