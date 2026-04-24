import {
    TDengineTypeCode,
    TDengineTypeLength,
    TDengineTypeName,
    PrecisionLength,
} from "../common/constant";
import { ErrorCode, TaosError } from "../common/wsError";
import { FieldBindParams } from "./FieldBindParams";
import { ColumnInfo } from "./wsColumnInfo";

export interface IDataEncoder {
    encode(): void;
    addParams(
        params: any[],
        dataType: string,
        typeLen: number,
        columnType: number
    ): void;
    mergeParams(bindParams: StmtBindParams): void;
}

export abstract class StmtBindParams {
    protected readonly precisionLength: number = PrecisionLength["ms"];
    protected readonly _params: ColumnInfo[];
    _fieldParams?: FieldBindParams[];
    protected _dataTotalLen: number = 0;
    protected paramsCount: number = 0;
    protected _rows = 0;
    protected _bindCount = 0;

    constructor(precision?: number, paramsCount?: number) {
        if (precision) {
            this.precisionLength = precision;
        }
        this._params = [];
        if (paramsCount && paramsCount > 0) {
            this.paramsCount = paramsCount;
            this._fieldParams = new Array(paramsCount);
        } else {
            this.paramsCount = 0;
            this._fieldParams = [];
        }
    }

    abstract encode(): void;

    abstract addParams(
        params: any[],
        dataType: string,
        typeLen: number,
        columnType: number
    ): void;

    addBindFieldParams(fieldParams: FieldBindParams): void {
        if (
            !fieldParams ||
            !fieldParams.params ||
            fieldParams.params.length == 0
        ) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "StmtBindParams params is invalid!"
            );
        }
        if (!this._fieldParams) {
            this._fieldParams = [];
        }
        this.addParams(
            fieldParams.params,
            fieldParams.dataType,
            fieldParams.typeLen,
            fieldParams.columnType
        );
    }

    abstract mergeParams(bindParams: StmtBindParams): void;

    getBindCount(): number {
        return this._bindCount;
    }

    getDataRows(): number {
        return this._rows;
    }

    getDataTotalLen(): number {
        return this._dataTotalLen;
    }

    public getParams(): ColumnInfo[] {
        return this._params;
    }

    setBoolean(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBooleanColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "boolean",
            TDengineTypeLength[TDengineTypeCode.BOOL],
            TDengineTypeCode.BOOL
        );
    }

    setTinyInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetTinyIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.TINYINT],
            TDengineTypeCode.TINYINT
        );
    }

    setUTinyInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetUTinyIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.TINYINT_UNSIGNED],
            TDengineTypeCode.TINYINT_UNSIGNED
        );
    }

    setSmallInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetSmallIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.SMALLINT],
            TDengineTypeCode.SMALLINT
        );
    }

    setUSmallInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetSmallIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.SMALLINT_UNSIGNED],
            TDengineTypeCode.SMALLINT_UNSIGNED
        );
    }

    setInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.INT],
            TDengineTypeCode.INT
        );
    }

    setUInt(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetUIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.INT_UNSIGNED],
            TDengineTypeCode.INT_UNSIGNED
        );
    }

    setBigint(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBigIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "bigint",
            TDengineTypeLength[TDengineTypeCode.BIGINT],
            TDengineTypeCode.BIGINT
        );
    }

    setUBigint(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetUBigIntColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "bigint",
            TDengineTypeLength[TDengineTypeCode.BIGINT_UNSIGNED],
            TDengineTypeCode.BIGINT_UNSIGNED
        );
    }

    setFloat(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetFloatColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.FLOAT],
            TDengineTypeCode.FLOAT
        );
    }

    setDouble(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetDoubleColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            "number",
            TDengineTypeLength[TDengineTypeCode.DOUBLE],
            TDengineTypeCode.DOUBLE
        );
    }

    setVarchar(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetVarcharColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[8], 0, TDengineTypeCode.VARCHAR);
    }

    setBinary(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBinaryColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[8], 0, TDengineTypeCode.BINARY);
    }

    setNchar(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetNcharColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[10], 0, TDengineTypeCode.NCHAR);
    }

    setJson(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetJsonColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[15], 0, TDengineTypeCode.JSON);
    }

    setVarBinary(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetVarBinaryColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[16], 0, TDengineTypeCode.VARBINARY);
    }

    setBlob(params: any[]) {
        throw new TaosError(
            ErrorCode.ERR_UNSUPPORTED_TDENGINE_TYPE,
            "setBlob is not supported in stmt1, please use stmt2 instead!"
        );
    }

    setGeometry(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetGeometryColumn params is invalid!"
            );
        }
        this.addParams(params, TDengineTypeName[20], 0, TDengineTypeCode.GEOMETRY);
    }

    setDecimal(params: any[]) {
        throw new TaosError(
            ErrorCode.ERR_UNSUPPORTED_TDENGINE_TYPE,
            "setDecimal is not supported in stmt1, please use stmt2 instead!"
        );
    }

    setTimestamp(params: any[]) {
        if (!params || params.length == 0) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SeTimestampColumn params is invalid!"
            );
        }
        this.addParams(
            params,
            TDengineTypeName[9],
            TDengineTypeLength[TDengineTypeCode.TIMESTAMP],
            TDengineTypeCode.TIMESTAMP
        );
    }

    protected writeDataToBuffer(
        dataBuffer: DataView,
        params: any,
        dataType: string = "number",
        typeLen: number,
        columnType: number,
        i: number
    ): void {
        if (typeof params == dataType) {
            switch (columnType) {
                case TDengineTypeCode.BOOL: {
                    if (params) {
                        dataBuffer.setInt8(i, 1);
                    } else {
                        dataBuffer.setInt8(i, 0);
                    }
                    break;
                }
                case TDengineTypeCode.TINYINT: {
                    dataBuffer.setInt8(i, params);
                    break;
                }
                case TDengineTypeCode.TINYINT_UNSIGNED: {
                    dataBuffer.setUint8(i, params);
                    break;
                }
                case TDengineTypeCode.SMALLINT: {
                    dataBuffer.setInt16(i * 2, params, true);
                    break;
                }
                case TDengineTypeCode.SMALLINT_UNSIGNED: {
                    dataBuffer.setUint16(i * 2, params, true);
                    break;
                }
                case TDengineTypeCode.INT: {
                    dataBuffer.setInt32(i * 4, params, true);
                    break;
                }
                case TDengineTypeCode.INT_UNSIGNED: {
                    dataBuffer.setUint32(i * 4, params, true);
                    break;
                }
                case TDengineTypeCode.BIGINT:
                case TDengineTypeCode.TIMESTAMP: {
                    dataBuffer.setBigInt64(i * 8, params, true);
                    break;
                }
                case TDengineTypeCode.BIGINT_UNSIGNED: {
                    dataBuffer.setBigUint64(i * 8, params, true);
                    break;
                }
                case TDengineTypeCode.FLOAT: {
                    dataBuffer.setFloat32(i * 4, params, true);
                    break;
                }
                case TDengineTypeCode.DOUBLE: {
                    dataBuffer.setFloat64(i * 8, params, true);
                    break;
                }
                default: {
                    throw new TaosError(
                        ErrorCode.ERR_UNSUPPORTED_TDENGINE_TYPE,
                        "unsupported type for column" + columnType
                    );
                }
            }
        } else {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetTinyIntColumn params is invalid! param:=" + params
            );
        }
    }
}
