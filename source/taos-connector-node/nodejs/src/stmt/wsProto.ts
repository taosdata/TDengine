import { WSQueryResponse } from "../client/wsResponse";
import { TDengineTypeCode, TDengineTypeLength } from "../common/constant";
import { MessageResp } from "../common/taosResult";
import { StmtBindParams } from "./wsParamsBase";
import { ColumnInfo } from "./wsColumnInfo";
import { ErrorCode, TaosResultError } from "../common/wsError";
import { TableInfo } from "./wsTableInfo";

export interface StmtMessageInfo {
    action: string;
    args: StmtParamsInfo;
}

interface StmtParamsInfo {
    req_id: number;
    sql?: string | undefined | null;
    stmt_id?: bigint | undefined | null;
    name?: string | undefined | null;
    tags?: Array<any> | undefined | null;
    paramArray?: Array<Array<any>> | undefined | null;
}

export interface StmtFieldInfo {
    name: string | undefined | null;
    field_type: number | undefined | null;
    precision: number | undefined | null;
    scale: number | undefined | null;
    bytes: number | undefined | null;
    bind_type: number | undefined | null;
}

export class WsStmtQueryResponse extends WSQueryResponse {
    affected: number | undefined | null;
    stmt_id?: bigint | undefined | null;
    is_insert?: boolean | undefined | null;
    fields?: Array<StmtFieldInfo> | undefined | null;

    constructor(resp: MessageResp) {
        super(resp);
        this.stmt_id = BigInt(resp.msg.stmt_id);
        this.affected = resp.msg.affected;
        this.is_insert = resp.msg.is_insert;
        this.fields = resp.msg.fields;
    }
}

export const enum StmtBindType {
    STMT_TYPE_TAG = 1,
    STMT_TYPE_BIND = 2,
}

export function binaryBlockEncode(
    bindParams: StmtBindParams,
    bindType: StmtBindType,
    stmtId: bigint,
    reqId: bigint,
    row: number
): ArrayBuffer {
    // Computing the length of data
    let columns = bindParams.getParams().length;
    let length = TDengineTypeLength[TDengineTypeCode.BIGINT] * 4;
    length += TDengineTypeLength[TDengineTypeCode.INT] * 5;
    length += columns * 5 + columns * 4;
    length += bindParams.getDataTotalLen();

    let arrayBuffer = new ArrayBuffer(length);
    let arrayView = new DataView(arrayBuffer);

    arrayView.setBigUint64(0, reqId, true);
    arrayView.setBigUint64(8, stmtId, true);
    arrayView.setBigUint64(16, BigInt(bindType), true);
    // version int32
    arrayView.setUint32(24, 1, true);
    // data length int32
    arrayView.setUint32(28, arrayBuffer.byteLength, true);
    // rows int32
    arrayView.setUint32(32, row, true);
    // columns int32
    arrayView.setUint32(36, columns, true);
    // flagSegment int32
    arrayView.setUint32(40, 0, true);
    // groupID uint64
    arrayView.setBigUint64(44, BigInt(0), true);
    // head length
    let offset = 52;
    // type data range
    let typeView = new DataView(arrayBuffer, offset);
    // length data range
    let lenView = new DataView(arrayBuffer, offset + columns * 5);
    // data range offset
    let dataOffset = offset + columns * 5 + columns * 4;
    let headOffset = 0;
    let columnsData = bindParams.getParams();
    for (let i = 0; i < columnsData.length; i++) {
        // set column data type
        typeView.setUint8(headOffset, columnsData[i].type);
        // set column type length
        typeView.setUint32(headOffset + 1, columnsData[i].typeLen, true);
        // set column data length
        lenView.setUint32(i * 4, columnsData[i].length, true);
        if (columnsData[i].data) {
            // get encode column data
            const sourceView = new Uint8Array(columnsData[i].data);
            const destView = new Uint8Array(
                arrayBuffer,
                dataOffset,
                columnsData[i].data.byteLength
            );
            // splicing data
            destView.set(sourceView);
            dataOffset += columnsData[i].data.byteLength;
        }
        headOffset += 5;
    }

    return arrayBuffer;
}

function writeColumnToView(
    column: ColumnInfo,
    view: DataView,
    offset: number
): number {
    let currentOffset = offset;

    // length, type, _rows
    view.setInt32(currentOffset, column.length, true);
    currentOffset += TDengineTypeLength[TDengineTypeCode.INT];
    view.setInt32(currentOffset, column.type, true);
    currentOffset += TDengineTypeLength[TDengineTypeCode.INT];
    view.setInt32(currentOffset, column._rows, true);
    currentOffset += TDengineTypeLength[TDengineTypeCode.INT];

    // isNull bitmap
    if (column.isNull && column.isNull.length > 0) {
        const isNullBuffer = new Uint8Array(column.isNull);
        new Uint8Array(view.buffer, view.byteOffset + currentOffset).set(
            isNullBuffer
        );
        currentOffset += isNullBuffer.length;
    }

    // _haveLength and _dataLengths
    view.setUint8(currentOffset, column._haveLength);
    currentOffset += 1;
    if (column._haveLength === 1 && column._dataLengths) {
        for (const length of column._dataLengths) {
            view.setInt32(currentOffset, length, true);
            currentOffset += TDengineTypeLength[TDengineTypeCode.INT];
        }
    }

    // data
    view.setInt32(currentOffset, column.data.byteLength, true);
    currentOffset += TDengineTypeLength[TDengineTypeCode.INT];
    if (column.data.byteLength > 0) {
        new Uint8Array(view.buffer, view.byteOffset + currentOffset).set(
            new Uint8Array(column.data)
        );
        currentOffset += column.data.byteLength;
    }

    return currentOffset - offset; // Return the total number of bytes written
}

export function stmt2BinaryBlockEncode(
    reqId: bigint,
    stmtTableInfoList: TableInfo[],
    stmt_id: bigint | undefined | null,
    toBeBindTableNameIndex: number | undefined | null,
    toBeBindTagCount: number,
    toBeBindColCount: number
): ArrayBuffer {
    if (!stmt_id) {
        throw new TaosResultError(
            ErrorCode.ERR_INVALID_PARAMS,
            "stmt_id is invalid"
        );
    }

    const listLength = stmtTableInfoList.length;
    const result = {
        totalTableNameSize: 0,
        totalTagSize: 0,
        totalColSize: 0,
        tableNameSizeList: new Array<number>(listLength),
        tagSizeList: new Array<number>(listLength),
        colSizeList: new Array<number>(listLength),
    };

    const hasTableName = toBeBindTableNameIndex != null && toBeBindTableNameIndex >= 0;
    const hasTags = toBeBindTagCount > 0;
    for (let i = 0; i < listLength; i++) {
        const tableInfo = stmtTableInfoList[i];
        if (hasTableName) {
            const tableName = tableInfo.getTableName();
            if (!tableName)
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Table name is empty"
                );
            const size = tableInfo.getTableNameLength();
            if (size > 0) {
                result.tableNameSizeList[i] = size + 1;
                result.totalTableNameSize += size + 1;
            } else {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Table name len is empty"
                );
            }
        }

        if (hasTags) {
            const tagParams = tableInfo.getTags();
            if (!tagParams)
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Bind tags is empty!"
                );

            tagParams.encode();
            const size = tagParams.getDataTotalLen();
            result.tagSizeList[i] = size;
            result.totalTagSize += size;
        }

        const params = tableInfo.getParams();
        if (!params)
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Bind params is empty!"
            );
        params.encode();
        const size = params.getDataTotalLen();
        result.colSizeList[i] = size;
        result.totalColSize += size;
    }

    let totalSize = result.totalTableNameSize + result.totalTagSize + result.totalColSize;
    let toBeBindTableNameCount = toBeBindTableNameIndex != null && toBeBindTableNameIndex >= 0 ? 1 : 0;
    totalSize +=
        stmtTableInfoList.length *
        (toBeBindTableNameCount * 2 +
            (toBeBindTagCount > 0 ? 1 : 0) * 4 +
            (toBeBindColCount > 0 ? 1 : 0) * 4);

    let headerSize = 28; // Fixed header size
    let msgHeaderSize = 30; // Fixed msg header size
    const buffer = new ArrayBuffer(headerSize + msgHeaderSize + totalSize);
    const view = new DataView(buffer);
    let offset = 0;
    view.setBigUint64(offset, reqId, true);
    offset += TDengineTypeLength[TDengineTypeCode.BIGINT];
    view.setBigUint64(offset, stmt_id, true);
    offset += TDengineTypeLength[TDengineTypeCode.BIGINT];
    view.setBigUint64(offset, 9n, true);
    offset += TDengineTypeLength[TDengineTypeCode.BIGINT]; // action: bind
    view.setInt16(offset, 1, true);
    offset += TDengineTypeLength[TDengineTypeCode.SMALLINT]; // version
    view.setInt32(offset, -1, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT];
    view.setInt32(offset, totalSize + headerSize, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT]; // total length

    view.setInt32(offset, stmtTableInfoList.length, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT];
    view.setInt32(offset, toBeBindTagCount, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT];
    view.setInt32(offset, toBeBindColCount, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT];

    view.setInt32(offset, toBeBindTableNameCount > 0 ? 0x1c : 0, true);
    offset += TDengineTypeLength[TDengineTypeCode.INT];
    if (toBeBindTagCount) {
        if (toBeBindTableNameCount > 0) {
            view.setInt32(
                offset,
                headerSize +
                result.totalTableNameSize +
                2 * stmtTableInfoList.length,
                true
            );
        } else {
            view.setInt32(offset, headerSize, true);
        }
    } else {
        view.setInt32(offset, 0, true);
    }
    offset += TDengineTypeLength[TDengineTypeCode.INT];

    if (toBeBindColCount > 0) {
        let skipSize = 0;
        if (toBeBindTableNameCount > 0) {
            skipSize += result.totalTableNameSize + 2 * stmtTableInfoList.length;
        }

        if (toBeBindTagCount > 0) {
            skipSize += result.totalTagSize + 4 * stmtTableInfoList.length;
        }
        // colOffset = 28 + skipSize
        view.setInt32(offset, skipSize + headerSize, true);
    } else {
        view.setInt32(offset, 0, true);
    }
    offset += TDengineTypeLength[TDengineTypeCode.INT];

    if (toBeBindTableNameCount > 0) {
        let dataOffset =
            offset +
            result.tableNameSizeList.length *
            TDengineTypeLength[TDengineTypeCode.SMALLINT];
        for (let i = 0; i < listLength; i++) {
            view.setInt16(offset, result.tableNameSizeList[i], true);
            offset += TDengineTypeLength[TDengineTypeCode.SMALLINT];

            let tableName = stmtTableInfoList[i].getTableName();
            if (tableName && tableName.length > 0) {
                new Uint8Array(buffer, dataOffset).set(tableName);
                dataOffset += tableName.length;
                view.setUint8(dataOffset, 0);
                dataOffset += 1;
            } else {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Table name is empty"
                );
            }
        }
        offset = dataOffset;
    }

    if (toBeBindTagCount > 0) {
        let dataOffset =
            offset +
            result.tagSizeList.length *
            TDengineTypeLength[TDengineTypeCode.INT];
        for (let i = 0; i < listLength; i++) {
            view.setInt32(offset, result.tagSizeList[i], true);
            offset += TDengineTypeLength[TDengineTypeCode.INT];
            let tags = stmtTableInfoList[i].getTags();

            if (tags && tags.getParams().length > 0) {
                for (const col of tags.getParams()) {
                    dataOffset += writeColumnToView(
                        col,
                        new DataView(buffer, dataOffset),
                        0
                    );
                }
            } else {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Tags are empty"
                );
            }
        }
        offset = dataOffset;
    }

    // ColumnDataLength
    if (toBeBindColCount > 0) {
        let dataOffset =
            offset +
            result.colSizeList.length *
            TDengineTypeLength[TDengineTypeCode.INT];
        for (let i = 0; i < listLength; i++) {
            view.setInt32(offset, result.colSizeList[i], true);
            offset += TDengineTypeLength[TDengineTypeCode.INT];

            let params = stmtTableInfoList[i].getParams();
            if (!params) {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Bind params is empty!"
                );
            }
            for (const col of params.getParams()) {
                dataOffset += writeColumnToView(
                    col,
                    new DataView(buffer, dataOffset),
                    0
                );
            }
        }
    }
    return buffer;
}
