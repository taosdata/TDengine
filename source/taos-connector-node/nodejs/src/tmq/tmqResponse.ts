import { WSQueryResponse } from "../client/wsResponse";
import {
    ColumnsBlockType,
    TDengineTypeCode,
    TDengineTypeLength,
} from "../common/constant";
import {
    MessageResp,
    TaosResult,
    _isVarType,
    getString,
    readBinary,
    readNchar,
    readSolidDataToArray,
    readVarchar,
} from "../common/taosResult";
import {
    WebSocketInterfaceError,
    ErrorCode,
    TDWebSocketClientError,
} from "../common/wsError";
import { TMQBlockInfo, TMQRawDataSchema } from "./constant";
import { zigzagDecode } from "../common/utils";
import logger from "../common/log";

export class WsPollResponse {
    code: number;
    message: string;
    action: string;
    req_id: number;
    have_message: boolean;
    topic: string;
    database: string;
    vgroup_id: number;
    message_id: bigint;
    id: bigint;
    message_type: number;
    totalTime: number;
    constructor(resp: MessageResp) {
        this.totalTime = resp.totalTime;
        this.code = resp.msg.code;
        this.message = resp.msg.message;
        this.action = resp.msg.action;
        this.req_id = resp.msg.req_id;
        this.have_message = resp.msg.have_message;
        this.topic = resp.msg.topic;
        this.database = resp.msg.database;
        this.vgroup_id = resp.msg.vgroup_id;
        this.message_id = BigInt(resp.msg.message_id);
        this.message_type = resp.msg.message_type;
        if (resp.msg.id) {
            this.id = BigInt(resp.msg.id);
        } else {
            this.id = BigInt(0);
        }
    }
}

//  resp: {"code":0,"message":"","action":"fetch","req_id":4,"message_id":1,"completed":false,"table_name":"ct2","rows":1,"fields_count":4,"fields_names":["ts","c1","c2","c3"],"fields_types":[9,4,6,8],"fields_lengths":[8,4,4,10],"precision":0}
export class WsTmqQueryResponse extends WSQueryResponse {
    completed: boolean;
    table_name: string;
    rows: number;
    message_id: number;

    constructor(resp: MessageResp) {
        super(resp);
        this.completed = resp.msg.completed;
        this.table_name = resp.msg.table_name;
        this.rows = resp.msg.rows;
        this.message_id = resp.msg.message_id;
    }
}

export class TaosTmqResult extends TaosResult {
    database: string;
    vgroup_id: number;
    constructor(pollResp: WsPollResponse) {
        super();
        this.setTopic(pollResp.topic);
        this.database = pollResp.database;
        this.vgroup_id = pollResp.vgroup_id;
    }
}

export class WSTmqFetchBlockInfo {
    withTableName?: boolean;
    withSchema?: boolean;
    blockInfos?: Array<TMQBlockInfo>;
    schema: Array<TMQRawDataSchema>;
    tableName?: string;
    taosResult: TaosResult;
    schemaLen: number;
    rows: number;
    textDecoder: TextDecoder;
    constructor(dataView: DataView, taosResult: TaosResult) {
        // this.totalTime = resp.totalTime
        // this.blockData = resp.msg
        this.textDecoder = new TextDecoder();
        this.taosResult = taosResult;
        this.schema = [];
        this.schemaLen = 0;
        let blockDataView: DataView = this.skipHead(dataView);
        this.rows = this.parseBlockInfos(blockDataView);
    }
    public getRows(): number {
        return this.rows;
    }
    private skipHead(dataView: DataView): DataView {
        let v = dataView.getUint8(0);
        if (v >= 100) {
            let skip = dataView.getUint32(1, true);
            return new DataView(
                dataView.buffer,
                dataView.byteOffset + skip + 5
            );
        }
        let skip1 = this.getTypeSkip(v);
        v = dataView.getUint8(1 + skip1);
        let skip2 = this.getTypeSkip(v);
        return new DataView(
            dataView.buffer,
            dataView.byteOffset + skip1 + 2 + skip2
        );
    }

    private getTypeSkip(v: number) {
        switch (v) {
            case 1:
                return 8;
            case 2:
            case 3:
                return 16;
            default:
                throw new TDWebSocketClientError(
                    ErrorCode.ERR_INVALID_FETCH_MESSAGE_DATA,
                    `FetchBlockRawResp getTypeSkip error, type: ${v}`
                );
        }
    }

    private parseBlockInfos(dataView: DataView): number {
        let blockNum = dataView.getUint32(0, true);
        if (blockNum == 0) {
            return 0;
        }
        this.withTableName = dataView.getUint8(4) == 1 ? true : false;
        this.withSchema = dataView.getUint8(5) == 1 ? true : false;

        let dataBuffer = new DataView(dataView.buffer, dataView.byteOffset + 6);
        let rows = 0;
        for (let i = 0; i < blockNum; i++) {
            const variableInfo = this.parseVariableByteInteger(dataBuffer);
            this.taosResult.setPrecision(variableInfo[1].getUint8(17));
            dataView = new DataView(
                variableInfo[1].buffer,
                variableInfo[1].byteOffset + 17
            );
            let offset = variableInfo[0] - 17;
            dataBuffer = this.parseSchemaInfo(dataView, offset);
            rows += this.parseTmqBlock(dataView, 1);
        }
        logger.info(
            "parseBlockInfos blockNum=" +
            blockNum +
            ", withTableName=" +
            this.withTableName +
            ", withSchema=" +
            this.withSchema +
            ", rows=" +
            rows
        );
        return rows;
    }

    private parseSchemaInfo(dataBuffer: DataView, offset: number) {
        if (this.withSchema) {
            let isSkip = this.schema.length > 0;
            if (!isSkip) {
                dataBuffer = new DataView(
                    dataBuffer.buffer,
                    dataBuffer.byteOffset + offset
                );
                let variableInfo = this.parseVariableByteInteger(dataBuffer);
                this.schemaLen = variableInfo[2];
                let cols = zigzagDecode(variableInfo[0]);
                variableInfo = this.parseVariableByteInteger(variableInfo[1]);
                this.schemaLen += variableInfo[2];
                let dataView = variableInfo[1];
                for (let index = 0; index < cols; index++) {
                    let schema = new TMQRawDataSchema();
                    schema.colType = dataView.getInt8(0);
                    schema.flag = dataView.getInt8(1);
                    variableInfo = this.parseVariableByteInteger(dataView, 2);
                    this.schemaLen += 2 + variableInfo[2];
                    schema.bytes = BigInt(zigzagDecode(variableInfo[0]));
                    variableInfo = this.parseVariableByteInteger(
                        variableInfo[1]
                    );
                    this.schemaLen += variableInfo[2];
                    schema.colID = zigzagDecode(variableInfo[0]);
                    variableInfo = this.parseVariableByteInteger(
                        variableInfo[1]
                    );
                    this.schemaLen += variableInfo[2];
                    schema.name = getString(
                        variableInfo[1],
                        0,
                        variableInfo[0],
                        this.textDecoder
                    );

                    if (!isSkip) {
                        this.taosResult.setMeta({
                            name: schema.name,
                            type: schema.colType,
                            length: Number(schema.bytes),
                        });
                        this.schema.push(schema);
                    }
                    dataView = new DataView(
                        variableInfo[1].buffer,
                        variableInfo[1].byteOffset + variableInfo[0]
                    );
                    this.schemaLen += variableInfo[0];
                }

                if (this.withTableName) {
                    variableInfo = this.parseVariableByteInteger(dataView);
                    this.schemaLen += variableInfo[2];
                    this.tableName = readVarchar(
                        variableInfo[1].buffer,
                        variableInfo[1].byteOffset,
                        variableInfo[0],
                        this.textDecoder
                    );
                    dataView = new DataView(
                        variableInfo[1].buffer,
                        variableInfo[1].byteOffset + variableInfo[0]
                    );
                    this.schemaLen += variableInfo[0];
                }

                return dataView;
            } else {
                return new DataView(
                    dataBuffer.buffer,
                    dataBuffer.byteOffset + this.schemaLen + offset
                );
            }
        }
        return dataBuffer;
    }

    private parseVariableByteInteger(
        dataView: DataView,
        offset: number = 0
    ): [number, DataView, number] {
        let value = 0;
        let multiplier = 1;
        let count = 0;
        while (true) {
            let encodedByte = dataView.getUint8(count + offset);
            value += (encodedByte & 127) * multiplier;
            if ((encodedByte & 128) == 0) {
                break;
            }
            multiplier *= 128;
            count++;
        }

        return [
            value,
            new DataView(
                dataView.buffer,
                dataView.byteOffset + count + 1 + offset
            ),
            count + 1,
        ];
    }

    private parseTmqBlock(dataView: DataView, startOffset: number): number {
        // let dataView = new DataView(dataBuffer)
        let rows = dataView.getInt32(8 + startOffset, true);
        if (rows == 0) {
            return rows;
        }

        let taosData = this.taosResult.getData();
        let metaData = this.taosResult.getMeta();
        if (metaData && rows && taosData) {
            let dataList: any[][] = new Array(rows);
            //get bitmap length
            let bitMapOffset: number = getBitmapLen(rows);
            //skip data head
            let bufferOffset = 28 + 5 * this.schema.length + startOffset;
            let metaLens: number[] = [];
            for (let i = 0; i < this.schema.length; i++) {
                //get data len
                metaLens.push(dataView.getInt32(bufferOffset + i * 4, true));
            }

            bufferOffset += this.schema.length * 4;
            for (let i = 0; i < this.schema.length; i++) {
                let data: any[] = [];
                //get type code
                let isVarType = _isVarType(this.schema[i].colType);
                //fixed length type
                if (isVarType == ColumnsBlockType.SOLID) {
                    // let bitMapArr = dataBuffer.slice(bufferOffset, bufferOffset + bitMapOffset);
                    let bitMapArr = new Uint8Array(
                        dataView.buffer,
                        dataView.byteOffset + bufferOffset,
                        bitMapOffset
                    );
                    bufferOffset += bitMapOffset;
                    //decode column data, data is array
                    data = readSolidDataToArray(
                        dataView,
                        bufferOffset,
                        rows,
                        this.schema[i].colType,
                        bitMapArr,
                        startOffset,
                        i
                    );
                } else {
                    //Variable length type
                    let start = bufferOffset;
                    let offsets: number[] = [];
                    for (
                        let i = 0;
                        i < rows;
                        i++, start += TDengineTypeLength[TDengineTypeCode.INT]
                    ) {
                        //get data length, -1 is null
                        offsets.push(dataView.getInt32(start, true));
                    }

                    for (let i = 0; i < rows; i++) {
                        let value: any = "";
                        if (-1 == offsets[i]) {
                            value = null;
                        } else {
                            let header = start + offsets[i];
                            let dataLength =
                                dataView.getInt16(header, true) & 0xffff;
                            if (isVarType == ColumnsBlockType.VARCHAR) {
                                //decode var char

                                value = readVarchar(
                                    dataView.buffer,
                                    dataView.byteOffset + header + 2,
                                    dataLength,
                                    this.textDecoder
                                );
                            } else if (
                                isVarType == ColumnsBlockType.GEOMETRY ||
                                isVarType == ColumnsBlockType.VARBINARY
                            ) {
                                //decode binary
                                value = readBinary(
                                    dataView.buffer,
                                    dataView.byteOffset + header + 2,
                                    dataLength
                                );
                            } else {
                                //decode nchar
                                value = readNchar(
                                    dataView.buffer,
                                    dataView.byteOffset + header + 2,
                                    dataLength
                                );
                            }
                        }

                        data.push(value);
                    }
                    bufferOffset += rows * 4;
                }

                bufferOffset += metaLens[i];
                //column data to row data
                for (let row = 0; row < data.length; row++) {
                    if (dataList[row] == null) {
                        dataList[row] = [];
                    }
                    dataList[row].push(data[row]);
                }
            }
            taosData.push(...dataList);
        }

        return rows;
    }
}

export class AssignmentResp {
    req_id: number;
    code: number;
    message: string;
    action: string;
    totalTime: number;
    timing: bigint;
    topicPartition: TopicPartition[];
    constructor(resp: MessageResp, topic: string) {
        this.timing = BigInt(resp.msg.timing);
        this.code = resp.msg.code;
        this.message = resp.msg.message;
        this.req_id = resp.msg.req_id;
        this.action = resp.msg.action;
        this.totalTime = resp.totalTime;
        this.topicPartition = resp.msg.assignment;
        for (let i in this.topicPartition) {
            this.topicPartition[i].topic = topic;
        }
    }
}
export class SubscriptionResp {
    req_id: number;
    code: number;
    message: string;
    action: string;
    totalTime: number;
    timing: bigint;
    topics: string[];
    constructor(resp: MessageResp) {
        this.timing = BigInt(resp.msg.timing);
        this.code = resp.msg.code;
        this.message = resp.msg.message;
        this.req_id = resp.msg.req_id;
        this.action = resp.msg.action;
        this.totalTime = resp.totalTime;
        this.topics = resp.msg.topics;
    }
}

export class PartitionsResp {
    req_id: number;
    code: number;
    message: string;
    action: string;
    totalTime: number;
    timing: bigint;
    positions: bigint[];
    constructor(resp: MessageResp) {
        this.timing = BigInt(resp.msg.timing);
        this.code = resp.msg.code;
        this.message = resp.msg.message;
        this.req_id = resp.msg.req_id;
        this.action = resp.msg.action;
        this.totalTime = resp.totalTime;
        this.positions = resp.msg.position
            ? resp.msg.position.map((pos: number) => BigInt(pos))
            : [];
    }

    setTopicPartitions(topicPartitions: TopicPartition[]): TopicPartition[] {
        if (topicPartitions.length != this.positions.length) {
            throw new WebSocketInterfaceError(
                ErrorCode.ERR_PARTITIONS_TOPIC_VGROUP_LENGTH_NOT_EQUAL,
                "TopicPartitions and positions are not equal in length"
            );
        }
        for (let i in this.positions) {
            topicPartitions[i].offset = this.positions[i];
        }
        return topicPartitions;
    }
}

export class CommittedResp extends PartitionsResp {
    constructor(resp: MessageResp) {
        super(resp);
        this.positions = resp.msg.committed
            ? resp.msg.committed.map((pos: number) => BigInt(pos))
            : [];
    }
}

export class TopicPartition {
    topic: string;
    vgroup_id: number;
    offset?: bigint;
    begin?: bigint;
    end?: bigint;
    constructor(msg: any) {
        this.vgroup_id = msg.vgroup_id;
        this.offset = BigInt(msg.offset);
        this.begin = BigInt(msg.begin);
        this.end = BigInt(msg.end);
        this.topic = "";
    }
}

function getBitmapLen(n: number) {
    return (n + 0x7) >> 3;
}
