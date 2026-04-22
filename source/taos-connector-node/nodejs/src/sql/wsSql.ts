import { WSRows } from "./wsRows";
import { parseBlock, TaosResult } from "../common/taosResult";
import { WsClient } from "../client/wsClient";
import {
    ErrorCode,
    TDWebSocketClientError,
    TaosResultError,
    WebSocketInterfaceError,
} from "../common/wsError";
import { WSConfig } from "../common/config";
import { compareVersions, getBinarySql, getDsn } from "../common/utils";
import { WSFetchBlockResponse, WSQueryResponse } from "../client/wsResponse";
import { Precision, SchemalessMessageInfo, SchemalessProto } from "./wsProto";
import { ReqId } from "../common/reqid";
import {
    BinaryQueryMessage,
    FetchRawBlockMessage,
    PrecisionLength,
    TSDB_OPTION_CONNECTION,
} from "../common/constant";
import logger from "../common/log";
import { WsStmt } from "../stmt/wsStmt";
import { WsStmt1 } from "../stmt/wsStmt1";
import { WsStmt2 } from "../stmt/wsStmt2";

export class WsSql {
    private wsConfig: WSConfig;
    private _wsClient: WsClient;

    constructor(wsConfig: WSConfig) {
        const dsn = getDsn(wsConfig);
        this.wsConfig = wsConfig;
        this._wsClient = new WsClient(dsn, wsConfig.getTimeOut());
    }

    static async open(wsConfig: WSConfig): Promise<WsSql> {
        if (!wsConfig.getUrl()) {
            throw new WebSocketInterfaceError(
                ErrorCode.ERR_INVALID_URL,
                "invalid url, password or username needed."
            );
        }
        let wsSql = new WsSql(wsConfig);
        let database = wsConfig.getDb();
        try {
            await wsSql._wsClient.connect(database);
            await wsSql._wsClient.checkVersion();
            if (database && database.length > 0) {
                await wsSql.exec(`use ${database}`);
            } else {
                await wsSql.exec("use information_schema");
            }
            let timezone = wsConfig.getTimezone();
            if (timezone && timezone.length > 0) {
                await wsSql._wsClient.setOptionConnection(
                    TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_TIMEZONE,
                    timezone
                );
            }
            let userApp = wsConfig.getUserApp();
            await wsSql._wsClient.setOptionConnection(
                TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP,
                userApp && userApp.length > 0 ? userApp : null
            );
            let userIp = wsConfig.getUserIp();
            await wsSql._wsClient.setOptionConnection(
                TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP,
                userIp && userIp.length > 0 ? userIp : null
            );
            return wsSql;
        } catch (e: any) {
            logger.error(`WsSql open is failed, ${e.code}, ${e.message}`);
            if (wsSql) {
                await wsSql.close();
            }
            throw e;
        }
    }

    state() {
        return this._wsClient.getState();
    }

    /**
     * return client version.
     */
    async version(): Promise<string> {
        return await this._wsClient.version();
    }

    async close(): Promise<void> {
        await this._wsClient.close();
    }

    async schemalessInsert(
        lines: Array<string>,
        protocol: SchemalessProto,
        precision: Precision,
        ttl: number,
        reqId?: number
    ): Promise<void> {
        let data = "";
        if (!lines || lines.length == 0 || !protocol) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsSchemaless Insert params is error!"
            );
        }

        lines.forEach((element, index) => {
            data += element;
            if (index < lines.length - 1) {
                data += "\n";
            }
        });

        let queryMsg = {
            action: "insert",
            args: {
                req_id: ReqId.getReqID(reqId),
                protocol: protocol,
                precision: precision,
                data: data,
                ttl: ttl,
            },
        };
        return await this.executeSchemalessInsert(queryMsg);
    }

    async stmtInit(reqId?: number): Promise<WsStmt> {
        if (this._wsClient) {
            try {
                let precision = PrecisionLength["ms"];
                if (this.wsConfig.getDb()) {
                    let sql =
                        "select `precision` from information_schema.ins_databases where name = '" +
                        this.wsConfig.getDb() +
                        "'";
                    let result = await this.exec(sql);
                    let data = result.getData();

                    if (data && data[0] && data[0][0]) {
                        precision = PrecisionLength[data[0][0]];
                    }
                }
                let version = await this.version();
                let result = compareVersions(
                    version,
                    this.wsConfig.getMinStmt2Version()
                );
                if (result < 0) {
                    return await WsStmt1.newStmt(
                        this._wsClient,
                        precision,
                        reqId
                    );
                }
                return await WsStmt2.newStmt(this._wsClient, precision, reqId);
            } catch (e: any) {
                logger.error(
                    `stmtInit failed, code: ${e.code}, message: ${e.message}`
                );
                throw e;
            }
        }
        throw new TDWebSocketClientError(
            ErrorCode.ERR_CONNECTION_CLOSED,
            "stmt connect closed"
        );
    }

    async exec(
        sql: string,
        reqId?: number,
        action: string = "binary_query"
    ): Promise<TaosResult> {
        try {
            let bigintReqId = BigInt(ReqId.getReqID(reqId));
            let wsQueryResponse: WSQueryResponse = await this._wsClient.sendBinaryMsg(
                bigintReqId,
                action,
                getBinarySql(
                    BinaryQueryMessage,
                    bigintReqId,
                    BigInt(0),
                    sql
                )
            );
            let taosResult = new TaosResult(wsQueryResponse);
            if (wsQueryResponse.is_update) {
                return taosResult;
            } else {
                if (wsQueryResponse.id) {
                    try {
                        while (true) {
                            let bigintReqId = BigInt(ReqId.getReqID(reqId));
                            let resp = await this._wsClient.sendBinaryMsg(
                                bigintReqId,
                                action,
                                getBinarySql(
                                    FetchRawBlockMessage,
                                    bigintReqId,
                                    BigInt(wsQueryResponse.id)
                                ),
                                false,
                                true
                            );

                            taosResult.addTotalTime(resp.totalTime);
                            let wsResponse = new WSFetchBlockResponse(resp.msg);
                            if (wsResponse.code != 0) {
                                logger.error(
                                    `Executing SQL statement returns error: ${wsResponse.code}, ${wsResponse.message}`
                                );
                                throw new TaosResultError(
                                    wsResponse.code,
                                    wsResponse.message
                                );
                            }

                            if (wsResponse.finished == 1) {
                                break;
                            }
                            parseBlock(wsResponse, taosResult);
                        }

                        return taosResult;
                    } catch (err: any) {
                        throw new TaosResultError(err.code, err.message);
                    } finally {
                        await this._wsClient.freeResult(wsQueryResponse);
                    }
                }
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_FETCH_MESSAGE_DATA,
                    "The result data of the query is incorrect"
                );
            }
        } catch (err: any) {
            throw new TaosResultError(err.code, err.message);
        }
    }

    private async executeSchemalessInsert(
        queryMsg: SchemalessMessageInfo
    ): Promise<void> {
        return new Promise(async (resolve, reject) => {
            try {
                let reqMsg = JSON.stringify(queryMsg);
                let result = await this._wsClient.exec(reqMsg);
                logger.debug("executeSchemalessInsert:", reqMsg, result);
                resolve();
            } catch (e: any) {
                reject(new TaosResultError(e.code, e.message));
            }
        });
    }

    async query(sql: string, reqId?: number): Promise<WSRows> {
        try {
            let bigintReqId = BigInt(ReqId.getReqID(reqId));
            let wsQueryResponse: WSQueryResponse =
                await this._wsClient.sendBinaryMsg(
                    bigintReqId,
                    "binary_query",
                    getBinarySql(
                        BinaryQueryMessage,
                        bigintReqId,
                        BigInt(0),
                        sql
                    )
                );
            return new WSRows(this._wsClient, wsQueryResponse);
        } catch (err: any) {
            throw new TaosResultError(err.code, err.message);
        }
    }
}
