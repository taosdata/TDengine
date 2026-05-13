import JSONBig from "json-bigint";
import { WsClient } from "../client/wsClient";
import {
    ErrorCode,
    TDWebSocketClientError,
    TaosError,
    TaosResultError,
} from "../common/wsError";
import {
    WsStmtQueryResponse,
    StmtMessageInfo,
    binaryBlockEncode,
    StmtBindType,
} from "./wsProto";
import { ReqId } from "../common/reqid";
import { PrecisionLength } from "../common/constant";
import { StmtBindParams } from "./wsParamsBase";
import { WsStmt } from "./wsStmt";
import logger from "../common/log";
import { Stmt1BindParams } from "./wsParams1";
import { WSRows } from "../sql/wsRows";

export class WsStmt1 implements WsStmt {
    private _wsClient: WsClient;
    private _stmt_id: bigint | undefined | null;
    private _precision: number = PrecisionLength["ms"];
    private lastAffected: number | undefined | null;

    private constructor(wsClient: WsClient, precision?: number) {
        this._wsClient = wsClient;
        if (precision) {
            this._precision = precision;
        }
    }

    static async newStmt(
        wsClient: WsClient,
        precision?: number,
        reqId?: number
    ): Promise<WsStmt> {
        try {
            let wsStmt = new WsStmt1(wsClient, precision);
            return await wsStmt.init(reqId);
        } catch (e: any) {
            logger.error(`WsStmt init is failed, ${e.code}, ${e.message}`);
            throw e;
        }
    }

    async prepare(sql: string): Promise<void> {
        let queryMsg = {
            action: "prepare",
            args: {
                req_id: ReqId.getReqID(),
                sql: sql,
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    async setTableName(tableName: string): Promise<void> {
        let queryMsg = {
            action: "set_table_name",
            args: {
                req_id: ReqId.getReqID(),
                name: tableName,
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    async setJsonTags(tags: Array<any>): Promise<void> {
        let queryMsg = {
            action: "set_tags",
            args: {
                req_id: ReqId.getReqID(),
                tags: tags,
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    newStmtParam(): Stmt1BindParams {
        return new Stmt1BindParams(this._precision);
    }

    async setTags(paramsArray: StmtBindParams): Promise<void> {
        if (!paramsArray || !this._stmt_id) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBinaryTags paramArray is invalid!"
            );
        }

        let columnInfos = paramsArray.getParams();
        if (!columnInfos) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBinaryTags paramArray is invalid!"
            );
        }
        let reqId = BigInt(ReqId.getReqID());
        let dataBlock = binaryBlockEncode(
            paramsArray,
            StmtBindType.STMT_TYPE_TAG,
            this._stmt_id,
            reqId,
            paramsArray.getDataRows()
        );
        return await this.sendBinaryMsg(reqId, "set_tags", dataBlock);
    }

    async bind(paramsArray: StmtBindParams): Promise<void> {
        if (!paramsArray || !this._stmt_id) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "BinaryBind paramArray is invalid!"
            );
        }

        let columnInfos = paramsArray.getParams();
        if (!columnInfos) {
            throw new TaosError(
                ErrorCode.ERR_INVALID_PARAMS,
                "BinaryBind paramArray is invalid!"
            );
        }
        let reqId = BigInt(ReqId.getReqID());
        let dataBlock = binaryBlockEncode(
            paramsArray,
            StmtBindType.STMT_TYPE_BIND,
            this._stmt_id,
            reqId,
            paramsArray.getDataRows()
        );
        return await this.sendBinaryMsg(reqId, "bind", dataBlock);
    }

    async jsonBind(paramArray: Array<Array<any>>): Promise<void> {
        let queryMsg = {
            action: "bind",
            args: {
                req_id: ReqId.getReqID(),
                columns: paramArray,
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    async batch(): Promise<void> {
        let queryMsg = {
            action: "add_batch",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    async exec(): Promise<void> {
        let queryMsg = {
            action: "exec",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg);
    }

    async resultSet(): Promise<WSRows> {
        throw new Error("Method not implemented.");
    }

    getLastAffected() {
        return this.lastAffected;
    }

    async close(): Promise<void> {
        let queryMsg = {
            action: "close",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        return await this.execute(queryMsg, false);
    }

    public getStmtId(): bigint | undefined | null {
        return this._stmt_id;
    }

    private async execute(
        queryMsg: StmtMessageInfo,
        register: boolean = true
    ): Promise<void> {
        try {
            if (this._wsClient.getState() <= 0) {
                throw new TDWebSocketClientError(
                    ErrorCode.ERR_CONNECTION_CLOSED,
                    "websocket connect has closed!"
                );
            }
            let reqMsg = JSONBig.stringify(queryMsg);
            if (register) {
                let result = await this._wsClient.exec(reqMsg, false);
                let resp = new WsStmtQueryResponse(result);
                if (resp.stmt_id) {
                    this._stmt_id = resp.stmt_id;
                }

                if (resp.affected) {
                    this.lastAffected = resp.affected;
                }
            } else {
                await this._wsClient.execNoResp(reqMsg);
                this._stmt_id = null;
                this.lastAffected = null;
            }
            return;
        } catch (e: any) {
            throw new TaosResultError(e.code, e.message);
        }
    }

    private async sendBinaryMsg(
        reqId: bigint,
        action: string,
        message: ArrayBuffer
    ): Promise<void> {
        if (this._wsClient.getState() <= 0) {
            throw new TDWebSocketClientError(
                ErrorCode.ERR_CONNECTION_CLOSED,
                "websocket connect has closed!"
            );
        }
        let result = await this._wsClient.sendBinaryMsg(
            reqId,
            action,
            message,
            false
        );
        let resp = new WsStmtQueryResponse(result);
        if (resp.stmt_id) {
            this._stmt_id = resp.stmt_id;
        }

        if (resp.affected) {
            this.lastAffected = resp.affected;
        }
    }

    private async init(reqId?: number): Promise<WsStmt1> {
        if (this._wsClient) {
            try {
                if (this._wsClient.getState() <= 0) {
                    await this._wsClient.connect();
                    await this._wsClient.checkVersion();
                }
                let queryMsg = {
                    action: "init",
                    args: {
                        req_id: ReqId.getReqID(reqId),
                    },
                };
                await this.execute(queryMsg);
                return this;
            } catch (e: any) {
                logger.error(`stmt init filed, ${e.code}, ${e.message}`);
                throw e;
            }
        }
        throw new TDWebSocketClientError(
            ErrorCode.ERR_CONNECTION_CLOSED,
            "stmt connect closed"
        );
    }
}
