import JSONBig from "json-bigint";
import { WsClient } from "../client/wsClient";
import { FieldBindType, PrecisionLength } from "../common/constant";
import logger from "../common/log";
import { ReqId } from "../common/reqid";
import {
    ErrorCode,
    TaosResultError,
    TDWebSocketClientError,
} from "../common/wsError";
import { StmtBindParams } from "./wsParamsBase";
import {
    stmt2BinaryBlockEncode,
    StmtFieldInfo,
    StmtMessageInfo,
    WsStmtQueryResponse,
} from "./wsProto";
import { WsStmt } from "./wsStmt";
import { Stmt2BindParams } from "./wsParams2";
import { TableInfo } from "./wsTableInfo";
import { WSRows } from "../sql/wsRows";
import { FieldBindParams } from "./FieldBindParams";

enum StmtStep {
    INIT,
    PREPARE,
    BIND,
    EXEC,
    RESULT,
}

export class WsStmt2 implements WsStmt {
    private _wsClient: WsClient;
    private _stmt_id: bigint | undefined | null;
    private _precision: number = PrecisionLength["ms"];
    private fields?: Array<StmtFieldInfo> | undefined | null;
    private lastAffected: number | undefined | null;
    private _stmtTableInfo: Map<string, TableInfo>;
    private _currentTableInfo: TableInfo;
    private _stmtTableInfoList: TableInfo[];
    private _toBeBindTagCount: number;
    private _toBeBindColCount: number;
    private _toBeBindTableNameIndex: number | undefined | null;
    private _isInsert: boolean = false;
    private _savedSql: string | undefined;
    private _savedBindBytes: ArrayBuffer | undefined;

    private constructor(wsClient: WsClient, precision?: number) {
        this._wsClient = wsClient;
        if (precision) {
            this._precision = precision;
        }
        this._stmtTableInfo = new Map<string, TableInfo>();
        this._currentTableInfo = new TableInfo();
        this._stmtTableInfoList = [];
        this._toBeBindColCount = 0;
        this._toBeBindTagCount = 0;
    }

    static async newStmt(
        wsClient: WsClient,
        precision?: number,
        reqId?: number
    ): Promise<WsStmt> {
        try {
            const wsStmt = new WsStmt2(wsClient, precision);
            return await wsStmt.init(reqId);
        } catch (e: any) {
            logger.error(`WsStmt2 init failed, ${e.code}, ${e.message}`);
            throw e;
        }
    }

    private async init(reqId: number | undefined): Promise<WsStmt2> {
        if (this._wsClient) {
            try {
                if (this._wsClient.getState() <= 0) {
                    await this._wsClient.connect();
                    await this._wsClient.checkVersion();
                }
                await this.doInit(reqId);
                return this;
            } catch (e: any) {
                if (this._wsClient.isNetworkError(e)) {
                    await this.recover(StmtStep.INIT);
                    return this;
                }
                logger.error(`stmt2 init failed, ${e.code}, ${e.message}`);
                throw e;
            }
        }
        throw new TDWebSocketClientError(
            ErrorCode.ERR_CONNECTION_CLOSED,
            "stmt2 connect closed"
        );
    }

    private async doInit(reqId?: number): Promise<void> {
        const msg = {
            action: "stmt2_init",
            args: {
                req_id: ReqId.getReqID(reqId),
                single_stb_insert: true,
                single_table_bind_once: true,
            },
        };
        await this.execute(msg);
    }

    async prepare(sql: string): Promise<void> {
        this._savedSql = sql;
        try {
            await this.doPrepare(sql);
        } catch (err: any) {
            if (!this._wsClient.isNetworkError(err)) {
                throw err;
            }
            await this.recover(StmtStep.PREPARE);
        }
    }

    private async doPrepare(sql: string): Promise<void> {
        const msg = {
            action: "stmt2_prepare",
            args: {
                req_id: ReqId.getReqID(),
                sql: sql,
                stmt_id: this._stmt_id,
                get_fields: true,
            },
        };
        const resp = await this.execute(msg);
        if (!resp) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "stmt2_prepare response is empty"
            );
        }

        if (this._isInsert && this.fields) {
            this._precision = this.fields[0].precision ? this.fields[0].precision : 0;
            this._toBeBindColCount = 0;
            this._toBeBindTagCount = 0;
            this.fields.forEach((field, index) => {
                if (field.bind_type == FieldBindType.TAOS_FIELD_TBNAME) {
                    this._toBeBindTableNameIndex = index;
                } else if (field.bind_type == FieldBindType.TAOS_FIELD_TAG) {
                    this._toBeBindTagCount++;
                } else if (field.bind_type == FieldBindType.TAOS_FIELD_COL) {
                    this._toBeBindColCount++;
                }
            });
        } else {
            if (resp.fields_count && resp.fields_count > 0) {
                this._stmtTableInfoList = [this._currentTableInfo];
                this._toBeBindColCount = resp.fields_count;
            } else {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "prepare No columns to bind!"
                );
            }
        }
    }

    async setTableName(tableName: string): Promise<void> {
        if (!tableName || tableName.length === 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Table name cannot be empty"
            );
        }
        const tableInfo = this._stmtTableInfo.get(tableName);
        if (!tableInfo) {
            this._currentTableInfo = new TableInfo(tableName);
            this._stmtTableInfo.set(tableName, this._currentTableInfo);
            this._stmtTableInfoList.push(this._currentTableInfo);
        } else {
            this._currentTableInfo = tableInfo;
        }
        return Promise.resolve();
    }

    async setTags(paramsArray: StmtBindParams): Promise<void> {
        if (!paramsArray || !this._stmt_id) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "SetBinaryTags paramArray is invalid!"
            );
        }
        if (!this._currentTableInfo) {
            this._currentTableInfo = new TableInfo();
            this._stmtTableInfoList.push(this._currentTableInfo);
        }
        await this._currentTableInfo.setTags(paramsArray);
        return Promise.resolve();
    }

    newStmtParam(): StmtBindParams {
        if (this._isInsert) {
            if (!this.fields || this.fields.length === 0) {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "No columns to bind!"
                );
            }
            return new Stmt2BindParams(
                this.fields.length,
                this._precision,
                this.fields
            );
        }
        return new Stmt2BindParams();
    }

    async bind(paramsArray: StmtBindParams): Promise<void> {
        if (!paramsArray || !this._stmt_id || !paramsArray._fieldParams) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Bind paramArray is invalid!"
            );
        }

        if (this._isInsert && this.fields && paramsArray.getBindCount() == this.fields.length) {
            const tableNameIndex = this._toBeBindTableNameIndex;
            if (tableNameIndex === null || tableNameIndex === undefined) {
                throw new TaosResultError(
                    ErrorCode.ERR_INVALID_PARAMS,
                    "Table name index is null or undefined!"
                );
            }

            const paramsCount = paramsArray._fieldParams[0].params.length;
            for (let i = 0; i < paramsCount; i++) {
                const tableName = paramsArray._fieldParams[tableNameIndex].params[i];
                await this.setTableName(tableName);
                for (let j = 0; j < paramsArray._fieldParams.length; j++) {
                    if (j == tableNameIndex) {
                        continue;
                    }
                    const fieldParam = paramsArray._fieldParams[j];
                    if (this.fields[j].bind_type == FieldBindType.TAOS_FIELD_TAG) {
                        if (!this._currentTableInfo.tags) {
                            this._currentTableInfo.tags = new Stmt2BindParams(
                                this._toBeBindTagCount,
                                this._precision,
                                this.fields
                            );
                        }
                        this._currentTableInfo.tags.addBindFieldParams(
                            new FieldBindParams(
                                [fieldParam.params[i]],
                                fieldParam.dataType,
                                fieldParam.typeLen,
                                fieldParam.columnType,
                                fieldParam.bindType
                            )
                        );
                    } else if (this.fields[j].bind_type == FieldBindType.TAOS_FIELD_COL) {
                        if (!this._currentTableInfo.params) {
                            this._currentTableInfo.params = new Stmt2BindParams(
                                this._toBeBindColCount,
                                this._precision,
                                this.fields
                            );
                        }
                        this._currentTableInfo.params.addBindFieldParams(
                            new FieldBindParams(
                                [fieldParam.params[i]],
                                fieldParam.dataType,
                                fieldParam.typeLen,
                                fieldParam.columnType,
                                fieldParam.bindType
                            )
                        );
                    }
                }
            }
        } else {
            await this._currentTableInfo.setParams(paramsArray);
        }

        return Promise.resolve();
    }

    async batch(): Promise<void> {
        Promise.resolve();
    }

    async exec(): Promise<void> {
        if (!this._currentTableInfo) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "table info is empty!"
            );
        }

        const params = this._currentTableInfo.getParams();
        if (!params) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "Bind params is empty!"
            );
        }

        const bytes = stmt2BinaryBlockEncode(
            BigInt(ReqId.getReqID()),
            this._stmtTableInfoList,
            this._stmt_id,
            this._toBeBindTableNameIndex,
            this._toBeBindTagCount,
            this._toBeBindColCount
        );
        this._savedBindBytes = bytes;

        try {
            await this.doSendBindBytes(bytes);
            await this.doExec();
        } catch (err: any) {
            if (!this._wsClient.isNetworkError(err)) {
                throw err;
            }
            await this.recover(StmtStep.EXEC);
        } finally {
            if (this._isInsert) {
                this.cleanup({ keepSavedSql: true });
            }
        }
    }

    private async doSendBindBytes(bytes: ArrayBuffer): Promise<void> {
        const reqId = new DataView(bytes).getBigUint64(0, true);
        await this.sendBinaryMsg(reqId, "stmt2_bind", bytes);
    }

    private async doExec(): Promise<void> {
        const msg = {
            action: "stmt2_exec",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        await this.execute(msg);
    }

    async resultSet(): Promise<WSRows> {
        try {
            return await this.doResult();
        } catch (err: any) {
            if (!this._wsClient.isNetworkError(err)) {
                throw err;
            }
            return await this.recover(StmtStep.RESULT);
        } finally {
            this.cleanup({ keepSavedSql: true });
        }
    }

    private async doResult(): Promise<WSRows> {
        const msg = {
            action: "stmt2_result",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        const resp = await this.execute(msg);
        if (!resp) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "ResultSet response is empty!"
            );
        }
        return new WSRows(this._wsClient, resp);
    }

    async close(): Promise<void> {
        const msg = {
            action: "stmt2_close",
            args: {
                req_id: ReqId.getReqID(),
                stmt_id: this._stmt_id,
            },
        };
        try {
            await this.execute(msg);
        } catch (err: any) {
            logger.warn("stmt2 close failed: " + err.message);
        } finally {
            this.cleanup();
        }
    }

    private cleanup(opts?: { keepSavedSql?: boolean }) {
        this._stmtTableInfo.clear();
        this._stmtTableInfoList = [];
        this._currentTableInfo = new TableInfo();
        if (!opts?.keepSavedSql) {
            this._savedSql = undefined;
        }
        this._savedBindBytes = undefined;
    }

    private buildBindBytes(): ArrayBuffer {
        if (this._savedBindBytes === undefined) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "bind bytes are missing for stmt2 rebuild"
            );
        }
        if (this._stmt_id === undefined || this._stmt_id === null) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "stmt_id is missing for stmt2 rebuild"
            );
        }

        const bytes = this._savedBindBytes.slice(0);
        const view = new DataView(bytes);
        view.setBigUint64(0, BigInt(ReqId.getReqID()), true);
        view.setBigUint64(8, this._stmt_id, true);
        return bytes;
    }

    private async recover(failedStep: StmtStep): Promise<any> {
        const retries = this._wsClient.getReconnectRetries();
        const maxAttempts = retries > 0 ? retries : 5;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                await this._wsClient.waitForReady();

                await this.doInit();
                if (failedStep === StmtStep.INIT) {
                    return;
                }

                if (this._savedSql === undefined) {
                    throw new TaosResultError(
                        ErrorCode.ERR_INVALID_PARAMS,
                        "prepare SQL is missing for stmt2 rebuild"
                    );
                }
                await this.doPrepare(this._savedSql);
                if (failedStep === StmtStep.PREPARE) {
                    return;
                }

                await this.doSendBindBytes(this.buildBindBytes());
                if (failedStep === StmtStep.BIND) {
                    return;
                }

                await this.doExec();
                if (failedStep === StmtStep.EXEC) {
                    return;
                }

                return await this.doResult();
            } catch (err: any) {
                if (!this._wsClient.isNetworkError(err)) {
                    throw err;
                }

                if (attempt === maxAttempts) {
                    const recoverError = new TaosResultError(
                        ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
                        `stmt2 recover exceeded max attempts (${maxAttempts}) at step ${StmtStep[failedStep]}: ${err?.message || err}`
                    );
                    (recoverError as any).cause = err;
                    throw recoverError;
                }
            }
        }

        throw new TaosResultError(
            ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
            `stmt2 recover exited unexpectedly at step ${StmtStep[failedStep]}`
        );
    }

    getStmtId(): bigint | undefined | null {
        return this._stmt_id;
    }

    getLastAffected(): number | null | undefined {
        return this.lastAffected;
    }

    private async execute(
        stmtMsg: StmtMessageInfo | ArrayBuffer,
        register: boolean = true
    ): Promise<WsStmtQueryResponse | void> {
        try {
            const reqMsg = JSONBig.stringify(stmtMsg);

            if (register) {
                const result = await this._wsClient.exec(reqMsg, false);
                const resp = new WsStmtQueryResponse(result);
                if (resp.stmt_id !== undefined && resp.stmt_id !== null) {
                    this._stmt_id = resp.stmt_id;
                }
                if (resp.affected !== undefined && resp.affected !== null) {
                    this.lastAffected = resp.affected;
                }
                if (resp.fields !== undefined && resp.fields !== null) {
                    this.fields = resp.fields;
                }
                if (resp.is_insert !== undefined && resp.is_insert !== null) {
                    this._isInsert = resp.is_insert;
                }
                return resp;
            }

            await this._wsClient.execNoResp(reqMsg);
            this._stmt_id = null;
            this.lastAffected = null;
        } catch (e: any) {
            throw new TaosResultError(e.code, e.message);
        }
    }

    private async sendBinaryMsg(
        reqId: bigint,
        action: string,
        message: ArrayBuffer
    ): Promise<void> {
        const result = await this._wsClient.sendBinaryMsg(
            reqId,
            action,
            message,
            false
        );
        const resp = new WsStmtQueryResponse(result);
        if (resp.stmt_id !== undefined && resp.stmt_id !== null) {
            this._stmt_id = resp.stmt_id;
        }
        if (resp.affected !== undefined && resp.affected !== null) {
            this.lastAffected = resp.affected;
        }
    }
}
