import JSONBig from "json-bigint";
import { WebSocketConnector } from "./wsConnector";
import type { SessionRecoveryHook } from "./wsConnector";
import { WebSocketConnectionPool } from "./wsConnectorPool";
import { Dsn, WS_SQL_ENDPOINT } from "../common/dsn";
import {
    ErrorCode,
    TDWebSocketClientError,
    WebSocketInterfaceError,
    WebSocketQueryError,
} from "../common/wsError";
import { WSVersionResponse, WSQueryResponse } from "./wsResponse";
import { ReqId } from "../common/reqid";
import logger from "../common/log";
import { safeDecodeURIComponent, compareVersions, maskSensitiveForLog } from "../common/utils";
import { w3cwebsocket } from "websocket";
import { ConnectorInfo, TSDB_OPTION_CONNECTION } from "../common/constant";

export class WsClient {
    private _wsConnector?: WebSocketConnector;
    private _timeout?: number | undefined | null;
    private _timezone?: string | undefined | null;
    private _userApp?: string | undefined | null;
    private _userIp?: string | undefined | null;
    private readonly _dsn: Dsn;
    private static readonly _minVersion = "3.3.2.0";
    private _version?: string | undefined | null;
    private _bearerToken?: string | undefined | null;
    private _connectedDatabase: string | null = null;
    private _connectionOptions: Map<TSDB_OPTION_CONNECTION, string | null> = new Map();
    private _customRecoveryHook: SessionRecoveryHook | null = null;

    constructor(dsn: Dsn, timeout?: number | undefined | null) {
        this.checkAuth(dsn);
        this._dsn = dsn;
        this._timeout = timeout;
        if (this._dsn.params.has("timezone")) {
            this._timezone = this._dsn.params.get("timezone") || undefined;
        }
        if (this._dsn.params.has("user_app")) {
            this._userApp = this._dsn.params.get("user_app") || undefined;
        }
        if (this._dsn.params.has("user_ip")) {
            this._userIp = this._dsn.params.get("user_ip") || undefined;
        }
        if (this._dsn.params.has("bearer_token")) {
            this._bearerToken = this._dsn.params.get("bearer_token") || undefined;
        }
    }

    private buildConnMessage(database?: string | undefined | null) {
        return {
            action: "conn",
            args: {
                req_id: ReqId.getReqID(),
                user: safeDecodeURIComponent(this._dsn.username),
                password: safeDecodeURIComponent(this._dsn.password),
                db: database,
                connector: ConnectorInfo,
                ...(this._timezone && { tz: this._timezone }),
                ...(this._userApp && { app: this._userApp }),
                ...(this._userIp && { ip: this._userIp }),
                ...(this._bearerToken && { bearer_token: this._bearerToken }),
            },
        };
    }

    private getWsConnector(): WebSocketConnector {
        if (!this._wsConnector) {
            throw new TDWebSocketClientError(
                ErrorCode.ERR_CONNECTION_CLOSED,
                "Invalid websocket connection"
            );
        }
        return this._wsConnector;
    }

    private bindReconnectRecoveryHook(): void {
        if (!this._wsConnector) {
            return;
        }
        this._wsConnector.setSessionRecoveryHook(async () => {
            if (this.isSqlPath()) {
                await this.recoverSqlSessionContext();
            }

            if (this._customRecoveryHook) {
                await this._customRecoveryHook();
            }
        });
    }

    private isSqlPath(): boolean {
        return this._dsn.endpoint === WS_SQL_ENDPOINT;
    }

    private normalizeConnectedDatabase(database?: string | null): string | null {
        if (database && database.length > 0) {
            return database;
        }
        return this.isSqlPath() ? "information_schema" : null;
    }

    private async recoverSqlSessionContext(): Promise<void> {
        if (!this._wsConnector) {
            return;
        }
        const connMsg = this.buildConnMessage(
            this.normalizeConnectedDatabase(this._connectedDatabase)
        );
        await this.sendMsgDirect(JSON.stringify(connMsg), false);

        if (this._connectionOptions.size <= 0) {
            this._wsConnector.markSessionReady();
            return;
        }

        const options = Array.from(this._connectionOptions.entries()).map(
            ([option, value]) => ({
                option,
                value,
            })
        );
        const optionsMsg = {
            action: "options_connection",
            args: {
                req_id: ReqId.getReqID(),
                options,
            },
        };
        await this.sendMsgDirect(JSONBig.stringify(optionsMsg), false);
        this._wsConnector.markSessionReady();
    }

    public setSessionRecoveryHook(
        hook: SessionRecoveryHook | null | undefined
    ): void {
        this._customRecoveryHook = hook || null;
        this.bindReconnectRecoveryHook();
    }

    async connect(database?: string | undefined | null): Promise<void> {
        const connMsg = this.buildConnMessage(database);
        const normalizedDatabase = this.normalizeConnectedDatabase(database ?? null);
        if (logger.isDebugEnabled()) {
            logger.debug("[wsClient.connect.connMsg]===>" + JSONBig.stringify(connMsg, (key, value) =>
                (key === "password" || key === "bearer_token") ? "[REDACTED]" : value
            ));
        }
        this._wsConnector = await WebSocketConnectionPool.instance().getConnection(
            this._dsn,
            this._timeout
        );
        this.bindReconnectRecoveryHook();
        try {
            if (this._wsConnector.readyState() === w3cwebsocket.OPEN) {
                this._connectedDatabase = normalizedDatabase;
                if (this.isSqlPath() && !this._wsConnector.isSessionReady()) {
                    await this.recoverSqlSessionContext();
                }
                return;
            }
            await this._wsConnector.ready();
            let result: any = await this._wsConnector.sendMsg(JSON.stringify(connMsg));
            if (result.msg.code == 0) {
                this._connectedDatabase = normalizedDatabase;
                this._wsConnector.markSessionReady();
                return;
            }
            await this.close();
            throw new WebSocketQueryError(result.msg.code, result.msg.message);
        } catch (e: any) {
            await this.close();
            logger.error(`connection creation failed, dsn:${this._dsn}, code:${e.code}, msg:${e.message}`);
            throw new TDWebSocketClientError(
                ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
                `connection creation failed, dsn:${this._dsn}, code:${e.code}, msg:${e.message}`
            );
        }
    }

    async setOptionConnection(option: TSDB_OPTION_CONNECTION, value: string | null): Promise<void> {
        logger.debug("[wsClient.setOptionConnection]===>" + option + ", " + value);
        let connMsg = {
            action: "options_connection",
            args: {
                req_id: ReqId.getReqID(),
                options: [
                    {
                        option: option,
                        value: value,
                    },
                ],
            },
        };
        try {
            await this.exec(JSONBig.stringify(connMsg), false);
            this._connectionOptions.set(option, value);
        } catch (e: any) {
            logger.error("[wsClient.setOptionConnection] failed: " + e.message);
            throw e;
        }
    }

    async execNoResp(message: string): Promise<void> {
        logger.debug("[wsClient.execNoResp]===>" + message);
        await this.getWsConnector().sendMsgNoResp(message);
    }

    async exec(message: string, bSqlQuery: boolean = true): Promise<any> {
        if (logger.isDebugEnabled()) {
            logger.debug("[wsClient.exec]===>" + maskSensitiveForLog(message));
        }

        const resp: any = await this.getWsConnector().sendMsg(message);
        if (resp.msg.code == 0) {
            if (bSqlQuery) {
                return new WSQueryResponse(resp);
            }
            return resp;
        }

        throw new WebSocketInterfaceError(
            resp.msg.code,
            resp.msg.message
        );
    }

    async sendMsgDirect(message: string, bSqlQuery: boolean = true): Promise<any> {
        if (logger.isDebugEnabled()) {
            logger.debug("[wsClient.sendMsgDirect]===>" + maskSensitiveForLog(message));
        }

        const resp: any = await this.getWsConnector().sendMsgDirect(message);
        if (resp.msg.code == 0) {
            if (bSqlQuery) {
                return new WSQueryResponse(resp);
            }
            return resp;
        }

        throw new WebSocketQueryError(
            resp.msg.code,
            resp.msg.message
        );
    }

    async sendBinaryMsg(
        reqId: bigint,
        action: string,
        message: ArrayBuffer,
        bSqlQuery: boolean = true,
        bResultBinary: boolean = false
    ): Promise<any> {
        const resp: any = await this.getWsConnector().sendBinaryMsg(reqId, action, message);
        if (bResultBinary) {
            return resp;
        }

        if (resp.msg.code == 0) {
            if (bSqlQuery) {
                return new WSQueryResponse(resp);
            }
            return resp;
        }

        throw new WebSocketInterfaceError(
            resp.msg.code,
            resp.msg.message
        );
    }

    getState() {
        if (this._wsConnector) {
            return this._wsConnector.readyState();
        }
        return -1;
    }

    async ready(): Promise<void> {
        try {
            this._wsConnector = await WebSocketConnectionPool.instance().getConnection(
                this._dsn,
                this._timeout
            );
            this.bindReconnectRecoveryHook();
            if (this._wsConnector.readyState() !== w3cwebsocket.OPEN) {
                await this._wsConnector.ready();
            }
            if (this.isSqlPath() && !this._wsConnector.isSessionReady()) {
                this._connectedDatabase = this.normalizeConnectedDatabase(this._connectedDatabase);
                await this.recoverSqlSessionContext();
            }
            if (logger.isDebugEnabled()) {
                logger.debug(
                    `ready status, dsn: ${this._dsn}, state: ${this._wsConnector.readyState()}`
                );
            }
            return;
        } catch (e: any) {
            logger.error(
                `connection creation failed, dsn: ${this._dsn}, code: ${e.code}, message: ${e.message}`
            );
            throw new TDWebSocketClientError(
                ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
                `connection creation failed, dsn: ${this._dsn}, code: ${e.code}, message: ${e.message}`
            );
        }
    }

    async waitForReady(): Promise<void> {
        await this.getWsConnector().ready();
    }

    isNetworkError(err: unknown): boolean {
        return this.getWsConnector().isNetworkError(err);
    }

    getReconnectRetries(): number {
        return this.getWsConnector().getReconnectRetries();
    }

    async sendMsg(message: string): Promise<any> {
        logger.debug("[wsClient.sendMsg]===>" + message);
        return this.getWsConnector().sendMsg(message);
    }

    async freeResult(res: WSQueryResponse): Promise<void> {
        const freeResultMsg = {
            action: "free_result",
            args: {
                req_id: ReqId.getReqID(),
                id: res.id,
            },
        };
        const jsonStr = JSONBig.stringify(freeResultMsg);
        logger.debug("[wsClient.freeResult]===>" + jsonStr);
        await this.getWsConnector().sendMsgNoResp(jsonStr);
    }

    async version(): Promise<string> {
        if (this._version) {
            return this._version;
        }

        let versionMsg = {
            action: "version",
            args: {
                req_id: ReqId.getReqID(),
            },
        };

        try {
            const connector = this.getWsConnector();
            if (connector.readyState() !== w3cwebsocket.OPEN) {
                await connector.ready();
            }
            let result: any = await connector.sendMsg(JSONBig.stringify(versionMsg));
            if (result.msg.code == 0) {
                return new WSVersionResponse(result).version;
            }
            throw new WebSocketInterfaceError(result.msg.code, result.msg.message);
        } catch (e: any) {
            logger.error(
                `connection creation failed, dsn: ${this._dsn}, code: ${e.code}, message: ${e.message}`
            );
            throw new TDWebSocketClientError(
                ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
                `connection creation failed, dsn: ${this._dsn}, code: ${e.code}, message: ${e.message}`
            );
        }
    }

    async close(): Promise<void> {
        if (this._wsConnector) {
            this._wsConnector.setSessionRecoveryHook(null);
            await WebSocketConnectionPool.instance().releaseConnection(
                this._wsConnector
            );
            this._wsConnector = undefined;
        }
        this._customRecoveryHook = null;
    }

    private checkAuth(dsn: Dsn): void {
        const hasToken = dsn.params.get("token") || dsn.params.get("bearer_token");
        if (!hasToken) {
            if (!(dsn.username || dsn.password)) {
                throw new WebSocketInterfaceError(
                    ErrorCode.ERR_INVALID_AUTHENTICATION,
                    `invalid url, provide non-empty "token" or "bearer_token", or provide username/password`
                );
            }
        }
    }

    async checkVersion() {
        this._version = await this.version();
        let result = compareVersions(this._version, WsClient._minVersion);
        if (result < 0) {
            logger.error(
                `TDengine version is too low, current version: ${this._version}, minimum required version: ${WsClient._minVersion}`
            );
            throw new WebSocketQueryError(
                ErrorCode.ERR_TDENIGNE_VERSION_IS_TOO_LOW,
                `Version mismatch. The minimum required TDengine version is ${WsClient._minVersion}`
            );
        }
    }
}
