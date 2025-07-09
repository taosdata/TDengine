"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WSInterface = void 0;
const taosResult_1 = require("./taosResult");
const wsClient_1 = require("./wsClient");
const wsError_1 = require("./wsError");
const wsQueryResponse_1 = require("./wsQueryResponse");
const json_bigint_1 = __importDefault(require("json-bigint"));
class WSInterface {
    constructor(url) {
        this._req_id = 0;
        this.checkURL(url);
        this._url = url;
        this._tz = url === null || url === void 0 ? void 0 : url.searchParams.get("tz");
        this._wsQueryClient = new wsClient_1.TDWebSocketClient(this._url);
    }
    connect(database) {
        let _db = this._url.pathname.split("/")[3];
        if (database) {
            _db = database;
        }
        this._reqIDIncrement();
        let connMsg = {
            action: "conn",
            args: {
                req_id: this._req_id,
                user: this._url.username,
                password: this._url.password,
                db: _db,
            },
        };
        // console.log(connMsg)
        return new Promise((resolve, reject) => {
            if (this._wsQueryClient.readyState() > 0) {
                this._wsQueryClient.sendMsg(JSON.stringify(connMsg)).then((e) => {
                    if (e.code == 0) {
                        resolve(e);
                    }
                    else {
                        reject(new wsError_1.WebSocketQueryError(`${e.message}, code ${e.code}`));
                    }
                });
            }
            else {
                this._wsQueryClient
                    .Ready()
                    .then((ws) => {
                    return ws.sendMsg(JSON.stringify(connMsg));
                })
                    .then((e) => {
                    if (e.code == 0) {
                        resolve(e);
                    }
                    else {
                        reject(new wsError_1.WebSocketQueryError(`${e.message}, code ${e.code}`));
                    }
                });
            }
        });
    }
    // need to construct Response.
    query(sql) {
        this._reqIDIncrement();
        // construct msg
        let queryMsg = {
            action: "query",
            args: {
                req_id: this._req_id,
                sql: sql,
            },
        };
        return new Promise((resolve, reject) => {
            let jsonStr = JSON.stringify(queryMsg);
            // console.log("[wsQueryInterface.query.queryMsg]===>" + jsonStr)
            this._wsQueryClient.sendMsg(jsonStr).then((e) => {
                if (e.code == 0) {
                    resolve(new wsQueryResponse_1.WSQueryResponse(e));
                }
                else {
                    reject(new wsError_1.WebSocketInterfaceError(`${e.message},code ${e.code}`));
                }
            });
        });
    }
    getState() {
        return this._wsQueryClient.readyState();
    }
    fetch(res) {
        this._reqIDIncrement();
        let fetchMsg = {
            action: "fetch",
            args: {
                req_id: this._req_id,
                id: res.id,
            },
        };
        // console.log("[wsQueryInterface.fetch()]===>wsQueryResponse\n")
        // console.log(res)
        return new Promise((resolve, reject) => {
            let jsonStr = json_bigint_1.default.stringify(fetchMsg);
            // console.log("[wsQueryInterface.fetch.fetchMsg]===>" + jsonStr)
            this._wsQueryClient
                .sendMsg(jsonStr)
                .then((e) => {
                if (e.code == 0) {
                    resolve(new wsQueryResponse_1.WSFetchResponse(e));
                }
                else {
                    reject(new wsError_1.WebSocketInterfaceError(`${e.message},code ${e.code}`));
                }
            })
                .catch((e) => {
                reject(e);
            });
        });
    }
    fetchBlock(fetchResponse, taosResult) {
        this._reqIDIncrement();
        let fetchBlockMsg = {
            action: "fetch_block",
            args: {
                req_id: this._req_id,
                id: fetchResponse.id,
            },
        };
        return new Promise((resolve, reject) => {
            let jsonStr = json_bigint_1.default.stringify(fetchBlockMsg);
            // console.log("[wsQueryInterface.fetchBlock.fetchBlockMsg]===>" + jsonStr)
            this._wsQueryClient
                .sendMsg(jsonStr)
                .then((e) => {
                resolve((0, taosResult_1.parseBlock)(fetchResponse, new wsQueryResponse_1.WSFetchBlockResponse(e), taosResult, this._tz));
                // if retrieve JSON then reject with message
                // else is binary , so parse raw block to TaosResult
            })
                .catch((e) => reject(e));
        });
    }
    freeResult(res) {
        this._reqIDIncrement();
        let freeResultMsg = {
            action: "free_result",
            args: {
                req_id: this._req_id,
                id: res.id,
            },
        };
        return new Promise((resolve, reject) => {
            let jsonStr = json_bigint_1.default.stringify(freeResultMsg);
            // console.log("[wsQueryInterface.freeResult.freeResultMsg]===>" + jsonStr)
            this._wsQueryClient
                .sendMsg(jsonStr, false)
                .then((e) => {
                resolve(e);
            })
                .catch((e) => reject(e));
        });
    }
    version() {
        this._reqIDIncrement();
        let versionMsg = {
            action: "version",
            args: {
                req_id: this._req_id,
            },
        };
        return new Promise((resolve, reject) => {
            if (this._wsQueryClient.readyState() > 0) {
                this._wsQueryClient
                    .sendMsg(json_bigint_1.default.stringify(versionMsg))
                    .then((e) => {
                    // console.log(e)
                    if (e.code == 0) {
                        resolve(new wsQueryResponse_1.WSVersionResponse(e).version);
                    }
                    else {
                        reject(new wsQueryResponse_1.WSVersionResponse(e).message);
                    }
                })
                    .catch((e) => reject(e));
            }
            this._wsQueryClient
                .Ready()
                .then((ws) => {
                return ws.sendMsg(json_bigint_1.default.stringify(versionMsg));
            })
                .then((e) => {
                // console.log(e)
                if (e.code == 0) {
                    resolve(new wsQueryResponse_1.WSVersionResponse(e).version);
                }
                else {
                    reject(new wsQueryResponse_1.WSVersionResponse(e).message);
                }
            })
                .catch((e) => reject(e));
        });
    }
    close() {
        this._wsQueryClient.close();
    }
    checkURL(url) {
        // Assert is cloud url
        if (!url.searchParams.has("token")) {
            if (!(url.username || url.password)) {
                throw new wsError_1.WebSocketInterfaceError("invalid url, password or username needed.");
            }
        }
    }
    _reqIDIncrement() {
        if (this._req_id == Number.MAX_SAFE_INTEGER) {
            this._req_id = 0;
        }
        else {
            this._req_id += 1;
        }
    }
}
exports.WSInterface = WSInterface;
