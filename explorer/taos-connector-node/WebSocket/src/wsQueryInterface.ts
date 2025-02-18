import { parseBlock, TaosResult } from './taosResult';
import { TDWebSocketClient } from './wsClient'
import { WebSocketInterfaceError, WebSocketQueryError } from './wsError'
import { WSVersionResponse, WSFetchBlockResponse, WSQueryResponse, WSFetchResponse, WSConnResponse } from './wsQueryResponse'
import JSONBig from 'json-bigint'

export class WSInterface {
    private _wsQueryClient: TDWebSocketClient;
    private _req_id = 0;
    private _url;

    constructor(url: URL) {
        this.checkURL(url);
        this._url = url;
        this._wsQueryClient = new TDWebSocketClient(this._url);
    }

    connect(database?: string): Promise<WSConnResponse> {

        let _db = this._url.pathname.split('/')[3];

        if (database) {
            _db = database;
        }
        this._reqIDIncrement()
        let connMsg = {
            action: 'conn',
            args: {
                req_id: this._req_id,
                user: this._url.username,
                password: this._url.password,
                db: _db,
            }
        }
        // console.log(connMsg)
        return new Promise((resolve, reject) => {
            if (this._wsQueryClient.readyState() > 0) {
                this._wsQueryClient.sendMsg(JSON.stringify(connMsg))
                    .then((e: any) => {
                        if (e.code == 0) {
                            resolve(e);
                        } else {
                            reject(new WebSocketQueryError(`${e.message}, code ${e.code}`))
                        }
                    })
            } else {
                this._wsQueryClient.Ready()
                    .then((ws: TDWebSocketClient) => {
                        return ws.sendMsg(JSON.stringify(connMsg))
                    })
                    .then((e: any) => {
                        if (e.code == 0) {
                            resolve(e);
                        } else {
                            reject(new WebSocketQueryError(`${e.message}, code ${e.code}`))
                        }
                    })
            }

        })
    }

    // need to construct Response.
    query(sql: string): Promise<WSQueryResponse> {
        this._reqIDIncrement()
        // construct msg
        let queryMsg = {
            action: 'query',
            args: {
                req_id: this._req_id,
                sql: sql
            },
        }
        return new Promise((resolve, reject) => {
            let jsonStr = JSON.stringify(queryMsg)
            // console.log("[wsQueryInterface.query.queryMsg]===>" + jsonStr)
            this._wsQueryClient.sendMsg(jsonStr)
                .then((e: any) => {
                    if (e.code == 0) {
                        resolve(new WSQueryResponse(e))
                    } else {
                        reject(new WebSocketInterfaceError(`${e.message},code ${e.code}`))
                    }
                })
        })
    }

    getState() {
        return this._wsQueryClient.readyState()
    }

    fetch(res: WSQueryResponse): Promise<WSFetchResponse> {
        this._reqIDIncrement()
        let fetchMsg = {
            action: 'fetch',
            args: {
                req_id: this._req_id,
                id: res.id
            }
        }
        // console.log("[wsQueryInterface.fetch()]===>wsQueryResponse\n")
        // console.log(res)
        return new Promise((resolve, reject) => {
            let jsonStr = JSONBig.stringify(fetchMsg)
            // console.log("[wsQueryInterface.fetch.fetchMsg]===>" + jsonStr)
            this._wsQueryClient.sendMsg(jsonStr)
                .then((e: any) => {
                    if (e.code == 0) {
                        resolve(new WSFetchResponse(e))
                    } else {
                        reject(new WebSocketInterfaceError(`${e.message},code ${e.code}`))
                    }
                }).catch(e => {
                    reject(e)
                });
        })
    }

    fetchBlock(fetchResponse: WSFetchResponse, taosResult: TaosResult): Promise<TaosResult> {
        this._reqIDIncrement()
        let fetchBlockMsg = {
            action: 'fetch_block',
            args: {
                'req_id': this._req_id,
                'id': fetchResponse.id,
            }
        }
        return new Promise((resolve, reject) => {
            let jsonStr = JSONBig.stringify(fetchBlockMsg)
            // console.log("[wsQueryInterface.fetchBlock.fetchBlockMsg]===>" + jsonStr)
            this._wsQueryClient.sendMsg(jsonStr).then((e: any) => {
                resolve(parseBlock(fetchResponse, new WSFetchBlockResponse(e), taosResult))
                // if retrieve JSON then reject with message
                // else is binary , so parse raw block to TaosResult
            }).catch(e => reject(e))
        })
    }

    freeResult(res: WSQueryResponse) {
        this._reqIDIncrement()
        let freeResultMsg = {
            action: 'free_result',
            args: {
                req_id: this._req_id,
                id: res.id
            }
        }
        return new Promise((resolve, reject) => {
            let jsonStr = JSONBig.stringify(freeResultMsg)
            // console.log("[wsQueryInterface.freeResult.freeResultMsg]===>" + jsonStr)
            this._wsQueryClient.sendMsg(jsonStr, false).then((e: any) => {
                resolve(e)
            }).catch(e => reject(e))
        })
    }

    version(): Promise<string> {
        this._reqIDIncrement()
        let versionMsg = {
            action: 'version',
            args: {
                req_id: this._req_id
            }
        }
        return new Promise((resolve, reject) => {
            if (this._wsQueryClient.readyState() > 0) {
                this._wsQueryClient.sendMsg(JSONBig.stringify(versionMsg))
                    .then((e: any) => {
                        // console.log(e)
                        if (e.code == 0) {
                            resolve(new WSVersionResponse(e).version)
                        } else {
                            reject(new WSVersionResponse(e).message)
                        }
                    }).catch(e => reject(e));
            }
            this._wsQueryClient.Ready()
                .then((ws: TDWebSocketClient) => {
                    return ws.sendMsg(JSONBig.stringify(versionMsg))
                }).then((e: any) => {
                    // console.log(e)
                    if (e.code == 0) {
                        resolve(new WSVersionResponse(e).version)
                    } else {
                        reject(new WSVersionResponse(e).message)
                    }
                }).catch(e => reject(e));
        })
    }

    close() {
        this._wsQueryClient.close();
    }

    checkURL(url: URL) {
        // Assert is cloud url
        if (!url.searchParams.has('token')) {
            if (!(url.username || url.password)) {
                throw new WebSocketInterfaceError("invalid url, password or username needed.")
            }
        }
    }

    private _reqIDIncrement() {
        if (this._req_id == Number.MAX_SAFE_INTEGER) {
            this._req_id = 0;
        } else {
            this._req_id += 1;
        }
    }

}