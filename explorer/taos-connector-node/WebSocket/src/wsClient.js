"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TDWebSocketClient = void 0;
const websocket_1 = require("websocket");
const wsError_1 = require("./wsError");
var _msgActionRegister = new Map();
class TDWebSocketClient {
    // create ws
    constructor(url) {
        this._timeout = 5000;
        // return w3bsocket3
        if (url) {
            this._wsURL = url;
            let origin = url.origin;
            let pathname = url.pathname;
            let search = url.search;
            this._wsConn = new websocket_1.w3cwebsocket(origin.concat(pathname).concat(search), undefined, undefined, {
            //   "Sec-Websocket-Extensions":
            //     "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
            //   "Sec-Websocket-Extensions": "",
            //   Hello: "world",
            });
            //   console.log("Sec-Websocket-Extensions:test");
            // this._wsConn.onopen = this._onopen
            console.log(this._wsConn);
            this._wsConn.onerror = function (err) {
                throw err;
            };
            this._wsConn.onclose = this._onclose;
            this._wsConn.onmessage = this._onmessage;
            this._wsConn._binaryType = "arraybuffer";
        }
        else {
            throw new wsError_1.WebSocketQueryError("websocket URL must be defined");
        }
    }
    Ready() {
        return new Promise((resolve, reject) => {
            this._wsConn.onopen = () => {
                // console.log("websocket connection opened")
                resolve(this);
            };
        });
    }
    _onclose(e) {
        return new Promise((resolve, reject) => {
            resolve("websocket connection closed");
        });
    }
    _onmessage(event) {
        let data = event.data;
        // console.log("[wsClient._onMessage()._msgActionRegister]\n")
        // console.log(_msgActionRegister)
        // console.log("===="+ (Object.prototype.toString.call(data)))
        if (Object.prototype.toString.call(data) === "[object ArrayBuffer]") {
            let id = new DataView(data, 8, 8).getBigUint64(0, true);
            // console.log("fetch block response id:" + id)
            let action = undefined;
            _msgActionRegister.forEach((v, k) => {
                if (k.id == id) {
                    action = v;
                    _msgActionRegister.delete(k);
                }
            });
            if (action) {
                action.resolve(data);
            }
            else {
                _msgActionRegister.clear();
                throw new wsError_1.TDWebSocketClientError(`no callback registered for fetch_block with id=${id}`);
            }
        }
        else if (Object.prototype.toString.call(data) === "[object Blob]") {
            data.arrayBuffer().then((d) => {
                let id = new DataView(d, 8, 8).getBigUint64(0, true);
                // console.log("fetch block response id:" + id)
                let action = undefined;
                _msgActionRegister.forEach((v, k) => {
                    if (k.id == id) {
                        action = v;
                        _msgActionRegister.delete(k);
                    }
                });
                if (action) {
                    action.resolve(d);
                }
                else {
                    _msgActionRegister.clear();
                    throw new wsError_1.TDWebSocketClientError(`no callback registered for fetch_block with id=${id}`);
                }
            });
        }
        else if (Object.prototype.toString.call(data) === "[object String]") {
            let msg = JSON.parse(data);
            // console.log("[_onmessage.stringType]==>:" + data);
            let action = undefined;
            _msgActionRegister.forEach((v, k) => {
                if (k.action == "version") {
                    action = v;
                    _msgActionRegister.delete(k);
                }
                if (k.req_id == msg.req_id && k.action == msg.action) {
                    action = v;
                    _msgActionRegister.delete(k);
                }
            });
            if (action) {
                action.resolve(msg);
            }
            else {
                _msgActionRegister.clear();
                throw new wsError_1.TDWebSocketClientError(`no callback registered for ${msg.action} with req_id=${msg.req_id}`);
            }
        }
        else {
            _msgActionRegister.clear();
            throw new wsError_1.TDWebSocketClientError(`invalid message type ${Object.prototype.toString.call(data)}`);
        }
    }
    close() {
        if (this._wsConn) {
            _msgActionRegister.clear();
            this._wsConn.close();
        }
        else {
            throw new wsError_1.TDWebSocketClientError("WebSocket connection is undefined.");
        }
    }
    readyState() {
        return this._wsConn.readyState;
    }
    sendMsg(message, register = true) {
        // console.log("[wsClient.sendMessage()]===>" + message)
        let msg = JSON.parse(message);
        // console.log(typeof msg.args.id)
        if (msg.args.id) {
            msg.args.id = BigInt(msg.args.id);
        }
        // console.log("[wsClient.sendMessage.msg]===>\n")
        // console.log(msg)
        return new Promise((resolve, reject) => {
            var _a;
            if (this._wsConn && this._wsConn.readyState > 0) {
                if (register) {
                    this._registerCallback({
                        action: msg.action,
                        req_id: msg.args.req_id,
                        id: msg.args.id === undefined ? msg.args.id : BigInt(msg.args.id),
                    }, resolve, reject);
                    // console.log("[wsClient.sendMessage._msgActionRegister]===>\n")
                    // console.log(_msgActionRegister)
                }
                this._wsConn.send(message);
            }
            else {
                reject(new wsError_1.WebSocketQueryError(`WebSocket connection is not ready,status :${(_a = this._wsConn) === null || _a === void 0 ? void 0 : _a.readyState}`));
            }
        });
    }
    _registerCallback(id, res, rej) {
        // console.log("register messageId:"+ JSON.stringify(id))
        _msgActionRegister.set(id, {
            reject: rej,
            resolve: res,
            timer: setTimeout(() => rej(new wsError_1.WebSocketQueryError(`action:${id.action},req_id:${id.req_id} timeout with ${this._timeout} milliseconds`)), this._timeout),
        });
    }
    configTimeout(ms) {
        this._timeout = ms;
    }
}
exports.TDWebSocketClient = TDWebSocketClient;
