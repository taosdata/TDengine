"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TDengineWebSocket = void 0;
const wsQuery_1 = require("./wsQuery");
const wsQueryInterface_1 = require("./wsQueryInterface");
class TDengineWebSocket {
    constructor(url) {
        this._data = [];
        this._meta = [];
        this._wsInterface = new wsQueryInterface_1.WSInterface(new URL(url));
    }
    connect(database) {
        return this._wsInterface.connect(database);
    }
    state() {
        return this._wsInterface.getState();
    }
    /**
     * return client version.
     */
    version() {
        return this._wsInterface.version();
    }
    query(sql) {
        return (0, wsQuery_1.execute)(sql, this._wsInterface);
    }
    close() {
        this._wsInterface.close();
    }
}
exports.TDengineWebSocket = TDengineWebSocket;
