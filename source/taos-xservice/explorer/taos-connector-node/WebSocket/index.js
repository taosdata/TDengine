"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaosResult = exports.connect = void 0;
var tdengineWebsocket_1 = require("./src/tdengineWebsocket");
var taosResult_1 = require("./src/taosResult");
Object.defineProperty(exports, "TaosResult", { enumerable: true, get: function () { return taosResult_1.TaosResult; } });
var connect = function (url) {
    return new tdengineWebsocket_1.TDengineWebSocket(url);
};
exports.connect = connect;
