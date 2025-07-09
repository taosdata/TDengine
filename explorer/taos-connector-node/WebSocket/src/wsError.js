"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TaosResultError = exports.WebSocketQueryInterFaceError = exports.WebSocketInterfaceError = exports.WebSocketQueryError = exports.TDWebSocketClientError = void 0;
class TDWebSocketClientError extends Error {
}
exports.TDWebSocketClientError = TDWebSocketClientError;
class WebSocketQueryError extends TDWebSocketClientError {
}
exports.WebSocketQueryError = WebSocketQueryError;
class WebSocketInterfaceError extends TDWebSocketClientError {
}
exports.WebSocketInterfaceError = WebSocketInterfaceError;
class WebSocketQueryInterFaceError extends WebSocketInterfaceError {
}
exports.WebSocketQueryInterFaceError = WebSocketQueryInterFaceError;
class TaosResultError extends Error {
}
exports.TaosResultError = TaosResultError;
;
