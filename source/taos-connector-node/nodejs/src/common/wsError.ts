export class TDWebSocketClientError extends Error {
    code: number = 0;
    constructor(code: number, message: string = "") {
        super(message);
        this.name = new.target.name;
        this.code = code;
        if (typeof (Error as any).captureStackTrace === "function") {
            (Error as any).captureStackTrace(this, new.target);
        }
        if (typeof Object.setPrototypeOf === "function") {
            Object.setPrototypeOf(this, new.target.prototype);
        } else {
            (this as any).__proto__ = new.target.prototype;
        }
    }
}
export class WebSocketQueryError extends TDWebSocketClientError { }
export class WebSocketInterfaceError extends TDWebSocketClientError { }
export class WebSocketQueryInterFaceError extends WebSocketInterfaceError { }
export class TaosResultError extends TDWebSocketClientError { }
export class TaosError extends TDWebSocketClientError { }

export enum ErrorCode {
    ERR_INVALID_PARAMS = 100,
    ERR_INVALID_URL = 101,
    ERR_WS_NO_CALLBACK = 102,
    ERR_INVALID_MESSAGE_TYPE = 103,
    ERR_WEBSOCKET_CONNECTION_FAIL = 104,
    ERR_WEBSOCKET_QUERY_TIMEOUT = 105,
    ERR_INVALID_AUTHENTICATION = 106,
    ERR_UNSUPPORTED_TDENGINE_TYPE = 107,
    ERR_CONNECTION_CLOSED = 108,
    ERR_INVALID_FETCH_MESSAGE_DATA = 109,
    ERR_WEBSOCKET_CONNECTION_ARRIVED_LIMIT = 110,
    ERR_PARTITIONS_TOPIC_VGROUP_LENGTH_NOT_EQUAL = 111,
    ERR_TDENIGNE_VERSION_IS_TOO_LOW = 112,
}
