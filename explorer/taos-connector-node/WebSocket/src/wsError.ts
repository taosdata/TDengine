export class TDWebSocketClientError extends Error { }
export class WebSocketQueryError extends TDWebSocketClientError { }
export class WebSocketInterfaceError extends TDWebSocketClientError {}
export class WebSocketQueryInterFaceError extends WebSocketInterfaceError{}
export class TaosResultError extends Error{};