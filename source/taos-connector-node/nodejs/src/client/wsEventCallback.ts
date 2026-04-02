import { Mutex } from "async-mutex";
import {
    ErrorCode,
    TDWebSocketClientError,
    WebSocketQueryError,
} from "../common/wsError";
import { MessageResp } from "../common/taosResult";
import logger from "../common/log";

interface MessageId {
    action: string;
    req_id: bigint;
    id?: bigint;
    timeout?: number;
    poolKey?: string;
}

interface MessageAction {
    reject: Function;
    resolve: Function;
    timer: ReturnType<typeof setTimeout>;
    sendTime: number;
}

export enum OnMessageType {
    MESSAGE_TYPE_ARRAYBUFFER = 1,
    MESSAGE_TYPE_BLOB = 2,
    MESSAGE_TYPE_STRING = 3,
    MESSAGE_TYPE_CONNECTION = 4,
}

const eventMutex = new Mutex();

export class WsEventCallback {
    private static _instance?: WsEventCallback;
    private static _msgActionRegister: Map<MessageId, MessageAction> = new Map();

    private constructor() { }

    public static instance(): WsEventCallback {
        if (!WsEventCallback._instance) {
            WsEventCallback._instance = new WsEventCallback();
        }
        return WsEventCallback._instance;
    }

    async registerCallback(
        id: MessageId,
        res: (args: unknown) => void,
        rej: (reason: any) => void
    ) {
        let release = await eventMutex.acquire();
        try {
            const timer = setTimeout(async () => {
                let removed = false;
                const timeoutRelease = await eventMutex.acquire();
                try {
                    removed = WsEventCallback._msgActionRegister.delete(id);
                } finally {
                    timeoutRelease();
                }
                if (!removed) {
                    return;
                }
                rej(
                    new WebSocketQueryError(
                        ErrorCode.ERR_WEBSOCKET_QUERY_TIMEOUT,
                        `action:${id.action},req_id:${id.req_id} timeout with ${id.timeout} milliseconds`
                    )
                );
            }, id.timeout);

            WsEventCallback._msgActionRegister.set(id, {
                sendTime: new Date().getTime(),
                reject: rej,
                resolve: res,
                timer,
            });
        } finally {
            release();
        }
    }

    async handleEventCallback(
        msg: MessageId,
        messageType: OnMessageType,
        data: any
    ) {
        let action: MessageAction | undefined = undefined;
        let keyToDelete: MessageId | undefined = undefined;
        let release = await eventMutex.acquire();
        logger.debug(`HandleEventCallback get lock msg=${msg}, ${messageType}`);
        logger.debug(WsEventCallback._msgActionRegister);
        try {
            for (let [k, v] of WsEventCallback._msgActionRegister) {
                if (messageType == OnMessageType.MESSAGE_TYPE_ARRAYBUFFER) {
                    if (k.id == msg.id || k.req_id == msg.id) {
                        action = v;
                        keyToDelete = k;
                        break;
                    }
                } else if (messageType == OnMessageType.MESSAGE_TYPE_BLOB) {
                    if (k.id == msg.id || k.req_id == msg.id) {
                        action = v;
                        keyToDelete = k;
                        break;
                    }
                } else if (messageType == OnMessageType.MESSAGE_TYPE_STRING) {
                    if (k.req_id == msg.req_id && k.action == msg.action) {
                        action = v;
                        keyToDelete = k;
                        break;
                    }
                } else if (messageType == OnMessageType.MESSAGE_TYPE_CONNECTION) {
                    if (k.req_id == msg.req_id && k.action == msg.action) {
                        action = v;
                        keyToDelete = k;
                        break;
                    }
                }
            }

            if (action && keyToDelete) {
                clearTimeout(action.timer);
                WsEventCallback._msgActionRegister.delete(keyToDelete);
            }
        } finally {
            release();
        }

        if (action) {
            let currTime = new Date().getTime();
            let resp: MessageResp = {
                msg: data,
                totalTime: Math.abs(currTime - action.sendTime),
            };
            action.resolve(resp);
        } else {
            logger.error("no find callback msg: ", msg);
            throw new TDWebSocketClientError(
                ErrorCode.ERR_WS_NO_CALLBACK,
                "no callback registered with req_id: " +
                msg.req_id +
                ", action: " +
                msg.action
            );
        }
    }

    async unregisterCallback(reqId: bigint): Promise<void> {
        const release = await eventMutex.acquire();
        try {
            for (let [k, v] of WsEventCallback._msgActionRegister) {
                if (k.req_id === reqId || k.id === reqId) {
                    clearTimeout(v.timer);
                    WsEventCallback._msgActionRegister.delete(k);
                    break;
                }
            }
        } finally {
            release();
        }
    }

    async rejectCallbacksExceptReqIds(
        keepReqIds: Set<bigint>,
        error: Error,
        poolKey?: string
    ): Promise<void> {
        const toReject: Function[] = [];
        const release = await eventMutex.acquire();
        try {
            for (const [k, v] of WsEventCallback._msgActionRegister) {
                if (poolKey !== undefined && k.poolKey !== poolKey) {
                    continue;
                }
                const keepByReqId = keepReqIds.has(k.req_id);
                const keepByCallbackId = k.id !== undefined && keepReqIds.has(k.id);
                if (keepByReqId || keepByCallbackId) {
                    continue;
                }
                clearTimeout(v.timer);
                WsEventCallback._msgActionRegister.delete(k);
                toReject.push(v.reject);
            }
        } finally {
            release();
        }

        for (const reject of toReject) {
            reject(error);
        }
    }
}
