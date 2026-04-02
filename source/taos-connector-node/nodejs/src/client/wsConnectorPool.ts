import { createHash } from "crypto";
import { Mutex } from "async-mutex";
import { Dsn } from "../common/dsn";
import { ErrorCode, TDWebSocketClientError } from "../common/wsError";
import logger from "../common/log";
import { w3cwebsocket } from "websocket";
import { WebSocketConnector } from "./wsConnector";

const mutex = new Mutex();

export class WebSocketConnectionPool {
    private static _instance?: WebSocketConnectionPool;
    private pool: Map<string, WebSocketConnector[]> = new Map();
    private readonly _maxConnections: number;
    private static sharedBuffer: SharedArrayBuffer;
    private static sharedArray: Int32Array;

    private constructor(maxConnections: number = -1) {
        this._maxConnections = maxConnections;
        WebSocketConnectionPool.sharedBuffer = new SharedArrayBuffer(4);
        WebSocketConnectionPool.sharedArray = new Int32Array(WebSocketConnectionPool.sharedBuffer);
        Atomics.store(WebSocketConnectionPool.sharedArray, 0, 0);
    }

    public static instance(maxConnections: number = -1): WebSocketConnectionPool {
        if (!WebSocketConnectionPool._instance) {
            WebSocketConnectionPool._instance = new WebSocketConnectionPool(maxConnections);
        }
        return WebSocketConnectionPool._instance;
    }

    private maskPoolKeyForLog(poolKey: string): string {
        return poolKey.replace(/#auth=[^#]*/, "#auth=[REDACTED]");
    }

    private buildAuthScope(dsn: Dsn): string {
        const token = dsn.params.get("token") || "";
        const bearerToken = dsn.params.get("bearer_token") || "";
        const raw = JSON.stringify({
            username: dsn.username,
            password: dsn.password,
            token,
            bearerToken,
        });
        return createHash("sha256").update(raw).digest("hex");
    }

    private getPoolKey(dsn: Dsn): string {
        const addrs = [...dsn.addresses]
            .sort((a, b) => `${a.host}:${a.port}`.localeCompare(`${b.host}:${b.port}`))
            .map((addr) => `${addr.host}:${addr.port}`)
            .join(",");
        const auth = this.buildAuthScope(dsn);
        const path = dsn.path();
        return `${dsn.scheme}://${addrs}/${path}#auth=${auth}`;
    }

    async getConnection(
        dsn: Dsn,
        timeout: number | undefined | null
    ): Promise<WebSocketConnector> {
        const poolKey = this.getPoolKey(dsn);
        const poolKeyForLog = this.maskPoolKeyForLog(poolKey);
        let connector: WebSocketConnector | undefined;
        const unlock = await mutex.acquire();
        try {
            if (this.pool.has(poolKey)) {
                const connectors = this.pool.get(poolKey);
                while (connectors && connectors.length > 0) {
                    const candidate = connectors.pop();
                    if (!candidate) {
                        continue;
                    }
                    if (candidate.readyState() === w3cwebsocket.OPEN) {
                        candidate.refreshRetryConfig(dsn);
                        connector = candidate;
                        break;
                    }
                    Atomics.add(WebSocketConnectionPool.sharedArray, 0, -1);
                    candidate.close();
                    logger.error("getConnection, current connection status fail, poolKey: " + poolKeyForLog);
                }
            }

            if (connector) {
                logger.debug(
                    "get connection success:" +
                    Atomics.load(WebSocketConnectionPool.sharedArray, 0)
                );
                return connector;
            }

            if (
                this._maxConnections != -1 &&
                Atomics.load(WebSocketConnectionPool.sharedArray, 0) > this._maxConnections
            ) {
                throw new TDWebSocketClientError(
                    ErrorCode.ERR_WEBSOCKET_CONNECTION_ARRIVED_LIMIT,
                    "websocket connect arrived limited:" +
                    Atomics.load(WebSocketConnectionPool.sharedArray, 0)
                );
            }

            Atomics.add(WebSocketConnectionPool.sharedArray, 0, 1);
            if (logger.isInfoEnabled()) {
                logger.info(
                    "getConnection, new connection count:" +
                    Atomics.load(WebSocketConnectionPool.sharedArray, 0) +
                    ", poolKey:" +
                    poolKeyForLog
                );
            }
            return new WebSocketConnector(dsn, poolKey, timeout);
        } finally {
            unlock();
        }
    }

    async releaseConnection(connector: WebSocketConnector): Promise<void> {
        if (!connector) {
            return;
        }
        const unlock = await mutex.acquire();
        try {
            if (connector.readyState() === w3cwebsocket.OPEN) {
                const poolKey = connector.getPoolKey();
                let connectors = this.pool.get(poolKey);
                if (!connectors) {
                    connectors = [];
                    this.pool.set(poolKey, connectors);
                }
                connectors.push(connector);
                logger.info("releaseConnection, current connection count:" + connectors.length);
            } else {
                Atomics.add(WebSocketConnectionPool.sharedArray, 0, -1);
                connector.close();
                logger.info(
                    "releaseConnection, current connection status fail:" +
                    Atomics.load(WebSocketConnectionPool.sharedArray, 0)
                );
            }
        } finally {
            unlock();
        }
    }

    destroyed() {
        let num = 0;
        if (this.pool) {
            for (let values of this.pool.values()) {
                for (let i in values) {
                    num++;
                    values[i].close();
                }
            }
        }
        logger.info(
            "destroyed connect: " + Atomics.load(WebSocketConnectionPool.sharedArray, 0) +
            ", current count:" + num
        );
        Atomics.store(WebSocketConnectionPool.sharedArray, 0, 0);
        this.pool = new Map();
    }
}

process.on("exit", (code) => {
    logger.info("begin destroy connect");
    WebSocketConnectionPool.instance().destroyed();
    process.exit();
});

process.on("SIGINT", () => {
    logger.info("Received SIGINT. Press Control-D to exit, begin destroy connect...");
    WebSocketConnectionPool.instance().destroyed();
    process.exit();
});

process.on("SIGTERM", () => {
    logger.info("Received SIGTERM. Press Control-D to exit, begin destroy connect...");
    WebSocketConnectionPool.instance().destroyed();
    process.exit();
});
