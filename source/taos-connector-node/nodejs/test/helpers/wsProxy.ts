import * as http from "http";
import {
    client as WebSocketClient,
    connection as WebSocketConnection,
    Message as WsMessage,
    request as WebSocketRequest,
    server as WebSocketServer,
} from "websocket";

const DEFAULT_UPSTREAM_BASE_URL = "ws://localhost:6041";

type WsProxyDirection = "client_to_upstream" | "upstream_to_client";
type WsProxyEventType =
    | "proxy_started"
    | "proxy_stopped"
    | "client_connected"
    | "upstream_connected"
    | "message"
    | "tunnel_closed"
    | "error";

interface WsProxyEventBase {
    type: WsProxyEventType;
    timestamp: number;
    connectionId?: number;
}

export interface WsProxyStartedEvent extends WsProxyEventBase {
    type: "proxy_started";
    host: string;
    port: number;
    url: string;
    reason?: string;
}

export interface WsProxyStoppedEvent extends WsProxyEventBase {
    type: "proxy_stopped";
    reason?: string;
}

export interface WsProxyClientConnectedEvent extends WsProxyEventBase {
    type: "client_connected";
    connectionId: number;
    path: string;
    remoteAddress: string;
}

export interface WsProxyUpstreamConnectedEvent extends WsProxyEventBase {
    type: "upstream_connected";
    connectionId: number;
    upstreamUrl: string;
}

export interface WsProxyMessageEvent extends WsProxyEventBase {
    type: "message";
    connectionId: number;
    direction: WsProxyDirection;
    isBinary: boolean;
    byteLength: number;
    rawData: Buffer | string;
    messageSeq: number;
}

export interface WsProxyTunnelClosedEvent extends WsProxyEventBase {
    type: "tunnel_closed";
    connectionId: number;
    reason: string;
    source: "client" | "upstream" | "proxy";
}

export interface WsProxyErrorEvent extends WsProxyEventBase {
    type: "error";
    error: Error;
    stage: string;
    sourceEventType?: WsProxyEventType;
    details?: string;
}

export type WsProxyEvent =
    | WsProxyStartedEvent
    | WsProxyStoppedEvent
    | WsProxyClientConnectedEvent
    | WsProxyUpstreamConnectedEvent
    | WsProxyMessageEvent
    | WsProxyTunnelClosedEvent
    | WsProxyErrorEvent;

export interface WsProxyControl {
    restart(opts?: { downtimeMs?: number; reason?: string }): Promise<void>;
    stop(reason?: string): Promise<void>;
    isRunning(): boolean;
}

export type WsProxyEventHandler = (
    event: WsProxyEvent,
    control: WsProxyControl
) => void | Promise<void>;

export interface WsProxyOptions {
    host: string;
    port: number;
    onEvent?: WsProxyEventHandler;
}

interface PendingFrame {
    isBinary: boolean;
    data: Buffer | string;
}

interface ProxyTunnel {
    id: number;
    path: string;
    closed: boolean;
    messageSeq: number;
    clientConn: WebSocketConnection;
    upstreamConn?: WebSocketConnection;
    upstreamClient?: WebSocketClient;
    pendingFrames: PendingFrame[];
}

export class WsProxy {
    private readonly _listenHost: string;
    private readonly _requestedPort: number;
    private _lockedPort: number | null = null;
    private readonly _onEvent?: WsProxyEventHandler;
    private readonly _control: WsProxyControl;

    private _httpServer: http.Server | null = null;
    private _wsServer: WebSocketServer | null = null;
    private _running = false;
    private _nextConnectionId = 1;
    private _lifecycleChain: Promise<void> = Promise.resolve();
    private readonly _tunnels: Map<number, ProxyTunnel> = new Map();
    private readonly _eventLog: WsProxyEvent[] = [];

    constructor(options: WsProxyOptions) {
        if (!options.host || options.host.trim().length === 0) {
            throw new Error("listen host must not be empty");
        }
        if (
            !Number.isInteger(options.port) ||
            options.port < 0 ||
            options.port > 65535
        ) {
            throw new Error(
                `invalid listen port: ${options.port}, expected 0-65535`
            );
        }
        this._listenHost = options.host;
        this._requestedPort = options.port;
        this._onEvent = options.onEvent;
        this._control = {
            restart: async (opts?: { downtimeMs?: number; reason?: string }) => {
                await this.restart(opts);
            },
            stop: async (reason?: string) => {
                await this.stop(reason);
            },
            isRunning: () => this.isRunning(),
        };
    }

    static async create(options: WsProxyOptions): Promise<WsProxy> {
        const proxy = new WsProxy(options);
        await proxy.start("initial start");
        return proxy;
    }

    async start(reason?: string): Promise<void> {
        await this.enqueueLifecycle(async () => {
            await this.startInternal(reason);
        });
    }

    async stop(reason?: string): Promise<void> {
        await this.enqueueLifecycle(async () => {
            await this.stopInternal(reason);
        });
    }

    async restart(opts?: { downtimeMs?: number; reason?: string }): Promise<void> {
        await this.enqueueLifecycle(async () => {
            const reason = opts?.reason || "restart";
            const downtimeMs = Math.max(0, opts?.downtimeMs || 0);
            await this.stopInternal(reason);
            if (downtimeMs > 0) {
                await sleep(downtimeMs);
            }
            await this.startInternal(reason);
        });
    }

    isRunning(): boolean {
        return this._running;
    }

    getPort(): number {
        if (this._lockedPort === null) {
            throw new Error("proxy has not started yet");
        }
        return this._lockedPort;
    }

    getHost(): string {
        return this._listenHost;
    }

    getUrl(): string {
        return `ws://${this._listenHost}:${this.getPort()}`;
    }

    getEventLog(): WsProxyEvent[] {
        return [...this._eventLog];
    }

    private async enqueueLifecycle(task: () => Promise<void>): Promise<void> {
        this._lifecycleChain = this._lifecycleChain.then(task, task);
        await this._lifecycleChain;
    }

    private async startInternal(reason?: string): Promise<void> {
        if (this._running) {
            return;
        }

        const listenPort = this._lockedPort === null
            ? this._requestedPort
            : this._lockedPort;
        const httpServer = http.createServer((_req, res) => {
            res.statusCode = 404;
            res.end();
        });

        await new Promise<void>((resolve, reject) => {
            const onError = (err: Error) => {
                httpServer.removeListener("listening", onListening);
                reject(err);
            };
            const onListening = () => {
                httpServer.removeListener("error", onError);
                resolve();
            };
            httpServer.once("error", onError);
            httpServer.once("listening", onListening);
            httpServer.listen(listenPort, this._listenHost);
        });

        const address = httpServer.address();
        if (!address || typeof address === "string") {
            httpServer.close();
            throw new Error("failed to get proxy listen address");
        }

        if (this._lockedPort === null) {
            this._lockedPort = address.port;
        }

        const wsServer = new WebSocketServer({
            httpServer,
            autoAcceptConnections: false,
        });
        wsServer.on("request", (request: WebSocketRequest) => {
            this.handleClientRequest(request);
        });

        this._httpServer = httpServer;
        this._wsServer = wsServer;
        this._running = true;

        this.emitEvent({
            type: "proxy_started",
            timestamp: Date.now(),
            host: this._listenHost,
            port: this.getPort(),
            url: this.getUrl(),
            reason,
        });
    }

    private async stopInternal(reason?: string): Promise<void> {
        if (!this._running && !this._httpServer && !this._wsServer) {
            return;
        }
        this._running = false;

        for (const tunnel of Array.from(this._tunnels.values())) {
            this.closeTunnel(
                tunnel.id,
                `proxy stopping${reason ? `: ${reason}` : ""}`,
                "proxy"
            );
        }

        if (this._wsServer) {
            try {
                this._wsServer.removeAllListeners("request");
                this._wsServer.closeAllConnections();
                this._wsServer.unmount();
            } catch (_err) {
                // Ignore shutdown errors and continue releasing resources.
            }
        }

        if (this._httpServer) {
            await new Promise<void>((resolve) => {
                if (!this._httpServer || !this._httpServer.listening) {
                    resolve();
                    return;
                }
                this._httpServer.close(() => resolve());
            });
        }

        this._wsServer = null;
        this._httpServer = null;

        this.emitEvent({
            type: "proxy_stopped",
            timestamp: Date.now(),
            reason,
        });
    }

    private handleClientRequest(request: WebSocketRequest): void {
        if (!this._running) {
            request.reject(503, "proxy is not running");
            return;
        }

        const path = request.resourceURL?.pathname || request.resource;

        const clientConn = request.accept(undefined, request.origin);
        const connectionId = this._nextConnectionId;
        this._nextConnectionId += 1;

        const tunnel: ProxyTunnel = {
            id: connectionId,
            path,
            closed: false,
            messageSeq: 0,
            clientConn,
            pendingFrames: [],
        };
        this._tunnels.set(connectionId, tunnel);

        this.emitEvent({
            type: "client_connected",
            timestamp: Date.now(),
            connectionId,
            path,
            remoteAddress: request.remoteAddress,
        });

        clientConn.on("message", (message: WsMessage) => {
            this.handleClientMessage(tunnel, message);
        });
        clientConn.on("close", (code: number, desc: string) => {
            this.closeTunnel(
                connectionId,
                `client closed: ${code} ${desc}`,
                "client"
            );
        });
        clientConn.on("error", (error: Error) => {
            this.emitError("client_error", error, connectionId);
            this.closeTunnel(
                connectionId,
                `client error: ${error.message}`,
                "client"
            );
        });

        this.connectUpstream(tunnel);
    }

    private connectUpstream(tunnel: ProxyTunnel): void {
        const upstreamClient = new WebSocketClient();
        tunnel.upstreamClient = upstreamClient;
        const upstreamUrl = this.buildUpstreamUrl(tunnel.path);

        upstreamClient.on("connect", (upstreamConn: WebSocketConnection) => {
            if (tunnel.closed) {
                upstreamConn.close();
                return;
            }

            tunnel.upstreamConn = upstreamConn;
            this.emitEvent({
                type: "upstream_connected",
                timestamp: Date.now(),
                connectionId: tunnel.id,
                upstreamUrl,
            });

            upstreamConn.on("message", (message: WsMessage) => {
                this.handleUpstreamMessage(tunnel, message);
            });
            upstreamConn.on("close", (code: number, desc: string) => {
                this.closeTunnel(
                    tunnel.id,
                    `upstream closed: ${code} ${desc}`,
                    "upstream"
                );
            });
            upstreamConn.on("error", (error: Error) => {
                this.emitError("upstream_error", error, tunnel.id);
                this.closeTunnel(
                    tunnel.id,
                    `upstream error: ${error.message}`,
                    "upstream"
                );
            });

            this.flushPendingFrames(tunnel);
        });

        upstreamClient.on("connectFailed", (error: Error) => {
            this.emitError("upstream_connect_failed", error, tunnel.id);
            this.closeTunnel(
                tunnel.id,
                `upstream connect failed: ${error.message}`,
                "upstream"
            );
        });

        upstreamClient.connect(upstreamUrl);
    }

    private buildUpstreamUrl(path: string): string {
        const upstream = new URL(DEFAULT_UPSTREAM_BASE_URL);
        upstream.pathname = ensureLeadingSlash(path);
        return upstream.toString();
    }

    private handleClientMessage(tunnel: ProxyTunnel, message: WsMessage): void {
        if (tunnel.closed) {
            return;
        }

        if (message.type === "utf8") {
            const rawData = message.utf8Data;
            this.emitMessageEvent(tunnel, "client_to_upstream", false, rawData);
            if (this.isConnected(tunnel.upstreamConn)) {
                tunnel.upstreamConn!.sendUTF(rawData);
                return;
            }
            tunnel.pendingFrames.push({
                isBinary: false,
                data: rawData,
            });
            return;
        }

        const rawData = Buffer.from(message.binaryData);
        this.emitMessageEvent(tunnel, "client_to_upstream", true, rawData);
        if (this.isConnected(tunnel.upstreamConn)) {
            tunnel.upstreamConn!.sendBytes(rawData);
            return;
        }
        tunnel.pendingFrames.push({
            isBinary: true,
            data: rawData,
        });
    }

    private handleUpstreamMessage(tunnel: ProxyTunnel, message: WsMessage): void {
        if (tunnel.closed || !this.isConnected(tunnel.clientConn)) {
            this.closeTunnel(
                tunnel.id,
                "client connection unavailable while forwarding upstream message",
                "client"
            );
            return;
        }

        if (message.type === "utf8") {
            const rawData = message.utf8Data;
            this.emitMessageEvent(tunnel, "upstream_to_client", false, rawData);
            tunnel.clientConn.sendUTF(rawData);
            return;
        }

        const rawData = Buffer.from(message.binaryData);
        this.emitMessageEvent(tunnel, "upstream_to_client", true, rawData);
        tunnel.clientConn.sendBytes(rawData);
    }

    private flushPendingFrames(tunnel: ProxyTunnel): void {
        if (!this.isConnected(tunnel.upstreamConn) || tunnel.closed) {
            return;
        }
        while (tunnel.pendingFrames.length > 0) {
            const frame = tunnel.pendingFrames.shift();
            if (!frame) {
                break;
            }
            if (frame.isBinary) {
                tunnel.upstreamConn!.sendBytes(frame.data as Buffer);
            } else {
                tunnel.upstreamConn!.sendUTF(frame.data as string);
            }
        }
    }

    private closeTunnel(
        connectionId: number,
        reason: string,
        source: "client" | "upstream" | "proxy",
    ): void {
        const tunnel = this._tunnels.get(connectionId);
        if (!tunnel || tunnel.closed) {
            return;
        }
        tunnel.closed = true;
        this._tunnels.delete(connectionId);

        const isProxyInjectedFailure = source === "proxy";
        this.safeCloseConnection(tunnel.clientConn, isProxyInjectedFailure);
        this.safeCloseConnection(tunnel.upstreamConn, isProxyInjectedFailure);

        this.emitEvent({
            type: "tunnel_closed",
            timestamp: Date.now(),
            connectionId,
            reason,
            source,
        });
    }

    private isConnected(conn?: WebSocketConnection): boolean {
        return !!conn && conn.connected;
    }

    private emitMessageEvent(
        tunnel: ProxyTunnel,
        direction: WsProxyDirection,
        isBinary: boolean,
        rawData: Buffer | string,
    ): void {
        tunnel.messageSeq += 1;
        this.emitEvent({
            type: "message",
            timestamp: Date.now(),
            connectionId: tunnel.id,
            direction,
            isBinary,
            byteLength: typeof rawData === "string"
                ? Buffer.byteLength(rawData)
                : rawData.byteLength,
            rawData,
            messageSeq: tunnel.messageSeq,
        });
    }

    private emitError(
        stage: string,
        error: unknown,
        connectionId?: number,
        sourceEventType?: WsProxyEventType,
        details?: string,
        notifyCallback: boolean = true,
    ): void {
        this.emitEvent({
            type: "error",
            timestamp: Date.now(),
            connectionId,
            error: normalizeError(error),
            stage,
            sourceEventType,
            details,
        }, notifyCallback);
    }

    private emitEvent(event: WsProxyEvent, notifyCallback: boolean = true): void {
        this._eventLog.push(event);
        if (!notifyCallback || !this._onEvent) {
            return;
        }
        void Promise.resolve(this._onEvent(event, this._control)).catch((err) => {
            this.emitError(
                "event_handler",
                err,
                event.connectionId,
                event.type,
                "event callback threw an error",
                false,
            );
        });
    }

    private safeCloseConnection(
        conn: WebSocketConnection | undefined,
        abnormal: boolean
    ): void {
        try {
            if (!this.isConnected(conn)) {
                return;
            }
            if (abnormal) {
                conn!.close(1012, "proxy restart");
                return;
            }
            conn!.close();
        } catch (_err) {
            // Ignore close failures.
        }
    }
}

function normalizeError(error: unknown): Error {
    if (error instanceof Error) {
        return error;
    }
    return new Error(String(error));
}

function ensureLeadingSlash(path: string): string {
    if (!path || path.trim().length === 0) {
        throw new Error("proxy supported path must not be empty");
    }
    const trimmed = path.trim();
    return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
