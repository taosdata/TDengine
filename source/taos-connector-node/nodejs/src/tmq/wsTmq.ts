import JSONBig from "json-bigint";
import { TmqConfig } from "./config";
import { TMQConstants, TMQMessageType } from "./constant";
import { WsClient } from "../client/wsClient";
import { TaosResult } from "../common/taosResult";
import {
    ErrorCode,
    TaosResultError,
    TDWebSocketClientError,
    WebSocketInterfaceError,
} from "../common/wsError";
import {
    AssignmentResp,
    CommittedResp,
    PartitionsResp,
    SubscriptionResp,
    TaosTmqResult,
    TopicPartition,
    WSTmqFetchBlockInfo,
    WsPollResponse,
} from "./tmqResponse";
import { ReqId } from "../common/reqid";
import logger from "../common/log";
import { WSFetchBlockResponse } from "../client/wsResponse";
import { maskTmqConfigForLog, safeDecodeURIComponent } from "../common/utils";
import { ConnectorInfo } from "../common/constant";

export class WsConsumer {
    private _wsClient: WsClient;
    private _config: TmqConfig;
    private _topics?: string[];
    private _commitTime?: number;
    private _lastMessageID?: bigint;

    private constructor(wsConfig: Map<string, any>) {
        this._config = new TmqConfig(wsConfig);
        if (logger.isDebugEnabled()) {
            logger.debug("WsConsumer config: " + maskTmqConfigForLog(this._config));
        }
        if (wsConfig.size == 0 || !this._config.dsn) {
            throw new WebSocketInterfaceError(
                ErrorCode.ERR_INVALID_URL,
                "invalid url, password or username needed."
            );
        }
        this._wsClient = new WsClient(this._config.dsn, this._config.timeout);
        this.bindSessionRecoveryHook();
        this._lastMessageID = BigInt(0);
    }

    private async init(): Promise<WsConsumer> {
        let wsSql = null;
        try {
            if (this._config.sqlDsn) {
                wsSql = new WsClient(this._config.sqlDsn, this._config.timeout);
                await wsSql.connect();
                await wsSql.checkVersion();
                await this._wsClient.ready();
            } else {
                throw new TDWebSocketClientError(
                    ErrorCode.ERR_WEBSOCKET_CONNECTION_FAIL,
                    `connection creation failed, dsn: ${this._config.dsn}`
                );
            }
        } catch (e: any) {
            await this._wsClient.close();
            throw e;
        } finally {
            if (wsSql) {
                await wsSql.close();
            }
        }
        return this;
    }

    static async newConsumer(wsConfig: Map<string, any>): Promise<WsConsumer> {
        if (wsConfig.size == 0 || !wsConfig.get(TMQConstants.WS_URL)) {
            throw new WebSocketInterfaceError(
                ErrorCode.ERR_INVALID_URL,
                "Invalid DSN"
            );
        }
        const wsConsumer = new WsConsumer(wsConfig);
        return await wsConsumer.init();
    }

    private bindSessionRecoveryHook(): void {
        this._wsClient.setSessionRecoveryHook(async () => {
            await this.recoverSessionContext();
        });
    }

    private buildSubscribeMessage(topics: Array<string>, reqId?: number) {
        const user = this._config.user === null
            ? null
            : safeDecodeURIComponent(this._config.user);
        const password = this._config.password === null
            ? null
            : safeDecodeURIComponent(this._config.password);

        return {
            action: TMQMessageType.Subscribe,
            args: {
                req_id: ReqId.getReqID(reqId),
                user,
                password,
                group_id: this._config.group_id,
                client_id: this._config.client_id,
                topics: topics,
                offset_rest: this._config.offset_rest,
                auto_commit: String(this._config.auto_commit),
                auto_commit_interval_ms: String(this._config.auto_commit_interval_ms),
                config: Object.fromEntries(this._config.otherConfigs),
                ...(this._config.userApp && { app: this._config.userApp }),
                ...(this._config.userIp && { ip: this._config.userIp }),
                connector: ConnectorInfo,
            },
        };
    }

    private async recoverSessionContext(): Promise<void> {
        if (!this._topics || this._topics.length === 0) {
            return;
        }
        logger.info(
            `Recovering TMQ session context, re-subscribing ${this._topics.length} topic(s).`
        );
        await this._wsClient.sendMsgDirect(
            JSON.stringify(this.buildSubscribeMessage(this._topics)),
            false
        );
    }

    async subscribe(topics: Array<string>, reqId?: number): Promise<void> {
        if (!topics || topics.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq Subscribe params is error!"
            );
        }

        let queryMsg = this.buildSubscribeMessage(topics, reqId);
        await this._wsClient.exec(JSON.stringify(queryMsg));
        this._topics = [...topics];
    }

    async unsubscribe(reqId?: number): Promise<void> {
        let queryMsg = {
            action: TMQMessageType.Unsubscribe,
            args: {
                req_id: ReqId.getReqID(reqId),
            },
        };
        await this._wsClient.exec(JSON.stringify(queryMsg));
        this._topics = undefined;
    }

    async poll(timeoutMs: number, reqId?: number): Promise<Map<string, TaosResult>> {
        if (this._config.auto_commit) {
            if (this._commitTime) {
                let currTime = new Date().getTime();
                let diff = Math.abs(currTime - this._commitTime);
                if (diff >= this._config.auto_commit_interval_ms) {
                    await this.doCommit();
                    this._commitTime = new Date().getTime();
                }
            } else {
                this._commitTime = new Date().getTime();
            }
        }
        return await this.pollData(timeoutMs, reqId);
    }

    async subscription(reqId?: number): Promise<Array<string>> {
        let queryMsg = {
            action: TMQMessageType.ListTopics,
            args: {
                req_id: ReqId.getReqID(reqId),
            },
        };
        let resp = await this._wsClient.exec(JSON.stringify(queryMsg), false);
        return new SubscriptionResp(resp).topics;
    }

    async commit(reqId?: number): Promise<Array<TopicPartition>> {
        await this.doCommit(reqId);
        return await this.assignment();
    }

    private async doCommit(reqId?: number): Promise<void> {
        let queryMsg = {
            action: TMQMessageType.Commit,
            args: {
                req_id: ReqId.getReqID(reqId),
                message_id: 0,
            },
        };
        await this._wsClient.exec(JSON.stringify(queryMsg));
    }

    async committed(
        partitions: Array<TopicPartition>,
        reqId?: number
    ): Promise<Array<TopicPartition>> {
        if (!partitions || partitions.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq Positions params is error!"
            );
        }

        let offsets: TopicPartition[] = new Array(partitions.length);
        for (let i = 0; i < partitions.length; i++) {
            offsets[i] = {
                topic: partitions[i].topic,
                vgroup_id: partitions[i].vgroup_id,
            };
            offsets[i].vgroup_id = partitions[i].vgroup_id;
        }

        let queryMsg = {
            action: TMQMessageType.Committed,
            args: {
                req_id: ReqId.getReqID(reqId),
                topic_vgroup_ids: offsets,
            },
        };

        let resp = await this._wsClient.exec(
            JSONBig.stringify(queryMsg),
            false
        );
        return new CommittedResp(resp).setTopicPartitions(offsets);
    }

    async commitOffsets(partitions: Array<TopicPartition>): Promise<Array<TopicPartition>> {
        if (!partitions || partitions.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq CommitOffsets params is error!"
            );
        }
        const allp: any[] = [];
        partitions.forEach((e) => {
            allp.push(this.commitOffset(e));
        });
        await Promise.all(allp);
        return await this.committed(partitions);
    }

    async commitOffset(partition: TopicPartition, reqId?: number): Promise<void> {
        if (!partition) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq CommitOffsets params is error!"
            );
        }

        let queryMsg = {
            action: TMQMessageType.CommitOffset,
            args: {
                req_id: ReqId.getReqID(reqId),
                vgroup_id: partition.vgroup_id,
                topic: partition.topic,
                offset: partition.offset,
            },
        };
        return await this._wsClient.exec(JSONBig.stringify(queryMsg));
    }

    async positions(
        partitions: Array<TopicPartition>,
        reqId?: number
    ): Promise<Array<TopicPartition>> {
        if (!partitions || partitions.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq Positions params is error!"
            );
        }

        let offsets: TopicPartition[] = new Array(partitions.length);
        for (let i = 0; i < partitions.length; i++) {
            offsets[i] = {
                topic: partitions[i].topic,
                vgroup_id: partitions[i].vgroup_id,
            };
            offsets[i].vgroup_id = partitions[i].vgroup_id;
        }
        let queryMsg = {
            action: TMQMessageType.Position,
            args: {
                req_id: ReqId.getReqID(reqId),
                topic_vgroup_ids: offsets,
            },
        };

        let resp = await this._wsClient.exec(JSON.stringify(queryMsg), false);
        return new PartitionsResp(resp).setTopicPartitions(offsets);
    }

    async seek(partition: TopicPartition, reqId?: number): Promise<void> {
        if (!partition) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq Seek params is error!"
            );
        }

        let queryMsg = {
            action: TMQMessageType.Seek,
            args: {
                req_id: ReqId.getReqID(reqId),
                vgroup_id: partition.vgroup_id,
                topic: partition.topic,
                offset: partition.offset,
            },
        };
        return await this._wsClient.exec(JSON.stringify(queryMsg));
    }

    async seekToBeginning(partitions: Array<TopicPartition>): Promise<void> {
        if (!partitions || partitions.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq SeekToBeginning params is error!"
            );
        }
        return await this.seekToBeginOrEnd(partitions);
    }

    async seekToEnd(partitions: Array<TopicPartition>): Promise<void> {
        if (!partitions || partitions.length == 0) {
            throw new TaosResultError(
                ErrorCode.ERR_INVALID_PARAMS,
                "WsTmq SeekToEnd params is error!"
            );
        }
        return await this.seekToBeginOrEnd(partitions, false);
    }

    async close(): Promise<void> {
        await this._wsClient.close();
    }

    private async fetchBlockData(
        pollResp: WsPollResponse,
        taosResult: TaosTmqResult
    ): Promise<boolean> {
        let fetchMsg = {
            action: "fetch_raw_data",
            args: {
                req_id: ReqId.getReqID(),
                message_id: pollResp.message_id,
            },
        };
        let jsonStr = JSONBig.stringify(fetchMsg);
        logger.debug("[wsQueryInterface.fetch.fetchMsg]===>" + jsonStr);
        let result = await this._wsClient.sendMsg(jsonStr);
        let wsResponse = new WSFetchBlockResponse(result.msg);
        if (wsResponse && wsResponse.data && wsResponse.blockLen > 0) {
            let wsTmqResponse = new WSTmqFetchBlockInfo(
                wsResponse.data,
                taosResult
            );
            logger.debug(
                "[WSTmqFetchBlockInfo.fetchBlockData]===>" +
                wsTmqResponse.taosResult
            );
            if (wsTmqResponse.rows > 0) {
                return true;
            }
        }
        return false;
    }

    private async pollData(
        timeoutMs: number,
        reqId?: number
    ): Promise<Map<string, TaosResult>> {
        let queryMsg = {
            action: TMQMessageType.Poll,
            args: {
                req_id: ReqId.getReqID(reqId),
                blocking_time: timeoutMs,
                message_id: this._lastMessageID,
            },
        };

        let resp = await this._wsClient.exec(
            JSONBig.stringify(queryMsg),
            false
        );
        let pollResp = new WsPollResponse(resp);
        let taosResult = new TaosTmqResult(pollResp);
        var taosResults: Map<string, TaosResult> = new Map();
        taosResults.set(pollResp.topic, taosResult);
        if (
            !pollResp.have_message ||
            pollResp.message_type != TMQMessageType.ResDataType
        ) {
            return taosResults;
        }

        this._lastMessageID = pollResp.message_id;
        let finish = false;

        while (!finish) {
            finish = await this.fetchBlockData(pollResp, taosResult);
        }

        return taosResults;
    }

    private async sendAssignmentReq(
        topic: string
    ): Promise<Array<TopicPartition>> {
        let queryMsg = {
            action: TMQMessageType.GetTopicAssignment,
            args: {
                req_id: ReqId.getReqID(),
                topic: topic,
            },
        };

        let resp = await this._wsClient.exec(JSON.stringify(queryMsg), false);
        let assignmentInfo = new AssignmentResp(resp, queryMsg.args.topic);
        return assignmentInfo.topicPartition;
    }

    async assignment(topics?: string[]): Promise<Array<TopicPartition>> {
        if (!topics || topics.length == 0) {
            topics = this._topics;
        }

        let topicPartitions: TopicPartition[] = [];

        if (topics && topics.length > 0) {
            const allp: any[] = [];
            for (let i in topics) {
                allp.push(this.sendAssignmentReq(topics[i]));
            }
            let result = await Promise.all(allp);
            result.forEach((e) => {
                topicPartitions.push(...e);
            });
        }
        return topicPartitions;
    }

    private async seekToBeginOrEnd(
        partitions: Array<TopicPartition>,
        bBegin: boolean = true
    ): Promise<void> {
        let topics: string[] = [];
        partitions.forEach((e) => {
            topics.push(e.topic);
        });

        let topicPartitions = await this.assignment(topics);
        let itemMap = topicPartitions.reduce((map, obj) => {
            map.set(obj.topic + "_" + obj.vgroup_id, obj);
            return map;
        }, new Map<string, TopicPartition>());

        const allp: any[] = [];
        for (let i in partitions) {
            if (
                itemMap.has(partitions[i].topic + "_" + partitions[i].vgroup_id)
            ) {
                let topicPartition = itemMap.get(
                    partitions[i].topic + "_" + partitions[i].vgroup_id
                );
                if (topicPartition) {
                    if (bBegin) {
                        topicPartition.offset = topicPartition.begin;
                    } else {
                        topicPartition.offset = topicPartition.end;
                    }
                    allp.push(this.seek(topicPartition));
                }
            }
        }
        await Promise.all(allp);
    }
}
