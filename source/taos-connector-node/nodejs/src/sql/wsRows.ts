import { TDengineMeta, TaosResult, parseBlock } from "../common/taosResult";
import { TaosResultError } from "../common/wsError";
import { WSFetchBlockResponse, WSQueryResponse } from "../client/wsResponse";
import { WsClient } from "../client/wsClient";
import logger from "../common/log";
import { ReqId } from "../common/reqid";
import { getBinarySql } from "../common/utils";
import { FetchRawBlockMessage } from "../common/constant";

export class WSRows {
    private _wsClient: WsClient;
    private readonly _wsQueryResponse: WSQueryResponse;
    private _taosResult: TaosResult;
    private _isClose: boolean;

    constructor(wsInterface: WsClient, resp: WSQueryResponse) {
        this._wsClient = wsInterface;
        this._wsQueryResponse = resp;
        this._taosResult = new TaosResult(resp);
        this._isClose = false;
    }

    async next(): Promise<boolean> {
        if (this._wsQueryResponse.is_update || this._isClose) {
            logger.debug(
                `WSRows::Next::End=> ${this._taosResult}, ${this._isClose}`
            );
            return false;
        }

        let data = this._taosResult.getData();
        if (this._taosResult && data != null) {
            if (
                data &&
                Array.isArray(this._taosResult.getData()) &&
                data.length > 0
            ) {
                return true;
            }
        }

        this._taosResult = await this.getBlockData();
        if (this._taosResult.getData()) {
            return true;
        }
        return false;
    }

    private async getBlockData(): Promise<TaosResult> {
        try {
            if (this._wsQueryResponse.id) {
                let bigintReqId = BigInt(ReqId.getReqID());
                let resp = await this._wsClient.sendBinaryMsg(
                    bigintReqId,
                    "binary_query",
                    getBinarySql(
                        FetchRawBlockMessage,
                        bigintReqId,
                        BigInt(this._wsQueryResponse.id)
                    ),
                    false,
                    true
                );

                this._taosResult.addTotalTime(resp.totalTime);
                let wsResponse = new WSFetchBlockResponse(resp.msg);
                if (wsResponse.code != 0) {
                    await this.close();
                    logger.error(
                        `Executing SQL statement returns error: ${wsResponse.code}, ${wsResponse.message}`
                    );
                    throw new TaosResultError(
                        wsResponse.code,
                        wsResponse.message
                    );
                }

                if (wsResponse.finished == 1) {
                    await this.close();
                    this._taosResult.setData(null);
                } else {
                    parseBlock(wsResponse, this._taosResult);
                }
            }
            return this._taosResult;
        } catch (err: any) {
            try {
                await this.close();
            } catch (closeErr: any) {
                logger.error(
                    `GetBlockData encountered an exception while calling the close method, reason: ${closeErr.message}`
                );
            }
            throw new TaosResultError(err.code, err.message);
        }
    }

    getMeta(): Array<TDengineMeta> | null {
        return this._taosResult.getMeta();
    }

    getData(): Array<any> | undefined {
        if (this._wsQueryResponse.is_update) {
            return undefined;
        }

        let data = this._taosResult.getData();
        if (this._taosResult && data != null) {
            if (Array.isArray(data) && data.length > 0) {
                return data.shift();
            }
        }
        return undefined;
    }

    async close(): Promise<void> {
        if (this._isClose) {
            return;
        }
        this._isClose = true;
        await this._wsClient.freeResult(this._wsQueryResponse);
    }
}
