package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class Fetch2Step implements Step {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Fetch2Step.class);
    private final RebalanceManager rebalanceManager;

    public Fetch2Step() {
        rebalanceManager = RebalanceManager.getInstance();
    }
    public Fetch2Step(RebalanceManager rebalanceManager) {
        this.rebalanceManager = rebalanceManager;
    }
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        final byte[] version = {1, 0};

        int totalLength = 26;
        long reqId = ReqId.getReqID();
        String action = Action.FETCH_BLOCK_NEW.getAction();
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(context.getResultId());
        buffer.writeLongLE(7); // fetch block action type
        buffer.writeBytes(version);

        // send query
        context.getWsClient().send(buffer);

        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try{
            context.getInFlightRequest().put(new FutureResponse(action, reqId, completableFuture));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, 0));
        }

        String reqString = "action:" + action + ", reqId:" + reqId + ", resultId:" + 0 + ", actionType:" + 6;

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, context.getParam().getRequestTimeout(), TimeUnit.MILLISECONDS, reqString);

        return responseFuture
                .thenApply(response -> {
                    context.getInFlightRequest().remove(action, reqId);

                    FetchBlockHealthCheckResp queryResp = (FetchBlockHealthCheckResp) response;
                    if (queryResp.isCompleted()) {
                        if (context.needMoreRetry()){
                            return new StepResponse(StepEnum.QUERY, context.getRecoveryInterval());
                        }
                        else {
                            rebalanceManager.endpointUp(context.getParam(), context.getEndpoint());
                            return new StepResponse(StepEnum.CLOSE, 0);
                        }
                    } else {
                        return new StepResponse(StepEnum.FETCH2, 0);
                    }
                })
                .exceptionally(ex -> {
                    log.info("fetch command failed. {}", ex.getMessage());
                    context.cleanUp();
                    return new StepResponse(StepEnum.FREE_RESULT, context.getRecoveryInterval());
                });
    }
}