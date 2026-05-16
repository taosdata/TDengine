package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class QueryStep implements Step {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(QueryStep.class);

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        String action = Action.BINARY_QUERY.getAction();
        long reqId = ReqId.getReqID();

        byte[] sqlBytes = "SHOW CLUSTER ALIVE;".getBytes(StandardCharsets.UTF_8);
        int totalLength = 30 + sqlBytes.length;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(0L); // result id
        buffer.writeLongLE(6L);
        buffer.writeShortLE(1);
        buffer.writeIntLE(sqlBytes.length);
        buffer.writeBytes(sqlBytes);


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

                    QueryResp queryResp = (QueryResp) response;
                    if (Code.SUCCESS.getCode() == queryResp.getCode()) {
                        context.setResultId(queryResp.getId());
                        context.addRecoveryCmdCount();
                        return new StepResponse(StepEnum.FETCH, 0);
                    } else {
                        context.cleanUp();
                        return new StepResponse(StepEnum.QUERY, 0);
                    }
                })
                .exceptionally(ex -> {
                    log.info("execute 'show cluster alive' failed. {}", ex.getMessage());
                    context.cleanUp();
                    return new StepResponse(StepEnum.QUERY, context.getRecoveryInterval());
                });
    }
}