package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class ConCmdStep implements Step {
    private static final Logger log = LoggerFactory.getLogger(ConCmdStep.class);
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // connection is closed
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        // send query
        ConnectReq connectReq = new ConnectReq(context.getParam());

        Request request = new Request(Action.CONN.getAction(), connectReq);
        String reqString = request.toString();
        context.getWsClient().send(reqString);

        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try{
            context.getInFlightRequest().put(new FutureResponse(request.getAction(), request.id(), completableFuture));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval()));
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, context.getParam().getRequestTimeout(), TimeUnit.MILLISECONDS, reqString);

        return responseFuture
                .thenApply(response -> {
                    context.getInFlightRequest().remove(request.getAction(), request.id());
                    ConnectResp connectResp = (ConnectResp) response;
                    if (Code.SUCCESS.getCode() == connectResp.getCode()) {
                        return new StepResponse(StepEnum.QUERY, 0);
                    } else {
                        context.cleanUp();
                        return new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval());
                    }
                })
                .exceptionally(ex -> {
                    log.info("Connection command failed. {}", ex.getMessage());
                    context.cleanUp();
                    return new StepResponse(StepEnum.CON_CMD, context.getRecoveryInterval());
                });
    }
}