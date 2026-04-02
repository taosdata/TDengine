package com.taosdata.jdbc.ws.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class ConnectStep implements Step {
    private static final Logger log = LoggerFactory.getLogger(ConnectStep.class);
    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        if (context.isCancelled()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.FINISH, 0));
        }

        if (context.getWsClient() != null && context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CON_CMD, 0));
        }

        // call the common method to get Channel asynchronously
        return context.getWsClient().getChannelAsync()
                .thenApply(channel -> {
                    log.info("Connection established successfully.");
                    return new StepResponse(StepEnum.CON_CMD, 0);
                })
                .exceptionally(ex -> {
                    log.info("Connection or handshake failed. endpoint: {}", context.getEndpoint());
                    context.cleanUp();
                    return new StepResponse(StepEnum.CONNECT, context.getNextInterval());
                });
    }
}