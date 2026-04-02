package com.taosdata.jdbc.ws.loadbalance;

import java.util.concurrent.CompletableFuture;

class CloseStep implements Step {

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // connection is closed
        if (context.getWsClient().isOpen()) {
            context.getWsClient().closeAsync();
        }
        return CompletableFuture.completedFuture(new StepResponse(StepEnum.FINISH, 0));
    }
}