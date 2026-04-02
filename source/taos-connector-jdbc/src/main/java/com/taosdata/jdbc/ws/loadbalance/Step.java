package com.taosdata.jdbc.ws.loadbalance;

import java.util.concurrent.CompletableFuture;

interface Step {
    // execute the current step (return the next step and interval)
    CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow);
}