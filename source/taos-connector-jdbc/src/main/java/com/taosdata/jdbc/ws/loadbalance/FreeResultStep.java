package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.FreeResultReq;
import com.taosdata.jdbc.ws.entity.Request;

import java.util.concurrent.CompletableFuture;

class FreeResultStep implements Step {

    @Override
    public CompletableFuture<StepResponse> execute(BgHealthCheck context, StepFlow flow) {
        // connection is closed
        if (!context.getWsClient().isOpen()) {
            return CompletableFuture.completedFuture(new StepResponse(StepEnum.CONNECT, context.getNextInterval()));
        }

        // send free result
        FreeResultReq freeResultReq = new FreeResultReq();
        freeResultReq.setReqId(ReqId.getReqID());
        freeResultReq.setId(context.getResultId());

        Request request = new Request(Action.FREE_RESULT.getAction(), freeResultReq);
        String reqString = request.toString();
        context.getWsClient().send(reqString);

        return CompletableFuture.completedFuture(new StepResponse(StepEnum.QUERY, context.getNextInterval()));
    }
}