package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.utils.Utils;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

class StepFlow {
    static final Logger log = org.slf4j.LoggerFactory.getLogger(StepFlow.class);
    private final List<Step> steps;
    private final BgHealthCheck context;
    private int currentStepIndex;

    public StepFlow(List<Step> steps, BgHealthCheck context) {
        this.steps = steps;
        this.context = context;
    }

    public void start() {
        currentStepIndex = 0;
        executeCurrentStep();
    }

    private void executeCurrentStep() {
        if (currentStepIndex >= steps.size()) {
            RebalanceManager.getInstance().removeBgHealthCheckInstance();
            RebalanceManager.getInstance().removeBgHealthCheck(context);
            return;
        }
        steps.get(currentStepIndex).execute(context, this)
                // handles the result of the step (regardless of success or failure, it is up to the controller to decide the next step)
                .whenComplete((stepResponse, ex) -> {
                    if (ex != null) {
                        log.info("Error executing step {}: {}", StepEnum.getNameByInt(currentStepIndex), ex.getMessage());
                    } else {
                        log.info("Step {} returned response: {}", StepEnum.getNameByInt(currentStepIndex), stepResponse);
                    }

                    if (stepResponse.getWaitSeconds() == 0){
                        currentStepIndex = stepResponse.getStep().get();
                        executeCurrentStep();
                    } else {
                        Utils.getEventLoopGroup().schedule(() -> {
                            currentStepIndex = stepResponse.getStep().get();
                            executeCurrentStep();
                        }, stepResponse.getWaitSeconds(), TimeUnit.SECONDS);
                    }
                });
    }
}