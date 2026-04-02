package com.taosdata.jdbc.ws.loadbalance;

public class StepResponse {
    private final StepEnum step;
    private final int waitSeconds;

    public StepResponse(StepEnum step, int waitSeconds) {
        this.step = step;
        this.waitSeconds = waitSeconds;
    }

    public StepEnum getStep() {
        return step;
    }
    public int getWaitSeconds() {
        return waitSeconds;
    }

    public String toString() {
        return "StepResponse{stepIndex=" + StepEnum.getNameByInt(step.get()) + ", waitSeconds=" + waitSeconds + "}";
    }
}
