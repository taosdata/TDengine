package com.taosdata.jdbc.ws.loadbalance;

public enum StepEnum {
    CONNECT(0),
    CON_CMD(1),
    QUERY(2),
    FETCH(3),
    FETCH2(4),
    FREE_RESULT(5),
    CLOSE(6),
    FINISH(7),

    ;
    private final int step;

    StepEnum(int step) {
        this.step = step;
    }

    public int get() {
        return step;
    }

    public static String getNameByInt(int step) {
        for (StepEnum stepEnum : values()) {
            if (stepEnum.step == step) {
                return stepEnum.name(); // return the enum name as a string
            }
        }
        throw new IllegalArgumentException("No matching StepEnum for int value: " + step);
    }
}
