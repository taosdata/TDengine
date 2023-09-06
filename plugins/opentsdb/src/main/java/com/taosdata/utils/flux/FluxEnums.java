package com.taosdata.utils.flux;

/**
 * 速度控制枚举类
 *
 * @author ZYP
 */
public enum FluxEnums {

    ReadData("ReadData", "Read Data", "#ReadData#Current speed of reading data"),
    PushData("PushData", "Push Data", "#PushData#Current speed of pushing data");

    /**
     * 编号
     */
    private String code;

    /**
     * 描述
     */
    private String desc;

    /**
     * 打印内容
     */
    private String print;

    FluxEnums(String code, String desc, String print) {
        this.code = code;
        this.desc = desc;
        this.print = print;
    }

    public String getCode() {
        return this.code;
    }

    public String getDesc() {
        return this.desc;
    }

    public String getPrint() {
        return this.print;
    }
}
