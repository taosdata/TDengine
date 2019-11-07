package com.taosdata.iot.mybatisTest.entities;

import java.sql.Timestamp;

/**
 * @author Jiangyi Hou
 * @since 18-11-9
 */

public class Record {

    // columns
    private Timestamp ts;
    private Integer c1;
    private String c2;
    private Short c3;
    private Long c4;
    private String c5;
    private Boolean c6;
    private Byte c7;
    private Float c8;
    private Double c9;

    // tags
    private Integer deviceId;
    private String t2;

    public Record() {
        super();
    }
    public Record(Timestamp ts, Integer c1, String c2, Short c3, Long c4, String c5, Boolean c6, Byte c7, Float c8, Double c9, Integer deviceId) {
        this.ts = ts;
        this.c1 = c1;
        this.c2 = c2;
        this.c3 = c3;
        this.c4 = c4;
        this.c5 = c5;
        this.c6 = c6;
        this.c7 = c7;
        this.c8 = c8;
        this.c9 = c9;
        this.deviceId = deviceId;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Integer getC1() {
        return c1;
    }

    public void setC1(Integer c1) {
        this.c1 = c1;
    }

    public String getC2() {
        return c2;
    }

    public void setC2(String c2) {
        this.c2 = c2;
    }

    public Short getC3() {
        return c3;
    }

    public void setC3(Short c3) {
        this.c3 = c3;
    }

    public Long getC4() {
        return c4;
    }

    public void setC4(Long c4) {
        this.c4 = c4;
    }

    public String getC5() {
        return c5;
    }

    public void setC5(String c5) {
        this.c5 = c5;
    }

    public Boolean getC6() {
        return c6;
    }

    public void setC6(Boolean c6) {
        this.c6 = c6;
    }

    public Byte getC7() {
        return c7;
    }

    public void setC7(Byte c7) {
        this.c7 = c7;
    }

    public Float getC8() {
        return c8;
    }

    public void setC8(Float c8) {
        this.c8 = c8;
    }

    public Double getC9() {
        return c9;
    }

    public void setC9(Double c9) {
        this.c9 = c9;
    }

    public Integer getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Integer deviceId) {
        this.deviceId = deviceId;
    }

    public String getT2() {
        return t2;
    }

    public void setT2(String t2) {
        this.t2 = t2;
    }

    @Override
    public String toString() {
//        return "Record: [ts " + ts + ", c1 " + c1 + ", c2 " + c2 + "]";
        return "Record: (" + ts + ", " + c1 + ", " + c2 + ", " + c3 + ", " + c4 + ", " + c5 + ", " + c6 + ", " + c7
                + ", " + c8 + ", " + c9 + ")";
    }
}
