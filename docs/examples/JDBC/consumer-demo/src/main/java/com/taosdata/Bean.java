package com.taosdata;

import java.sql.Timestamp;

public class Bean {
    private Timestamp ts;
    private Integer c1;
    private String c2;

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Bean {");
        sb.append("ts=").append(ts);
        sb.append(", c1=").append(c1);
        sb.append(", c2='").append(c2).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
