package com.taosdata.flink.entity;

import java.sql.Timestamp;

public class ResultBean {
    private Timestamp ts;
    private int voltage;
    private Float current;
    private Float phase;

    private String location;

    private int groupid;

    private String tbname;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public Float getCurrent() {
        return current;
    }

    public void setCurrent(Float current) {
        this.current = current;
    }


    public Float getPhase() {
        return phase;
    }

    public void setPhase(Float phase) {
        this.phase = phase;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ResultBean{");
        sb.append("ts=").append(ts);
        sb.append(", current=").append(current);
        sb.append(", voltage=").append(voltage);
        sb.append(", phase='").append(phase);
        sb.append('}');
        return sb.toString();
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getGroupid() {
        return groupid;
    }

    public void setGroupid(int groupid) {
        this.groupid = groupid;
    }

    public String getTbname() {
        return tbname;
    }

    public void setTbname(String tbname) {
        this.tbname = tbname;
    }
}
