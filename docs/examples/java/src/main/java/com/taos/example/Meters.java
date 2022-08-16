package com.taos.example;

import java.sql.Timestamp;

public class Meters {
    private Timestamp ts;
    private float current;
    private int voltage;
    private int groupid;
    private String location;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getCurrent() {
        return current;
    }

    public void setCurrent(float current) {
        this.current = current;
    }

    public int getVoltage() {
        return voltage;
    }

    public void setVoltage(int voltage) {
        this.voltage = voltage;
    }

    public int getGroupid() {
        return groupid;
    }

    public void setGroupid(int groupid) {
        this.groupid = groupid;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Meters{" +
                "ts=" + ts +
                ", current=" + current +
                ", voltage=" + voltage +
                ", groupid=" + groupid +
                ", location='" + location + '\'' +
                '}';
    }
}
