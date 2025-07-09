package com.taos.example.highvolume;

import java.sql.Timestamp;

public class Meters {
    String tableName;
    Timestamp ts;
    float current;
    int voltage;
    float phase;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

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

    public float getPhase() {
        return phase;
    }

    public void setPhase(float phase) {
        this.phase = phase;
    }

    @Override
    public String toString() {
        return tableName + "," +
                ts.toString() + "," +
                current + "," +
                voltage + "," +
                phase;
    }

    public static Meters fromString(String str) {
        String[] parts = str.split(",");
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid input format");
        }
        Meters meters = new Meters();
        meters.setTableName(parts[0]);
        meters.setTs(Timestamp.valueOf(parts[1]));
        meters.setCurrent(Float.parseFloat(parts[2]));
        meters.setVoltage(Integer.parseInt(parts[3]));
        meters.setPhase(Float.parseFloat(parts[4]));
        return meters;
    }

}
