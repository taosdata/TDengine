package com.taosdata.jdbc.springbootdemo.domain;

import java.sql.Timestamp;

public class Weather {

    private Timestamp ts;

    private int temperature;

    private float humidity;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public float getHumidity() {
        return humidity;
    }

    public void setHumidity(float humidity) {
        this.humidity = humidity;
    }
}
