package com.taosdata.example.jdbcTemplate.domain;

import java.sql.Timestamp;

public class Weather {

    private Timestamp ts;
    private float temperature;
    private int humidity;

    public Weather() {
    }

    public Weather(Timestamp ts, float temperature, int humidity) {
        this.ts = ts;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "ts=" + ts +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }


}
