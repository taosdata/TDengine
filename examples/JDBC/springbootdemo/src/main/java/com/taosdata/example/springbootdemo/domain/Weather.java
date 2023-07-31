package com.taosdata.example.springbootdemo.domain;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class Weather {

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "GMT+8")
    private Timestamp ts;
    private Float temperature;
    private Float humidity;
    private String location;
    private String note;
    // In rest mode, the byte[] type is not recommended.
    // UTF-8 is used to encode the byte arrays, that result may affect the SQL correctness
    private byte[] bytes;
    private int groupId;

    public Weather() {
    }

    public Weather(Timestamp ts, float temperature, float humidity) {
        this.ts = ts;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Float getTemperature() {
        return temperature;
    }

    public void setTemperature(Float temperature) {
        this.temperature = temperature;
    }

    public Float getHumidity() {
        return humidity;
    }

    public void setHumidity(Float humidity) {
        this.humidity = humidity;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Weather{");
        sb.append("ts=").append(ts);
        sb.append(", temperature=").append(temperature);
        sb.append(", humidity=").append(humidity);
        sb.append(", location='").append(location).append('\'');
        sb.append(", note='").append(note).append('\'');
        sb.append(", bytes -> string=");
        if (bytes == null) sb.append("null");
        else {
            sb.append(new String(bytes, StandardCharsets.UTF_8));
        }
        sb.append(", groupId=").append(groupId);
        sb.append('}');
        return sb.toString();
    }
}
