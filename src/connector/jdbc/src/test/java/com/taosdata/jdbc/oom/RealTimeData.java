package com.taosdata.jdbc.oom;

public class RealTimeData {
    private long ts; // BIGINT
    private long longitude;
    private long latitude;
    private long altitude;
    private int speed; // int
    private String guid;
    private String originByte;// NCHAR

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getLongitude() {
        return longitude;
    }

    public void setLongitude(long longitude) {
        this.longitude = longitude;
    }

    public long getLatitude() {
        return latitude;
    }

    public void setLatitude(long latitude) {
        this.latitude = latitude;
    }

    public long getAltitude() {
        return altitude;
    }

    public void setAltitude(long altitude) {
        this.altitude = altitude;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getOriginByte() {
        return originByte;
    }

    public void setOriginByte(String originByte) {
        this.originByte = originByte;
    }
}
