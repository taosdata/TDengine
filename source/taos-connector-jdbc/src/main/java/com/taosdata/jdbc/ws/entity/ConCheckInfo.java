package com.taosdata.jdbc.ws.entity;

public class ConCheckInfo {
    private final long lastCheckTime;

    public long getLastCheckTime() {
        return lastCheckTime;
    }
    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public boolean isExpired(long checkTime) {
        boolean result = System.currentTimeMillis() > (lastCheckTime + checkTime * 1000);
        return result;
    }
    private boolean valid;

    public ConCheckInfo(long lastCheckTime, boolean valid) {
        this.lastCheckTime = lastCheckTime;
        this.valid = valid;
    }
}
