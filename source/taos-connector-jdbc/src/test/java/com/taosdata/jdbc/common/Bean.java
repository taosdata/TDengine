package com.taosdata.jdbc.common;

import java.sql.Blob;

public class Bean extends Bean336 {
    private Blob c20;
    @Override
    public String toString() {
        String superStr = super.toString();
        return superStr.substring(0, superStr.length() - 1)
                + ",c20=" + getC20() + "}";
    }
    public Blob getC20() {
        return c20;
    }

    public void setC20(Blob c20) {
        this.c20 = c20;
    }
}
