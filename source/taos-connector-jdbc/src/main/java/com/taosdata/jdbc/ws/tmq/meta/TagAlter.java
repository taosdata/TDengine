package com.taosdata.jdbc.ws.tmq.meta;

import java.util.Objects;

public class TagAlter {
    private String colName;
    private String colValue;
    private boolean colValueNull;

    public TagAlter() {
    }
    public TagAlter(String colName, String colValue, boolean colValueNull) {
        this.colName = colName;
        this.colValue = colValue;
        this.colValueNull = colValueNull;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColValue() {
        return colValue;
    }

    public void setColValue(String colValue) {
        this.colValue = colValue;
    }

    public boolean isColValueNull() {
        return colValueNull;
    }

    public void setColValueNull(boolean colValueNull) {
        this.colValueNull = colValueNull;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagAlter tagAlter = (TagAlter) o;
        return colValueNull == tagAlter.colValueNull && Objects.equals(colName, tagAlter.colName) && Objects.equals(colValue, tagAlter.colValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colName, colValue, colValueNull);
    }
}