package com.taosdata;


import com.alibaba.fastjson.annotation.JSONField;

public class BoolAlert {
    @JSONField(name = "col_name")
    private String colName;
    private String toTrueLevel;
    private String toFalseLevel;

    // Getters and Setters
    public String getColName() {
        return colName;
    }

    public void setColName(String col_name) {
        this.colName = col_name;
    }

    public String getToTrueLevel() {
        return toTrueLevel;
    }

    public void setToTrueLevel(String toTrueLevel) {
        this.toTrueLevel = toTrueLevel;
    }

    public String getToFalseLevel() {
        return toFalseLevel;
    }

    public void setToFalseLevel(String toFalseLevel) {
        this.toFalseLevel = toFalseLevel;
    }
}