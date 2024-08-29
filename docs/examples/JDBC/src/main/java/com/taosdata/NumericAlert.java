package com.taosdata;


import com.alibaba.fastjson.annotation.JSONField;

public class NumericAlert {
    @JSONField(name = "col_name")
    private String colName;
    private double min;
    private double max;
    private String level;

    // Getters and Setters
    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}