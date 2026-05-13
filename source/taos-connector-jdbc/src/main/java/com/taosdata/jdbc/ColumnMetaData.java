package com.taosdata.jdbc;

public class ColumnMetaData {

    private int colType = 0;         //taosType
    private String colName = null;
    private int colSize = -1;
    private int colIndex = 0;

    public int getColSize() {
        return colSize;
    }

    public void setColSize(int colSize) {
        this.colSize = colSize;
    }

    public int getColType() {
        return colType;
    }

    public void setColType(int colType) {
        this.colType = colType;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public int getColIndex() {
        return colIndex;
    }

    public void setColIndex(int colIndex) {
        this.colIndex = colIndex;
    }

    @Override
    public String toString() {
        return "ColumnMetaData{" +
                "colType=" + colType +
                ", colName='" + colName + '\'' +
                ", colSize=" + colSize +
                ", colIndex=" + colIndex +
                '}';
    }
}
