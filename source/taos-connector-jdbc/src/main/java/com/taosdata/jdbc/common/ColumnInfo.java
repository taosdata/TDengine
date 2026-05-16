package com.taosdata.jdbc.common;

import java.util.List;

public class ColumnInfo implements Comparable<ColumnInfo> {
    private final List<Object> dataList;
    // taos data type
    private final int type;
    private final int index;
    private int serializeSize;

    public ColumnInfo(int columnIndex, List<Object> dataList, int type) {
        this.index = columnIndex;
        this.dataList = dataList;
        this.type = type;
    }

    public void add(Object data) {
        this.dataList.add(data);
    }

    public List<Object> getDataList() {
        return dataList;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public int getSerializeSize() {
        return serializeSize;
    }

    public void setSerializeSize(int serializeSize) {
        this.serializeSize = serializeSize;
    }

    @Override
    public int compareTo(ColumnInfo c) {
        return this.index > c.index ? 1 : -1;
    }

}
