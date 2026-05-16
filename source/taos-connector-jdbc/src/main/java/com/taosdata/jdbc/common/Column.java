package com.taosdata.jdbc.common;

public class Column {
    private final Object data;
    // taos data type
    private final int type;

    public Object getData() {
        return data;
    }

    public int getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    private final int index;

    public Column(Object data, int type, int index) {
        this.data = data;
        this.type = type;
        this.index = index;
    }
}
