package com.taosdata.jdbc.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    private  List<ColumnInfo> dataList;
    private  ByteBuffer tableName;
    private  List<ColumnInfo> tagInfo;

    public TableInfo(List<ColumnInfo> dataList, ByteBuffer tableName, List<ColumnInfo> tagInfo) {
        this.dataList = dataList;
        this.tableName = tableName;
        this.tagInfo = tagInfo;
    }

    public static TableInfo getEmptyTableInfo() {
        return new TableInfo(new ArrayList<>(), ByteBuffer.wrap(new byte[]{}), new ArrayList<>());
    }
    public List<ColumnInfo> getDataList() {
        return dataList;
    }

    public ByteBuffer getTableName() {
        return tableName;
    }

    public List<ColumnInfo> getTagInfo() {
        return tagInfo;
    }

    public void setDataList(List<ColumnInfo> dataList) {
        this.dataList = dataList;
    }

    public void setTableName(ByteBuffer tableName) {
        this.tableName = tableName;
    }

    public void setTagInfo(List<ColumnInfo> tagInfo) {
        this.tagInfo = tagInfo;
    }


}
