package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaAlterTable extends Meta {
//          "alterType":1, // 4-set tag=new value, 5-add column, 6-drop column, 7-modify column length, 10-rename column name
//                         // 1-add tag, 2-drop tag, 3-rename tag name, 8-modify tag length, 9-modify table option
//          "colName":"t1",               // enable for alterType from 1-10
//          "colNewName":"t1new",         // enable for alterType 3,10
//          "colType":0,                  // enable for alterType 5,7,1,8
//          "colLength":0,                // enable for alterType 5,1,7,8
//          "colValue":"new data",        // enable for alterType 4, "new data" needs to be convert if type is not string
//          "colValueNull":false,         // enable for alterType 4,
    private int alterType;
    private String colName;
    private String colNewName;
    private int colType;
    private int colLength;
    private String colValue;
    private boolean colValueNull;
    private List<TagAlter> tags;

    public int getAlterType() {
        return alterType;
    }

    public void setAlterType(int alterType) {
        this.alterType = alterType;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColNewName() {
        return colNewName;
    }

    public void setColNewName(String colNewName) {
        this.colNewName = colNewName;
    }

    public int getColType() {
        return colType;
    }

    public void setColType(int colType) {
        this.colType = colType;
    }

    public int getColLength() {
        return colLength;
    }

    public void setColLength(int colLength) {
        this.colLength = colLength;
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

    public List<TagAlter> getTags() {
        return tags;
    }

    public void setTags(List<TagAlter> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaAlterTable that = (MetaAlterTable) o;
        return alterType == that.alterType
                && colType == that.colType
                && colLength == that.colLength
                && colValueNull == that.colValueNull
                && Objects.equals(colName, that.colName)
                && Objects.equals(colNewName, that.colNewName)
                && Objects.equals(colValue, that.colValue)
                && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), alterType, colName, colNewName, colType, colLength, colValue, colValueNull, tags);
    }

}