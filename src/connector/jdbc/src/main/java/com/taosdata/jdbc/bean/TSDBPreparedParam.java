package com.taosdata.jdbc.bean;

import java.util.List;

/**
 * tdengine batch insert or import param object
 */
public class TSDBPreparedParam {

    /**
     * tableName, if sTable Name is not null, and this is sub table name.
     */
    private String tableName;

    /**
     * sub middle param list
     */
    private List<Object> middleParamList;

    /**
     * value list
     */
    private List<Object> valueList;

    public TSDBPreparedParam(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Object> getMiddleParamList() {
        return middleParamList;
    }

    public void setMiddleParamList(List<Object> middleParamList) {
        this.middleParamList = middleParamList;
    }

    public void setMiddleParam(int parameterIndex, Object x){
        this.middleParamList.set(parameterIndex, x);
    }

    public List<Object> getValueList() {
        return valueList;
    }

    public void setValueList(List<Object> valueList) {
        this.valueList = valueList;
    }


    public void setValueParam(int parameterIndex, Object x){
        this.valueList.set(parameterIndex, x);
    }

}
