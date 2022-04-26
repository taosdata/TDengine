package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * query result pojo
 */
public class QueryResp extends Response {
    private int code;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    private String message;

    private long id;

    @JSONField(name = "is_update")
    private boolean isUpdate;

    @JSONField(name = "affected_rows")
    private int affectedRows;

    @JSONField(name = "fields_count")
    private int fieldsCount;

    @JSONField(name = "fields_names")
    private String[] fieldsNames;

    @JSONField(name = "fields_types")
    private int[] fieldsTypes;

    @JSONField(name = "fields_lengths")
    private int[] fieldsLengths;

    private int precision;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public void setUpdate(boolean update) {
        isUpdate = update;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }

    public int getFieldsCount() {
        return fieldsCount;
    }

    public void setFieldsCount(int fieldsCount) {
        this.fieldsCount = fieldsCount;
    }

    public String[] getFieldsNames() {
        return fieldsNames;
    }

    public void setFieldsNames(String[] fieldsNames) {
        this.fieldsNames = fieldsNames;
    }

    public int[] getFieldsTypes() {
        return fieldsTypes;
    }

    public void setFieldsTypes(int[] fieldsTypes) {
        this.fieldsTypes = fieldsTypes;
    }

    public int[] getFieldsLengths() {
        return fieldsLengths;
    }

    public void setFieldsLengths(int[] fieldsLengths) {
        this.fieldsLengths = fieldsLengths;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
}
