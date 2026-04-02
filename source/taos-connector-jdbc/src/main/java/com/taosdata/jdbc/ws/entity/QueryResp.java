package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;

/**
 * query result pojo
 */
public class QueryResp extends CommonResp {

    @JsonDeserialize(using = UInt64Deserializer.class)
    @JsonProperty("id")
    private long id;

    @JsonProperty("is_update")
    private boolean isUpdate;

    @JsonProperty("affected_rows")
    private int affectedRows;

    @JsonProperty("fields_count")
    private int fieldsCount;

    @JsonProperty("fields_names")
    private String[] fieldsNames;

    @JsonProperty("fields_types")
    private int[] fieldsTypes;

    @JsonProperty("fields_lengths")
    private int[] fieldsLengths;

    @JsonProperty("fields_precisions")
    private int[] fieldsPrecisions;
    @JsonProperty("fields_scales")
    private int[] fieldsScales;
    @JsonProperty("precision")
    private int precision;

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

    public int[] getFieldsPrecisions() {
        return fieldsPrecisions;
    }

    public void setFieldsPrecisions(int[] fieldsPrecisions) {
        this.fieldsPrecisions = fieldsPrecisions;
    }

    public int[] getFieldsScales() {
        return fieldsScales;
    }

    public void setFieldsScales(int[] fieldsScales) {
        this.fieldsScales = fieldsScales;
    }
}
