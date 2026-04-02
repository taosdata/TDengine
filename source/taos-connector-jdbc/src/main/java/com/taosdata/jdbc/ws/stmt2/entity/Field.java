package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Field {
    @JsonProperty("name")
    private String name;
    @JsonProperty("field_type")
    private byte fieldType;
    @JsonProperty("precision")
    private byte precision;
    @JsonProperty("scale")
    private byte scale;
    @JsonProperty("bytes")
    private int bytes;

    @JsonProperty("bind_type")
    private byte bindType;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte getFieldType() {
        return fieldType;
    }

    public void setFieldType(byte fieldType) {
        this.fieldType = fieldType;
    }

    public byte getPrecision() {
        return precision;
    }

    public void setPrecision(byte precision) {
        this.precision = precision;
    }

    public byte getScale() {
        return scale;
    }

    public void setScale(byte scale) {
        this.scale = scale;
    }

    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }

    public byte getBindType() {
        return bindType;
    }

    public void setBindType(byte bindType) {
        this.bindType = bindType;
    }
}