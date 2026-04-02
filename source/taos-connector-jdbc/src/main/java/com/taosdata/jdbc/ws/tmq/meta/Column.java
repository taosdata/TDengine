package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Column {
    private String name;
    private int type;
    private Integer length;
    @JsonProperty("isPrimarykey")
    private Boolean primarykey;

    private String encode;
    private String compress;
    private String level;

    public Column() {
    }

    public Column(String name, int type, Integer length, Boolean primarykey, String encode, String compress, String level) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.primarykey = primarykey;
        this.encode = encode;
        this.compress = compress;
        this.level = level;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Boolean isPrimarykey() {
        return primarykey;
    }

    public void setPrimarykey(Boolean primarykey) {
        this.primarykey = primarykey;
    }

    public String getEncode() {
        return encode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return type == column.type && Objects.equals(name, column.name) && Objects.equals(length, column.length) && Objects.equals(primarykey, column.primarykey) && Objects.equals(encode, column.encode) && Objects.equals(compress, column.compress) && Objects.equals(level, column.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, length, primarykey, encode, compress, level);
    }
}