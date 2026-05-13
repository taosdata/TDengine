package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

public class Tag {
    private String name;
    private int type;
    private JsonNode value;

    public Tag() {
    }
    public Tag(String name, int type, JsonNode value) {
        this.name = name;
        this.type = type;
        this.value = value;
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

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tag tag = (Tag) o;
        return type == tag.type && Objects.equals(name, tag.name) && Objects.equals(value, tag.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, value);
    }
}