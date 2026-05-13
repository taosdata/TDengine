package com.taosdata.jdbc.ws.tmq.meta;

import java.util.Objects;

public class MetaDeleteData extends Meta {
    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaDeleteData that = (MetaDeleteData) o;
        return Objects.equals(sql, that.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sql);
    }
}
