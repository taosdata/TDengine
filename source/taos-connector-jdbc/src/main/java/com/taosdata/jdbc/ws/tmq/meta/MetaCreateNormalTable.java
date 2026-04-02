package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaCreateNormalTable extends Meta {

    private List<Column> columns;
    private List<Column> tags;

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getTags() {
        return tags;
    }

    public void setTags(List<Column> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaCreateNormalTable that = (MetaCreateNormalTable) o;
        return Objects.equals(columns, that.columns) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns, tags);
    }
}