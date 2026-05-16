package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaDropTable extends Meta {
    private List<String> tableNameList;

    public List<String> getTableNameList() {
        return tableNameList;
    }

    public void setTableNameList(List<String> tableNameList) {
        this.tableNameList = tableNameList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MetaDropTable that = (MetaDropTable) o;
        return Objects.equals(tableNameList, that.tableNameList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableNameList);
    }
}
