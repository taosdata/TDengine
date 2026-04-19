package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public abstract class Meta {
  private MetaType type;
  private String tableName;
  private TableType tableType;

  @JsonProperty("isVirtual")
  private Boolean isVirtual;

  public MetaType getType() {
    return type;
  }

  public void setType(MetaType type) {
    this.type = type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableType getTableType() {
    return tableType;
  }

  public void setTableType(TableType tableType) {
    this.tableType = tableType;
  }

  public Boolean getIsVirtual() {
    return isVirtual;
  }

  public void setIsVirtual(Boolean isVirtual) {
    this.isVirtual = isVirtual;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Meta meta = (Meta) o;
    return type == meta.type
        && Objects.equals(tableName, meta.tableName)
        && tableType == meta.tableType
        && Objects.equals(isVirtual, meta.isVirtual);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, tableName, tableType, isVirtual);
  }
}
