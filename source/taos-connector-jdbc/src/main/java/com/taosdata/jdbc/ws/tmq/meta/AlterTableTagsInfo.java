package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class AlterTableTagsInfo {
  private String tableName;
  private List<TagAlter> tags;

  public AlterTableTagsInfo() {}

  public AlterTableTagsInfo(String tableName, List<TagAlter> tags) {
    this.tableName = tableName;
    this.tags = tags;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<TagAlter> getTags() {
    return tags;
  }

  public void setTags(List<TagAlter> tags) {
    this.tags = tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AlterTableTagsInfo that = (AlterTableTagsInfo) o;
    return Objects.equals(tableName, that.tableName) && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, tags);
  }

  @Override
  public String toString() {
    return "AlterTableTagsInfo{"
        + "tableName='" + tableName + '\''
        + ", tags=" + tags
        + '}';
  }
}
