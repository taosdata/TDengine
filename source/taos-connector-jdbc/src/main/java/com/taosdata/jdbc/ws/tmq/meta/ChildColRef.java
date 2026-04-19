package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.Objects;

public class ChildColRef {
  private String colName;
  private String refDbName;
  private String refTableName;
  private String refColName;

  public ChildColRef() {}

  public ChildColRef(String colName, String refDbName, String refTableName, String refColName) {
    this.colName = colName;
    this.refDbName = refDbName;
    this.refTableName = refTableName;
    this.refColName = refColName;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getRefDbName() {
    return refDbName;
  }

  public void setRefDbName(String refDbName) {
    this.refDbName = refDbName;
  }

  public String getRefTableName() {
    return refTableName;
  }

  @JsonAlias("refTbName")
  public void setRefTableName(String refTableName) {
    this.refTableName = refTableName;
  }

  public String getRefColName() {
    return refColName;
  }

  public void setRefColName(String refColName) {
    this.refColName = refColName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChildColRef that = (ChildColRef) o;
    return Objects.equals(colName, that.colName)
        && Objects.equals(refDbName, that.refDbName)
        && Objects.equals(refTableName, that.refTableName)
        && Objects.equals(refColName, that.refColName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(colName, refDbName, refTableName, refColName);
  }

  @Override
  public String toString() {
    return "ChildColRef{"
        + "colName='" + colName + '\''
        + ", refDbName='" + refDbName + '\''
        + ", refTableName='" + refTableName + '\''
        + ", refColName='" + refColName + '\''
        + '}';
  }
}
