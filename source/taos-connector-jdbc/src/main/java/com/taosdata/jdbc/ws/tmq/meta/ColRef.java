package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.Objects;

public class ColRef {
  private String refDbName;
  private String refTableName;
  private String refColName;

  public ColRef() {}

  public ColRef(String refDbName, String refTableName, String refColName) {
    this.refDbName = refDbName;
    this.refTableName = refTableName;
    this.refColName = refColName;
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
    ColRef colRef = (ColRef) o;
    return Objects.equals(refDbName, colRef.refDbName)
        && Objects.equals(refTableName, colRef.refTableName)
        && Objects.equals(refColName, colRef.refColName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(refDbName, refTableName, refColName);
  }

  @Override
  public String toString() {
    return "ColRef{"
        + "refDbName='" + refDbName + '\''
        + ", refTableName='" + refTableName + '\''
        + ", refColName='" + refColName + '\''
        + '}';
  }
}
