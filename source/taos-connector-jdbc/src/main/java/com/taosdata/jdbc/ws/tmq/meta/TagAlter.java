package com.taosdata.jdbc.ws.tmq.meta;

import java.util.Objects;

public class TagAlter {
  private String colName;
  private String colValue;
  private boolean colValueNull;
  private String regexp;
  private String replacement;

  public TagAlter() {}

  public TagAlter(String colName, String colValue, boolean colValueNull) {
    this.colName = colName;
    this.colValue = colValue;
    this.colValueNull = colValueNull;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getColValue() {
    return colValue;
  }

  public void setColValue(String colValue) {
    this.colValue = colValue;
  }

  public boolean isColValueNull() {
    return colValueNull;
  }

  public void setColValueNull(boolean colValueNull) {
    this.colValueNull = colValueNull;
  }

  public String getRegexp() {
    return regexp;
  }

  public void setRegexp(String regexp) {
    this.regexp = regexp;
  }

  public String getReplacement() {
    return replacement;
  }

  public void setReplacement(String replacement) {
    this.replacement = replacement;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TagAlter tagAlter = (TagAlter) o;
    return colValueNull == tagAlter.colValueNull
        && Objects.equals(colName, tagAlter.colName)
        && Objects.equals(colValue, tagAlter.colValue)
        && Objects.equals(regexp, tagAlter.regexp)
        && Objects.equals(replacement, tagAlter.replacement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(colName, colValue, colValueNull, regexp, replacement);
  }
}
