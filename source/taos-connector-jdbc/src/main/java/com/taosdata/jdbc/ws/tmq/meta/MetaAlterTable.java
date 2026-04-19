package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaAlterTable extends Meta {
  //          "alterType":1, // 4-set tag=new value, 5-add column, 6-drop column, 7-modify column
  // length, 10-rename column name
  //                         // 1-add tag, 2-drop tag, 3-rename tag name, 8-modify tag length,
  // 9-modify table option
  //                         // 11-add tag index, 13-update column compress, 14-add column with
  // compress
  //                         // 15-set multi tag (deprecated, replaced by 19), 16-alter column ref,
  // 17-set ref null
  //                         // 18-add column with ref, 19-alter tag with multi tables, 20-alter
  // stable tag with filter
  //          "colName":"t1",               // enable for alterType from 1-18
  //          "colNewName":"t1new",         // enable for alterType 3,10
  //          "colType":0,                  // enable for alterType 5,7,1,8
  //          "colLength":0,                // enable for alterType 5,1,7,8
  //          "colValue":"new data",        // enable for alterType 4
  //          "colValueNull":false,         // enable for alterType 4
  //          "encode","compress","level"   // enable for alterType 13,14
  //          "refDbName","refTbName","refColName" // enable for alterType 16,18
  //          "tables"                      // enable for alterType 19
  //          "where"                       // enable for alterType 20
  private int alterType;
  private String colName;
  private String colNewName;
  private int colType;
  private int colLength;
  private String colValue;
  private boolean colValueNull;
  private List<TagAlter> tags;
  private String encode;
  private String compress;
  private String level;
  private String refDbName;
  private String refTbName;
  private String refColName;
  private List<AlterTableTagsInfo> tables;
  private String where;

  public int getAlterType() {
    return alterType;
  }

  public void setAlterType(int alterType) {
    this.alterType = alterType;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getColNewName() {
    return colNewName;
  }

  public void setColNewName(String colNewName) {
    this.colNewName = colNewName;
  }

  public int getColType() {
    return colType;
  }

  public void setColType(int colType) {
    this.colType = colType;
  }

  public int getColLength() {
    return colLength;
  }

  public void setColLength(int colLength) {
    this.colLength = colLength;
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

  public List<TagAlter> getTags() {
    return tags;
  }

  public void setTags(List<TagAlter> tags) {
    this.tags = tags;
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

  public String getRefDbName() {
    return refDbName;
  }

  public void setRefDbName(String refDbName) {
    this.refDbName = refDbName;
  }

  public String getRefTbName() {
    return refTbName;
  }

  public void setRefTbName(String refTbName) {
    this.refTbName = refTbName;
  }

  public String getRefColName() {
    return refColName;
  }

  public void setRefColName(String refColName) {
    this.refColName = refColName;
  }

  public List<AlterTableTagsInfo> getTables() {
    return tables;
  }

  public void setTables(List<AlterTableTagsInfo> tables) {
    this.tables = tables;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MetaAlterTable that = (MetaAlterTable) o;
    return alterType == that.alterType
        && colType == that.colType
        && colLength == that.colLength
        && colValueNull == that.colValueNull
        && Objects.equals(colName, that.colName)
        && Objects.equals(colNewName, that.colNewName)
        && Objects.equals(colValue, that.colValue)
        && Objects.equals(tags, that.tags)
        && Objects.equals(encode, that.encode)
        && Objects.equals(compress, that.compress)
        && Objects.equals(level, that.level)
        && Objects.equals(refDbName, that.refDbName)
        && Objects.equals(refTbName, that.refTbName)
        && Objects.equals(refColName, that.refColName)
        && Objects.equals(tables, that.tables)
        && Objects.equals(where, that.where);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        alterType,
        colName,
        colNewName,
        colType,
        colLength,
        colValue,
        colValueNull,
        tags,
        encode,
        compress,
        level,
        refDbName,
        refTbName,
        refColName,
        tables,
        where);
  }
}
