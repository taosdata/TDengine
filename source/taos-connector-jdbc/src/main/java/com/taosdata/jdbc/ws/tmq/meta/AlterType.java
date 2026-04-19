package com.taosdata.jdbc.ws.tmq.meta;

public enum AlterType {
  ADD_TAG(1),
  DROP_TAG(2),
  RENAME_TAG_NAME(3),
  SET_TAG(4),
  ADD_COLUMN(5),
  DROP_COLUMN(6),
  MODIFY_COLUMN_LENGTH(7),
  MODIFY_TAG_LENGTH(8),
  MODIFY_TABLE_OPTION(9),
  RENAME_COLUMN_NAME(10),
  ADD_TAG_INDEX(11),
  UPDATE_COLUMN_COMPRESS(13),
  ADD_COLUMN_WITH_COMPRESS(14),
  SET_MULTI_TAG(15),
  ALTER_COLUMN_REF(16),
  SET_REF_NULL(17),
  ADD_COLUMN_WITH_REF(18),
  ALTER_MULTI_TABLE_TAG(19),
  ALTER_STABLE_TAG_WITH_FILTER(20);
  private final int value;

  AlterType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
