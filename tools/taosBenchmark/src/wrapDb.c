/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "bench.h"
#include "benchLog.h"
#include "wrapDb.h"

// executeSql
int32_t executeSql(TAOS* taos, char* sql) {
  // execute sql
  TAOS_RES* res = taos_query(taos, sql);
  int32_t   code = taos_errno(res);
  if (code != 0) {
    printErrCmdCodeStr(sql, code, res);
    return code;
  }

  // if calc by ts
  taos_free_result(res);
  return code;
}


int32_t queryCnt(TAOS* taos, char* sql, int64_t* pVal) {
  // execute sql
  TAOS_RES* res = taos_query(taos, sql);
  int32_t   code = taos_errno(res);
  if (code != 0) {
    printErrCmdCodeStr(sql, code, res);
    return code;
  }

  // count
  TAOS_FIELD* fields = taos_fetch_fields(res);
  TAOS_ROW row = taos_fetch_row(res);
  code = taos_errno(res);
  if (code != 0 || row == NULL) {
    printErrCmdCodeStr(sql, code, res);
    return code;
  }

  // int32_t* lengths = taos_fetch_lengths(res);
  if (fields[0].type == TSDB_DATA_TYPE_BIGINT) {
    *pVal = *(int64_t*)row[0];
  }

  // if calc by ts
  taos_free_result(res);
  return code;
}

int32_t queryTS(TAOS* taos, char* sql, int64_t* pVal) {
  // execute sql
  TAOS_RES* res = taos_query(taos, sql);
  int32_t   code = taos_errno(res);
  if (code != 0) {
    printErrCmdCodeStr(sql, code, res);
    return code;
  }

  // count
  TAOS_FIELD* fields = taos_fetch_fields(res);
  TAOS_ROW row = taos_fetch_row(res);
  code = taos_errno(res);
  if (code != 0 || row == NULL) {
    printErrCmdCodeStr(sql, code, res);
    return code;
  }

  // int32_t* lengths = taos_fetch_lengths(res);
  if (fields[0].type == TSDB_DATA_TYPE_TIMESTAMP) {
    *pVal = *(int64_t*)row[0];
  }

  // if calc by ts
  taos_free_result(res);
  return code;
}
