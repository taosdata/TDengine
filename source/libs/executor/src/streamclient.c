/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <curl/curl.h>

#include "executorInt.h"
#include "tjson.h"

static int32_t buildSessionResultSql(SSHashObj* pRangeMap, char** ppSql) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  // todo(liuyao) add
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doProcessSql(const char* pSql, SJson** ppJsonResult) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  // todo(liuyao) add
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTransformResult(const SJson** ppJsonResult, SSDataBlock** ppRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  // todo(liuyao) add
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamClientGetResultRange(SSHashObj* pRangeMap, SSDataBlock** ppRangeRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char* pSql = NULL;
  code = buildSessionResultSql(pRangeMap, &pSql);
  QUERY_CHECK_CODE(code, lino, _end);

  SJson* pJsRes = NULL;
  code = doProcessSql(pSql, &pJsRes);
  QUERY_CHECK_CODE(code, lino, _end);
  code = doTransformResult(pJsRes, ppRangeRes);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}