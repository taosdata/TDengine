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

#include "planInt.h"

static char* getUsageErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PLAN_EXPECTED_TS_EQUAL:
      return "left.ts = right.ts is expected in join expression";
    case TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN:
      return "not support cross join";
    default:
      break;
  }
  return "Unknown error";
}

int32_t generateUsageErrMsg(char* pBuf, int32_t len, int32_t errCode, ...) {
  va_list vArgList;
  va_start(vArgList, errCode);
  vsnprintf(pBuf, len, getUsageErrFormat(errCode), vArgList);
  va_end(vArgList);
  return errCode;
}
