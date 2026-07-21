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

#ifndef TDENGINE_JOIN_UTIL_H
#define TDENGINE_JOIN_UTIL_H

#include <ctype.h>

#include "scalar.h"
#include "querynodes.h"
#include "ttime.h"

/*
 * Resolve TIMETRUNCATE parameter layout.
 *
 * TIMETRUNCATE always has 7 parameters:
 *   [0] ts, [1] unit, [2] use_curr_tz, [3] precision,
 *   [4] tz_name, [5] fdow, [6] unitCh
 */
static inline int32_t joinResolveTruncateParams(
    SNodeList* pParamList, int32_t numOfParams,
    SValueNode** ppUnit, SValueNode** ppCurrTz, SValueNode** ppTimeZone) {
  if (numOfParams != 7) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  *ppUnit     = (SValueNode*)nodesListGetNode(pParamList, 1);
  *ppCurrTz   = (SValueNode*)nodesListGetNode(pParamList, 2);
  *ppTimeZone = (SValueNode*)nodesListGetNode(pParamList, 4);

  return TSDB_CODE_SUCCESS;
}

static inline bool joinIsFixedOffsetTimezoneLiteral(const char* tzStr) {
  if (tzStr == NULL) {
    return false;
  }

  char buf[TD_TIMEZONE_LEN] = {0};
  tstrncpy(buf, tzStr, sizeof(buf));

  int64_t offset = 0;
  if (offsetOfTimezone(buf, &offset) != TSDB_CODE_SUCCESS) {
    return false;
  }

  return true;
}

static inline int64_t joinOffsetFromTimezoneLiteral(const char* tzStr, int64_t factor) {
  char buf[TD_TIMEZONE_LEN] = {0};
  tstrncpy(buf, tzStr, sizeof(buf));

  int64_t offsetSeconds = 0;
  if (offsetOfTimezone(buf, &offsetSeconds) != TSDB_CODE_SUCCESS) {
    return 0;
  }

  return offsetSeconds * factor;
}

static inline int32_t joinResolveTruncateTimezone(SValueNode* pCurrTz, SValueNode* pTimeZone, int64_t truncateUnit,
                                                  int32_t precision, timezone_t* pTz, int64_t* pTimezoneUnit) {
  *pTz = NULL;
  *pTimezoneUnit = 0;

  if ((pCurrTz != NULL && pCurrTz->typeData != 1) ||
      truncateUnit < (86400 * TSDB_TICK_PER_SECOND(precision))) {
    return TSDB_CODE_SUCCESS;
  }

  if (pTimeZone == NULL || pTimeZone->datum.p == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  char* tzStr = varDataVal(pTimeZone->datum.p);
  int64_t factor = TSDB_TICK_PER_SECOND(precision);

  if (joinIsFixedOffsetTimezoneLiteral(tzStr)) {
    *pTimezoneUnit = joinOffsetFromTimezoneLiteral(tzStr, factor);
    return TSDB_CODE_SUCCESS;
  }

  if (strchr(tzStr, '/') != NULL || strncmp(tzStr, "UTC", 3) == 0 ||
      strncmp(tzStr, "GMT", 3) == 0) {
    timezone_t tz = NULL;
    if (taosValidateTimezone(tzStr, &tz) == TSDB_CODE_SUCCESS) {
      *pTz = tz;
      return TSDB_CODE_SUCCESS;
    }
  }

  if (tzStr[0] == '+' || tzStr[0] == '-') {
    *pTimezoneUnit = joinOffsetFromTimezoneLiteral(tzStr, factor);
  }

  return TSDB_CODE_SUCCESS;
}

#endif /* TDENGINE_JOIN_UTIL_H */
