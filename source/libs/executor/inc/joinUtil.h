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

#include "querynodes.h"

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

#endif /* TDENGINE_JOIN_UTIL_H */
