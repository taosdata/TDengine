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

#ifndef _TD_QUERY_INT_H_
#define _TD_QUERY_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define VALIDNUMOFCOLS(x) ((x) >= TSDB_MIN_COLUMNS && (x) <= TSDB_MAX_COLUMNS)
#define VALIDNUMOFTAGS(x) ((x) >= 0 && (x) <= TSDB_MAX_TAGS)

#define QUERY_PARAM_CHECK(_p)                                                \
  do {                                                                       \
    if ((_p) == NULL) {                                                      \
      qError("function:%s, param invalid, line:%d", __FUNCTION__, __LINE__); \
      return TSDB_CODE_TSC_INVALID_INPUT;                                    \
    }                                                                        \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
