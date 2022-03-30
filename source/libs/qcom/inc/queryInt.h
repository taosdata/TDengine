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
#include "nodes.h"
#include "plannodes.h"

//newline area
#define EXPLAIN_TAG_SCAN_FORMAT "Tag scan on %s columns=%d"
#define EXPLAIN_TBL_SCAN_FORMAT "Table scan on %s columns=%d"
#define EXPLAIN_SYSTBL_SCAN_FORMAT "System table scan on %s columns=%d"
#define EXPLAIN_PROJECTION_FORMAT "Projection columns=%d width=%d"
#define EXPLAIN_JOIN_FORMAT "%s between %d tables width=%d"
#define EXPLAIN_AGG_FORMAT "Aggragate functions=%d groups=%d width=%d"
#define EXPLAIN_EXCHANGE_FORMAT "Exchange %d:1 width=%d"
#define EXPLAIN_SORT_FORMAT "Sort on %d columns width=%d"
#define EXPLAIN_INTERVAL_FORMAT "Interval on column %s functions=%d interval=%" PRId64 "%c offset=%" PRId64 "%c sliding=%" PRId64 "%c width=%d"
#define EXPLAIN_SESSION_FORMAT "Session gap=%" PRId64 " functions=%d width=%d"
#define EXPLAIN_ORDER_FORMAT "Order: %s"
#define EXPLAIN_FILTER_FORMAT "Filter: "
#define EXPLAIN_FILL_FORMAT "Fill: %s"
#define EXPLAIN_ON_CONDITIONS_FORMAT "Join Cond: "
#define EXPLAIN_TIMERANGE_FORMAT "Time range: [%" PRId64 ", %" PRId64 "]"

//append area
#define EXPLAIN_LOOPS_FORMAT "loops=%d"
#define EXPLAIN_REVERSE_FORMAT "reverse=%d"


typedef struct SExplainResNode {
  SNodeList*  pChildren;
  SPhysiNode* pNode;
  void*       pExecInfo;
} SExplainResNode;

typedef struct SQueryExplainRowInfo {
  int32_t level;
  int32_t len;
  char   *buf;
} SQueryExplainRowInfo;

typedef struct SExplainRowCtx {
  int32_t totalSize;
  SArray *rows;
} SExplainRowCtx;

#define EXPLAIN_ORDER_STRING(_order) ((TSDB_ORDER_ASC == _order) ? "Ascending" : "Descending")
#define EXPLAIN_JOIN_STRING(_type) ((JOIN_TYPE_INNER == _type) ? "Inner join" : "Join")

#define EXPLAIN_ROW_NEW(level, ...)                                                                     \
  do {                                                                                                  \
    tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE, "%*s", (level) * 2, "");            \
    tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - tlen, __VA_ARGS__);  \
  } while (0)
  
#define EXPLAIN_ROW_APPEND(...) tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - tlen, __VA_ARGS__)
#define EXPLAIN_ROW_END() do { varDataSetLen(tbuf, tlen); tlen += VARSTR_HEADER_SIZE; } while (0)

#define QRY_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define QRY_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define QRY_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
