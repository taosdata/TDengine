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

// clang-format off
#include "nodes.h"
#include "plannodes.h"
#include "ttime.h"

#define EXPLAIN_MAX_GROUP_NUM 100

//newline area
#define EXPLAIN_TAG_SCAN_FORMAT "Tag Scan on %s"
#define EXPLAIN_TBL_SCAN_FORMAT "Table Scan on %s"
#define EXPLAIN_TBL_MERGE_SCAN_FORMAT "Table Merge Scan on %s"
#define EXPLAIN_SYSTBL_SCAN_FORMAT "System Table Scan on %s"
#define EXPLAIN_DISTBLK_SCAN_FORMAT "Block Dist Scan on %s"
#define EXPLAIN_LASTROW_SCAN_FORMAT "Last Row Scan on %s"
#define EXPLAIN_TABLE_COUNT_SCAN_FORMAT "Table Count Row Scan on %s"
#define EXPLAIN_PROJECTION_FORMAT "Projection"
#define EXPLAIN_JOIN_FORMAT "%s"
#define EXPLAIN_AGG_FORMAT "%s"
#define EXPLAIN_INDEF_ROWS_FORMAT "Indefinite Rows Function"
#define EXPLAIN_EXCHANGE_FORMAT "Data Exchange %d:1"
#define EXPLAIN_SORT_FORMAT "Sort"
#define EXPLAIN_GROUP_SORT_FORMAT "Group Sort"
#define EXPLAIN_INTERVAL_FORMAT "Interval on Column %s"
#define EXPLAIN_MERGE_INTERVAL_FORMAT "Merge Interval on Column %s"
#define EXPLAIN_MERGE_ALIGNED_INTERVAL_FORMAT "Merge Aligned Interval on Column %s"
#define EXPLAIN_FILL_FORMAT "Fill"
#define EXPLAIN_SESSION_FORMAT "Session"
#define EXPLAIN_STATE_WINDOW_FORMAT "StateWindow on Column %s"
#define EXPLAIN_PARITION_FORMAT "Partition on Column %s"
#define EXPLAIN_ORDER_FORMAT "Order: %s"
#define EXPLAIN_FILTER_FORMAT "Filter: "
#define EXPLAIN_MERGEBLOCKS_FORMAT "Merge ResBlocks: %s"
#define EXPLAIN_FILL_VALUE_FORMAT "Fill Values: "
#define EXPLAIN_ON_CONDITIONS_FORMAT "Join Cond: "
#define EXPLAIN_TIMERANGE_FORMAT "Time Range: [%" PRId64 ", %" PRId64 "]"
#define EXPLAIN_OUTPUT_FORMAT "Output: "
#define EXPLAIN_TIME_WINDOWS_FORMAT "Time Window: interval=%" PRId64 "%c offset=%" PRId64 "%c sliding=%" PRId64 "%c"
#define EXPLAIN_WINDOW_FORMAT "Window: gap=%" PRId64
#define EXPLAIN_RATIO_TIME_FORMAT "Ratio: %f"
#define EXPLAIN_MERGE_FORMAT "Merge"
#define EXPLAIN_MERGE_KEYS_FORMAT "Merge Key: "
#define EXPLAIN_IGNORE_GROUPID_FORMAT "Ignore Group Id: %s"
#define EXPLAIN_PARTITION_KETS_FORMAT "Partition Key: "
#define EXPLAIN_INTERP_FORMAT "Interp"
#define EXPLAIN_EVENT_FORMAT "Event"
#define EXPLAIN_EVENT_START_FORMAT "Start Cond: "
#define EXPLAIN_EVENT_END_FORMAT "End Cond: "
#define EXPLAIN_GROUP_CACHE_FORMAT "Group Cache"
#define EXPLAIN_DYN_QRY_CTRL_FORMAT "Dynamic Query Control for %s"
#define EXPLAIN_COUNT_FORMAT "Count"
#define EXPLAIN_COUNT_INFO_FORMAT "Window Count Info"
#define EXPLAIN_COUNT_NUM_FORMAT "Window Count=%" PRId64
#define EXPLAIN_COUNT_SLIDING_FORMAT "Window Sliding=%" PRId64

#define EXPLAIN_PLANNING_TIME_FORMAT "Planning Time: %.3f ms"
#define EXPLAIN_EXEC_TIME_FORMAT "Execution Time: %.3f ms"

//append area
#define EXPLAIN_LIMIT_FORMAT "limit=%" PRId64
#define EXPLAIN_SLIMIT_FORMAT "slimit=%" PRId64
#define EXPLAIN_LEFT_PARENTHESIS_FORMAT " ("
#define EXPLAIN_RIGHT_PARENTHESIS_FORMAT ")"
#define EXPLAIN_BLANK_FORMAT " "
#define EXPLAIN_COMMA_FORMAT ", "
#define EXPLAIN_COST_FORMAT "cost=%.2f..%.2f"
#define EXPLAIN_ROWS_FORMAT "rows=%" PRIu64
#define EXPLAIN_COLUMNS_FORMAT "columns=%d"
#define EXPLAIN_PSEUDO_COLUMNS_FORMAT "pseudo_columns=%d"
#define EXPLAIN_WIDTH_FORMAT "width=%d"
#define EXPLAIN_SCAN_ORDER_FORMAT "order=[asc|%d desc|%d]"
#define EXPLAIN_SCAN_MODE_FORMAT "mode=%s"
#define EXPLAIN_SCAN_DATA_LOAD_FORMAT "data_load=%s"
#define EXPLAIN_GROUPS_FORMAT "groups=%d"
#define EXPLAIN_WIDTH_FORMAT "width=%d"
#define EXPLAIN_INTERVAL_VALUE_FORMAT "interval=%" PRId64 "%c"
#define EXPLAIN_FUNCTIONS_FORMAT "functions=%d"
#define EXPLAIN_EXECINFO_FORMAT "cost=%.3f..%.3f rows=%" PRIu64
#define EXPLAIN_MODE_FORMAT "mode=%s"
#define EXPLAIN_STRING_TYPE_FORMAT "%s"
#define EXPLAIN_INPUT_ORDER_FORMAT "input_order=%s"
#define EXPLAIN_OUTPUT_ORDER_TYPE_FORMAT "output_order=%s"
#define EXPLAIN_OFFSET_FORMAT "offset=%" PRId64
#define EXPLAIN_SOFFSET_FORMAT "soffset=%" PRId64
#define EXPLAIN_PARTITIONS_FORMAT "partitions=%d"
#define EXPLAIN_GLOBAL_GROUP_FORMAT "global_group=%d"
#define EXPLAIN_GROUP_BY_UID_FORMAT "group_by_uid=%d"
#define EXPLAIN_BATCH_SCAN_FORMAT "batch_scan=%d"
#define EXPLAIN_VGROUP_SLOT_FORMAT "vgroup_slot=%d,%d"
#define EXPLAIN_UID_SLOT_FORMAT "uid_slot=%d,%d"
#define EXPLAIN_SRC_SCAN_FORMAT "src_scan=%d,%d"
#define EXPLAIN_PLAN_BLOCKING "blocking=%d"
#define EXPLAIN_MERGE_MODE_FORMAT "mode=%s"

#define COMMAND_RESET_LOG "resetLog"
#define COMMAND_SCHEDULE_POLICY "schedulePolicy"
#define COMMAND_ENABLE_RESCHEDULE "enableReSchedule"
#define COMMAND_CATALOG_DEBUG "catalogDebug"
#define COMMAND_ENABLE_MEM_DEBUG "enableMemDebug"
#define COMMAND_DISABLE_MEM_DEBUG "disableMemDebug"

typedef struct SExplainGroup {
  int32_t   nodeNum;
  int32_t   nodeIdx;
  int32_t   physiPlanExecNum;
  int32_t   physiPlanExecIdx;
  bool      singleChannel;
  SRWLatch  lock;
  SSubplan *plan;
  SArray   *nodeExecInfo;      //Array<SExplainRsp>
} SExplainGroup;

typedef struct SExplainResNode {
  SNodeList*        pChildren;
  SPhysiNode*       pNode;
  SArray*           pExecInfo; // Array<SExplainExecInfo>
} SExplainResNode;

typedef struct SQueryExplainRowInfo {
  int32_t level;
  int32_t len;
  char   *buf;
} SQueryExplainRowInfo;

typedef struct SExplainCtx {
  EExplainMode mode;
  double       ratio;
  bool         verbose;

  SRWLatch     lock;
  int32_t      rootGroupId;
  int32_t      dataSize;
  bool         execDone;
  int64_t      reqStartTs;
  int64_t      jobStartTs;
  int64_t      jobDoneTs;
  char        *tbuf;
  SArray      *rows;
  int32_t      groupDoneNum;
  SHashObj    *groupHash;     // Hash<SExplainGroup>
} SExplainCtx;

#define EXPLAIN_ORDER_STRING(_order) ((ORDER_ASC == _order) ? "asc" : ORDER_DESC == _order ? "desc" : "unknown")
#define EXPLAIN_JOIN_STRING(_type) ((JOIN_TYPE_INNER == _type) ? "Inner join" : "Join")
#define EXPLAIN_MERGE_MODE_STRING(_mode) ((_mode) == MERGE_TYPE_SORT ? "sort" : ((_mode) == MERGE_TYPE_NON_SORT ? "merge" : "column"))

#define INVERAL_TIME_FROM_PRECISION_TO_UNIT(_t, _u, _p) (((_u) == 'n' || (_u) == 'y') ? (_t) : (convertTimeFromPrecisionToUnit(_t, _p, _u)))

#define EXPLAIN_ROW_NEW(level, ...)                                                                               \
  do {                                                                                                            \
    if (isVerboseLine) {                                                                                          \
      tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, "%*s", (level) * 3 + 3, "");       \
    } else {                                                                                                      \
      tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, "%*s%s", (level) * 3, "", "-> ");  \
    }                                                                                                             \
    tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE - tlen, __VA_ARGS__);         \
  } while (0)

#define EXPLAIN_ROW_APPEND(...) tlen += snprintf(tbuf + VARSTR_HEADER_SIZE + tlen, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE - tlen, __VA_ARGS__)
#define EXPLAIN_ROW_END() do { varDataSetLen(tbuf, tlen); tlen += VARSTR_HEADER_SIZE; isVerboseLine = true; } while (0)

#define EXPLAIN_SUM_ROW_NEW(...) tlen = snprintf(tbuf + VARSTR_HEADER_SIZE, TSDB_EXPLAIN_RESULT_ROW_SIZE - VARSTR_HEADER_SIZE, __VA_ARGS__)
#define EXPLAIN_SUM_ROW_END() do { varDataSetLen(tbuf, tlen); tlen += VARSTR_HEADER_SIZE; } while (0)

#define EXPLAIN_ROW_APPEND_LIMIT_IMPL(_pLimit, sl) do {                                            \
  if (_pLimit) {                                                                                   \
    EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);                                                      \
    SLimitNode* pLimit = (SLimitNode*)_pLimit;                                                     \
    EXPLAIN_ROW_APPEND(((sl) ? EXPLAIN_SLIMIT_FORMAT : EXPLAIN_LIMIT_FORMAT), pLimit->limit);      \
    if (pLimit->offset) {                                                                          \
      EXPLAIN_ROW_APPEND(EXPLAIN_BLANK_FORMAT);                                                    \
      EXPLAIN_ROW_APPEND(((sl) ? EXPLAIN_SOFFSET_FORMAT : EXPLAIN_OFFSET_FORMAT), pLimit->offset);\
    }                                                                                              \
  }                                                                                                \
} while (0)

#define EXPLAIN_ROW_APPEND_LIMIT(_pLimit) EXPLAIN_ROW_APPEND_LIMIT_IMPL(_pLimit, false)
#define EXPLAIN_ROW_APPEND_SLIMIT(_pLimit) EXPLAIN_ROW_APPEND_LIMIT_IMPL(_pLimit, true)

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
