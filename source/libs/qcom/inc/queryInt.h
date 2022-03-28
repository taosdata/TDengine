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

#define QUERY_EXPLAIN_MAX_RES_LEN 1024

#define EXPLAIN_TAG_SCAN_FORMAT "Tag scan on %s"
#define EXPLAIN_ORDER_FORMAT "Order: "

typedef struct SQueryExplainRowInfo {
  int32_t level;
  int32_t len;
  char   *buf;
} SQueryExplainRowInfo;


#define QUERY_EXPLAIN_NEWLINE(_format, ...) tlen = snprintf(tbuf + tlen, QUERY_EXPLAIN_MAX_RES_LEN - tlen, _format, __VA_ARGS__)
#define QUERY_EXPLAIN_APPEND(_format, ...) tlen += snprintf(tbuf + tlen, QUERY_EXPLAIN_MAX_RES_LEN - tlen, _format, __VA_ARGS__)


#define QRY_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define QRY_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define QRY_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)


#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
