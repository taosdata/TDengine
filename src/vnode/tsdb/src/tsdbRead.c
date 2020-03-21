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

#include "os.h"
#include "tsdb.h"

tsdb_query_handle_t *tsdbQueryByTableId(STsdbQueryCond *pCond, SArray *idList, SArray *pColumnInfo) {

}

bool tsdbNextDataBlock(tsdb_query_handle_t *pQueryHandle) {
  return false;
}

SDataBlockInfo tsdbRetrieveDataBlockInfo(tsdb_query_handle_t *pQueryHandle) {

}

int32_t tsdbRetrieveDataBlockStatisInfo(tsdb_query_handle_t *pQueryHandle, SDataStatis **pBlockStatis) {

}

SArray *tsdbRetrieveDataBlock(tsdb_query_handle_t *pQueryHandle, SArray *pIdList) {

}

int32_t tsdbResetQuery(tsdb_query_handle_t *pQueryHandle, STimeWindow* window, tsdbpos_t position, int16_t order) {

}

int32_t tsdbDataBlockSeek(tsdb_query_handle_t *pQueryHandle, tsdbpos_t pos) {

}

tsdbpos_t tsdbDataBlockTell(tsdb_query_handle_t *pQueryHandle) {
  return NULL;
}

SArray *tsdbRetrieveDataRow(tsdb_query_handle_t *pQueryHandle, SArray *pIdList, SQueryRowCond *pCond) {

}

tsdb_query_handle_t *tsdbQueryFromTagConds(STsdbQueryCond *pCond, int16_t stableId, const char *pTagFilterStr) {

}

STableIDList *tsdbGetTableList(tsdb_query_handle_t *pQueryHandle) {

}

STableIDList *tsdbQueryTableList(int16_t stableId, const char *pTagCond) {

}

