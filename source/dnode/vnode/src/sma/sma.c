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

#include "sma.h"


// TODO: Who is responsible for resource allocate and release?
int32_t tdProcessTSmaInsert(SSma* pSma, int64_t indexUid, const char* msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaInsertImpl(pSma, indexUid, msg)) < 0) {
    smaWarn("vgId:%d insert tsma data failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  // TODO: destroy SSDataBlocks(msg)
  return code;
}

int32_t tdProcessTSmaCreate(SSma* pSma, int64_t version, const char* msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaCreateImpl(pSma, version, msg)) < 0) {
    smaWarn("vgId:%d create tsma failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  // TODO: destroy SSDataBlocks(msg)
  return code;
}

int32_t tdUpdateExpireWindow(SSma* pSma, SSubmitReq* pMsg, int64_t version) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tdUpdateExpiredWindowImpl(pSma, pMsg, version)) < 0) {
    smaWarn("vgId:%d update expired sma window failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  return code;
}

int32_t tdGetTSmaData(SSma* pSma, char* pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tdGetTSmaDataImpl(pSma, pData, indexUid, querySKey, nMaxResult)) < 0) {
    smaWarn("vgId:%d get tSma data failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  return code;
}
