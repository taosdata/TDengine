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

#include "tsdbDef.h"

/**
 * @brief insert TS data
 *
 * @param pTsdb
 * @param pMsg
 * @param pRsp
 * @return int
 */
int tsdbInsertData(STsdb *pTsdb, SSubmitReq *pMsg, SSubmitRsp *pRsp) {
  // Check if mem is there. If not, create one.
  if (pTsdb->mem == NULL) {
    pTsdb->mem = tsdbNewMemTable(pTsdb);
    if (pTsdb->mem == NULL) {
      return -1;
    }
  }
  return tsdbMemTableInsert(pTsdb, pTsdb->mem, pMsg, NULL);
}

#if 0
/**
 * @brief Insert/Update tSma(Time-range-wise SMA) data from stream computing engine
 * 
 * @param pTsdb 
 * @param param 
 * @param msg 
 * @return int32_t 
 * TODO: Who is responsible for resource allocate and release?
 */
int32_t tsdbInsertTSmaData(STsdb *pTsdb, char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbInsertTSmaDataImpl(pTsdb, msg)) < 0) {
    tsdbWarn("vgId:%d insert tSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

int32_t tsdbUpdateSmaWindow(STsdb *pTsdb, int8_t smaType, char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbUpdateExpiredWindow(pTsdb, smaType, msg)) < 0) {
    tsdbWarn("vgId:%d update expired sma window failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

/**
 * @brief Insert Time-range-wise Rollup Sma(RSma) data
 *
 * @param pTsdb
 * @param param
 * @param msg
 * @return int32_t
 */
int32_t tsdbInsertRSmaData(STsdb *pTsdb, char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbInsertRSmaDataImpl(pTsdb, msg)) < 0) {
    tsdbWarn("vgId:%d insert rSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

#endif