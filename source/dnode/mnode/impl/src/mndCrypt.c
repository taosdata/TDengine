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

#include "mndCrypt.h"
#include "mndShow.h"

int32_t mndInitCrypt(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CRYPT, mndRetrieveCrypt);

  SSdbTable table = {
      .sdbType = SDB_CRYPT,
      .keyType = SDB_KEY_INT32,
  };

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCrypt(SMnode *pMnode) {
  mDebug("mnd crypt cleanup");
}

int32_t mndRetrieveCrypt(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows){
  SMnode     *pMnode = pReq->info.node;
  int32_t     numOfRows = 0;

  while (numOfRows < 1) {
    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char tmpBuf[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};

    int32_t dnode = 1;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&dnode, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    strncpy(varDataVal(tmpBuf), "sm4", TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    strncpy(varDataVal(tmpBuf), "tsdb", TSDB_SHOW_SQL_LEN);
    varDataSetLen(tmpBuf, strlen(varDataVal(tmpBuf)));
    colDataSetVal(pColInfo, numOfRows, (const char *)tmpBuf, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;    
  return numOfRows;
}