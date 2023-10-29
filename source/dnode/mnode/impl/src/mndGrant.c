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

#define _DEFAULT_SOURCE
#include "mndGrant.h"
#include "mndShow.h"

#ifndef _GRANT

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char    tmp[32];

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = "community";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "false";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static int32_t mndProcessGrantHB(SRpcMsg *pReq) { return TSDB_CODE_SUCCESS; }

int32_t mndInitGrant(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  return 0;
}

void    mndCleanupGrant() {}
void    grantParseParameter() { mError("can't parsed parameter k"); }
void    grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {}
void    grantAdd(EGrantType grant, uint64_t value) {}
void    grantRestore(EGrantType grant, uint64_t value) {}
int32_t dmProcessGrantReq(void* pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t grantAlterActiveCode(int32_t did, const char *old, const char *new, char *out, int8_t type) {
  return TSDB_CODE_SUCCESS;
}

#endif

void mndGenerateMachineCode() { grantParseParameter(); }
