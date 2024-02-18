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

#define GRANT_ITEM_SHOW(display)                       \
  do {                                                 \
    cols++;                                            \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols); \
    src = (display);                                   \
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);          \
    colDataSetVal(pColInfo, numOfRows, tmp, false);    \
  } while (0)

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

    GRANT_ITEM_SHOW("unlimited");
    GRANT_ITEM_SHOW("limited");
    GRANT_ITEM_SHOW("false");
    GRANT_ITEM_SHOW("ungranted");
    GRANT_ITEM_SHOW("unlimited");
    GRANT_ITEM_SHOW("unlimited");
    GRANT_ITEM_SHOW("unlimited");

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }
static int32_t mndRetrieveGrantLogs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }

static int32_t mndProcessGrantHB(SRpcMsg *pReq) { return TSDB_CODE_SUCCESS; }

int32_t mndInitGrant(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndRetrieveGrantFull);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOGS, mndRetrieveGrantLogs);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndRetrieveMachines);
  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  return 0;
}

void    mndCleanupGrant() {}
void    grantParseParameter() { mError("can't parsed parameter k"); }
void    grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {}
void    grantAdd(EGrantType grant, uint64_t value) {}
void    grantRestore(EGrantType grant, uint64_t value) {}
char   *tGetMachineId() { return NULL; };
int32_t dmProcessGrantReq(void *pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) { return 0; }
#endif

void mndGenerateMachineCode() { grantParseParameter(); }