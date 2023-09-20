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

#include "mndView.h"

int32_t mndInitView(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_VIEW, mndProcessCreateViewReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_VIEW, mndProcessDropViewReq);
  mndSetMsgHandle(pMnode, TDMT_MND_NODECHECK_TIMER, mndProcessNodeCheck);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndRetrieveView);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndCancelGetNextView);

#ifdef TD_ENTERPRISE
  SSdbTable table = {
      .sdbType = SDB_VIEW,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndViewActionEncode,
      .decodeFp = (SdbDecodeFp)mndViewActionDecode,
      .insertFp = (SdbInsertFp)mndViewActionInsert,
      .updateFp = (SdbUpdateFp)mndViewActionUpdate,
      .deleteFp = (SdbDeleteFp)mndViewActionDelete,
  };

  return sdbSetTable(pMnode->pSdb, table);
#else
  return TSDB_CODE_SUCCESS;
#endif
}

void mndCleanupView(SMnode *pMnode) {
  taosArrayDestroy(execNodeList.pTaskList);
  taosHashCleanup(execNodeList.pTaskMap);
  taosThreadMutexDestroy(&execNodeList.lock);
  mDebug("mnd view cleanup");
}

static int32_t mndProcessCreateViewReq(SRpcMsg *pReq) {
  SCMCreateViewReq   createViewReq = {0};

#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  if (tDeserializeSCMCreateViewReq(pReq->pCont, pReq->contLen, &createViewReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("start to create view:%s, sql:%s", createViewReq.name, createViewReq.sql);

  return mndProcessCreateViewReqImpl(&createViewReq, pReq);
#endif
}

static int32_t mndProcessDropViewReq(SRpcMsg *pReq) {
#ifndef TD_ENTERPRISE
    return TSDB_CODE_OPS_NOT_SUPPORT;
#else
    if (tDeserializeSCMDropViewReq(pReq->pCont, pReq->contLen, &createViewReq) != 0) {
      terrno = TSDB_CODE_INVALID_MSG;
      goto _OVER;
    }
  
    mInfo("start to drop view:%s, sql:%s", createViewReq.name, createViewReq.sql);
  
    return mndProcessDropViewReqImpl(&createViewReq, pReq);
#endif
}

static int32_t mndRetrieveView(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SViewObj *pView = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VIEW, pShow->pIter, (void **)&pView);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char viewName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(viewName, mndGetDbStr(pView->name), sizeof(viewName));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)viewName, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pView->createTime, false);

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(sql, pView->sql, sizeof(sql));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);

    char status[20 + VARSTR_HEADER_SIZE] = {0};
    char status2[20] = {0};
    mndShowViewStatus(status2, pView);
    STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);

    char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pView->sourceDb), sizeof(sourceDB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);

    char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pView->targetDb), sizeof(targetDB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);

    if (pView->targetSTbName[0] == 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, NULL, true);
    } else {
      char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pView->targetSTbName), sizeof(targetSTB));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pView->conf.watermark, false);

    char trigger[20 + VARSTR_HEADER_SIZE] = {0};
    char trigger2[20] = {0};
    mndShowViewTrigger(trigger2, pView);
    STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);

    numOfRows++;
    sdbRelease(pSdb, pView);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextView(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}



