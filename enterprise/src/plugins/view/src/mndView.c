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
#include "mndTrans.h"
#include "audit.h"

#define MND_VIEW_VER_NUMBER 1

void tFreeViewObj(SViewObj *pView) {
  taosMemoryFree(pView->sql);
  taosMemoryFree(pView->pSchema);
}

SSdbRaw *mndViewActionEncode(SViewObj *pView) {
  terrno = TSDB_CODE_SUCCESS;
  void *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t tlen = tSerializeSCMCreateViewReq(NULL, 0, (SCMCreateViewReq*)pView);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }
  
  int32_t  size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_VIEW, MND_VIEW_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  tlen = tSerializeSCMCreateViewReq(buf, tlen, (SCMCreateViewReq*)pView);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_ENCODE_OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, VIEW_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, VIEW_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, VIEW_ENCODE_OVER);


VIEW_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view:%s, failed to encode to raw:%p since %s", pView->fullname, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("view:%s, encode to raw:%p, row:%p", pView->fullname, pRaw, pView);
  return pRaw;
}

SSdbRow *mndViewActionDecode(SSdbRaw *pRaw) {
  SSdbRow    *pRow = NULL;
  SViewObj   *pView = NULL;
  void       *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto VIEW_DECODE_OVER;
  }

  if (sver != MND_VIEW_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("view read invalid ver, data ver: %d, curr ver: %d", sver, MND_VIEW_VER_NUMBER);
    goto VIEW_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SViewObj));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  pView = sdbGetRowObj(pRow);
  if (pView == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, VIEW_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, VIEW_DECODE_OVER);

  if (tDeserializeSCMCreateViewReq(buf, tlen, (SCMCreateViewReq*)pView) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto VIEW_DECODE_OVER;
  }

  taosInitRWLatch(&pView->lock);

VIEW_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view:%s, failed to decode from raw:%p since %s", pView == NULL ? "null" : pView->fullname, pRaw,
           terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("view:%s, decode from raw:%p, row:%p", pView->fullname, pRaw, pView);
  return pRow;
}

int32_t mndViewActionInsert(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform insert action", pView->fullname);
  return 0;
}

int32_t mndViewActionDelete(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform delete action", pView->fullname);
  tFreeViewObj(pView);
  return 0;
}

int32_t mndViewActionUpdate(SSdb *pSdb, SViewObj *pOldView, SViewObj *pNewView) {
  taosWLockLatch(&pOldView->lock);

  mTrace("view:%s, perform update action, old row:%p new row:%p", pOldView->fullname, pOldView, pNewView);

  pOldView->orReplace = pNewView->orReplace;
  pOldView->precision = pNewView->precision;
  pOldView->numOfCols = pNewView->numOfCols;
  TSWAP(pOldView->querySql, pNewView->querySql);
  TSWAP(pOldView->sql, pNewView->sql);
  TSWAP(pOldView->pSchema, pNewView->pSchema);

  taosWUnLockLatch(&pOldView->lock);

  return 0;
}

SViewObj *mndAcquireView(SMnode *pMnode, char *viewName) {
  SSdb       *pSdb = pMnode->pSdb;
  SViewObj   *pView = sdbAcquire(pSdb, SDB_VIEW, viewName);
  if (pView == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pView;
}

void mndReleaseView(SMnode *pMnode, SViewObj *pView) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pView);
}

static int32_t mndCreateView(SMnode *pMnode, SCMCreateViewReq *pCreate, SRpcMsg *pReq) {
  SViewObj *pView = (SViewObj*)pCreate;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to create since %s", pCreate->fullname, terrstr());
    return -1;
  }

  mInfo("trans:%d, used to create view:%s", pTrans->id, pCreate->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  
  return 0;
}

static int32_t mndDropView(SMnode *pMnode, SRpcMsg *pReq, SViewObj *pView) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-view");
  if (pTrans == NULL) {
    mError("view:%s, failed to drop since %s", pView->fullname, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to drop view:%s", pTrans->id, pView->fullname);

  SSdbRaw *pCommitRaw = mndViewActionEncode(pView);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}


static void mndLogCreateViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMCreateViewReq* pCreateViewReq) {
  char detail[2000] = {0};
  sprintf(detail, "orReplace:%d, precision:%d, numOfCols:%d",
          pCreateViewReq->orReplace, pCreateViewReq->precision, pCreateViewReq->numOfCols);

  auditRecord(pReq, pMnode->clusterId, "createView", pCreateViewReq->dbFName, pCreateViewReq->name, detail);
}

static void mndLogDropViewAudit(SRpcMsg *pReq, SMnode *pMnode, SCMDropViewReq* pDropViewReq) {
  char detail[100] = {0};
  sprintf(detail, "igNotExists:%d", pDropViewReq->igNotExists);

  auditRecord(pReq, pMnode->clusterId, "dropView", pDropViewReq->dbFName, pDropViewReq->name, detail);
}


int32_t mndProcessCreateViewReqImpl(SCMCreateViewReq* pCreateView, SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SViewObj          *pView = NULL;
  SDbObj            *pDb = NULL;
  SViewObj           newObj = {0};

  pView = mndAcquireView(pMnode, pCreateView->fullname);
  if (pView != NULL) {
    if (!pCreateView->orReplace) {
      terrno = TSDB_CODE_MND_VIEW_ALREADY_EXIST;
      goto _OVER;
    } else {
      mInfo("view %s already exist, or replace is set", pCreateView->fullname);
    }
  } else if (terrno != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  if (mndCreateView(pMnode, pCreateView, pReq) < 0) {
    mError("view:%s, failed to create since %s", pCreateView->fullname, terrstr());
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogCreateViewAudit(pReq, pMnode, pCreateView);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to create view %s since %s", pCreateView->fullname, terrstr());
  }

  mndReleaseView(pMnode, pView);

  tFreeSCMCreateViewReq(pCreateView);
  return code;
}

int32_t mndProcessDropViewReqImpl(SCMDropViewReq* pDropView, SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SViewObj   *pView = mndAcquireView(pMnode, pDropView->fullname);

  if (pView == NULL) {
    if (pDropView->igNotExists) {
      mInfo("view:%s, not exist, ignore not exist is set", pDropView->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_VIEW_NOT_EXIST;
      return -1;
    }
  }

  if (mndDropView(pMnode, pReq, pView) < 0) {
    goto _OVER;
  }

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  mndLogDropViewAudit(pReq, pMnode, pDropView);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to drop view %s since %s", pDropView->fullname, terrstr());
  }

  sdbRelease(pMnode->pSdb, pView);

  return code;
}

static int32_t mndRetrieveViewImpl(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
#if 0
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
#else
  return 0;
#endif
}

static void mndCancelGetNextViewImpl(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}



