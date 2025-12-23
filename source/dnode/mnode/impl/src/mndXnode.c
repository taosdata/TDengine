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
#include <stdio.h>
#include "mndDef.h"
#include "tdatablock.h"
#include "tdef.h"
#include "types.h"

#include <curl/curl.h>
#include "audit.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndXnode.h"
#include "sdb.h"
#include "taoserror.h"
#include "tjson.h"
#include "xnode.h"

#define TSDB_XNODE_VER_NUMBER   1
#define TSDB_XNODE_RESERVE_SIZE 64
#define XNODED_URL              "http://localhost:6051"
typedef enum {
  HTTP_TYPE_GET = 0,
  HTTP_TYPE_POST,
  HTTP_TYPE_DELETE,
} EHttpType;
typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

const int32_t defaultTimeout = 5000;

/** xnodes systable actions */
static SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj);
static SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj);
static int32_t  mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew);
static int32_t  mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj);

/** @section xnodes request handlers */
static int32_t mndProcessCreateXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessDrainXnodeReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnode(SMnode *pMnode, void *pIter);

/** @section xnode task handlers */
static SSdbRaw *mndXnodeTaskActionEncode(SXnodeTaskObj *pObj);
static SSdbRow *mndXnodeTaskActionDecode(SSdbRaw *pRaw);
static int32_t  mndXnodeTaskActionInsert(SSdb *pSdb, SXnodeTaskObj *pObj);
static int32_t  mndXnodeTaskActionUpdate(SSdb *pSdb, SXnodeTaskObj *pOld, SXnodeTaskObj *pNew);
static int32_t  mndXnodeTaskActionDelete(SSdb *pSdb, SXnodeTaskObj *pObj);

static int32_t mndProcessCreateXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessStartXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessStopXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodeTasks(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeTask(SMnode *pMnode, void *pIter);

/** @section xnode task job handlers */
static SSdbRaw *mndXnodeJobActionEncode(SXnodeJobObj *pObj);
static SSdbRow *mndXnodeJobActionDecode(SSdbRaw *pRaw);
static int32_t  mndXnodeJobActionInsert(SSdb *pSdb, SXnodeJobObj *pObj);
static int32_t  mndXnodeJobActionUpdate(SSdb *pSdb, SXnodeJobObj *pOld, SXnodeJobObj *pNew);
static int32_t  mndXnodeJobActionDelete(SSdb *pSdb, SXnodeJobObj *pObj);

static int32_t mndProcessCreateXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessRebalanceXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeJobReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodeJobs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeJob(SMnode *pMnode, void *pIter);

/** @section xnode user pass handlers */
static SSdbRaw *mndXnodeUserPassActionEncode(SXnodeUserPassObj *pObj);
static SSdbRow *mndXnodeUserPassActionDecode(SSdbRaw *pRaw);
static int32_t  mndXnodeUserPassActionInsert(SSdb *pSdb, SXnodeUserPassObj *pObj);
static int32_t  mndXnodeUserPassActionUpdate(SSdb *pSdb, SXnodeUserPassObj *pOld, SXnodeUserPassObj *pNew);
static int32_t  mndXnodeUserPassActionDelete(SSdb *pSdb, SXnodeUserPassObj *pObj);

/** @section xnode agent handlers */
static int32_t mndRetrieveXnodeAgents(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeAgent(SMnode *pMnode, void *pIter);

static int32_t mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen);
static int32_t mndGetXnodeTaskStatus(SXnodeTaskObj *pObj, char *status, int32_t statusLen);

/** @section xnoded mgmt */
void mndStartXnoded(SMnode *pMnode, int32_t userLen, char *user, int32_t passLen, char *pass);

SXnodeTaskObj *mndAcquireXnodeTask(SMnode *pMnode, int32_t tid);
SJson         *mndSendReqRetJson(const char *url, EHttpType type, int64_t timeout, const char *buf, int64_t bufLen);
static int32_t mndSetDropXnodeJobInfoToTrans(STrans *pTrans, SXnodeJobObj *pObj, bool force);
void           mndReleaseXnodeJob(SMnode *pMnode, SXnodeJobObj *pObj);

int32_t mndInitXnode(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_XNODE,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeActionDelete,
  };

  int32_t code = sdbSetTable(pMnode->pSdb, table);
  if (code != 0) {
    return code;
  }

  SSdbTable tasks = {
      .sdbType = SDB_XNODE_TASK,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeTaskActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeTaskActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeTaskActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeTaskActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeTaskActionDelete,
  };

  code = sdbSetTable(pMnode->pSdb, tasks);
  if (code != 0) {
    return code;
  }

  SSdbTable jobs = {
      .sdbType = SDB_XNODE_JOB,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeJobActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeJobActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeJobActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeJobActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeJobActionDelete,
  };

  code = sdbSetTable(pMnode->pSdb, jobs);
  if (code != 0) {
    return code;
  }

  SSdbTable userPass = {
      .sdbType = SDB_XNODE_USER_PASS,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeUserPassActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeUserPassActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeUserPassActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeUserPassActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeUserPassActionDelete,
  };

  code = sdbSetTable(pMnode->pSdb, userPass);
  if (code != 0) {
    return code;
  }

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE, mndProcessCreateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE, mndProcessUpdateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE, mndProcessDropXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DRAIN_XNODE, mndProcessDrainXnodeReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODES, mndRetrieveXnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODES, mndCancelGetNextXnode);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE_TASK, mndProcessCreateXnodeTaskReq);
  mndSetMsgHandle(pMnode, TDMT_MND_START_XNODE_TASK, mndProcessStartXnodeTaskReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STOP_XNODE_TASK, mndProcessStopXnodeTaskReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE_TASK, mndProcessUpdateXnodeTaskReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE_TASK, mndProcessDropXnodeTaskReq);
  // todo: stop
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_TASKS, mndRetrieveXnodeTasks);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_TASKS, mndCancelGetNextXnodeTask);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE_AGENT, mndProcessCreateXnodeReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE_AGENT, mndProcessDropXnodeReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_AGENTS, mndRetrieveXnodeAgents);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_AGENTS, mndCancelGetNextXnodeAgent);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE_JOB, mndProcessCreateXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE_JOB, mndProcessUpdateXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_REBALANCE_XNODE_JOB, mndProcessRebalanceXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE_JOB, mndProcessDropXnodeJobReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_JOBS, mndRetrieveXnodeJobs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_JOBS, mndCancelGetNextXnodeJob);

  return 0;
}

int32_t checkPasswordFmt(const char *pwd) {
  if (strcmp(pwd, "taosdata") == 0) {
    return 0;
  }

  if (tsEnableStrongPassword == 0) {
    for (char c = *pwd; c != 0; c = *(++pwd)) {
      if (c == ' ' || c == '\'' || c == '\"' || c == '`' || c == '\\') {
        return TSDB_CODE_MND_INVALID_PASS_FORMAT;
      }
    }
    return 0;
  }

  int32_t len = strlen(pwd);
  if (len < TSDB_PASSWORD_MIN_LEN) {
    return TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY;
  }

  if (len > TSDB_PASSWORD_MAX_LEN) {
    return TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG;
  }

  if (taosIsComplexString(pwd)) {
    return 0;
  }

  return TSDB_CODE_MND_INVALID_PASS_FORMAT;
}

void swapFields(int32_t *newLen, char **ppNewStr, int32_t *oldLen, char **ppOldStr) {
  if (*newLen > 0) {
    int32_t tempLen = *newLen;
    *newLen = *oldLen;
    *oldLen = tempLen;

    char *tempStr = *ppNewStr;
    *ppNewStr = *ppOldStr;
    *ppOldStr = tempStr;
  }
}

void mndCleanupXnode(SMnode *pMnode) {}

SXnodeObj *mndAcquireXnode(SMnode *pMnode, int32_t xnodeId) {
  SXnodeObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE, &xnodeId);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseXnode(SMnode *pMnode, SXnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(SXnodeObj) + TSDB_XNODE_RESERVE_SIZE + pObj->urlLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->urlLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->statusLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SXnodeObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->urlLen, _OVER)
  if (pObj->urlLen > 0) {
    pObj->url = taosMemoryCalloc(pObj->urlLen, 1);
    if (pObj->url == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  } else {
    pObj->url = NULL;
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->statusLen, _OVER)
  if (pObj->statusLen > 0) {
    pObj->status = taosMemoryCalloc(pObj->statusLen, 1);
    if (pObj->status == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  } else {
    pObj->status = NULL;
  }
  SDB_GET_INT64(pRaw, dataPos, &pObj->createTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->url);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static void mndFreeXnode(SXnodeObj *pObj) {
  if (pObj == NULL) return;
  if (pObj->url != NULL) {
    taosMemoryFreeClear(pObj->url);
    pObj->urlLen = 0;
  }
  if (pObj->status != NULL) {
    taosMemoryFreeClear(pObj->status);
    pObj->statusLen = 0;
  }
}

static int32_t mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj) {
  mDebug("xnode:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj) {
  mDebug("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnode(pObj);
  return 0;
}

static int32_t mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew) {
  mDebug("xnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  pOld->updateTime = pNew->updateTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SXnodeUserPassObj *mndAcquireFirstXnodeUserPass(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeUserPassObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_USER_PASS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    if (pObj != NULL) {
      sdbCancelFetch(pSdb, pIter);
      return pObj;
    }

    sdbRelease(pSdb, pObj);
  }
  terrno = TSDB_CODE_MND_XNODE_USER_PASS_NOT_EXIST;
  return NULL;
}

static int32_t mndSetCreateXnodeUserPassRedoLogs(STrans *pTrans, SXnodeUserPassObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeUserPassActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeUserPassCommitLogs(STrans *pTrans, SXnodeUserPassObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeUserPassActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeUndoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndCreateXnode(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE);

  xnodeObj.urlLen = pCreate->urlLen;
  if (xnodeObj.urlLen > TSDB_XNODE_URL_LEN) {
    code = TSDB_CODE_MND_XNODE_TOO_LONG_URL;
    goto _OVER;
  }
  xnodeObj.url = taosMemoryCalloc(1, pCreate->urlLen);
  if (xnodeObj.url == NULL) goto _OVER;
  (void)memcpy(xnodeObj.url, pCreate->url, pCreate->urlLen);

  xnodeObj.createTime = taosGetTimestampMs();
  xnodeObj.updateTime = xnodeObj.createTime;
  mInfo("create xnode, xnode.id:%d, xnode.url: %s, xnode.time:%ld", xnodeObj.id, xnodeObj.url, xnodeObj.createTime);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mInfo("failed to create transaction for xnode:%s, code:0x%x:%s", pCreate->url, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode:%s as xnode:%d", pTrans->id, pCreate->url, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeRedoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUndoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndFreeXnode(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static SXnodeObj *mndAcquireXnodeByURL(SMnode *pMnode, char *url) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeObj *pXnode = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE, pIter, (void **)&pXnode);
    if (pIter == NULL) break;

    if (strcasecmp(url, pXnode->url) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pXnode;
    }

    sdbRelease(pSdb, pXnode);
  }

  mError("xnode:%s, not found", url);
  terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  return NULL;
}
static SXnodeTaskObj *mndAcquireXnodeTaskById(SMnode *pMnode, int32_t tid) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeTaskObj *pTask = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_TASK, pIter, (void **)&pTask);
    if (pIter == NULL) break;

    if (pTask->id == tid) {
      sdbCancelFetch(pSdb, pIter);
      return pTask;
    }

    sdbRelease(pSdb, pTask);
  }

  mError("xnode task:%d, not found", tid);
  terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  return NULL;
}
static SXnodeTaskObj *mndAcquireXnodeTaskByName(SMnode *pMnode, const char *name) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeTaskObj *pTask = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_TASK, pIter, (void **)&pTask);
    if (pIter == NULL) break;
    if (pTask->name == NULL) {
      continue;
    }

    if (strcasecmp(name, pTask->name) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pTask;
    }

    sdbRelease(pSdb, pTask);
  }

  mError("xnode task:%s, not found", name);
  // terrno = TSDB_CODE_MND_XNODE_NOT_EXIST;
  return NULL;
}

static int32_t mndAcquireXnodeJobsByTaskId(SMnode *pMnode, int32_t tid, SArray **ppArray) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppArray = taosArrayInit(16, sizeof(SXnodeJobObj));
  if (ppArray == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  int32_t idx = 0;
  void   *pIter = NULL;
  while (1) {
    SXnodeJobObj *pJob = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_JOB, pIter, (void **)&pJob);
    if (pIter == NULL) break;

    if (pJob->taskId == tid) {
      taosArrayInsert(*ppArray, idx++, pJob);
      continue;
    }

    sdbRelease(pSdb, pJob);
  }
  sdbCancelFetch(pSdb, pIter);

_exit:
  return code;
}

static void mndFreeXnodeTask(SXnodeTaskObj *pObj) {
  taosMemoryFreeClear(pObj->name);
  taosMemoryFreeClear(pObj->sourceDsn);
  taosMemoryFreeClear(pObj->sinkDsn);
  taosMemoryFreeClear(pObj->parser);
  taosMemoryFreeClear(pObj->reason);
  taosMemoryFreeClear(pObj->status);
}

static SSdbRaw *mndXnodeTaskActionEncode(SXnodeTaskObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  int32_t totalStrLen = pObj->nameLen + pObj->sourceDsnLen + pObj->sinkDsnLen + pObj->parserLen + pObj->reasonLen;
  int32_t rawDataLen = sizeof(SXnodeTaskObj) + TSDB_XNODE_RESERVE_SIZE + totalStrLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_TASK, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->statusLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->via, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->xnodeId, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->nameLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->name, pObj->nameLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->sourceType, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->sourceDsnLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->sourceDsn, pObj->sourceDsnLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->sinkType, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->sinkDsnLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->sinkDsn, pObj->sinkDsnLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->parserLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->parser, pObj->parserLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->reasonLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->reason, pObj->reasonLen, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode task:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode task:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndXnodeTaskActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow       *pRow = NULL;
  SXnodeTaskObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeTaskObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->createTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->statusLen, _OVER)
  if (pObj->statusLen > 0) {
    pObj->status = taosMemoryCalloc(pObj->statusLen + 1, 1);
    if (pObj->status == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->via, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->xnodeId, _OVER)

  SDB_GET_INT32(pRaw, dataPos, &pObj->nameLen, _OVER)
  if (pObj->nameLen > 0) {
    pObj->name = taosMemoryCalloc(pObj->nameLen + 1, 1);
    if (pObj->name == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->name, pObj->nameLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->sourceType, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->sourceDsnLen, _OVER)
  if (pObj->sourceDsnLen > 0) {
    pObj->sourceDsn = taosMemoryCalloc(pObj->sourceDsnLen + 1, 1);
    if (pObj->sourceDsn == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->sourceDsn, pObj->sourceDsnLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->sinkType, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->sinkDsnLen, _OVER)
  if (pObj->sinkDsnLen > 0) {
    pObj->sinkDsn = taosMemoryCalloc(pObj->sinkDsnLen + 1, 1);
    if (pObj->sinkDsn == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->sinkDsn, pObj->sinkDsnLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->parserLen, _OVER)
  if (pObj->parserLen > 0) {
    pObj->parser = taosMemoryCalloc(pObj->parserLen + 1, 1);
    if (pObj->parser == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->parser, pObj->parserLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->reasonLen, _OVER)
  if (pObj->reasonLen > 0) {
    pObj->reason = taosMemoryCalloc(pObj->reasonLen + 1, 1);
    if (pObj->reason == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->reason, pObj->reasonLen, _OVER)
  }

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode task:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->name);
      taosMemoryFreeClear(pObj->sourceDsn);
      taosMemoryFreeClear(pObj->sinkDsn);
      taosMemoryFreeClear(pObj->parser);
      taosMemoryFreeClear(pObj->reason);
      taosMemoryFreeClear(pObj->status);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndXnodeTaskActionInsert(SSdb *pSdb, SXnodeTaskObj *pObj) {
  mDebug("xtask:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndXnodeTaskActionDelete(SSdb *pSdb, SXnodeTaskObj *pObj) {
  mDebug("xtask:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnodeTask(pObj);
  return 0;
}

static int32_t mndXnodeTaskActionUpdate(SSdb *pSdb, SXnodeTaskObj *pOld, SXnodeTaskObj *pNew) {
  mDebug("xtask:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  pOld->via = pNew->via;
  pOld->xnodeId = pNew->xnodeId;
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  swapFields(&pNew->nameLen, &pNew->name, &pOld->nameLen, &pOld->name);
  swapFields(&pNew->sourceDsnLen, &pNew->sourceDsn, &pOld->sourceDsnLen, &pOld->sourceDsn);
  swapFields(&pNew->sinkDsnLen, &pNew->sinkDsn, &pOld->sinkDsnLen, &pOld->sinkDsn);
  swapFields(&pNew->parserLen, &pNew->parser, &pOld->parserLen, &pOld->parser);
  swapFields(&pNew->reasonLen, &pNew->reason, &pOld->reasonLen, &pOld->reason);
  pOld->updateTime = pNew->updateTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

static int32_t mndSetCreateXnodeTaskRedoLogs(STrans *pTrans, SXnodeTaskObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeTaskActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeTaskUndoLogs(STrans *pTrans, SXnodeTaskObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeTaskActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeTaskCommitLogs(STrans *pTrans, SXnodeTaskObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeTaskActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}
void mndReleaseXnodeTask(SMnode *pMnode, SXnodeTaskObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static const char *getXTaskOptionByName(xTaskOptions *pOptions, const char *name) {
  if (pOptions == NULL || name == NULL) return NULL;
  for (int32_t i = 0; i < pOptions->optionsNum; ++i) {
    CowStr option = pOptions->options[i];
    if (option.ptr != NULL && strncasecmp(option.ptr, name, strlen(name)) == 0 && option.ptr[strlen(name)] == '=') {
      return option.ptr + strlen(name) + 1;
    }
  }
  return NULL;
}

static int32_t mndCreateXnodeTask(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeTaskReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SXnodeTaskObj xnodeObj = {0};
  xnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_TASK);
  xnodeObj.createTime = taosGetTimestampMs();
  xnodeObj.updateTime = xnodeObj.createTime;
  xnodeObj.via = pCreate->options.via;
  xnodeObj.xnodeId = pCreate->xnodeId;

  xnodeObj.nameLen = pCreate->name.len;
  xnodeObj.name = taosMemoryCalloc(1, pCreate->name.len);
  if (xnodeObj.name == NULL) goto _OVER;
  (void)memcpy(xnodeObj.name, pCreate->name.ptr, pCreate->name.len);

  xnodeObj.sourceType = pCreate->source.type;
  xnodeObj.sourceDsnLen = pCreate->source.cstr.len;
  xnodeObj.sourceDsn = taosMemoryCalloc(1, pCreate->source.cstr.len);
  if (xnodeObj.sourceDsn == NULL) goto _OVER;
  (void)memcpy(xnodeObj.sourceDsn, pCreate->source.cstr.ptr, pCreate->source.cstr.len);

  xnodeObj.sinkType = pCreate->sink.type;
  xnodeObj.sinkDsnLen = pCreate->sink.cstr.len;
  xnodeObj.sinkDsn = taosMemoryCalloc(1, pCreate->sink.cstr.len);
  if (xnodeObj.sinkDsn == NULL) goto _OVER;
  (void)memcpy(xnodeObj.sinkDsn, pCreate->sink.cstr.ptr, pCreate->sink.cstr.len);

  xnodeObj.parserLen = pCreate->options.parser.len;
  if (xnodeObj.parserLen > 0) {
    xnodeObj.parser = taosMemoryCalloc(1, xnodeObj.parserLen);
    if (xnodeObj.parser == NULL) goto _OVER;
    (void)memcpy(xnodeObj.parser, pCreate->options.parser.ptr, xnodeObj.parserLen);
  }

  const char *status = getXTaskOptionByName(&pCreate->options, "status");
  if (status != NULL) {
    xnodeObj.statusLen = strlen(status) + 1;
    xnodeObj.status = taosMemoryCalloc(1, xnodeObj.statusLen);
    if (xnodeObj.status == NULL) goto _OVER;
    (void)memcpy(xnodeObj.status, status, xnodeObj.statusLen);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode-task");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) {
      code = terrno;
    }
    mInfo("failed to create transaction for xnode-task:%s, code:0x%x:%s", pCreate->name.ptr, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mDebug("trans:%d, used to create xnode task:%s as task:%d", pTrans->id, pCreate->name.ptr, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeTaskRedoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeTaskUndoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeTaskCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndFreeXnodeTask(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// Helper function to validate grant and permissions
static int32_t mndValidateXnodeTaskPermissions(SMnode *pMnode, SRpcMsg *pReq) {
  int32_t code = grantCheck(TSDB_GRANT_XNODE);  // xxxzgc todo: check xnode task grant
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    return code;
  }

  return mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_XNODE);
}

// Helper function to parse and validate the request
static int32_t mndValidateCreateXnodeTaskReq(SRpcMsg *pReq, SMCreateXnodeTaskReq *pCreateReq) {
  mInfo("xnode task:%s, start validate create request", pCreateReq->name.ptr);
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *srcDsn = NULL;
  char   *sinkDsn = NULL;
  char   *parser = NULL;
  char   *pContStr = NULL;

  // from, to, parser check
  char xnodeUrl[TSDB_XNODE_URL_LEN] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/check", XNODED_URL);
  postContent = tjsonCreateObject();
  if (postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  srcDsn = taosStrndupi(pCreateReq->source.cstr.ptr, (int64_t)pCreateReq->source.cstr.len);
  if (srcDsn == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "from", srcDsn), NULL, _OVER);

  sinkDsn = taosStrndupi(pCreateReq->sink.cstr.ptr, (int64_t)pCreateReq->sink.cstr.len);
  if (sinkDsn == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "to", sinkDsn), NULL, _OVER);

  if (pCreateReq->options.parser.len > 0) {
    parser = taosStrndupi(pCreateReq->options.parser.ptr, (int64_t)pCreateReq->options.parser.len);
    if (parser == NULL) {
      code = terrno;
      goto _OVER;
    }
    TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "parser", parser), NULL, _OVER);
  }

  if (pCreateReq->xnodeId > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "xnodeId", (double)pCreateReq->xnodeId), NULL, _OVER);
  }

  pContStr = tjsonToUnformattedString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }

  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (srcDsn != NULL) taosMemoryFreeClear(srcDsn);
  if (sinkDsn != NULL) taosMemoryFreeClear(sinkDsn);
  if (parser != NULL) taosMemoryFreeClear(parser);
  if (pContStr != NULL) taosMemoryFreeClear(pContStr);
  if (postContent != NULL) tjsonDelete(postContent);
  if (pJson != NULL) tjsonDelete(pJson);

  return code;
}

// Helper function to check if xnode task already exists
static int32_t mndCheckXnodeTaskExists(SMnode *pMnode, const char *name) {
  SXnodeTaskObj *pObj = mndAcquireXnodeTaskByName(pMnode, name);
  if (pObj != NULL) {
    mError("xnode task:%s already exists", name);
    return TSDB_CODE_MND_XNODE_TASK_ALREADY_EXIST;
  }
  return TSDB_CODE_SUCCESS;
}

// Helper function to handle the creation result
static int32_t mndHandleCreateXnodeTaskResult(int32_t createCode) {
  if (createCode == 0) {
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
  return createCode;
}

static int32_t mndProcessCreateXnodeTaskReq(SRpcMsg *pReq) {
  mInfo("xnode create task request received, contLen:%d\n", pReq->contLen);
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = -1;
  SXnodeTaskObj       *pObj = NULL;
  SMCreateXnodeTaskReq createReq = {0};

  // Step 1: Validate permissions
  code = mndValidateXnodeTaskPermissions(pMnode, pReq);
  if (code != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  code = tDeserializeSMCreateXnodeTaskReq(pReq->pCont, pReq->contLen, &createReq);
  if (code != 0) {
    mError("failed to deserialize create xnode task request, code:%s", tstrerror(code));
    return code;
  }

  // Step 2: Check if task already exists
  TAOS_CHECK_GOTO(mndCheckXnodeTaskExists(pMnode, createReq.name.ptr), NULL, _OVER);

  // Step 3: Parse and validate request
  TAOS_CHECK_GOTO(mndValidateCreateXnodeTaskReq(pReq, &createReq), NULL, _OVER);

  // Step 4: Create the xnode task
  TAOS_CHECK_GOTO(mndCreateXnodeTask(pMnode, pReq, &createReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndHandleCreateXnodeTaskResult(code), NULL, _OVER);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%s, failed to create since %s", createReq.name.ptr ? createReq.name.ptr : "unknown",
           tstrerror(code));
  }

  mndReleaseXnodeTask(pMnode, pObj);
  tFreeSMCreateXnodeTaskReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t httpStartXnodeTask(SXnodeTaskObj *pObj) {
  int32_t code = 0;
  struct {
    char   xnodeUrl[TSDB_XNODE_URL_LEN + 1];
    SJson *postContent;
    SJson *pJson;
    char  *pContStr;
    char  *srcDsn;
    char  *sinkDsn;
    char  *parser;
  } req = {0};

  snprintf(req.xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/%d", XNODED_URL, pObj->id);
  req.postContent = tjsonCreateObject();
  if (req.postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  req.srcDsn = taosStrndupi(pObj->sourceDsn, (int64_t)pObj->sourceDsnLen);
  if (req.srcDsn == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(req.postContent, "from", req.srcDsn), NULL, _OVER);

  req.sinkDsn = taosStrndupi(pObj->sinkDsn, (int64_t)pObj->sinkDsnLen);
  if (req.sinkDsn == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(req.postContent, "to", req.sinkDsn), NULL, _OVER);

  if (pObj->parserLen > 0) {
    req.parser = taosStrndupi(pObj->parser, (int64_t)pObj->parserLen);
    if (req.parser == NULL) {
      code = terrno;
      goto _OVER;
    }
    TAOS_CHECK_GOTO(tjsonAddStringToObject(req.postContent, "parser", req.parser), NULL, _OVER);
  }

  if (pObj->xnodeId > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(req.postContent, "xnodeId", (double)pObj->xnodeId), NULL, _OVER);
  }

  req.pContStr = tjsonToUnformattedString(req.postContent);
  if (req.pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }
  mDebug("start xnode post content:%s", req.pContStr);
  req.pJson = mndSendReqRetJson(req.xnodeUrl, HTTP_TYPE_POST, 30000, req.pContStr, strlen(req.pContStr));
  if (req.pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (req.pContStr != NULL) taosMemoryFreeClear(req.pContStr);
  if (req.postContent != NULL) tjsonDelete(req.postContent);
  if (req.pJson != NULL) tjsonDelete(req.pJson);
  if (req.srcDsn != NULL) taosMemoryFreeClear(req.srcDsn);
  if (req.sinkDsn != NULL) taosMemoryFreeClear(req.sinkDsn);
  if (req.parser != NULL) taosMemoryFreeClear(req.parser);
  return code;
}

static int32_t mndProcessStartXnodeTaskReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeTaskObj      *pObj = NULL;
  SMStartXnodeTaskReq startReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMStartXnodeTaskReq(pReq->pCont, pReq->contLen, &startReq), NULL, _OVER);

  mInfo("Xnode start xnode task with tid:%d", startReq.tid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_START_XNODE_TASK), NULL, _OVER);

  if (startReq.tid <= 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeTask(pMnode, startReq.tid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request
  TAOS_CHECK_GOTO(httpStartXnodeTask(pObj), NULL, _OVER);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to start since %s", startReq.tid, tstrerror(code));
  }
  tFreeSMStartXnodeTaskReq(&startReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessStopXnodeTaskReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SXnodeTaskObj     *pObj = NULL;
  SMStopXnodeTaskReq stopReq = {0};
  SJson             *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMStopXnodeTaskReq(pReq->pCont, pReq->contLen, &stopReq), NULL, _OVER);

  mDebug("Stop xnode task with tid:%d", stopReq.tid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_STOP_XNODE_TASK), NULL, _OVER);

  if (stopReq.tid <= 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeTask(pMnode, stopReq.tid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/%d", XNODED_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to stop since %s", stopReq.tid, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  tFreeSMStopXnodeTaskReq(&stopReq);
  TAOS_RETURN(code);
}

static int32_t mndUpdateXnodeTask(SMnode *pMnode, SRpcMsg *pReq, const SXnodeTaskObj *pOld,
                                  SMUpdateXnodeTaskReq *pUpdate) {
  mDebug("xnode task:%d, start to update", pUpdate->tid);
  int32_t      code = -1;
  STrans       *pTrans = NULL;
  SXnodeTaskObj taskObj = *pOld;
  struct {
    bool status;
    bool name;
    bool source;
    bool sink;
    bool parser;
    bool reason;
  } isChange = {0};

  if (pUpdate->via > 0) {
    taskObj.via = pUpdate->via;
  }
  if (pUpdate->xnodeId > 0) {
    taskObj.xnodeId = pUpdate->xnodeId;
  }
  if (pUpdate->status.len > 0) {
    taskObj.statusLen = pUpdate->status.len;
    taskObj.status = taosMemoryCalloc(1, taskObj.statusLen);
    if (taskObj.status == NULL) goto _OVER;
    (void)memcpy(taskObj.status, pUpdate->status.ptr, taskObj.statusLen);
    isChange.status = true;
  }
  if (pUpdate->name.len > 0) {
    taskObj.nameLen = pUpdate->name.len;
    taskObj.name = taosMemoryCalloc(1, pUpdate->name.len);
    if (taskObj.name == NULL) goto _OVER;
    (void)memcpy(taskObj.name, pUpdate->name.ptr, pUpdate->name.len);
    isChange.name = true;
  }
  if (pUpdate->source.cstr.len > 0) {
    taskObj.sourceType = pUpdate->source.type;
    taskObj.sourceDsnLen = pUpdate->source.cstr.len;
    taskObj.sourceDsn = taosMemoryCalloc(1, pUpdate->source.cstr.len);
    if (taskObj.sourceDsn == NULL) goto _OVER;
    (void)memcpy(taskObj.sourceDsn, pUpdate->source.cstr.ptr, pUpdate->source.cstr.len);
    isChange.source = true;
  }
  if (pUpdate->sink.cstr.len > 0) {
    taskObj.sinkType = pUpdate->sink.type;
    taskObj.sinkDsnLen = pUpdate->sink.cstr.len;
    taskObj.sinkDsn = taosMemoryCalloc(1, pUpdate->sink.cstr.len);
    if (taskObj.sinkDsn == NULL) goto _OVER;
    (void)memcpy(taskObj.sinkDsn, pUpdate->sink.cstr.ptr, pUpdate->sink.cstr.len);
    isChange.sink = true;
  }
  if (pUpdate->parser.len > 0) {
    taskObj.parserLen = pUpdate->parser.len;
    taskObj.parser = taosMemoryCalloc(1, pUpdate->parser.len);
    if (taskObj.parser == NULL) goto _OVER;
    (void)memcpy(taskObj.parser, pUpdate->parser.ptr, pUpdate->parser.len);
    isChange.parser = true;
  }
  if (pUpdate->reason.len > 0) {
    taskObj.reasonLen = pUpdate->reason.len;
    taskObj.reason = taosMemoryCalloc(1, pUpdate->reason.len);
    if (taskObj.reason == NULL) goto _OVER;
    (void)memcpy(taskObj.reason, pUpdate->reason.ptr, pUpdate->reason.len);
    isChange.reason = true;
  }
  taskObj.updateTime = taosGetTimestampMs();

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode-task");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update xnode task:%d", pTrans->id, taskObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeTaskCommitLogs(pTrans, &taskObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  if (NULL != taskObj.name && isChange.name) {
    taosMemoryFree(taskObj.name);
  }
  if (NULL != taskObj.status && isChange.status) {
    taosMemoryFree(taskObj.status);
  }
  if (NULL != taskObj.sourceDsn && isChange.source) {
    taosMemoryFree(taskObj.sourceDsn);
  }
  if (NULL != taskObj.sinkDsn && isChange.sink) {
    taosMemoryFree(taskObj.sinkDsn);
  }
  if (NULL != taskObj.parser && isChange.parser) {
    taosMemoryFree(taskObj.parser);
  }
  if (NULL != taskObj.reason && isChange.reason) {
    taosMemoryFree(taskObj.reason);
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeTaskReq(SRpcMsg *pReq) {
  mInfo("xnode update task request received, contLen:%d\n", pReq->contLen);
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeTaskObj       *pObj = NULL;
  SMUpdateXnodeTaskReq updateReq = {0};

  if ((code = grantCheck(TSDB_GRANT_TD_GPT)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeTaskReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);

  pObj = mndAcquireXnodeTaskById(pMnode, updateReq.tid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = mndUpdateXnodeTask(pMnode, pReq, pObj, &updateReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to update since %s", updateReq.tid, tstrerror(code));
  }

  mndReleaseXnodeTask(pMnode, pObj);
  tFreeSMUpdateXnodeTaskReq(&updateReq);
  TAOS_RETURN(code);
  return 0;
}

SXnodeTaskObj *mndAcquireXnodeTask(SMnode *pMnode, int32_t tid) {
  SXnodeTaskObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE_TASK, &tid);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_TASK_NOT_EXIST;
  }
  return pObj;
}

static int32_t mndSetDropXnodeTaskRedoLogs(STrans *pTrans, SXnodeTaskObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeTaskActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return code;
}

static int32_t mndSetDropXnodeTaskCommitLogs(STrans *pTrans, SXnodeTaskObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeTaskActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}
static int32_t mndSetDropXnodeTaskInfoToTrans(SMnode *pMnode, STrans *pTrans, SXnodeTaskObj *pObj, bool force) {
  if (pObj == NULL) {
    return 0;
  }
  TAOS_CHECK_RETURN(mndSetDropXnodeTaskRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropXnodeTaskCommitLogs(pTrans, pObj));
  return 0;
}

static int32_t mndDropXnodeTask(SMnode *pMnode, SRpcMsg *pReq, SXnodeTaskObj *pTask) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray *pArray = NULL;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode-task");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, to drop xnode:%d", pTrans->id, pTask->id);

  // delete relative jobs
  TAOS_CHECK_GOTO(mndAcquireXnodeJobsByTaskId(pMnode, pTask->id, &pArray), NULL, _OVER);
  for (int i = 0; i < pArray->size; i++) {
    SXnodeJobObj *pJob = taosArrayGet(pArray, i);
    if (pJob == NULL) continue;
    mDebug("xnode drop xnode task %d trans:%d, to drop xnode job:%d", pTask->id, pTrans->id, pJob->id);
    TAOS_CHECK_GOTO(mndSetDropXnodeJobInfoToTrans(pTrans, pJob, false), NULL, _OVER);
  }

  code = mndSetDropXnodeTaskInfoToTrans(pMnode, pTrans, pTask, false);
  mndReleaseXnodeTask(pMnode, pTask);

  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  if (pArray != NULL) {
    for (int i = 0; i < pArray->size; i++) {
      SXnodeJobObj *pJob = taosArrayGet(pArray, i);
      if (pJob == NULL) continue;
      mndReleaseXnodeJob(pMnode, pJob);
    }
  }
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropXnodeTaskReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SXnodeTaskObj     *pObj = NULL;
  SMDropXnodeTaskReq dropReq = {0};
  SJson             *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeTaskReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("DropXnodeTask with tid:%d, start to drop", dropReq.tid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_XNODE_TASK), NULL, _OVER);

  if (dropReq.tid <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeTask(pMnode, dropReq.tid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request to drop xnode task
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/%d", XNODED_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);
  if (pJson == NULL) {
    code = terrno;
    if (!dropReq.force) {
      goto _OVER;
    }
  }

  code = mndDropXnodeTask(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to drop since %s", dropReq.tid, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  tFreeSMDropXnodeTaskReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndStoreXnodeUserPass(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SXnodeUserPassObj upObj = {0};
  upObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_USER_PASS);

  upObj.userLen = pCreate->userLen;
  if (upObj.userLen > TSDB_USER_LEN) {
    code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    goto _OVER;
  }
  upObj.user = taosMemoryCalloc(1, pCreate->userLen);
  if (upObj.user == NULL) goto _OVER;
  (void)memcpy(upObj.user, pCreate->user, pCreate->userLen);

  upObj.passLen = pCreate->passLen;
  if (upObj.passLen > TSDB_USER_PASSWORD_LONGLEN) {
    code = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto _OVER;
  }
  upObj.pass = taosMemoryCalloc(1, pCreate->passLen);
  if (upObj.pass == NULL) goto _OVER;
  (void)memcpy(upObj.pass, pCreate->pass, pCreate->passLen);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mInfo("failed to create transaction for xnode:%s, code:0x%x:%s", pCreate->url, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode:%s as xnode:%d", pTrans->id, pCreate->url, upObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassRedoLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassCommitLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  if (upObj.user != NULL) {
    taosMemoryFree(upObj.user);
  }
  if (upObj.pass != NULL) {
    taosMemoryFree(upObj.pass);
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t httpCreateXnode(SXnodeObj *pObj) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *pContStr = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode", XNODED_URL);
  postContent = tjsonCreateObject();
  if (postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "id", (double)pObj->id), NULL, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "url", pObj->url), NULL, _OVER);
  pContStr = tjsonToString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (postContent != NULL) {
    tjsonDelete(postContent);
  }
  if (pContStr != NULL) {
    taosMemFree(pContStr);
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessCreateXnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj       *pObj = NULL;
  SMCreateXnodeReq createReq = {0};

  if ((code = grantCheck(TSDB_GRANT_XNODE)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mInfo("xnode:%s, start to create", createReq.url);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_XNODE), NULL, _OVER);

  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  }

  int32_t numOfRows = sdbGetSize(pMnode->pSdb, SDB_XNODE_USER_PASS);
  if (numOfRows <= 0) {
    if (strlen(createReq.user) == 0 || strlen(createReq.pass) == 0) {
      code = TSDB_CODE_MND_XNODE_NEED_USER_PASS;
      goto _OVER;
    }
    TAOS_CHECK_GOTO(checkPasswordFmt(createReq.pass), NULL, _OVER);
    // store user pass
    code = mndStoreXnodeUserPass(pMnode, pReq, &createReq);
    if (code != 0) goto _OVER;
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    mndStartXnoded(pMnode, createReq.userLen, createReq.user, createReq.passLen, createReq.pass);
  }

  code = mndCreateXnode(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  taosMsleep(100);
  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_XNODE_NOT_EXIST;
    goto _OVER;
  }
  // send request
  TAOS_CHECK_GOTO(httpCreateXnode(pObj), NULL, _OVER);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%s, failed to create since %s", createReq.url, tstrerror(code));
  }
  mndReleaseXnode(pMnode, pObj);
  tFreeSMCreateXnodeReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndUpdateXnode(SMnode *pMnode, SXnodeObj *pXnode, SRpcMsg *pReq) {
  mInfo("xnode:%d, start to update", pXnode->id);
  int32_t   code = -1;
  STrans   *pTrans = NULL;
  SXnodeObj xnodeObj = {0};
  xnodeObj.id = pXnode->id;
  xnodeObj.updateTime = taosGetTimestampMs();

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update xnode:%d", pTrans->id, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  mndFreeXnode(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndUpdateAllXnodes(SMnode *pMnode, SRpcMsg *pReq) {
  mInfo("update all xnodes");
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;
  int32_t rows = 0;
  int32_t numOfRows = sdbGetSize(pSdb, SDB_XNODE);

  void *pIter = NULL;
  while (1) {
    SXnodeObj *pObj = NULL;
    ESdbStatus objStatus = 0;
    pIter = sdbFetchAll(pSdb, SDB_XNODE, pIter, (void **)&pObj, &objStatus, true);
    if (pIter == NULL) break;

    rows++;
    void *transReq = NULL;
    if (rows == numOfRows) transReq = pReq;
    code = mndUpdateXnode(pMnode, pObj, transReq);
    sdbRelease(pSdb, pObj);

    if (code != 0) break;
  }

  if (code == 0 && rows == numOfRows) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  return code;
}

static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj       *pObj = NULL;
  SMUpdateXnodeReq updateReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_UPDATE_XNODE), NULL, _OVER);

  if (updateReq.xnodeId == -1) {
    code = mndUpdateAllXnodes(pMnode, pReq);
  } else {
    pObj = mndAcquireXnode(pMnode, updateReq.xnodeId);
    if (pObj == NULL) {
      code = TSDB_CODE_MND_XNODE_NOT_EXIST;
      goto _OVER;
    }
    code = mndUpdateXnode(pMnode, pObj, pReq);
    if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (updateReq.xnodeId != -1) {
      mError("xnode:%d, failed to update since %s", updateReq.xnodeId, tstrerror(code));
    }
  }

  mndReleaseXnode(pMnode, pObj);
  tFreeSMUpdateXnodeReq(&updateReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeRedoLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return code;
}

static int32_t mndSetDropXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeInfoToTrans(SMnode *pMnode, STrans *pTrans, SXnodeObj *pObj, bool force) {
  if (pObj == NULL) return 0;
  TAOS_CHECK_RETURN(mndSetDropXnodeRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropXnodeCommitLogs(pTrans, pObj));
  return 0;
}

static int32_t mndDropXnode(SMnode *pMnode, SRpcMsg *pReq, SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, to drop xnode:%d", pTrans->id, pObj->id);

  code = mndSetDropXnodeInfoToTrans(pMnode, pTrans, pObj, false);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndDrainXnode(SMnode *pMnode, SRpcMsg *pReq, SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = pObj->id;
  xnodeObj.status = "drain";
  xnodeObj.statusLen = strlen(xnodeObj.status);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drain-xnode");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, to drain xnode:%d", pTrans->id, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SXnodeObj     *pObj = NULL;
  SMDropXnodeReq dropReq = {0};
  SJson         *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("xnode:%d, start to drop", dropReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_XNODE), NULL, _OVER);

  if (dropReq.xnodeId <= 0 && (dropReq.url == NULL || strlen(dropReq.url) <= 0)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (dropReq.url != NULL && strlen(dropReq.url) > 0) {
    pObj = mndAcquireXnodeByURL(pMnode, dropReq.url);
    if (pObj == NULL) {
      code = terrno;
      goto _OVER;
    }
  } else {
    pObj = mndAcquireXnode(pMnode, dropReq.xnodeId);
    if (pObj == NULL) {
      code = terrno;
      goto _OVER;
    }
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/%d?force=%s", XNODED_URL, pObj->id,
           dropReq.force ? "true" : "false");
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);
  if (pJson == NULL) {
    code = terrno;
    if (!dropReq.force) {
      goto _OVER;
    }
  }

  code = mndDropXnode(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", dropReq.xnodeId, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  if (pObj != NULL) {
    mndReleaseXnode(pMnode, pObj);
  }
  tFreeSMDropXnodeReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessDrainXnodeReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SXnodeObj      *pObj = NULL;
  SMDrainXnodeReq drainReq = {0};
  SJson          *pJson = NULL;
  SJson          *postContent = NULL;
  char           *pContStr = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMDrainXnodeReq(pReq->pCont, pReq->contLen, &drainReq), NULL, _OVER);

  mInfo("xnode:%d, start to drain", drainReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DRAIN_XNODE), NULL, _OVER);

  if (drainReq.xnodeId <= 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnode(pMnode, drainReq.xnodeId);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_XNODE_NOT_EXIST;
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/drain/%d", XNODED_URL, pObj->id);
  postContent = tjsonCreateObject();
  if (postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "xnode", pObj->url), NULL, _OVER);
  pContStr = tjsonToString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }

  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = mndDrainXnode(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    goto _OVER;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drain since %s", drainReq.xnodeId, tstrerror(code));
  }

  if (postContent != NULL) {
    tjsonDelete(postContent);
  }
  if (pContStr != NULL) {
    taosMemoryFree(pContStr);
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  mndReleaseXnode(pMnode, pObj);
  tFreeSMDrainXnodeReq(&drainReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SXnodeObj *pObj = NULL;
  char       buf[TSDB_XNODE_URL_LEN + VARSTR_HEADER_SIZE] = {0};
  char       status[TSDB_XNODE_STATUS_LEN] = {0};
  int32_t    code = 0;
  mDebug("show.type:%d, %s:%d: retrieve xnodes with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    if (code != 0) goto _end;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->url, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    if (mndGetXnodeStatus(pObj, status, TSDB_XNODE_STATUS_LEN) == 0) {
      STR_TO_VARSTR(buf, status);
    } else {
      mDebug("xnode:%d, status request err: %s", pObj->id, tstrerror(terrno));
      STR_TO_VARSTR(buf, "offline");
    }
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
    if (code != 0) goto _end;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createTime, false);
    if (code != 0) goto _end;

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->updateTime, false);
    if (code != 0) goto _end;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextXnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE);
}

static int32_t mndRetrieveXnodeTasks(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  int32_t        numOfRows = 0;
  int32_t        cols = 0;
  SXnodeTaskObj *pObj = NULL;
  char           buf[VARSTR_HEADER_SIZE +
           TMAX(TSDB_XNODE_TASK_NAME_LEN,
                          TMAX(TSDB_XNODE_TASK_SOURCE_LEN, TMAX(TSDB_XNODE_TASK_SINK_LEN, TSDB_XNODE_TASK_PARSER_LEN)))];
  int32_t        code = 0;
  mDebug("show.type:%d, %s:%d: retrieve xnode tasks with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_TASK, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    // id
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    if (code != 0) goto _end;

    // name
    buf[0] = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->name, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    // from
    buf[0] = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->sourceDsn, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    // to
    buf[0] = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->sinkDsn, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    // parser
    if (pObj->parserLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->parser, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // via
    if (pObj->via != 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->via, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // xnode_id
    if (pObj->xnodeId != 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->xnodeId, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // status
    if (pObj->statusLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->status, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // reason
    if (pObj->reasonLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->reason, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // create_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createTime, false);
    if (code != 0) goto _end;

    // update_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->updateTime, false);
    if (code != 0) goto _end;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextXnodeTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE_TASK);
}

static void mndFreeXnodeJob(SXnodeJobObj *pObj) {
  if (NULL != pObj->config) {
    taosMemoryFreeClear(pObj->config);
  }
  if (NULL != pObj->reason) {
    taosMemoryFreeClear(pObj->reason);
  }
  if (NULL != pObj->status) {
    taosMemoryFreeClear(pObj->status);
  }
}

static SSdbRaw *mndXnodeJobActionEncode(SXnodeJobObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  mDebug("xnode tid:%d, jid:%d, start to encode to raw, row:%p", pObj->taskId, pObj->id, pObj);

  int32_t rawDataLen = sizeof(SXnodeJobObj) + TSDB_XNODE_RESERVE_SIZE + pObj->configLen + pObj->reasonLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_JOB, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->taskId, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->configLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->config, pObj->configLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->via, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->xnodeId, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->statusLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->reasonLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->reason, pObj->reasonLen, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode tid:%d, jid:%d, failed to encode to raw:%p since %s", pObj->taskId, pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode tid:%d, jid:%d, encode to raw:%p, row:%p", pObj->taskId, pObj->id, pRaw, pObj);
  return pRaw;
}

static SSdbRow *mndXnodeJobActionDecode(SSdbRaw *pRaw) {
  mInfo("xnode, start to decode from raw:%p", pRaw);
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow      *pRow = NULL;
  SXnodeJobObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeJobObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->taskId, _OVER)

  SDB_GET_INT32(pRaw, dataPos, &pObj->configLen, _OVER)
  if (pObj->configLen > 0) {
    pObj->config = taosMemoryCalloc(pObj->configLen, 1);
    if (pObj->config == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->config, pObj->configLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->via, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->xnodeId, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->statusLen, _OVER)
  if (pObj->statusLen > 0) {
    pObj->status = taosMemoryCalloc(pObj->statusLen, 1);
    if (pObj->status == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->reasonLen, _OVER)
  if (pObj->reasonLen > 0) {
    pObj->reason = taosMemoryCalloc(pObj->reasonLen, 1);
    if (pObj->reason == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->reason, pObj->reasonLen, _OVER)
  }

  SDB_GET_INT64(pRaw, dataPos, &pObj->createTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode tid:%d, jid:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->taskId,
           pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->config);
      taosMemoryFreeClear(pObj->reason);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

static int32_t mndXnodeJobActionInsert(SSdb *pSdb, SXnodeJobObj *pObj) {
  mInfo("xnode tid:%d, jid:%d, perform insert action, row:%p", pObj->taskId, pObj->id, pObj);
  return 0;
}

static int32_t mndXnodeJobActionDelete(SSdb *pSdb, SXnodeJobObj *pObj) {
  mDebug("xnode tid:%d, jid:%d, perform delete action, row:%p", pObj->taskId, pObj->id, pObj);
  mndFreeXnodeJob(pObj);
  return 0;
}

static int32_t mndXnodeJobActionUpdate(SSdb *pSdb, SXnodeJobObj *pOld, SXnodeJobObj *pNew) {
  mDebug("xnode tid:%d, jid:%d, perform update action, old row:%p new row:%p", pOld->taskId, pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  pOld->via = pNew->via;
  pOld->xnodeId = pNew->xnodeId;
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  swapFields(&pNew->configLen, &pNew->config, &pOld->configLen, &pOld->config);
  swapFields(&pNew->reasonLen, &pNew->reason, &pOld->reasonLen, &pOld->reason);
  pOld->updateTime = pNew->updateTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

/* xnode user pass actions */
static SSdbRaw *mndXnodeUserPassActionEncode(SXnodeUserPassObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(SXnodeUserPassObj) + TSDB_XNODE_RESERVE_SIZE + pObj->userLen + pObj->passLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_USER_PASS, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->userLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->user, pObj->userLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->passLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->pass, pObj->passLen, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode user pass:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode user pass:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}
static SSdbRow *mndXnodeUserPassActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow           *pRow = NULL;
  SXnodeUserPassObj *pObj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeUserPassObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->userLen, _OVER)
  if (pObj->userLen > 0) {
    pObj->user = taosMemoryCalloc(pObj->userLen, 1);
    if (pObj->user == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->user, pObj->userLen, _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->passLen, _OVER)
  if (pObj->passLen > 0) {
    pObj->pass = taosMemoryCalloc(pObj->passLen, 1);
    if (pObj->pass == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->pass, pObj->passLen, _OVER)
  }
  SDB_GET_INT64(pRaw, dataPos, &pObj->createTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode user pass:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      if (pObj->user != NULL) {
        taosMemoryFreeClear(pObj->user);
      }
      if (pObj->pass != NULL) {
        taosMemoryFreeClear(pObj->pass);
      }
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode user pass:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}
static int32_t mndXnodeUserPassActionInsert(SSdb *pSdb, SXnodeUserPassObj *pObj) {
  mDebug("xnode user pass:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}
static int32_t mndXnodeUserPassActionUpdate(SSdb *pSdb, SXnodeUserPassObj *pOld, SXnodeUserPassObj *pNew) {
  mDebug("xnode user pass:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}
static int32_t mndXnodeUserPassActionDelete(SSdb *pSdb, SXnodeUserPassObj *pObj) {
  mDebug("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  if (pObj->user != NULL) {
    taosMemoryFreeClear(pObj->user);
  }
  if (pObj->pass != NULL) {
    taosMemoryFreeClear(pObj->pass);
  }
  return 0;
}

static int32_t mndSetCreateXnodeJobRedoLogs(STrans *pTrans, SXnodeJobObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeJobActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeJobUndoLogs(STrans *pTrans, SXnodeJobObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeJobActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeJobCommitLogs(STrans *pTrans, SXnodeJobObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeJobActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

static int32_t mndCreateXnodeJob(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeJobReq *pCreate) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  SXnodeJobObj jobObj = {0};
  jobObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_JOB);
  jobObj.taskId = pCreate->tid;

  jobObj.configLen = pCreate->configLen;
  if (jobObj.configLen > TSDB_XNODE_TASK_JOB_CONFIG_LEN) {
    code = TSDB_CODE_MND_XNODE_TASK_JOB_CONFIG_TOO_LONG;
    goto _OVER;
  }
  jobObj.config = taosMemoryCalloc(1, pCreate->configLen);
  if (jobObj.config == NULL) goto _OVER;
  (void)memcpy(jobObj.config, pCreate->config, pCreate->configLen);

  jobObj.via = pCreate->via;
  jobObj.xnodeId = pCreate->xnodeId;

  jobObj.statusLen = pCreate->status.len;
  if (pCreate->status.len > 0) {
    jobObj.status = taosMemoryCalloc(1, jobObj.statusLen);
    if (jobObj.status == NULL) goto _OVER;
    (void)memmove(jobObj.status, pCreate->status.ptr, jobObj.statusLen);
  }

  jobObj.reasonLen = pCreate->reasonLen;
  if (jobObj.reasonLen > TSDB_XNODE_TASK_REASON_LEN) {
    code = TSDB_CODE_MND_XNODE_TASK_REASON_TOO_LONG;
    goto _OVER;
  }
  if (jobObj.reasonLen > 0) {
    jobObj.reason = taosMemoryCalloc(1, pCreate->reasonLen);
    if (jobObj.reason == NULL) goto _OVER;
    (void)memcpy(jobObj.reason, pCreate->reason, pCreate->reasonLen);
  }

  jobObj.createTime = taosGetTimestampMs();
  jobObj.updateTime = jobObj.createTime;

  mDebug("create xnode job, id:%d, tid:%d, config:%s, time:%ld", jobObj.id, jobObj.taskId, jobObj.config,
         jobObj.createTime);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode-job");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mInfo("failed to create transaction for xnode-job:%d, code:0x%x:%s", pCreate->tid, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode job on %d as jid:%d", pTrans->id, pCreate->tid, jobObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeJobRedoLogs(pTrans, &jobObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeJobUndoLogs(pTrans, &jobObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeJobCommitLogs(pTrans, &jobObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndFreeXnodeJob(&jobObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndUpdateXnodeJob(SMnode *pMnode, SRpcMsg *pReq, SXnodeJobObj *pOld, SMUpdateXnodeJobReq *pUpdate) {
  mInfo("xnode job:%d, start to update", pUpdate->jid);
  int32_t      code = -1;
  STrans      *pTrans = NULL;
  SXnodeJobObj jobObj = *pOld;
  struct {
    bool status;
    bool config;
    bool reason;
  } isChange = {0};

  jobObj.id = pUpdate->jid;
  if (pUpdate->via > 0) {
    jobObj.via = pUpdate->via;
  }
  if (pUpdate->xnodeId > 0) {
    jobObj.xnodeId = pUpdate->xnodeId;
  }
  if (pUpdate->status.len > 0) {
    jobObj.statusLen = pUpdate->status.len;
    jobObj.status = taosMemoryCalloc(1, jobObj.statusLen);
    if (jobObj.status == NULL) goto _OVER;
    (void)memcpy(jobObj.status, pUpdate->status.ptr, jobObj.statusLen);
    isChange.status = true;
  }
  if (pUpdate->configLen > 0) {
    jobObj.configLen = pUpdate->configLen;
    jobObj.config = taosMemoryCalloc(1, pUpdate->configLen);
    if (jobObj.config == NULL) goto _OVER;
    (void)memcpy(jobObj.config, pUpdate->config, pUpdate->configLen);
    isChange.config = true;
  }
  if (pUpdate->reasonLen > 0) {
    jobObj.reasonLen = pUpdate->reasonLen;
    jobObj.reason = taosMemoryCalloc(1, pUpdate->reasonLen);
    if (jobObj.reason == NULL) goto _OVER;
    (void)memcpy(jobObj.reason, pUpdate->reason, pUpdate->reasonLen);
    isChange.reason = true;
  }
  jobObj.updateTime = taosGetTimestampMs();

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update xnode job:%d", pTrans->id, jobObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeJobCommitLogs(pTrans, &jobObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  if (NULL != jobObj.status && isChange.status) {
    taosMemoryFree(jobObj.status);
  }
  if (NULL != jobObj.config && isChange.config) {
    taosMemoryFree(jobObj.config);
  }
  if (NULL != jobObj.reason && isChange.reason) {
    taosMemoryFree(jobObj.reason);
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

void mndReleaseXnodeTaskJob(SMnode *pMnode, SXnodeJobObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

SXnodeJobObj *mndAcquireXnodeJob(SMnode *pMnode, int32_t jid) {
  SXnodeJobObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE_JOB, &jid);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_JOB_NOT_EXIST;
  }
  return pObj;
}
void mndReleaseXnodeJob(SMnode *pMnode, SXnodeJobObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static int32_t mndSetDropXnodeJobRedoLogs(STrans *pTrans, SXnodeJobObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeJobActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return code;
}

static int32_t mndSetDropXnodeJobCommitLogs(STrans *pTrans, SXnodeJobObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeJobActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}
static int32_t mndSetDropXnodeJobInfoToTrans(STrans *pTrans, SXnodeJobObj *pObj, bool force) {
  if (pObj == NULL) {
    return 0;
  }
  TAOS_CHECK_RETURN(mndSetDropXnodeJobRedoLogs(pTrans, pObj));
  TAOS_CHECK_RETURN(mndSetDropXnodeJobCommitLogs(pTrans, pObj));
  return 0;
}

static int32_t mndDropXnodeJob(SMnode *pMnode, SRpcMsg *pReq, SXnodeJobObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode-job");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mInfo("trans:%d, to drop xnode:%d", pTrans->id, pObj->id);

  code = mndSetDropXnodeJobInfoToTrans(pTrans, pObj, false);
  mndReleaseXnodeJob(pMnode, pObj);

  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  return code;
}
static int32_t mndProcessCreateXnodeJobReq(SRpcMsg *pReq) {
  mInfo("create xnode job req, content len:%d", pReq->contLen);
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SMCreateXnodeJobReq createReq = {0};

  if ((code = grantCheck(TSDB_GRANT_XNODE)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeJobReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mDebug("xnode create job on xnode:%d", createReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_XNODE_JOB), NULL, _OVER);

  code = mndCreateXnodeJob(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task job on task id:%d, failed to create since %s", createReq.tid, tstrerror(code));
  }

  tFreeSMCreateXnodeJobReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeJobReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeJobObj       *pObj = NULL;
  SMUpdateXnodeJobReq updateReq = {0};

  if ((code = grantCheck(TSDB_GRANT_TD_GPT)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeJobReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);

  pObj = mndAcquireXnodeJob(pMnode, updateReq.jid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = mndUpdateXnodeJob(pMnode, pReq, pObj, &updateReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task job on jid:%d, failed to update since %s", updateReq.jid, tstrerror(code));
  }

  mndReleaseXnodeJob(pMnode, pObj);
  tFreeSMUpdateXnodeJobReq(&updateReq);
  TAOS_RETURN(code);

  return 0;
}

static int32_t mndProcessRebalanceXnodeJobReq(SRpcMsg *pReq) {
  SMnode                *pMnode = pReq->info.node;
  int32_t                code = -1;
  SXnodeJobObj          *pObj = NULL;
  SMRebalanceXnodeJobReq rebalanceReq = {0};
  SJson                 *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMRebalanceXnodeJobReq(pReq->pCont, pReq->contLen, &rebalanceReq), NULL, _OVER);

  mInfo("RebalanceXnodeJob with jid:%d, xnode_id:%d, start to rebalance", rebalanceReq.jid, rebalanceReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_REBALANCE_XNODE_JOB), NULL, _OVER);

  if (rebalanceReq.jid <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeJob(pMnode, rebalanceReq.jid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/rebalance/manual/%d/%d/%d", XNODED_URL, pObj->taskId, pObj->id,
           rebalanceReq.xnodeId);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, NULL, 0);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", rebalanceReq.jid, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  mndReleaseXnodeJob(pMnode, pObj);
  tFreeSMRebalanceXnodeJobReq(&rebalanceReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeJobReq(SRpcMsg *pReq) {
  SMnode           *pMnode = pReq->info.node;
  int32_t           code = -1;
  SXnodeJobObj     *pObj = NULL;
  SMDropXnodeJobReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeJobReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("DropXnodeJob with jid:%d, tid:%d, start to drop", dropReq.jid, dropReq.tid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_XNODE_JOB), NULL, _OVER);

  if (dropReq.jid <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeJob(pMnode, dropReq.jid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = mndDropXnodeJob(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", dropReq.jid, tstrerror(code));
  }

  mndReleaseXnodeJob(pMnode, pObj);
  tFreeSMDropXnodeJobReq(&dropReq);
  TAOS_RETURN(code);
}

/**
 * @brief Mapping the columns of show xnode jobs
 *
 * See [xnodeTaskJobSchema] in systable.h.
 *
 *  {.name = "jid", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "tid", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "config", .bytes = TSDB_XNODE_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo =
 false},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    // {.name = "reason", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "update_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
 * @param pReq
 * @param pShow
 * @param pBlock
 * @param rows
 * @return int32_t
 */
static int32_t mndRetrieveXnodeJobs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  int32_t       numOfRows = 0;
  int32_t       cols = 0;
  SXnodeJobObj *pObj = NULL;
  char          buf[VARSTR_HEADER_SIZE + TMAX(TSDB_XNODE_TASK_JOB_CONFIG_LEN, TSDB_XNODE_TASK_REASON_LEN)];
  char          status[64] = {0};
  int32_t       code = 0;
  mInfo("show.type:%d, %s:%d: retrieve xnode jobs with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_JOB, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    // id
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    if (code != 0) goto _end;
    // tid
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->taskId, false);
    if (code != 0) goto _end;

    // config
    buf[0] = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->config, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
    if (code != 0) goto _end;

    // via
    if (pObj->via != 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->via, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // xnode_id
    if (pObj->xnodeId != 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->xnodeId, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // status
    if (pObj->statusLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->status, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // reason
    if (pObj->reasonLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->reason, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // create_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->createTime, false);
    if (code != 0) goto _end;

    // update_time
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->updateTime, false);
    if (code != 0) goto _end;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}
static void mndCancelGetNextXnodeJob(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE_JOB);
}

static int32_t mndRetrieveXnodeAgents(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SXnodeObj *pObj = NULL;
  char       buf[TSDB_ANALYTIC_ALGO_NAME_LEN + VARSTR_HEADER_SIZE];
  int32_t    code = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_AGENT, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    // for (int32_t t = 0; t < pObj->numOfAlgos; ++t) {
    // SArray *algos = pObj->algos[t];

    // for (int32_t a = 0; a < taosArrayGetSize(algos); ++a) {
    //   SXnodeAlgo *algo = taosArrayGet(algos, a);

    //   cols = 0;
    //   SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    //   code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    //   if (code != 0) goto _end;

    //   STR_TO_VARSTR(buf, taosAnalysisAlgoType(t));
    //   pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    //   code = colDataSetVal(pColInfo, numOfRows, buf, false);
    //   if (code != 0) goto _end;

    //   STR_TO_VARSTR(buf, algo->name);
    //   pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    //   code = colDataSetVal(pColInfo, numOfRows, buf, false);
    //   if (code != 0) goto _end;

    //   numOfRows++;
    // }
    // }

    sdbRelease(pSdb, pObj);
  }

_end:
  if (code != 0) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextXnodeAgent(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE_AGENT);
}

SJson *taosGetTasks(const char *url) {
  // This function should send an HTTP request to the given URL and return the JSON response.
  // The implementation is not provided here as it is not part of the original code.
  return NULL;  // Placeholder
}

static size_t taosCurlWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SCurlResp *pRsp = userdata;
  if (contLen == 0 || nmemb == 0 || pCont == NULL) {
    pRsp->dataLen = 0;
    pRsp->data = NULL;
    uError("curl response is received, len:%" PRId64, pRsp->dataLen);
    return 0;
  }

  int64_t newDataSize = (int64_t)contLen * nmemb;
  int64_t size = pRsp->dataLen + newDataSize;

  if (pRsp->data == NULL) {
    pRsp->data = taosMemoryMalloc(size + 1);
    if (pRsp->data == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }
  } else {
    char *p = taosMemoryRealloc(pRsp->data, size + 1);
    if (p == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }

    pRsp->data = p;
  }

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data + pRsp->dataLen, pCont, newDataSize);

    pRsp->dataLen = size;
    pRsp->data[size] = 0;

    uDebugL("curl response is received, len:%" PRId64 ", content:%s", size, pRsp->data);
    return newDataSize;
  } else {
    pRsp->dataLen = 0;
    uError("failed to malloc curl response");
    return 0;
  }
}

static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp, int32_t timeout) {
  CURL    *curl = NULL;
  int32_t  retCode = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout) != 0) goto _OVER;

  uDebug("curl get request will sent, url:%s", url);
  CURLcode curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      mError("xnode failed to perform curl action, code:%d", curlCode);
      retCode = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    uError("failed to perform curl action, code:%d", curlCode);
    retCode = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code != 200) {
    retCode = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return retCode;
}

static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen, int32_t timeout) {
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  int32_t            retCode = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    mError("xnode failed to create curl handle");
    return -1;
  }

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  if (curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POST, 1) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L) != 0) goto _OVER;

  mDebug("xnode curl post request will sent, url:%s len:%d content:%s", url, bufLen, buf);
  CURLcode curlCode = curl_easy_perform(curl);

  if (curlCode != CURLE_OK) {
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      mError("xnode failed to perform curl action, code:%d", curlCode);
      retCode = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    mError("xnode failed to perform curl action, code:%d", curlCode);
    retCode = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code != 200) {
    mError("xnode failed to perform curl action, http code:%ld", http_code);
    retCode = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return retCode;
}

static int32_t taosCurlDeleteRequest(const char *url, SCurlResp *pRsp, int32_t timeout) {
  CURL   *curl = NULL;
  int32_t retCode = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE") != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout) != 0) goto _OVER;

  uDebug("curl get request will sent, url:%s", url);
  CURLcode curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      mError("xnode failed to perform curl action, code:%d", curlCode);
      retCode = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    uError("failed to perform curl action, code:%d", curlCode);
    retCode = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code != 200 && http_code != 204) {
    retCode = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return retCode;
}

SJson *mndSendReqRetJson(const char *url, EHttpType type, int64_t timeout, const char *buf, int64_t bufLen) {
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};

  if (type == HTTP_TYPE_GET) {
    if ((terrno = taosCurlGetRequest(url, &curlRsp, timeout)) != 0) {
      goto _OVER;
    }
  } else if (type == HTTP_TYPE_POST) {
    if ((terrno = taosCurlPostRequest(url, &curlRsp, buf, bufLen, timeout)) != 0) {
      goto _OVER;
    }
  } else if (type == HTTP_TYPE_DELETE) {
    if ((terrno = taosCurlDeleteRequest(url, &curlRsp, timeout)) != 0) {
      goto _OVER;
    }
  } else {
    uError("invalid http type:%d", type);
    terrno = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
    pJson = tjsonCreateObject();
    goto _OVER;
  }

  pJson = tjsonParse(curlRsp.data);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

_OVER:
  if (curlRsp.data != NULL) taosMemoryFreeClear(curlRsp.data);
  return pJson;
}

static int32_t mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen) {
  int32_t code = 0;
  SJson  *pJson = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/%d", XNODED_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_GET, defaultTimeout, NULL, 0);
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  code = tjsonGetStringValue2(pJson, "status", status, statusLen);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  if (strlen(status) == 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  TAOS_RETURN(code);
}

static int32_t mndGetXnodeTaskStatus(SXnodeTaskObj *pObj, char *status, int32_t statusLen) {
  int32_t code = 0;
  int32_t protocol = 0;
  double  tmp = 0;
  char    xnodeUrl[TSDB_XNODE_TASK_NAME_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_TASK_NAME_LEN, "%s/%s", pObj->name, "status");

  SJson *pJson = NULL;  // taosAnalySendReqRetJson(xnodeUrl, ANALYTICS_HTTP_TYPE_GET, NULL, 0);
  if (pJson == NULL) return terrno;

  code = tjsonGetDoubleValue(pJson, "protocol", &tmp);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  protocol = (int32_t)(tmp * 1000);
  if (protocol != 100 && protocol != 1000) {
    code = TSDB_CODE_MND_XNODE_INVALID_PROTOCOL;
    goto _OVER;
  }

  code = tjsonGetStringValue2(pJson, "status", status, statusLen);
  if (code < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }
  if (strlen(status) == 0) {
    code = TSDB_CODE_MND_XNODE_INVALID_PROTOCOL;
    goto _OVER;
  }

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  TAOS_RETURN(code);
}

// mgmt xnode
void mndStartXnoded(SMnode *pMnode, int32_t userLen, char *user, int32_t passLen, char *pass) {
  int32_t   code = 0;
  SXnodeOpt pOption = {0};

  pOption.dnodeId = pMnode->selfDnodeId;
  pOption.clusterId = pMnode->clusterId;

  SEpSet epset = mndGetDnodeEpsetById(pMnode, pMnode->selfDnodeId);
  if (epset.numOfEps == 0) {
    mError("xnode failed to start xnoded, dnode:%d", pMnode->selfDnodeId);
    return;
  }
  pOption.ep = epset.eps[0];
  // add user password
  pOption.upLen = userLen + passLen;
  snprintf(pOption.userPass, XNODE_USER_PASS_LEN, "%s:%s", user, pass);

  if ((code = mndOpenXnd(&pOption)) != 0) {
    mError("xnode failed to open xnd since %s, dnodeId:%d", tstrerror(code), pOption.dnodeId);
    return;
  }
}

void mndXnodeHandleBecomeLeader(SMnode *pMnode) {
  mInfo("mndxnode start to process mnode become leader");
  SXnodeUserPassObj *pObj = mndAcquireFirstXnodeUserPass(pMnode);
  if (pObj == NULL) return;

  mndStartXnoded(pMnode, pObj->userLen, pObj->user, pObj->passLen, pObj->pass);
}

void mndXnodeHandleBecomeNotLeader() {
  mInfo("mndxnode handle mnode become not leader");
  mndCloseXnd();
}
