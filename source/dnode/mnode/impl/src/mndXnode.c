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
#include "types.h"
#ifndef WINDOWS
#include <curl/curl.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#endif
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

#define TSDB_XNODE_RESERVE_SIZE 64
#define XNODED_PIPE_SOCKET_URL "http://localhost"
typedef enum {
  HTTP_TYPE_GET = 0,
  HTTP_TYPE_POST,
  HTTP_TYPE_DELETE,
} EHttpType;
typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

const int32_t defaultTimeout = 1000;

/** xnodes systable actions */
SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj);
SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw);
int32_t  mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj);
int32_t  mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew);
int32_t  mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj);

/** @section xnodes request handlers */
static int32_t mndProcessCreateXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq);
static int32_t mndProcessDrainXnodeReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodes(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnode(SMnode *pMnode, void *pIter);

/** @section xnode task handlers */
SSdbRaw *mndXnodeTaskActionEncode(SXnodeTaskObj *pObj);
SSdbRow *mndXnodeTaskActionDecode(SSdbRaw *pRaw);
int32_t  mndXnodeTaskActionInsert(SSdb *pSdb, SXnodeTaskObj *pObj);
int32_t  mndXnodeTaskActionUpdate(SSdb *pSdb, SXnodeTaskObj *pOld, SXnodeTaskObj *pNew);
int32_t  mndXnodeTaskActionDelete(SSdb *pSdb, SXnodeTaskObj *pObj);

static int32_t mndProcessCreateXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessStartXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessStopXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeTaskReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodeTasks(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeTask(SMnode *pMnode, void *pIter);

/** @section xnode task job handlers */
SSdbRaw *mndXnodeJobActionEncode(SXnodeJobObj *pObj);
SSdbRow *mndXnodeJobActionDecode(SSdbRaw *pRaw);
int32_t  mndXnodeJobActionInsert(SSdb *pSdb, SXnodeJobObj *pObj);
int32_t  mndXnodeJobActionUpdate(SSdb *pSdb, SXnodeJobObj *pOld, SXnodeJobObj *pNew);
int32_t  mndXnodeJobActionDelete(SSdb *pSdb, SXnodeJobObj *pObj);

static int32_t mndProcessCreateXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessRebalanceXnodeJobReq(SRpcMsg *pReq);
static int32_t mndProcessRebalanceXnodeJobsWhereReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeJobReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodeJobs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeJob(SMnode *pMnode, void *pIter);

/** @section xnode user pass handlers */
SSdbRaw *mndXnodeUserPassActionEncode(SXnodeUserPassObj *pObj);
SSdbRow *mndXnodeUserPassActionDecode(SSdbRaw *pRaw);
int32_t  mndXnodeUserPassActionInsert(SSdb *pSdb, SXnodeUserPassObj *pObj);
int32_t  mndXnodeUserPassActionUpdate(SSdb *pSdb, SXnodeUserPassObj *pOld, SXnodeUserPassObj *pNew);
int32_t  mndXnodeUserPassActionDelete(SSdb *pSdb, SXnodeUserPassObj *pObj);

/** @section xnode agent handlers */
SSdbRaw *mndXnodeAgentActionEncode(SXnodeAgentObj *pObj);
SSdbRow *mndXnodeAgentActionDecode(SSdbRaw *pRaw);
int32_t  mndXnodeAgentActionInsert(SSdb *pSdb, SXnodeAgentObj *pObj);
int32_t  mndXnodeAgentActionUpdate(SSdb *pSdb, SXnodeAgentObj *pOld, SXnodeAgentObj *pNew);
int32_t  mndXnodeAgentActionDelete(SSdb *pSdb, SXnodeAgentObj *pObj);

static int32_t mndProcessCreateXnodeAgentReq(SRpcMsg *pReq);
static int32_t mndProcessUpdateXnodeAgentReq(SRpcMsg *pReq);
static int32_t mndProcessDropXnodeAgentReq(SRpcMsg *pReq);
static int32_t mndRetrieveXnodeAgents(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextXnodeAgent(SMnode *pMnode, void *pIter);

/** @section xnoded mgmt */
void mndStartXnoded(SMnode *pMnode, const char *user, const char *pass, const char *token);
void mndRestartXnoded(SMnode *pMnode);

/** @section others */
static int32_t mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen);
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

  SSdbTable agents = {
      .sdbType = SDB_XNODE_AGENT,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndXnodeAgentActionEncode,
      .decodeFp = (SdbDecodeFp)mndXnodeAgentActionDecode,
      .insertFp = (SdbInsertFp)mndXnodeAgentActionInsert,
      .updateFp = (SdbUpdateFp)mndXnodeAgentActionUpdate,
      .deleteFp = (SdbDeleteFp)mndXnodeAgentActionDelete,
  };

  code = sdbSetTable(pMnode->pSdb, agents);
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
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_TASKS, mndRetrieveXnodeTasks);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_TASKS, mndCancelGetNextXnodeTask);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE_JOB, mndProcessCreateXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE_JOB, mndProcessUpdateXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_REBALANCE_XNODE_JOB, mndProcessRebalanceXnodeJobReq);
  mndSetMsgHandle(pMnode, TDMT_MND_REBALANCE_XNODE_JOBS_WHERE, mndProcessRebalanceXnodeJobsWhereReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE_JOB, mndProcessDropXnodeJobReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_JOBS, mndRetrieveXnodeJobs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_JOBS, mndCancelGetNextXnodeJob);

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XNODE_AGENT, mndProcessCreateXnodeAgentReq);
  mndSetMsgHandle(pMnode, TDMT_MND_UPDATE_XNODE_AGENT, mndProcessUpdateXnodeAgentReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_XNODE_AGENT, mndProcessDropXnodeAgentReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_XNODE_AGENTS, mndRetrieveXnodeAgents);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_XNODE_AGENTS, mndCancelGetNextXnodeAgent);

  return 0;
}

/** tools section **/

int32_t xnodeCheckPasswordFmt(const char *pwd) {
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

static void swapFields(int32_t *newLen, char **ppNewStr, int32_t *oldLen, char **ppOldStr) {
  if (*newLen > 0) {
    int32_t tempLen = *newLen;
    *newLen = *oldLen;
    *oldLen = tempLen;

    char *tempStr = *ppNewStr;
    *ppNewStr = *ppOldStr;
    *ppOldStr = tempStr;
  }
}

/** xnode section **/

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

SSdbRaw *mndXnodeActionEncode(SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t rawDataLen = sizeof(SXnodeObj) + TSDB_XNODE_RESERVE_SIZE + pObj->urlLen + pObj->statusLen;

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

SSdbRow *mndXnodeActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SXnodeObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

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
    pObj->url = taosMemoryCalloc(1, pObj->urlLen + 1);
    if (pObj->url == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->url, pObj->urlLen, _OVER)
  } else {
    pObj->url = NULL;
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->statusLen, _OVER)
  if (pObj->statusLen > 0) {
    pObj->status = taosMemoryCalloc(1, pObj->statusLen + 1);
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
  }
  if (pObj->status != NULL) {
    taosMemoryFreeClear(pObj->status);
  }
}

int32_t mndXnodeActionInsert(SSdb *pSdb, SXnodeObj *pObj) {
  mDebug("xnode:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

int32_t mndXnodeActionDelete(SSdb *pSdb, SXnodeObj *pObj) {
  mDebug("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnode(pObj);
  return 0;
}

int32_t mndXnodeActionUpdate(SSdb *pSdb, SXnodeObj *pOld, SXnodeObj *pNew) {
  mDebug("xnode:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

void mndReleaseXnodeUserPass(SMnode *pMnode, SXnodeUserPassObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
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
  mInfo("create xnode, xnode.id:%d, xnode.url: %s, xnode.time:%" PRId64, xnodeObj.id, xnodeObj.url,
        xnodeObj.createTime);

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

static int32_t mndStoreXnodeUserPassToken(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeReq *pCreate) {
  int32_t code = 0;
  STrans *pTrans = NULL;

  SXnodeUserPassObj upObj = {0};
  upObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_USER_PASS);

  if (pCreate->user) {
    upObj.userLen = pCreate->userLen;
    if (upObj.userLen > TSDB_USER_LEN) {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
      goto _OVER;
    }
    upObj.user = taosMemoryCalloc(1, pCreate->userLen);
    if (upObj.user == NULL) goto _OVER;
    (void)memcpy(upObj.user, pCreate->user, pCreate->userLen);
  }
  if (pCreate->pass) {
    upObj.passLen = pCreate->passLen;
    if (upObj.passLen > TSDB_USER_PASSWORD_LONGLEN) {
      code = TSDB_CODE_MND_INVALID_PASS_FORMAT;
      goto _OVER;
    }
    upObj.pass = taosMemoryCalloc(1, pCreate->passLen);
    if (upObj.pass == NULL) goto _OVER;
    (void)memcpy(upObj.pass, pCreate->pass, pCreate->passLen);
  }

  if (pCreate->token.ptr) {
    upObj.tokenLen = pCreate->token.len + 1;
    upObj.token = taosMemoryCalloc(1, upObj.tokenLen);
    if (upObj.token == NULL) goto _OVER;
    (void)memcpy(upObj.token, pCreate->token.ptr, pCreate->token.len);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-userpass");
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

_OVER:
  taosMemoryFreeClear(upObj.user);
  taosMemoryFreeClear(upObj.pass);
  taosMemoryFreeClear(upObj.token);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

#ifndef TD_ENTERPRISE

int32_t mndXnodeCreateDefaultToken(SRpcMsg* pReq, char** ppToken) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

#endif

static int32_t httpCreateXnode(SXnodeObj *pObj) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *pContStr = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode", XNODED_PIPE_SOCKET_URL);
  postContent = tjsonCreateObject();
  if (postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "id", (double)pObj->id), NULL, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "url", pObj->url), NULL, _OVER);
  pContStr = tjsonToUnformattedString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));

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
  int32_t          code = 0, lino = 0;
  SXnodeObj       *pObj = NULL;
  SMCreateXnodeReq createReq = {0};
  char            *pToken = NULL;

  if ((code = grantCheck(TSDB_GRANT_XNODE)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeReq(pReq->pCont, pReq->contLen, &createReq), &lino, _OVER);

  mDebug("xnode:%s, start to create", createReq.url);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_CREATE_XNODE), &lino, _OVER);

  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  }

  int32_t numOfRows = sdbGetSize(pMnode->pSdb, SDB_XNODE_USER_PASS);
  if (numOfRows <= 0) {
    if (createReq.token.ptr != NULL) {
      code = mndStoreXnodeUserPassToken(pMnode, pReq, &createReq);
      if (code != 0) goto _OVER;
      mndStartXnoded(pMnode, NULL, NULL, createReq.token.ptr);
    } else if (createReq.user != NULL) {
      TAOS_CHECK_GOTO(xnodeCheckPasswordFmt(createReq.pass), &lino, _OVER);
      // store user pass
      code = mndStoreXnodeUserPassToken(pMnode, pReq, &createReq);
      if (code != 0) goto _OVER;
      mndStartXnoded(pMnode, createReq.user, createReq.pass, NULL);
    } else {
      TAOS_CHECK_GOTO(mndXnodeCreateDefaultToken(pReq, &pToken), &lino, _OVER);
      createReq.token = xCreateCowStr(strlen(pToken), pToken, false);
      code = mndStoreXnodeUserPassToken(pMnode, pReq, &createReq);
      if (code != 0) goto _OVER;
      mndStartXnoded(pMnode, NULL, NULL, createReq.token.ptr);
    }
    taosMsleep(defaultTimeout);
  }

  TAOS_CHECK_GOTO(mndCreateXnode(pMnode, pReq, &createReq), &lino, _OVER);

  taosMsleep(100);
  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_XNODE_NOT_EXIST;
    goto _OVER;
  }
  // send request
  (void)httpCreateXnode(pObj);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%s, failed to create since %s, line:%d", createReq.url, tstrerror(code), lino);
  }
  taosMemoryFreeClear(pToken);
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

static int32_t mndUpdateXnodeUserPassToken(SMnode *pMnode, SRpcMsg *pReq, SMUpdateXnodeReq *pUpdate) {
  int32_t           code = 0, lino = 0;
  STrans           *pTrans = NULL;
  SXnodeUserPassObj upObj = {0};
  upObj.id = pUpdate->id;
  upObj.updateTime = taosGetTimestampMs();

  if (pUpdate->user.ptr != NULL) {
    upObj.userLen = pUpdate->user.len + 1;
    upObj.user = taosMemoryCalloc(1, upObj.userLen);
    if (upObj.user == NULL) goto _OVER;
    (void)memcpy(upObj.user, pUpdate->user.ptr, pUpdate->user.len);
  }
  if (pUpdate->pass.ptr != NULL) {
    upObj.passLen = pUpdate->pass.len + 1;
    upObj.pass = taosMemoryCalloc(1, upObj.passLen);
    if (upObj.pass == NULL) goto _OVER;
    (void)memcpy(upObj.pass, pUpdate->pass.ptr, pUpdate->pass.len);
  }
  if (pUpdate->token.ptr != NULL) {
    upObj.tokenLen = pUpdate->token.len + 1;
    upObj.token = taosMemoryCalloc(1, upObj.tokenLen);
    if (upObj.token == NULL) goto _OVER;
    (void)memcpy(upObj.token, pUpdate->token.ptr, pUpdate->token.len);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode-userpass");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mDebug("trans:%d, used to update xnode userpass or token:%d", pTrans->id, upObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassCommitLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;
_OVER:
  taosMemoryFreeClear(upObj.user);
  taosMemoryFreeClear(upObj.pass);
  taosMemoryFreeClear(upObj.token);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SXnodeObj       *pObj = NULL;
  SMUpdateXnodeReq updateReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_UPDATE_XNODE), NULL, _OVER);

  if (updateReq.token.ptr != NULL || (updateReq.user.ptr != NULL && updateReq.pass.ptr != NULL)) {
    code = mndUpdateXnodeUserPassToken(pMnode, pReq, &updateReq);
    if (code != 0) goto _OVER;
    taosMsleep(200);
    mndRestartXnoded(pMnode);
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (updateReq.id != -1) {
      mError("xnode:%d, failed to update since %s", updateReq.id, tstrerror(code));
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
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeCommitLogs(STrans *pTrans, SXnodeObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
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
  mDebug("trans:%d, to drop xnode:%d", pTrans->id, pObj->id);

  code = mndSetDropXnodeInfoToTrans(pMnode, pTrans, pObj, false);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndDrainXnode(SMnode *pMnode, SRpcMsg *pReq, SXnodeObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = pObj->id;
  xnodeObj.status = "drain";
  xnodeObj.statusLen = strlen(xnodeObj.status) + 1;
  xnodeObj.updateTime = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drain-xnode");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mDebug("trans:%d, to drain xnode:%d", pTrans->id, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SXnodeObj     *pObj = NULL;
  SMDropXnodeReq dropReq = {0};
  SJson         *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mDebug("xnode:%d, start to drop", dropReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_DROP_XNODE), NULL, _OVER);

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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/%d?force=%s", XNODED_PIPE_SOCKET_URL, pObj->id,
           dropReq.force ? "true" : "false");
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);

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

  mDebug("xnode:%d, start to drain", drainReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_DRAIN_XNODE), NULL, _OVER);

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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/drain/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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

/** xnode task section **/

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

  mDebug("xnode task:%d, not found", tid);
  terrno = TSDB_CODE_MND_XNODE_TASK_NOT_EXIST;
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

  mDebug("xnode task:%s, not found", name);
  terrno = TSDB_CODE_MND_XNODE_TASK_NOT_EXIST;
  return NULL;
}

static void mndFreeXnodeTask(SXnodeTaskObj *pObj) {
  taosMemoryFreeClear(pObj->name);
  taosMemoryFreeClear(pObj->sourceDsn);
  taosMemoryFreeClear(pObj->sinkDsn);
  taosMemoryFreeClear(pObj->parser);
  taosMemoryFreeClear(pObj->reason);
  taosMemoryFreeClear(pObj->status);
  taosMemoryFreeClear(pObj->createdBy);
  taosMemoryFreeClear(pObj->labels);
}

SSdbRaw *mndXnodeTaskActionEncode(SXnodeTaskObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t totalStrLen =
      pObj->nameLen + pObj->sourceDsnLen + pObj->sinkDsnLen + pObj->parserLen + pObj->reasonLen + pObj->statusLen;
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
  SDB_SET_INT32(pRaw, dataPos, pObj->createdByLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->createdBy, pObj->createdByLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->labelsLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->labels, pObj->labelsLen, _OVER)

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

SSdbRow *mndXnodeTaskActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow       *pRow = NULL;
  SXnodeTaskObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

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

  SDB_GET_INT32(pRaw, dataPos, &pObj->createdByLen, _OVER)
  if (pObj->createdByLen > 0) {
    pObj->createdBy = taosMemoryCalloc(pObj->createdByLen + 1, 1);
    if (pObj->createdBy == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->createdBy, pObj->createdByLen, _OVER)
  }

  SDB_GET_INT32(pRaw, dataPos, &pObj->labelsLen, _OVER)
  if (pObj->labelsLen > 0) {
    pObj->labels = taosMemoryCalloc(pObj->labelsLen + 1, 1);
    if (pObj->labels == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->labels, pObj->labelsLen, _OVER)
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

int32_t mndXnodeTaskActionInsert(SSdb *pSdb, SXnodeTaskObj *pObj) {
  mDebug("xtask:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

int32_t mndXnodeTaskActionDelete(SSdb *pSdb, SXnodeTaskObj *pObj) {
  mDebug("xtask:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnodeTask(pObj);
  return 0;
}

int32_t mndXnodeTaskActionUpdate(SSdb *pSdb, SXnodeTaskObj *pOld, SXnodeTaskObj *pNew) {
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
  swapFields(&pNew->labelsLen, &pNew->labels, &pOld->labelsLen, &pOld->labels);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
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

  xnodeObj.nameLen = pCreate->name.len + 1;
  xnodeObj.name = taosMemoryCalloc(1, xnodeObj.nameLen);
  if (xnodeObj.name == NULL) goto _OVER;
  (void)memcpy(xnodeObj.name, pCreate->name.ptr, pCreate->name.len);

  xnodeObj.sourceType = pCreate->source.type;
  xnodeObj.sourceDsnLen = pCreate->source.cstr.len + 1;
  xnodeObj.sourceDsn = taosMemoryCalloc(1, xnodeObj.sourceDsnLen);
  if (xnodeObj.sourceDsn == NULL) goto _OVER;
  (void)memcpy(xnodeObj.sourceDsn, pCreate->source.cstr.ptr, pCreate->source.cstr.len);

  xnodeObj.sinkType = pCreate->sink.type;
  xnodeObj.sinkDsnLen = pCreate->sink.cstr.len + 1;
  xnodeObj.sinkDsn = taosMemoryCalloc(1, xnodeObj.sinkDsnLen);
  if (xnodeObj.sinkDsn == NULL) goto _OVER;
  (void)memcpy(xnodeObj.sinkDsn, pCreate->sink.cstr.ptr, pCreate->sink.cstr.len);

  xnodeObj.parserLen = pCreate->options.parser.len + 1;
  if (pCreate->options.parser.ptr != NULL) {
    xnodeObj.parser = taosMemoryCalloc(1, xnodeObj.parserLen);
    if (xnodeObj.parser == NULL) goto _OVER;
    (void)memcpy(xnodeObj.parser, pCreate->options.parser.ptr, pCreate->options.parser.len);
  }

  const char *status = getXTaskOptionByName(&pCreate->options, "status");
  if (status != NULL) {
    xnodeObj.statusLen = strlen(status) + 1;
    xnodeObj.status = taosMemoryCalloc(1, xnodeObj.statusLen);
    if (xnodeObj.status == NULL) goto _OVER;
    (void)memcpy(xnodeObj.status, status, xnodeObj.statusLen - 1);
  }

  xnodeObj.createdByLen = strlen(pReq->info.conn.user) + 1;
  xnodeObj.createdBy = taosMemoryCalloc(1, xnodeObj.createdByLen);
  if (xnodeObj.createdBy == NULL) goto _OVER;
  (void)memcpy(xnodeObj.createdBy, pReq->info.conn.user, xnodeObj.createdByLen - 1);

  const char *labels = getXTaskOptionByName(&pCreate->options, "labels");
  if (labels != NULL) {
    xnodeObj.labelsLen = strlen(labels) + 1;
    xnodeObj.labels = taosMemoryCalloc(1, xnodeObj.labelsLen);
    if (xnodeObj.labels == NULL) goto _OVER;
    (void)memcpy(xnodeObj.labels, labels, xnodeObj.labelsLen - 1);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode-task");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) {
      code = terrno;
    }
    mError("failed to create transaction for xnode-task:%s, code:0x%x:%s", pCreate->name.ptr, code, tstrerror(code));
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
  int32_t code = grantCheck(TSDB_GRANT_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    return code;
  }

  return mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_CREATE_XNODE);
}

// Helper function to parse and validate the request
static int32_t mndValidateCreateXnodeTaskReq(SRpcMsg *pReq, SMCreateXnodeTaskReq *pCreateReq) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *srcDsn = NULL;
  char   *sinkDsn = NULL;
  char   *parser = NULL;
  char   *pContStr = NULL;

  // from, to, parser check
  char xnodeUrl[TSDB_XNODE_URL_LEN] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/check", XNODED_PIPE_SOCKET_URL);
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

  if (pCreateReq->options.parser.len > 0 && pCreateReq->options.parser.ptr != NULL) {
    parser = taosStrndupi(pCreateReq->options.parser.ptr, (int64_t)pCreateReq->options.parser.len);
    if (parser == NULL) {
      code = terrno;
      goto _OVER;
    }
    TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "parser", parser), NULL, _OVER);
  }

  if (pCreateReq->xnodeId > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "xnode_id", (double)pCreateReq->xnodeId), NULL, _OVER);
  }

  pContStr = tjsonToUnformattedString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }

  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, 60000, pContStr, strlen(pContStr));
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  // todo: only4test
  // (void)mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, 60000, pContStr, strlen(pContStr));
  // code = TSDB_CODE_SUCCESS;

_OVER:
  if (srcDsn != NULL) taosMemoryFreeClear(srcDsn);
  if (sinkDsn != NULL) taosMemoryFreeClear(sinkDsn);
  if (parser != NULL) taosMemoryFreeClear(parser);
  if (pContStr != NULL) taosMemoryFreeClear(pContStr);
  if (postContent != NULL) tjsonDelete(postContent);
  if (pJson != NULL) tjsonDelete(pJson);

  TAOS_RETURN(code);
}

// Helper function to check if xnode task already exists
static int32_t mndCheckXnodeTaskExists(SMnode *pMnode, const char *name) {
  SXnodeTaskObj *pObj = mndAcquireXnodeTaskByName(pMnode, name);
  if (pObj != NULL) {
    mError("xnode task:%s already exists", name);
    mndReleaseXnodeTask(pMnode, pObj);
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
  mDebug("xnode create task request received, contLen:%d\n", pReq->contLen);
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = -1;
  SMCreateXnodeTaskReq createReq = {0};

  // Step 1: Validate permissions
  code = mndValidateXnodeTaskPermissions(pMnode, pReq);
  if (code != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  code = tDeserializeSMCreateXnodeTaskReq(pReq->pCont, pReq->contLen, &createReq);
  if (code != 0) {
    mError("failed to deserialize create xnode task request, code:%s", tstrerror(code));
    TAOS_RETURN(code);
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

  snprintf(req.xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(req.postContent, "xnode_id", (double)pObj->xnodeId), NULL, _OVER);
  }

  if (pObj->via > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(req.postContent, "via", (double)pObj->via), NULL, _OVER);
  }

  if (pObj->createdBy != NULL) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(req.postContent, "created_by", pObj->createdBy), NULL, _OVER);
  }

  if (pObj->labels != NULL) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(req.postContent, "labels", pObj->labels), NULL, _OVER);
  }

  req.pContStr = tjsonToUnformattedString(req.postContent);
  if (req.pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }
  mDebug("start xnode post content:%s", req.pContStr);
  req.pJson = mndSendReqRetJson(req.xnodeUrl, HTTP_TYPE_POST, defaultTimeout, req.pContStr, strlen(req.pContStr));

_OVER:
  if (req.pContStr != NULL) taosMemoryFreeClear(req.pContStr);
  if (req.postContent != NULL) tjsonDelete(req.postContent);
  if (req.pJson != NULL) tjsonDelete(req.pJson);
  if (req.srcDsn != NULL) taosMemoryFreeClear(req.srcDsn);
  if (req.sinkDsn != NULL) taosMemoryFreeClear(req.sinkDsn);
  if (req.parser != NULL) taosMemoryFreeClear(req.parser);
  TAOS_RETURN(code);
}

static int32_t mndProcessStartXnodeTaskReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeTaskObj      *pObj = NULL;
  SMStartXnodeTaskReq startReq = {0};
  SXnodeTaskObj      *pObjClone = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMStartXnodeTaskReq(pReq->pCont, pReq->contLen, &startReq), NULL, _OVER);

  mDebug("xnode start xnode task with tid:%d", startReq.tid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_START_XNODE_TASK), NULL, _OVER);

  if (startReq.tid <= 0 && (startReq.name.len <= 0 || startReq.name.ptr == NULL)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (startReq.tid > 0) {
    pObj = mndAcquireXnodeTask(pMnode, startReq.tid);
  } else {
    pObj = mndAcquireXnodeTaskByName(pMnode, startReq.name.ptr);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  (void)httpStartXnodeTask(pObj);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to start since %s", startReq.tid, tstrerror(code));
  }
  tFreeSMStartXnodeTaskReq(&startReq);
  if (pObj != NULL) {
    mndReleaseXnodeTask(pMnode, pObj);
  }
  if (pObjClone != NULL) {
    mndFreeXnodeTask(pObjClone);
    taosMemFree(pObjClone);
  }
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
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_STOP_XNODE_TASK), NULL, _OVER);
  if (stopReq.tid <= 0 && (stopReq.name.len <= 0 || stopReq.name.ptr == NULL)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (stopReq.tid > 0) {
    pObj = mndAcquireXnodeTask(pMnode, stopReq.tid);
  } else {
    pObj = mndAcquireXnodeTaskByName(pMnode, stopReq.name.ptr);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);

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
    bool labels;
  } isChange = {0};

  if (pUpdate->via > 0) {
    taskObj.via = pUpdate->via;
  }
  if (pUpdate->xnodeId > 0) {
    taskObj.xnodeId = pUpdate->xnodeId;
  }
  if (pUpdate->status.ptr != NULL) {
    taskObj.statusLen = pUpdate->status.len + 1;
    taskObj.status = taosMemoryCalloc(1, taskObj.statusLen);
    if (taskObj.status == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.status, pUpdate->status.ptr, pUpdate->status.len);
    isChange.status = true;
  }
  if (pUpdate->updateName.ptr != NULL) {
    taskObj.nameLen = pUpdate->updateName.len + 1;
    taskObj.name = taosMemoryCalloc(1, taskObj.nameLen);
    if (taskObj.name == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.name, pUpdate->updateName.ptr, pUpdate->updateName.len);
    isChange.name = true;
  }
  if (pUpdate->source.cstr.ptr != NULL) {
    taskObj.sourceType = pUpdate->source.type;
    taskObj.sourceDsnLen = pUpdate->source.cstr.len + 1;
    taskObj.sourceDsn = taosMemoryCalloc(1, taskObj.sourceDsnLen);
    if (taskObj.sourceDsn == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.sourceDsn, pUpdate->source.cstr.ptr, pUpdate->source.cstr.len);
    isChange.source = true;
  }
  if (pUpdate->sink.cstr.ptr != NULL) {
    taskObj.sinkType = pUpdate->sink.type;
    taskObj.sinkDsnLen = pUpdate->sink.cstr.len + 1;
    taskObj.sinkDsn = taosMemoryCalloc(1, taskObj.sinkDsnLen);
    if (taskObj.sinkDsn == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.sinkDsn, pUpdate->sink.cstr.ptr, pUpdate->sink.cstr.len);
    isChange.sink = true;
  }
  if (pUpdate->parser.ptr != NULL) {
    taskObj.parserLen = pUpdate->parser.len + 1;
    taskObj.parser = taosMemoryCalloc(1, taskObj.parserLen);
    if (taskObj.parser == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.parser, pUpdate->parser.ptr, pUpdate->parser.len);
    isChange.parser = true;
  }
  if (pUpdate->reason.ptr != NULL) {
    taskObj.reasonLen = pUpdate->reason.len + 1;
    taskObj.reason = taosMemoryCalloc(1, taskObj.reasonLen);
    if (taskObj.reason == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.reason, pUpdate->reason.ptr, pUpdate->reason.len);
    isChange.reason = true;
  }
  if (pUpdate->labels.ptr != NULL) {
    taskObj.labelsLen = pUpdate->labels.len + 1;
    taskObj.labels = taosMemoryCalloc(1, taskObj.labelsLen);
    if (taskObj.labels == NULL) {
      code = terrno;
      goto _OVER;
    }
    (void)memcpy(taskObj.labels, pUpdate->labels.ptr, pUpdate->labels.len);
    isChange.labels = true;
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
  if (NULL != taskObj.labels && isChange.labels) {
    taosMemoryFree(taskObj.labels);
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeTaskReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeTaskObj       *pObj = NULL;
  SMUpdateXnodeTaskReq updateReq = {0};

  if ((code = grantCheck(TSDB_GRANT_TD_GPT)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeTaskReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);

  if (updateReq.tid > 0) {
    pObj = mndAcquireXnodeTaskById(pMnode, updateReq.tid);
  } else {
    pObj = mndAcquireXnodeTaskByName(pMnode, updateReq.name.ptr);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (updateReq.updateName.len > 0) {
    SXnodeTaskObj *tmpObj = mndAcquireXnodeTaskByName(pMnode, updateReq.updateName.ptr);
    if (tmpObj != NULL) {
      mndReleaseXnodeTask(pMnode, tmpObj);
      code = TSDB_CODE_MND_XNODE_NAME_DUPLICATE;
      goto _OVER;
    }
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

  TAOS_RETURN(code);
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
  mDebug("trans:%d, to drop xnode:%d", pTrans->id, pTask->id);

  // delete relative jobs
  // TAOS_CHECK_GOTO(mndAcquireXnodeJobsByTaskId(pMnode, pTask->id, &pArray), NULL, _OVER);
  // for (int i = 0; i < pArray->size; i++) {
  //   SXnodeJobObj *pJob = taosArrayGet(pArray, i);
  //   if (pJob == NULL) continue;
  //   mDebug("xnode drop xnode task %d trans:%d, to drop xnode job:%d", pTask->id, pTrans->id, pJob->id);
  //   TAOS_CHECK_GOTO(mndSetDropXnodeJobInfoToTrans(pTrans, pJob, false), NULL, _OVER);
  // }

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
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeTaskReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SXnodeTaskObj     *pObj = NULL;
  SMDropXnodeTaskReq dropReq = {0};
  SJson             *pJson = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeTaskReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mDebug("DropXnodeTask with tid:%d, start to drop", dropReq.id);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_DROP_XNODE_TASK), NULL, _OVER);

  if (dropReq.id <= 0 && (dropReq.name.len <= 0 || dropReq.name.ptr == NULL)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (dropReq.name.len > 0 && dropReq.name.ptr != NULL) {
    pObj = mndAcquireXnodeTaskByName(pMnode, dropReq.name.ptr);
  } else {
    pObj = mndAcquireXnodeTask(pMnode, dropReq.id);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request to drop xnode task
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/task/drop/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);

  code = mndDropXnodeTask(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to drop since %s", dropReq.id, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  tFreeSMDropXnodeTaskReq(&dropReq);
  TAOS_RETURN(code);
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
    if (pObj->parserLen > 0 && pObj->parser != NULL) {
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

    // create_by
    if (pObj->createdByLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->createdBy, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

    // labels
    if (pObj->labelsLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->labels, pShow->pMeta->pSchemas[cols].bytes);
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
  if (code != 0 && pObj != NULL) sdbRelease(pSdb, pObj);

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextXnodeTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE_TASK);
}

/** xnode job section **/

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
      if (NULL == taosArrayInsert(*ppArray, idx++, pJob)) {
        code = terrno;
        sdbRelease(pSdb, pJob);
        goto _exit;
      }
    }

    sdbRelease(pSdb, pJob);
  }
  sdbCancelFetch(pSdb, pIter);

_exit:
  TAOS_RETURN(code);
}

static int32_t mndAcquireXnodeJobsAll(SMnode *pMnode, SArray **ppArray) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppArray = taosArrayInit(64, sizeof(SXnodeJobObj));
  if (ppArray == NULL) {
    code = terrno;
    goto _exit;
  }

  int32_t idx = 0;
  void   *pIter = NULL;
  while (1) {
    SXnodeJobObj *pJob = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_JOB, pIter, (void **)&pJob);
    if (pIter == NULL) break;
    if (NULL == taosArrayInsert(*ppArray, idx++, pJob)) {
      code = terrno;
      goto _exit;
    }
    sdbRelease(pSdb, pJob);
  }
  sdbCancelFetch(pSdb, pIter);

_exit:
  TAOS_RETURN(code);
}

static void mndFreeXnodeJob(SXnodeJobObj *pObj) {
  if (NULL == pObj) {
    return;
  }
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

SSdbRaw *mndXnodeJobActionEncode(SXnodeJobObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

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

SSdbRow *mndXnodeJobActionDecode(SSdbRaw *pRaw) {
  mInfo("xnode, start to decode from raw:%p", pRaw);
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow      *pRow = NULL;
  SXnodeJobObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

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
      taosMemoryFreeClear(pObj->status);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

int32_t mndXnodeJobActionInsert(SSdb *pSdb, SXnodeJobObj *pObj) {
  mInfo("xnode tid:%d, jid:%d, perform insert action, row:%p", pObj->taskId, pObj->id, pObj);
  return 0;
}

int32_t mndXnodeJobActionDelete(SSdb *pSdb, SXnodeJobObj *pObj) {
  mDebug("xnode tid:%d, jid:%d, perform delete action, row:%p", pObj->taskId, pObj->id, pObj);
  mndFreeXnodeJob(pObj);
  return 0;
}

int32_t mndXnodeJobActionUpdate(SSdb *pSdb, SXnodeJobObj *pOld, SXnodeJobObj *pNew) {
  mDebug("xnode tid:%d, jid:%d, perform update action, old row:%p new row:%p", pOld->taskId, pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  pOld->via = pNew->via;
  pOld->xnodeId = pNew->xnodeId;
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  swapFields(&pNew->configLen, &pNew->config, &pOld->configLen, &pOld->config);
  swapFields(&pNew->reasonLen, &pNew->reason, &pOld->reasonLen, &pOld->reason);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

/* xnode user pass actions */
SSdbRaw *mndXnodeUserPassActionEncode(SXnodeUserPassObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t rawDataLen =
      sizeof(SXnodeUserPassObj) + TSDB_XNODE_RESERVE_SIZE + pObj->userLen + pObj->passLen + pObj->tokenLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_USER_PASS, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->userLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->user, pObj->userLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->passLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->pass, pObj->passLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->tokenLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->token, pObj->tokenLen, _OVER)
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
SSdbRow *mndXnodeUserPassActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow           *pRow = NULL;
  SXnodeUserPassObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

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
    pObj->user = taosMemoryCalloc(1, pObj->userLen + 1);
    if (pObj->user == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->user, pObj->userLen, _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->passLen, _OVER)
  if (pObj->passLen > 0) {
    pObj->pass = taosMemoryCalloc(1, pObj->passLen + 1);
    if (pObj->pass == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->pass, pObj->passLen, _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->tokenLen, _OVER)
  if (pObj->tokenLen > 0) {
    pObj->token = taosMemoryCalloc(1, pObj->tokenLen + 1);
    if (pObj->token == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->token, pObj->tokenLen, _OVER)
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
int32_t mndXnodeUserPassActionInsert(SSdb *pSdb, SXnodeUserPassObj *pObj) {
  mDebug("xnode user pass:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}
int32_t mndXnodeUserPassActionUpdate(SSdb *pSdb, SXnodeUserPassObj *pOld, SXnodeUserPassObj *pNew) {
  mDebug("xnode user pass:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
  char* tmp = NULL;
  pOld->userLen = pNew->userLen;
  tmp = pOld->user;
  pOld->user = pNew->user;
  pNew->user = tmp;

  pOld->passLen = pNew->passLen;
  tmp = pOld->pass;
  pOld->pass = pNew->pass;
  pNew->pass = tmp;

  pOld->tokenLen = pNew->tokenLen;
  tmp = pOld->token;
  pOld->token = pNew->token;
  pNew->token = tmp;
  
  // swapFields(&pNew->userLen, &pNew->user, &pOld->userLen, &pOld->user);
  // swapFields(&pNew->passLen, &pNew->pass, &pOld->passLen, &pOld->pass);
  // swapFields(&pNew->tokenLen, &pNew->token, &pOld->tokenLen, &pOld->token);
  // SXnodeUserPassObj* tmp = pNew;
  // pNew = pOld;
  // pOld = tmp;
  
  taosWUnLockLatch(&pOld->lock);
  return 0;
}
int32_t mndXnodeUserPassActionDelete(SSdb *pSdb, SXnodeUserPassObj *pObj) {
  mDebug("xnode:%d, perform delete action, row:%p", pObj->id, pObj);
  taosMemoryFreeClear(pObj->user);
  taosMemoryFreeClear(pObj->pass);
  taosMemoryFreeClear(pObj->token);
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

  jobObj.configLen = pCreate->config.len + 1;
  if (jobObj.configLen > TSDB_XNODE_TASK_JOB_CONFIG_LEN) {
    code = TSDB_CODE_MND_XNODE_TASK_JOB_CONFIG_TOO_LONG;
    goto _OVER;
  }
  jobObj.config = taosMemoryCalloc(1, jobObj.configLen);
  if (jobObj.config == NULL) goto _OVER;
  (void)memcpy(jobObj.config, pCreate->config.ptr, pCreate->config.len);

  jobObj.via = pCreate->via;
  jobObj.xnodeId = pCreate->xnodeId;

  if (pCreate->status.ptr != NULL) {
    jobObj.statusLen = pCreate->status.len + 1;
    jobObj.status = taosMemoryCalloc(1, jobObj.statusLen);
    if (jobObj.status == NULL) goto _OVER;
    (void)memmove(jobObj.status, pCreate->status.ptr, pCreate->status.len);
  }

  if (jobObj.reason != NULL) {
    jobObj.reasonLen = pCreate->reason.len + 1;
    if (jobObj.reasonLen > TSDB_XNODE_TASK_REASON_LEN) {
      code = TSDB_CODE_MND_XNODE_TASK_REASON_TOO_LONG;
      goto _OVER;
    }
    jobObj.reason = taosMemoryCalloc(1, jobObj.reasonLen);
    if (jobObj.reason == NULL) goto _OVER;
    (void)memcpy(jobObj.reason, pCreate->reason.ptr, pCreate->reason.len);
  }

  jobObj.createTime = taosGetTimestampMs();
  jobObj.updateTime = jobObj.createTime;

  mDebug("create xnode job, id:%d, tid:%d, config:%s, time:%" PRId64, jobObj.id, jobObj.taskId, jobObj.config,
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
  if (pUpdate->status.ptr != NULL) {
    jobObj.statusLen = pUpdate->status.len + 1;
    jobObj.status = taosMemoryCalloc(1, jobObj.statusLen);
    if (jobObj.status == NULL) goto _OVER;
    (void)memcpy(jobObj.status, pUpdate->status.ptr, pUpdate->status.len);
    isChange.status = true;
  }
  if (pUpdate->config != NULL) {
    jobObj.configLen = pUpdate->configLen + 1;
    jobObj.config = taosMemoryCalloc(1, jobObj.configLen);
    if (jobObj.config == NULL) goto _OVER;
    (void)memcpy(jobObj.config, pUpdate->config, pUpdate->configLen);
    isChange.config = true;
  }
  if (pUpdate->reason != NULL) {
    jobObj.reasonLen = pUpdate->reasonLen + 1;
    jobObj.reason = taosMemoryCalloc(1, jobObj.reasonLen);
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

  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  mndTransDrop(pTrans);
  return code;
}
static int32_t mndProcessCreateXnodeJobReq(SRpcMsg *pReq) {
  mDebug("create xnode job req, content len:%d", pReq->contLen);
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SMCreateXnodeJobReq createReq = {0};

  if ((code = grantCheck(TSDB_GRANT_XNODE)) != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeJobReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);

  mDebug("xnode create job on xnode:%d", createReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_CREATE_XNODE_JOB), NULL, _OVER);

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

  mDebug("RebalanceXnodeJob with jid:%d, xnode_id:%d, start to rebalance", rebalanceReq.jid, rebalanceReq.xnodeId);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_REBALANCE_XNODE_JOB), NULL, _OVER);

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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/rebalance/manual/%d/%d/%d", XNODED_PIPE_SOCKET_URL, pObj->taskId, pObj->id,
           rebalanceReq.xnodeId);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, NULL, 0);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to rebalance xnode job since %s", rebalanceReq.jid, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  mndReleaseXnodeJob(pMnode, pObj);
  tFreeSMRebalanceXnodeJobReq(&rebalanceReq);
  TAOS_RETURN(code);
}

typedef struct {
  SValueNode nd;
  bool       shouldFree;
} SXndRefValueNode;

static void freeSXndRefValueNode(void *pNode) {
  if (pNode == NULL) return;

  SXndRefValueNode *pRefNode = (SXndRefValueNode *)pNode;
  if (pRefNode->shouldFree) {
    taosMemoryFreeClear(pRefNode->nd.datum.p);
  }
}

typedef struct {
  SArray   *stack;
  SHashObj *pMap;
  int32_t   code;
} SXndWhereContext;

void freeSXndWhereContext(SXndWhereContext *pCtx) {
  if (pCtx == NULL) return;

  if (pCtx->pMap != NULL) {
    taosHashCleanup(pCtx->pMap);
    pCtx->pMap = NULL;
  }
  if (pCtx->stack != NULL) {
    for (int32_t i = 0; i < pCtx->stack->size; i++) {
      SXndRefValueNode *pRefNode = (SXndRefValueNode *)taosArrayGet(pCtx->stack, i);
      if (pRefNode != NULL) {
        freeSXndRefValueNode(pRefNode);
      }
    }
    taosArrayDestroy(pCtx->stack);
    pCtx->stack = NULL;
  }
}

static SHashObj *convertJob2Map(const SXnodeJobObj *pJob) {
  SHashObj *pMap = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (pMap == NULL) {
    return NULL;
  }
  taosHashSetFreeFp(pMap, freeSXndRefValueNode);
  // id
  SXndRefValueNode id = {0};
  id.nd.node.type = QUERY_NODE_VALUE;
  id.nd.node.resType.type = TSDB_DATA_TYPE_UBIGINT;
  id.nd.datum.u = pJob->id;
  int32_t code = taosHashPut(pMap, "id", strlen("id") + 1, &id, sizeof(SXndRefValueNode));
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // task id
  SXndRefValueNode taskId = {0};
  taskId.nd.node.type = QUERY_NODE_VALUE;
  taskId.nd.node.resType.type = TSDB_DATA_TYPE_UBIGINT;
  taskId.nd.datum.u = pJob->taskId;
  if (pJob->taskId == 0) {
    taskId.nd.isNull = true;
  }
  code = taosHashPut(pMap, "task_id", strlen("task_id") + 1, &taskId, sizeof(SXndRefValueNode));
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // via
  SXndRefValueNode via = {0};
  via.nd.node.type = QUERY_NODE_VALUE;
  via.nd.node.resType.type = TSDB_DATA_TYPE_UBIGINT;
  via.nd.datum.u = pJob->via;
  if (pJob->via == 0) {
    via.nd.isNull = true;
  }
  code = taosHashPut(pMap, "via", strlen("via") + 1, &via, sizeof(SXndRefValueNode));
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // xnode id
  SXndRefValueNode xnodeId = {0};
  xnodeId.nd.node.type = QUERY_NODE_VALUE;
  xnodeId.nd.node.resType.type = TSDB_DATA_TYPE_UBIGINT;
  xnodeId.nd.datum.u = pJob->xnodeId;
  if (pJob->xnodeId == 0) {
    xnodeId.nd.isNull = true;
  }
  code = taosHashPut(pMap, "xnode_id", strlen("xnode_id") + 1, &xnodeId, sizeof(SXndRefValueNode));
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // config
  SXndRefValueNode config = {0};
  config.nd.node.type = QUERY_NODE_VALUE;
  if (pJob->configLen > 0) {
    config.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    config.nd.datum.p = taosStrndupi(pJob->config, strlen(pJob->config) + 1);
    config.shouldFree = true;
    code = taosHashPut(pMap, "config", strlen("config") + 1, &config, sizeof(SXndRefValueNode));
  } else {
    config.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    config.nd.datum.p = NULL;
    config.nd.isNull = true;
    code = taosHashPut(pMap, "config", strlen("config") + 1, &config, sizeof(SXndRefValueNode));
  }
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // status
  SXndRefValueNode status = {0};
  status.nd.node.type = QUERY_NODE_VALUE;
  if (pJob->statusLen > 0) {
    status.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    status.nd.datum.p = taosStrndupi(pJob->status, strlen(pJob->status) + 1);
    status.shouldFree = true;
    code = taosHashPut(pMap, "status", strlen("status") + 1, &status, sizeof(SXndRefValueNode));
  } else {
    status.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    status.nd.datum.p = NULL;
    status.nd.isNull = true;
    code = taosHashPut(pMap, "status", strlen("status") + 1, &status, sizeof(SXndRefValueNode));
  }
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // reason
  SXndRefValueNode reason = {0};
  reason.nd.node.type = QUERY_NODE_VALUE;
  if (pJob->reasonLen > 0) {
    reason.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    reason.nd.datum.p = taosStrndupi(pJob->reason, strlen(pJob->reason) + 1);
    reason.shouldFree = true;
    code = taosHashPut(pMap, "reason", strlen("reason") + 1, &reason, sizeof(SXndRefValueNode));
  } else {
    reason.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
    reason.nd.datum.p = NULL;
    reason.nd.isNull = true;
    code = taosHashPut(pMap, "reason", strlen("reason") + 1, &reason, sizeof(SXndRefValueNode));
  }
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // create time
  SXndRefValueNode createTime = {0};
  createTime.nd.node.type = QUERY_NODE_VALUE;
  createTime.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
  createTime.nd.datum.p = taosMemoryCalloc(1, TD_TIME_STR_LEN);
  createTime.nd.datum.p = formatTimestampLocal(createTime.nd.datum.p, pJob->createTime, TSDB_TIME_PRECISION_MILLI);
  createTime.shouldFree = true;
  code = taosHashPut(pMap, "create_time", strlen("create_time") + 1, &createTime, sizeof(SXndRefValueNode));
  if (code != 0) {
    taosHashCleanup(pMap);
    return NULL;
  }
  // update time
  SXndRefValueNode updateTime = {0};
  updateTime.nd.node.type = QUERY_NODE_VALUE;
  updateTime.nd.node.resType.type = TSDB_DATA_TYPE_BINARY;
  updateTime.nd.datum.p = taosMemoryCalloc(1, TD_TIME_STR_LEN);
  updateTime.nd.datum.p = formatTimestampLocal(updateTime.nd.datum.p, pJob->updateTime, TSDB_TIME_PRECISION_MILLI);
  updateTime.shouldFree = true;
  code = taosHashPut(pMap, "update_time", strlen("update_time") + 1, &updateTime, sizeof(SXndRefValueNode));
  return pMap;
}

typedef bool (*FOpCmp)(SValueNode *pval1, SValueNode *pval2);

#define XNODE_DEF_OP_FUNC(NAME, OP)                             \
  static bool NAME(SValueNode *pval1, SValueNode *pval2) {      \
    switch (pval1->node.resType.type) {                         \
      case TSDB_DATA_TYPE_BOOL:                                 \
        return pval1->datum.b OP pval2->datum.b;                \
      case TSDB_DATA_TYPE_UBIGINT:                              \
        return pval1->datum.u OP pval2->datum.u;                \
      case TSDB_DATA_TYPE_BIGINT:                               \
        return pval1->datum.i OP pval2->datum.i;                \
      case TSDB_DATA_TYPE_FLOAT:                                \
        return pval1->datum.d OP pval2->datum.d;                \
      case TSDB_DATA_TYPE_BINARY: {                             \
        if (pval1->datum.p == NULL || pval2->datum.p == NULL) { \
          return pval1->datum.p OP pval2->datum.p;              \
        }                                                       \
        return strcmp(pval1->datum.p, pval2->datum.p) OP 0;     \
      }                                                         \
      default:                                                  \
        return false;                                           \
    }                                                           \
  }

XNODE_DEF_OP_FUNC(op_greater_than, >)
XNODE_DEF_OP_FUNC(op_greater_equal, >=)
XNODE_DEF_OP_FUNC(op_lower_than, <)
XNODE_DEF_OP_FUNC(op_lower_equal, <=)
XNODE_DEF_OP_FUNC(op_equal, ==)
XNODE_DEF_OP_FUNC(op_not_equal, !=)

static int32_t call_op_cmp(SXndWhereContext *pctx, FOpCmp opFn) {
  int32_t     code = 0;
  SValueNode *pval2 = (SValueNode *)taosArrayPop(pctx->stack);
  SValueNode *pval1 = (SValueNode *)taosArrayPop(pctx->stack);
  bool        ret = false;

  if (pval1->node.type != pval2->node.type) {
    code = TSDB_CODE_MND_XNODE_WHERE_COL_TYPE_DIFF;
    mError("xnode type not same, v1 type: %d, v2 type: %d", pval1->node.type, pval2->node.type);
    goto _OVER;
  } else {
    mDebug("xnode type v1:%d, is null: %d, UB: %" PRIu64 ", v2 type: %d, is null: %d, UB: %" PRIu64, pval1->node.type,
           pval1->isNull, pval1->datum.u, pval2->node.type, pval2->isNull, pval2->datum.u);

    ret = (*opFn)(pval1, pval2);
  }
  SXndRefValueNode pval = {0};
  pval.nd.node.type = QUERY_NODE_VALUE;
  pval.nd.node.resType.type = TSDB_DATA_TYPE_BOOL;
  pval.nd.datum.b = ret;
  if (NULL == taosArrayPush(pctx->stack, &pval)) {
    mError("xnode evaluate walker array push error: %s", tstrerror(terrno));
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (pval1 != NULL) freeSXndRefValueNode((SXndRefValueNode *)pval1);
  if (pval2 != NULL) freeSXndRefValueNode((SXndRefValueNode *)pval2);
  TAOS_RETURN(code);
}

#define XND_WALKER_CHECK_GOTO(CMD, LABEL)    \
  do {                                       \
    pctx->code = (CMD);                      \
    if ((pctx->code != TSDB_CODE_SUCCESS)) { \
      goto LABEL;                            \
    }                                        \
  } while (0);

static EDealRes evaluateWaker(SNode *pNode, void *pWhereCtx) {
  int32_t           code = 0;
  SXndWhereContext *pctx = (SXndWhereContext *)pWhereCtx;

  if (nodeType(pNode) == QUERY_NODE_COLUMN) {
    SColumnNode *colNode = (SColumnNode *)pNode;
    SXndRefValueNode *pval =
        (SXndRefValueNode *)taosHashGet(pctx->pMap, colNode->colName, strlen(colNode->colName) + 1);
    if (pval == NULL) {
      mError("xnode evaluateWhereCond hash get error: %s", tstrerror(terrno));
      pctx->code = TSDB_CODE_MND_XNODE_WHERE_COL_NOT_EXIST;
      return DEAL_RES_END;
    }
    if (NULL == taosArrayPush(pctx->stack, pval)) {
      mError("xnode evaluate walker array push error: %s", tstrerror(terrno));
      pctx->code = TSDB_CODE_FAILED;
      return DEAL_RES_END;
    }
    return DEAL_RES_CONTINUE;
  }
  if (nodeType(pNode) == QUERY_NODE_VALUE) {
    SValueNode *pval = (SValueNode *)pNode;
    if (pval->node.resType.type == TSDB_DATA_TYPE_UBIGINT) {
      pval->datum.u = taosStr2Int64(pval->literal, NULL, 10);
    }
    if (pval->node.resType.type == TSDB_DATA_TYPE_BINARY) {
      if (pval->datum.p == NULL) {
        pval->datum.p = taosStrndupi(pval->literal, strlen(pval->literal) + 1);
      }
    }
    if (pval->node.resType.type == TSDB_DATA_TYPE_NULL) {
      pval->isNull = true;
      pval->datum.p = NULL;
    }
    if (pval->node.resType.type == TSDB_DATA_TYPE_BOOL) {
      pval->datum.b = pval->literal[0] == 't' || pval->literal[0] == 'T';
    }
    SXndRefValueNode refVal = {0};
    refVal.nd = *pval;
    refVal.shouldFree = false;
    if (NULL == taosArrayPush(pctx->stack, &refVal)) {
      mError("xnode evaluate walker array push error: %s", tstrerror(terrno));
      pctx->code = TSDB_CODE_FAILED;
      return DEAL_RES_END;
    }
    return DEAL_RES_CONTINUE;
  }

  if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
    SOperatorNode *opNode = (SOperatorNode *)pNode;
    switch (opNode->opType) {
      case OP_TYPE_GREATER_THAN: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_greater_than), _exit);
        break;
      }
      case OP_TYPE_GREATER_EQUAL: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_greater_equal), _exit);
        break;
      }
      case OP_TYPE_LOWER_THAN: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_lower_than), _exit);
        break;
      }
      case OP_TYPE_LOWER_EQUAL: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_lower_equal), _exit);
        break;
      }
      case OP_TYPE_EQUAL: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_equal), _exit);
        break;
      }
      case OP_TYPE_NOT_EQUAL: {
        XND_WALKER_CHECK_GOTO(call_op_cmp(pctx, op_not_equal), _exit);
        break;
      }
      default:
        pctx->code = TSDB_CODE_MND_XNODE_WHERE_OP_NOT_SUPPORT;
        return DEAL_RES_CONTINUE;
    }
    return DEAL_RES_CONTINUE;
  }

  if (nodeType(pNode) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode *logicNode = (SLogicConditionNode *)pNode;
    SXndRefValueNode     pval = {0};
    pval.nd.node.type = QUERY_NODE_VALUE;
    pval.nd.node.resType.type = TSDB_DATA_TYPE_BOOL;

    switch (logicNode->condType) {
      case LOGIC_COND_TYPE_AND: {
        SValueNode *pval2 = (SValueNode *)taosArrayPop(pctx->stack);
        SValueNode *pval1 = (SValueNode *)taosArrayPop(pctx->stack);

        pval.nd.datum.b = pval1->datum.b && pval2->datum.b;
        if (NULL == taosArrayPush(pctx->stack, &pval)) {
          mError("xnode walker AND array push err: %s", tstrerror(terrno));
          pctx->code = TSDB_CODE_FAILED;
          return DEAL_RES_END;
        }

        freeSXndRefValueNode((SXndRefValueNode *)pval1);
        freeSXndRefValueNode((SXndRefValueNode *)pval2);
        break;
      }
      case LOGIC_COND_TYPE_OR: {
        SValueNode *pval2 = (SValueNode *)taosArrayPop(pctx->stack);
        SValueNode *pval1 = (SValueNode *)taosArrayPop(pctx->stack);

        pval.nd.datum.b = pval1->datum.b || pval2->datum.b;
        if (NULL == taosArrayPush(pctx->stack, &pval)) {
          mError("xnode walker OR array push err: %s", tstrerror(terrno));
          pctx->code = TSDB_CODE_FAILED;
          return DEAL_RES_END;
        }

        freeSXndRefValueNode((SXndRefValueNode *)pval1);
        freeSXndRefValueNode((SXndRefValueNode *)pval2);
        break;
      }
      case LOGIC_COND_TYPE_NOT: {
        SValueNode *pval1 = (SValueNode *)taosArrayPop(pctx->stack);

        pval.nd.datum.b = !pval1->datum.b;
        if (NULL == taosArrayPush(pctx->stack, &pval)) {
          mError("xnode walker NOT array push err: %s", tstrerror(terrno));
          pctx->code = TSDB_CODE_FAILED;
          return DEAL_RES_END;
        }

        freeSXndRefValueNode((SXndRefValueNode *)pval1);
        break;
      }
      default:
        break;
    }
    return DEAL_RES_CONTINUE;
  }

  pctx->code = TSDB_CODE_MND_XNODE_INVALID_MSG;

_exit:
  return DEAL_RES_END;
}

static bool evaluateWhereCond(SNode *pWhere, SHashObj *pDataMap, int32_t *code) {
  bool             ret = false;
  SXndWhereContext ctx = {0};

  ctx.stack = taosArrayInit(64, sizeof(SXndRefValueNode));
  if (ctx.stack == NULL) {
    mError("xnode evaluateWhereCond error: %s", tstrerror(terrno));
    *code = terrno;
    goto _exit;
  }
  ctx.pMap = pDataMap;

  // walkexpr pnode
  nodesWalkExprPostOrder(pWhere, evaluateWaker, &ctx);
  if (ctx.code != TSDB_CODE_SUCCESS) {
    *code = ctx.code;
    mError("xnode walkExpr error: %s", tstrerror(ctx.code));
    goto _exit;
  }
  SValueNode *pval = taosArrayGetLast(ctx.stack);
  if (pval == NULL) {
    *code = terrno;
    mError("xnode evaluateWhereCond error: %s", tstrerror(terrno));
    goto _exit;
  }
  mDebug("xnode ctx stack size:%lu, last nd type:%d, bool:%d", ctx.stack->size, pval->node.type, pval->datum.b);
  ret = pval->datum.b;

_exit:
  freeSXndWhereContext(&ctx);
  return ret;
}

static int32_t filterJobsByWhereCond(SNode *pWhere, SArray *pArray, SArray **ppResult) {
  int32_t code = 0;

  *ppResult = taosArrayInit(64, sizeof(SXnodeJobObj));
  if (*ppResult == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < pArray->size; i++) {
    SXnodeJobObj *pJob = taosArrayGet(pArray, i);

    SHashObj *pDataMap = NULL;
    if ((pDataMap = convertJob2Map(pJob)) == NULL) {
      mError("xnode evaluate convertJow2Map error: %s", tstrerror(terrno));
      goto _exit;
    }
    if (evaluateWhereCond(pWhere, pDataMap, &code)) {
      if (NULL == taosArrayPush(*ppResult, pJob)) {
        mError("xnode filterJobsByWhereCond array push err: %s", tstrerror(terrno));
        code = TSDB_CODE_FAILED;
        goto _exit;
      }
    }
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }

_exit:
  TAOS_RETURN(code);
}

#define XND_LOG_END(code, lino)                                                                 \
  do {                                                                                          \
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {                    \
      mError("xnode:%s failed at line %d code: %d, since %s", __func__, lino, code, tstrerror(code)); \
    }                                                                                           \
  } while (0)

void httpRebalanceAuto(SArray *pResult) {
  int32_t code = 0;
  int32_t lino = 0;
  SJson *pJsonArr = NULL;
  char  *pContStr = NULL;
  // convert pResult to [(tid, jid)*]
  pJsonArr = tjsonCreateArray();
  if (pJsonArr == NULL) {
    code = terrno;
    mError("xnode json array error: %s", tstrerror(code));
    goto _OVER;
  }
  for (int32_t i = 0; i < pResult->size; i++) {
    SXnodeJobObj *pJob = taosArrayGet(pResult, i);
    SJson        *pJsonObj = tjsonCreateObject();
    if (pJsonObj == NULL) {
      code = terrno;
      mError("xnode json object error: %s", tstrerror(code));
      goto _OVER;
    }
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJsonObj, "tid", pJob->taskId), &lino, _OVER);
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJsonObj, "jid", pJob->id), &lino, _OVER);
    TAOS_CHECK_GOTO(tjsonAddItemToArray(pJsonArr, pJsonObj), &lino, _OVER);
  }

  pContStr = tjsonToUnformattedString(pJsonArr);
  if (pContStr == NULL) {
    mError("xnode to json string error: %s", tstrerror(terrno));
    goto _OVER;
  }
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/rebalance/auto", XNODED_PIPE_SOCKET_URL);
  SJson* pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));
  if (pJson) {
    tjsonDelete(pJson);
  }

_OVER:
  if (pJsonArr != NULL) tjsonDelete(pJsonArr);
  if (pContStr != NULL) taosMemoryFree(pContStr);
  XND_LOG_END(code, lino);
  return;
}

static int32_t mndProcessRebalanceXnodeJobsWhereReq(SRpcMsg *pReq) {
  mDebug("xnode reblance xnode jobs where req, content len:%d", pReq->contLen);
  int32_t                      code = 0;
  SMnode                      *pMnode = pReq->info.node;
  SMRebalanceXnodeJobsWhereReq rebalanceReq = {0};
  SNode                       *pWhere = NULL;
  SArray                      *pArray = NULL;
  SArray                      *pResult = NULL;

  TAOS_CHECK_GOTO(tDeserializeSMRebalanceXnodeJobsWhereReq(pReq->pCont, pReq->contLen, &rebalanceReq), NULL, _OVER);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_REBALANCE_XNODE_JOB), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAcquireXnodeJobsAll(pMnode, &pArray), NULL, _OVER);
  if (NULL != rebalanceReq.ast.ptr) {
    TAOS_CHECK_GOTO(nodesStringToNode(rebalanceReq.ast.ptr, &pWhere), NULL, _OVER);

    TAOS_CHECK_GOTO(filterJobsByWhereCond(pWhere, pArray, &pResult), NULL, _OVER);
    httpRebalanceAuto(pResult);
  } else {
    httpRebalanceAuto(pArray);
  }

_OVER:
  if (pWhere != NULL) {
    nodesDestroyNode(pWhere);
  }
  if (pArray != NULL) {
    taosArrayDestroy(pArray);
  }
  if (pResult != NULL) {
    taosArrayDestroy(pResult);
  }
  tFreeSMRebalanceXnodeJobsWhereReq(&rebalanceReq);
  TAOS_RETURN(code);
}

static int32_t dropXnodeJobById(SMnode *pMnode, SRpcMsg *pReq, int32_t jid) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SXnodeJobObj *pObj = NULL;

  pObj = mndAcquireXnodeJob(pMnode, jid);
  if (pObj == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _OVER;
  }
  code = mndDropXnodeJob(pMnode, pReq, pObj);

_OVER:
  XND_LOG_END(code, lino);
  mndReleaseXnodeJob(pMnode, pObj);
  return code;
}

static int32_t dropXnodeJobByWhereCond(SMnode *pMnode, SRpcMsg *pReq, SMDropXnodeJobReq *dropReq) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SXnodeJobObj *pObj = NULL;
  SNode        *pWhere = NULL;
  SArray       *pArray = NULL;
  SArray       *pResult = NULL;

  if (NULL != dropReq->ast.ptr) {
    TAOS_CHECK_GOTO(mndAcquireXnodeJobsAll(pMnode, &pArray), &lino, _OVER);
    TAOS_CHECK_GOTO(nodesStringToNode(dropReq->ast.ptr, &pWhere), &lino, _OVER);
    TAOS_CHECK_GOTO(filterJobsByWhereCond(pWhere, pArray, &pResult), &lino, _OVER);

    for (int32_t i = 0; i < pResult->size; i++) {
      pObj = taosArrayGet(pResult, i);
      TAOS_CHECK_GOTO(mndDropXnodeJob(pMnode, pReq, pObj), &lino, _OVER);
    }
  }

_OVER:
  XND_LOG_END(code, lino);
  if (pResult != NULL) {
    taosArrayDestroy(pResult);
  }
  if (pWhere != NULL) {
    nodesDestroyNode(pWhere);
  }
  if (pArray != NULL) {
    taosArrayDestroy(pArray);
  }
  return code;
}

static int32_t mndProcessDropXnodeJobReq(SRpcMsg *pReq) {
  mDebug("drop xnode job req, content len:%d", pReq->contLen);
  SMnode           *pMnode = pReq->info.node;
  int32_t           code = -1;
  SMDropXnodeJobReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeJobReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mDebug("Xnode drop job with jid:%d", dropReq.jid);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_DROP_XNODE_JOB), NULL, _OVER);

  if (dropReq.jid <= 0 && dropReq.ast.ptr == NULL) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }
  if (dropReq.jid > 0) {
    TAOS_CHECK_GOTO(dropXnodeJobById(pMnode, pReq, dropReq.jid), NULL, _OVER);
  } else {
    TAOS_CHECK_GOTO(dropXnodeJobByWhereCond(pMnode, pReq, &dropReq), NULL, _OVER);
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drop since %s", dropReq.jid, tstrerror(code));
  }
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
  mDebug("show.type:%d, %s:%d: retrieve xnode jobs with rows: %d", pShow->type, __FILE__, __LINE__, rows);

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

#ifndef WINDOWS
static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp, int32_t timeout, const char *socketPath) {
  CURL   *curl = NULL;
  int32_t code = 0;
  int32_t lino = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_URL, url), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout), &lino, _OVER);

  uDebug("curl get request will sent, url:%s", url);
  CURLcode curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      mError("xnode failed to perform curl action, code:%d", curlCode);
      code = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    uError("failed to perform curl action, code:%d", curlCode);
    code = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  TAOS_CHECK_GOTO(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code), &lino, _OVER);
  if (http_code != 200) {
    code = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  XND_LOG_END(code, lino);
  return code;
}

static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen, int32_t timeout,
                                   const char *socketPath) {
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  int32_t            code = 0;
  int32_t            lino = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    mError("xnode failed to create curl handle");
    return -1;
  }

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_URL, url), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POST, 1), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L), &lino, _OVER);

  mDebug("xnode curl post request will sent, url:%s len:%d content:%s", url, bufLen, buf);
  CURLcode curlCode = curl_easy_perform(curl);

  if (curlCode != CURLE_OK) {
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      mError("xnode failed to perform curl action, code:%d", curlCode);
      code = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    uError("xnode failed to perform curl action, code:%d", curlCode);
    code = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  TAOS_CHECK_GOTO(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code), &lino, _OVER);
  if (http_code != 200) {
    mError("xnode failed to perform curl action, http code:%ld", http_code);
    code = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  XND_LOG_END(code, lino);
  return code;
}

static int32_t taosCurlDeleteRequest(const char *url, SCurlResp *pRsp, int32_t timeout, const char *socketPath) {
  CURL   *curl = NULL;
  int32_t code = 0;
  int32_t lino = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("xnode failed to create curl handle");
    return -1;
  }

  if (curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath)) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_URL, url) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE") != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp) != 0) goto _OVER;
  if (curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout) != 0) goto _OVER;

  uDebug("xnode curl get request will sent, url:%s", url);
  CURLcode curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    uError("xnode failed to perform curl action, curl code:%d", curlCode);
    if (curlCode == CURLE_OPERATION_TIMEDOUT) {
      code = TSDB_CODE_MND_XNODE_URL_RESP_TIMEOUT;
      goto _OVER;
    }
    code = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _OVER;
  }

  long http_code = 0;
  TAOS_CHECK_GOTO(curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code), &lino, _OVER);
  if (http_code != 200 && http_code != 204) {
    uError("xnode curl request response http code:%ld", http_code);
    code = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  XND_LOG_END(code, lino);
  return code;
}
#else
static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp, int32_t timeout, const char *socketPath) { return 0; }
static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen, int32_t timeout,
                                   const char *socketPath) {
  return 0;
}
static int32_t taosCurlDeleteRequest(const char *url, SCurlResp *pRsp, int32_t timeout, const char *socketPath) { return 0; }
#endif
SJson *mndSendReqRetJson(const char *url, EHttpType type, int64_t timeout, const char *buf, int64_t bufLen) {
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};
  char      socketPath[PATH_MAX] = {0};

  getXnodedPipeName(socketPath, sizeof(socketPath));
  if (type == HTTP_TYPE_GET) {
    if ((terrno = taosCurlGetRequest(url, &curlRsp, timeout, socketPath)) != 0) {
      goto _OVER;
    }
  } else if (type == HTTP_TYPE_POST) {
    if ((terrno = taosCurlPostRequest(url, &curlRsp, buf, bufLen, timeout, socketPath)) != 0) {
      goto _OVER;
    }
  } else if (type == HTTP_TYPE_DELETE) {
    if ((terrno = taosCurlDeleteRequest(url, &curlRsp, timeout, socketPath)) != 0) {
      goto _OVER;
    }
  } else {
    uError("xnode invalid http type:%d", type);
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
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("xnode failed to send request, url: %s, since:%s", url, tstrerror(terrno));
  }
  return pJson;
}

static int32_t mndGetXnodeStatus(SXnodeObj *pObj, char *status, int32_t statusLen) {
  int32_t code = 0;
  SJson  *pJson = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/xnode/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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

/** xnode agent section **/

SSdbRaw *mndXnodeAgentActionEncode(SXnodeAgentObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t rawDataLen =
      sizeof(SXnodeAgentObj) + TSDB_XNODE_RESERVE_SIZE + pObj->nameLen + pObj->tokenLen + pObj->statusLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_AGENT, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->nameLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->name, pObj->nameLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->tokenLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->token, pObj->tokenLen, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->statusLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->status, pObj->statusLen, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->createTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_XNODE_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("xnode agent:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("xnode agent:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}

SSdbRow *mndXnodeAgentActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow        *pRow = NULL;
  SXnodeAgentObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_XNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SXnodeAgentObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->nameLen, _OVER)
  if (pObj->nameLen > 0) {
    pObj->name = taosMemoryCalloc(pObj->nameLen, 1);
    if (pObj->name == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->name, pObj->nameLen, _OVER)
  } else {
    pObj->name = NULL;
  }
  SDB_GET_INT32(pRaw, dataPos, &pObj->tokenLen, _OVER)
  if (pObj->tokenLen > 0) {
    pObj->token = taosMemoryCalloc(pObj->tokenLen, 1);
    if (pObj->token == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->token, pObj->tokenLen, _OVER)
  } else {
    pObj->token = NULL;
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
    mError("xnode agent:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->name);
      taosMemoryFreeClear(pObj->token);
      taosMemoryFreeClear(pObj->status);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("xnode agent:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

int32_t mndXnodeAgentActionInsert(SSdb *pSdb, SXnodeAgentObj *pObj) {
  mDebug("xnode agent:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

int32_t mndXnodeAgentActionUpdate(SSdb *pSdb, SXnodeAgentObj *pOld, SXnodeAgentObj *pNew) {
  mDebug("xnode agent:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);

  taosWLockLatch(&pOld->lock);
  swapFields(&pNew->nameLen, &pNew->name, &pOld->nameLen, &pOld->name);
  swapFields(&pNew->tokenLen, &pNew->token, &pOld->tokenLen, &pOld->token);
  swapFields(&pNew->statusLen, &pNew->status, &pOld->statusLen, &pOld->status);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

static void mndFreeXnodeAgent(SXnodeAgentObj *pObj) {
  if (pObj == NULL) return;
  if (pObj->name != NULL) {
    taosMemoryFreeClear(pObj->name);
  }
  if (pObj->token != NULL) {
    taosMemoryFreeClear(pObj->token);
  }
  if (pObj->status != NULL) {
    taosMemoryFreeClear(pObj->status);
  }
}

int32_t mndXnodeAgentActionDelete(SSdb *pSdb, SXnodeAgentObj *pObj) {
  mDebug("xnode agent:%d, perform delete action, row:%p", pObj->id, pObj);
  mndFreeXnodeAgent(pObj);
  return 0;
}

static int32_t mndSetCreateXnodeAgentRedoLogs(STrans *pTrans, SXnodeAgentObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeAgentActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeAgentUndoLogs(STrans *pTrans, SXnodeAgentObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndXnodeAgentActionEncode(pObj);
  if (pUndoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateXnodeAgentCommitLogs(STrans *pTrans, SXnodeAgentObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeAgentActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

void mndReleaseXnodeAgent(SMnode *pMnode, SXnodeAgentObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static int32_t mndValidateXnodePermissions(SMnode *pMnode, SRpcMsg *pReq, EOperType oper) {
  int32_t code = grantCheck(TSDB_GRANT_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create xnode, code:%s", tstrerror(code));
    return code;
  }

  return mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, oper);
}

SXnodeAgentObj *mndAcquireXnodeAgentById(SMnode *pMnode, int32_t id) {
  SXnodeAgentObj *pObj = sdbAcquire(pMnode->pSdb, SDB_XNODE_AGENT, &id);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_XNODE_AGENT_NOT_EXIST;
  }
  return pObj;
}

static SXnodeAgentObj *mndAcquireXnodeAgentByName(SMnode *pMnode, const char *name) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SXnodeAgentObj *pAgent = NULL;
    pIter = sdbFetch(pSdb, SDB_XNODE_AGENT, pIter, (void **)&pAgent);
    if (pIter == NULL) break;
    if (pAgent->name == NULL) {
      continue;
    }

    if (strcasecmp(name, pAgent->name) == 0) {
      sdbCancelFetch(pSdb, pIter);
      return pAgent;
    }

    sdbRelease(pSdb, pAgent);
  }

  mDebug("xnode agent:%s, not found", name);
  terrno = TSDB_CODE_MND_XNODE_AGENT_NOT_EXIST;
  return NULL;
}

static int32_t mndCheckXnodeAgentExists(SMnode *pMnode, const char *name) {
  SXnodeAgentObj *pObj = mndAcquireXnodeAgentByName(pMnode, name);
  if (pObj != NULL) {
    mError("xnode agent:%s already exists", name);
    mndReleaseXnodeAgent(pMnode, pObj);
    return TSDB_CODE_MND_XNODE_AGENT_ALREADY_EXIST;
  }
  terrno = TSDB_CODE_SUCCESS;
  return TSDB_CODE_SUCCESS;
}

#ifndef WINDOWS
typedef struct {
  int64_t sub;  // agent ID
  int64_t iat;  // issued at time
} agentTokenField;

const unsigned char MNDXNODE_DEFAULT_SECRET[] = {126, 222, 130, 137, 43,  122, 41,  173, 144, 146, 116,
                                                 138, 153, 244, 251, 99,  50,  55,  140, 238, 218, 232,
                                                 15,  161, 226, 54,  130, 40,  211, 234, 111, 171};

agentTokenField mndXnodeCreateAgentTokenField(long agent_id, time_t issued_at) {
  agentTokenField field = {0};
  field.sub = agent_id;
  field.iat = issued_at;
  return field;
}

static char *mndXnodeBase64UrlEncodeOpenssl(const unsigned char *input, size_t input_len) {
  __uint64_t code = 0;
  int32_t lino = 0;
  BIO     *bio = NULL, *b64 = NULL;
  BUF_MEM *bufferPtr = NULL;
  char    *base64_str = NULL;

  b64 = BIO_new(BIO_f_base64());
  if (!b64) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }

  bio = BIO_new(BIO_s_mem());
  if (!bio) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }

  // BIO chain:b64 → bio
  bio = BIO_push(b64, bio);
  if (!bio) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }
  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);

  int32_t write_ret = BIO_write(bio, input, input_len);
  if (write_ret <= 0 || (size_t)write_ret != input_len) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }
  int32_t flush_ret = BIO_flush(bio);
  if (flush_ret != 1) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }
  int64_t ret = BIO_get_mem_ptr(bio, &bufferPtr);
  if (ret <= 0) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
}
  if (!bufferPtr || !bufferPtr->data || bufferPtr->length == 0) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }
  base64_str = taosMemoryMalloc(bufferPtr->length + 1);
  if (!base64_str) {
    lino = __LINE__;
    code = ERR_get_error();
    goto _err;
  }
  memcpy(base64_str, bufferPtr->data, bufferPtr->length);
  base64_str[bufferPtr->length] = '\0';
  // url safe
  for (size_t i = 0; i < bufferPtr->length; i++) {
    if (base64_str[i] == '+') {
      base64_str[i] = '-';
    } else if (base64_str[i] == '/') {
      base64_str[i] = '_';
    }
  }
  // remove padding char '='
  size_t len = strlen(base64_str);
  while (len > 0 && base64_str[len - 1] == '=') {
    base64_str[len - 1] = '\0';
    len--;
  }
  goto _exit;

_err:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent: line: %d failed to encode base64 since %s", lino, ERR_error_string(code, NULL));
  }
  if (base64_str) {
    taosMemoryFree(base64_str);
    base64_str = NULL;
  }

_exit:
  if (bio) {
    BIO_free_all(bio);  // will release b64 and bio
  } else if (b64) {
    int32_t ret = BIO_free(b64);
    if (ret != 1) {
      lino = __LINE__;
      code = ERR_get_error();
      mError("xnode agent: line: %d BIO failed to free since %s", lino, ERR_error_string(code, NULL));
    }
  }

  return base64_str;
}

static char *mndXnodeCreateTokenHeader() {
  int32_t code = 0, lino = 0;
  cJSON  *headerJson = NULL;
  char   *headerJsonStr = NULL;
  char   *encoded = NULL;

  headerJson = tjsonCreateObject();
  if (!headerJson) {
    code = terrno;
    goto _exit;
  }

  TAOS_CHECK_EXIT(tjsonAddStringToObject(headerJson, "alg", "HS256"));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(headerJson, "typ", "JWT"));

  headerJsonStr = tjsonToUnformattedString(headerJson);
  if (!headerJsonStr) {
    code = terrno;
    goto _exit;
  }
  encoded = mndXnodeBase64UrlEncodeOpenssl((const unsigned char *)headerJsonStr, strlen(headerJsonStr));
  if (!encoded) {
    code = terrno;
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent: line: %d failed to create header since %s", lino, tstrerror(code));
    taosMemoryFree(encoded);
    encoded = NULL;
  }

  if (headerJsonStr) {
    taosMemoryFree(headerJsonStr);
  }
  if (headerJson) {
    tjsonDelete(headerJson);
  }

  return encoded;
}

static char *mndXnodeCreateTokenPayload(const agentTokenField *claims) {
  int32_t code = 0, lino = 0;
  cJSON  *payloadJson = NULL;
  char   *payloadStr = NULL;
  char   *encoded = NULL;

  if (!claims) {
    code = TSDB_CODE_INVALID_PARA;
    terrno = code;
    return NULL;
  }

  payloadJson = tjsonCreateObject();
  if (!payloadJson) {
    code = terrno;
    goto _exit;
  }

  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(payloadJson, "iat", claims->iat));
  TAOS_CHECK_EXIT(tjsonAddDoubleToObject(payloadJson, "sub", claims->sub));

  payloadStr = tjsonToUnformattedString(payloadJson);
  if (!payloadStr) {
    code = terrno;
    goto _exit;
  }
  encoded = mndXnodeBase64UrlEncodeOpenssl((const unsigned char *)payloadStr, strlen(payloadStr));
  if (!encoded) {
    code = terrno;
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent line: %d failed to create payload since %s", lino, tstrerror(code));
    taosMemoryFree(encoded);
    encoded = NULL;
  }
  if (payloadStr) {
    taosMemoryFree(payloadStr);
  }
  if (payloadJson) {
    tjsonDelete(payloadJson);
  }
  return encoded;
}

static char *mndXnodeCreateTokenSignature(const char *header_payload, const unsigned char *secret, size_t secret_len) {
  int32_t       code = 0, lino = 0;
  unsigned char hash[EVP_MAX_MD_SIZE] = {0};
  unsigned int  hash_len = 0;
  char         *encoded = NULL;

  // HMAC-SHA256
  if (!HMAC(EVP_sha256(), secret, secret_len, (const unsigned char *)header_payload, strlen(header_payload), hash,
            &hash_len)) {
    code = terrno;
    goto _exit;
  }

  encoded = mndXnodeBase64UrlEncodeOpenssl(hash, hash_len);
  if (!encoded) {
    code = terrno;
    goto _exit;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent line: %d failed create signature since %s", lino, tstrerror(code));
    taosMemoryFree(encoded);
    encoded = NULL;
  }
  return encoded;
}

char *mndXnodeCreateAgentToken(const agentTokenField *claims, const unsigned char *secret, size_t secret_len) {
  int32_t code = 0, lino = 0;
  char   *header = NULL, *payload = NULL;
  char   *headerPayload = NULL;
  char   *signature = NULL;
  char   *token = NULL;

  if (!claims) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }

  if (!secret || secret_len == 0) {
    secret = MNDXNODE_DEFAULT_SECRET;
    secret_len = sizeof(MNDXNODE_DEFAULT_SECRET);
  }

  header = mndXnodeCreateTokenHeader();
  if (!header) {
    code = terrno;
    goto _exit;
  }

  payload = mndXnodeCreateTokenPayload(claims);
  if (!payload) {
    code = terrno;
    goto _exit;
  }

  size_t header_payload_len = strlen(header) + strlen(payload) + 2;
  headerPayload = taosMemoryMalloc(header_payload_len);
  if (!headerPayload) {
    code = terrno;
    goto _exit;
  }
  snprintf(headerPayload, header_payload_len, "%s.%s", header, payload);

  signature = mndXnodeCreateTokenSignature(headerPayload, secret, secret_len);
  if (!signature) {
    code = terrno;
    goto _exit;
  }

  size_t token_len = strlen(headerPayload) + strlen(signature) + 2;
  token = taosMemoryCalloc(1, token_len);
  if (!token) {
    code = terrno;
    goto _exit;
  }

  snprintf(token, token_len, "%s.%s", headerPayload, signature);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent line: %d failed create token since %s", lino, tstrerror(code));
    taosMemoryFree(token);
    token = NULL;
  }
  taosMemoryFree(signature);
  taosMemoryFree(headerPayload);
  taosMemoryFree(payload);
  taosMemoryFree(header);

  return token;
}
#endif

int32_t mndXnodeGenAgentToken(const SXnodeAgentObj *pAgent, char *pTokenBuf) {
  int32_t code = 0, lino = 0;
  #ifndef WINDOWS
  // char *token =
  //     "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Njc1OTc3NzIsInN1YiI6MTIzNDV9.i7HvYf_S-yWGEExDzQESPUwVX23Ok_"
  //     "7Fxo93aqgKrtw";
  agentTokenField claims = {
      .iat = pAgent->createTime,
      .sub = pAgent->id,
  };
  char *token = mndXnodeCreateAgentToken(&claims, MNDXNODE_DEFAULT_SECRET, sizeof(MNDXNODE_DEFAULT_SECRET));
  if (!token) {
    code = terrno;
    lino = __LINE__;
    goto _exit;
  }
  (void)memcpy(pTokenBuf, token, TMIN(strlen(token) + 1, TSDB_XNODE_AGENT_TOKEN_LEN));

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mError("xnode agent line: %d failed gen token since %s", lino, tstrerror(code));
  }
  taosMemoryFree(token);
  #endif
  TAOS_RETURN(code);
}

static int32_t mndCreateXnodeAgent(SMnode *pMnode, SRpcMsg *pReq, SMCreateXnodeAgentReq *pCreate,
                                   SXnodeAgentObj **ppObj) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  if ((*ppObj) == NULL) {
    *ppObj = taosMemoryCalloc(1, sizeof(SXnodeAgentObj));
    if (*ppObj == NULL) {
      code = terrno;
      goto _OVER;
    }
  }
  SXnodeAgentObj *pAgentObj = *ppObj;

  pAgentObj->id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_AGENT);
  pAgentObj->createTime = taosGetTimestampMs();
  pAgentObj->updateTime = pAgentObj->createTime;

  pAgentObj->nameLen = pCreate->name.len + 1;
  pAgentObj->name = taosMemoryCalloc(1, pAgentObj->nameLen);
  if (pAgentObj->name == NULL) goto _OVER;
  (void)memcpy(pAgentObj->name, pCreate->name.ptr, pCreate->name.len);

  if (pCreate->status.ptr != NULL) {
    pAgentObj->statusLen = pCreate->status.len + 1;
    pAgentObj->status = taosMemoryCalloc(1, pAgentObj->statusLen);
    if (pAgentObj->status == NULL) goto _OVER;
    (void)memcpy(pAgentObj->status, pCreate->status.ptr, pCreate->status.len);
  }
  // gen token
  char token[TSDB_XNODE_AGENT_TOKEN_LEN] = {0};
  TAOS_CHECK_GOTO(mndXnodeGenAgentToken(pAgentObj, token), NULL, _OVER);
  pAgentObj->tokenLen = strlen(token) + 1;
  pAgentObj->token = taosMemoryCalloc(1, pAgentObj->tokenLen);
  if (pAgentObj->token == NULL) goto _OVER;
  (void)memcpy(pAgentObj->token, token, pAgentObj->tokenLen - 1);

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-xnode-agent");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) {
      code = terrno;
    }
    mError("failed to create transaction for xnode-agent:%s, code:0x%x:%s", pCreate->name.ptr, code, tstrerror(code));
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mDebug("trans:%d, used to create xnode agent:%s as agent:%d", pTrans->id, pCreate->name.ptr, pAgentObj->id);
  TAOS_CHECK_GOTO(mndSetCreateXnodeAgentRedoLogs(pTrans, pAgentObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeAgentUndoLogs(pTrans, pAgentObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeAgentCommitLogs(pTrans, pAgentObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t httpCreateAgent(SXnodeAgentObj *pObj) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *pContStr = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/agent", XNODED_PIPE_SOCKET_URL);
  postContent = tjsonCreateObject();
  if (postContent == NULL) {
    code = terrno;
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "token", pObj->token), NULL, _OVER);
  pContStr = tjsonToUnformattedString(postContent);
  if (pContStr == NULL) {
    code = terrno;
    goto _OVER;
  }
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, defaultTimeout, pContStr, strlen(pContStr));

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

static int32_t mndProcessCreateXnodeAgentReq(SRpcMsg *pReq) {
  SMnode               *pMnode = pReq->info.node;
  int32_t               code = 0;
  SXnodeAgentObj       *pObj = NULL;
  SMCreateXnodeAgentReq createReq = {0};

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_CREATE_XNODE_AGENT);
  if (code != TSDB_CODE_SUCCESS) {
    goto _OVER;
  }

  code = tDeserializeSMCreateXnodeAgentReq(pReq->pCont, pReq->contLen, &createReq);
  if (code != 0) {
    mError("failed to deserialize create xnode agent request, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndCheckXnodeAgentExists(pMnode, createReq.name.ptr), NULL, _OVER);

  TAOS_CHECK_GOTO(mndCreateXnodeAgent(pMnode, pReq, &createReq, &pObj), NULL, _OVER);

  (void)httpCreateAgent(pObj);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode agent:%s, failed to create since %s", createReq.name.ptr ? createReq.name.ptr : "unknown",
           tstrerror(code));
  }
  if (pObj != NULL) {
    mndFreeXnodeAgent(pObj);
    taosMemoryFree(pObj);
  }
  tFreeSMCreateXnodeAgentReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndUpdateXnodeAgent(SMnode *pMnode, SRpcMsg *pReq, const SXnodeAgentObj *pOld,
                                   SMUpdateXnodeAgentReq *pUpdate) {
  mDebug("xnode agent:%d, start to update", pUpdate->id);
  int32_t        code = -1;
  STrans        *pTrans = NULL;
  struct {
    bool status;
    bool name;
  } isChange = {0};
  SXnodeAgentObj agentObjRef = *pOld;

  const char *status = getXTaskOptionByName(&pUpdate->options, "status");
  if (status != NULL) {
    isChange.status = true;
    agentObjRef.statusLen = strlen(status) + 1;
    agentObjRef.status = taosMemoryCalloc(1, agentObjRef.statusLen);
    if (agentObjRef.status == NULL) goto _OVER;
    (void)memcpy(agentObjRef.status, status, agentObjRef.statusLen);
  }
  const char *name = getXTaskOptionByName(&pUpdate->options, "name");
  if (name != NULL) {
    isChange.name = true;
    agentObjRef.nameLen = strlen(name) + 1;
    agentObjRef.name = taosMemoryCalloc(1, agentObjRef.nameLen);
    if (agentObjRef.name == NULL) goto _OVER;
    (void)memcpy(agentObjRef.name, name, agentObjRef.nameLen);
  }
  agentObjRef.updateTime = taosGetTimestampMs();

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-xnode-agent");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update xnode agent:%d", pTrans->id, agentObjRef.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeAgentCommitLogs(pTrans, &agentObjRef), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = 0;

_OVER:
  if (isChange.status) {
    taosMemoryFree(agentObjRef.status);
  }
  if (isChange.name) {
    taosMemoryFree(agentObjRef.name);
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeAgentReq(SRpcMsg *pReq) {
  SMnode               *pMnode = pReq->info.node;
  int32_t               code = -1;
  SXnodeAgentObj       *pObj = NULL;
  SMUpdateXnodeAgentReq updateReq = {0};

  if ((code = grantCheck(TSDB_GRANT_XNODE)) != TSDB_CODE_SUCCESS) {
    mError("failed grant update xnode agent, code:%s", tstrerror(code));
    goto _OVER;
  }

  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeAgentReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);
  mDebug("xnode update agent request id:%d, nameLen:%d\n", updateReq.id, updateReq.name.len);

  if (updateReq.id <= 0 && (updateReq.name.len <= 0 || updateReq.name.ptr == NULL)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (updateReq.id > 0) {
    pObj = mndAcquireXnodeAgentById(pMnode, updateReq.id);
  } else {
    pObj = mndAcquireXnodeAgentByName(pMnode, updateReq.name.ptr);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  const char *nameRef = getXTaskOptionByName(&updateReq.options, "name");
  if (nameRef != NULL) {
    SXnodeAgentObj* tmpObj = mndAcquireXnodeAgentByName(pMnode, nameRef);
    if (tmpObj != NULL) {
      mndReleaseXnodeAgent(pMnode, tmpObj);
      code = TSDB_CODE_MND_XNODE_NAME_DUPLICATE;
      goto _OVER;
    }
  }

  code = mndUpdateXnodeAgent(pMnode, pReq, pObj, &updateReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode agent:%d, failed to update since %s", updateReq.id, tstrerror(code));
  }

  mndReleaseXnodeAgent(pMnode, pObj);
  tFreeSMUpdateXnodeAgentReq(&updateReq);
  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeAgentRedoLogs(STrans *pTrans, SXnodeAgentObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndXnodeAgentActionEncode(pObj);
  if (pRedoRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  TAOS_RETURN(code);
}

static int32_t mndSetDropXnodeAgentCommitLogs(STrans *pTrans, SXnodeAgentObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndXnodeAgentActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = terrno;
    return code;
  }

  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndDropXnodeAgent(SMnode *pMnode, SRpcMsg *pReq, SXnodeAgentObj *pAgent) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pAgent == NULL) {
    mError("xnode agent fail to drop since pAgent is NULL");
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drop-xnode-agent");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);
  mndTransSetSerial(pTrans);
  mDebug("trans:%d, to drop xnode agent:%d", pTrans->id, pAgent->id);

  TAOS_CHECK_GOTO(mndSetDropXnodeAgentRedoLogs(pTrans, pAgent), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropXnodeAgentCommitLogs(pTrans, pAgent), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeAgentReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeAgentObj     *pObj = NULL;
  SMDropXnodeAgentReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeAgentReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);
  mDebug("xnode drop agent with id:%d, start to drop", dropReq.id);

  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, NULL, MND_OPER_DROP_XNODE_AGENT), NULL, _OVER);

  if (dropReq.id <= 0 && (dropReq.name.len <= 0 || dropReq.name.ptr == NULL)) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }

  if (dropReq.name.len > 0 && dropReq.name.ptr != NULL) {
    pObj = mndAcquireXnodeAgentByName(pMnode, dropReq.name.ptr);
  } else {
    pObj = mndAcquireXnodeAgentById(pMnode, dropReq.id);
  }
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }

  // send request to drop xnode task
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN, "%s/agent/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
  SJson *pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);
  if (pJson) {
    tjsonDelete(pJson);
  }

  code = mndDropXnodeAgent(pMnode, pReq, pObj);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to drop since %s", dropReq.id, tstrerror(code));
  }
  mndReleaseXnodeAgent(pMnode, pObj);
  tFreeSMDropXnodeAgentReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveXnodeAgents(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t         code = 0;
  SMnode         *pMnode = pReq->info.node;
  SSdb           *pSdb = pMnode->pSdb;
  int32_t         numOfRows = 0;
  int32_t         cols = 0;
  char            buf[VARSTR_HEADER_SIZE + TSDB_XNODE_AGENT_TOKEN_LEN];
  SXnodeAgentObj *pObj = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_AGENT, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pObj->id, false);
    if (code != 0) goto _end;

    STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->name, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
    if (code != 0) goto _end;

    if (pObj->tokenLen > 0) {
      buf[0] = 0;
      STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->token, pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      if (code != 0) goto _end;
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetNULL(pColInfo, numOfRows);
    }

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

static void mndCancelGetNextXnodeAgent(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_XNODE_AGENT);
}

/** xnoded mgmt section **/

void mndStartXnoded(SMnode *pMnode, const char *user, const char *pass, const char *token) {
  int32_t   code = 0;
  SXnodeOpt pOption = {0};

  if ((user == NULL || pass == NULL) && token == NULL) {
    mError("xnode failed to start xnoded, dnode:%d", pMnode->selfDnodeId);
    return;
  }

  pOption.dnodeId = pMnode->selfDnodeId;
  pOption.clusterId = pMnode->clusterId;

  SEpSet epset = mndGetDnodeEpsetById(pMnode, pMnode->selfDnodeId);
  if (epset.numOfEps == 0) {
    mError("xnode failed to start xnoded, dnode:%d", pMnode->selfDnodeId);
    return;
  }
  pOption.ep = epset.eps[0];
  // add user password
  if (user != NULL && pass != NULL) {
    pOption.upLen = strlen(user) + strlen(pass) + 1;
    snprintf(pOption.userPass, XNODE_USER_PASS_LEN, "%s:%s", user, pass);
  }
  // add token
  if (token != NULL) {
    snprintf(pOption.token, TSDB_TOKEN_LEN, "%s", token);
  }
  if ((code = mndOpenXnd(&pOption)) != 0) {
    mError("xnode failed to open xnd since %s, dnodeId:%d", tstrerror(code), pOption.dnodeId);
    return;
  }
}

void mndXnodeHandleBecomeLeader(SMnode *pMnode) {
  mInfo("mndxnode start to process mnode become leader");
  SXnodeUserPassObj *pObj = mndAcquireFirstXnodeUserPass(pMnode);
  if (pObj == NULL) {
    mInfo("mndXnode found no xnoded user pass");
    return;
  }

  mndStartXnoded(pMnode, pObj->user, pObj->pass, pObj->token);
  mndReleaseXnodeUserPass(pMnode, pObj);
}

void mndXnodeHandleBecomeNotLeader() {
  mInfo("mndxnode handle mnode become not leader");
  mndCloseXnd();
}

void mndRestartXnoded(SMnode *pMnode) {
  mInfo("mndxnode restart xnoded");
  mndCloseXnd();

  SXnodeUserPassObj *pObj = mndAcquireFirstXnodeUserPass(pMnode);
  if (pObj == NULL) {
    mInfo("mndXnode found no xnoded user pass");
    return;
  }
  mndStartXnoded(pMnode, pObj->user, pObj->pass, pObj->token);
  mndReleaseXnodeUserPass(pMnode, pObj);
  mInfo("mndxnode xnoded restarted");
  return;
}