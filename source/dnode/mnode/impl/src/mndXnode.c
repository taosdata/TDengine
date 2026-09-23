/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <curl/curl.h>
#ifndef WINDOWS
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#else
#include "tsha.h"
#include "tbase64.h"
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
#include "tglobal.h"
#include "thash.h"
#include "tjson.h"
#include "xnode.h"
#include "osMemory.h"
#include <cJSON.h>
#ifndef WINDOWS
#include "envelope_crypto.h"
#endif

#define TSDB_XNODE_RESERVE_SIZE 64
#ifdef WINDOWS
#define XNODED_PIPE_SOCKET_URL "http://localhost:6051"
#else
#define XNODED_PIPE_SOCKET_URL "http://localhost"
#endif
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
const int32_t migrationTimeout = 60000;
static const char xnodeDrainStatus[] = "drain";

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

static int32_t mndXnodeCheckCreateTaskSixStar(const SMCreateXnodeTaskReq *pCreate);
static int32_t mndXnodeCheckCreateJobSixStar(const SMCreateXnodeJobReq *pCreate);
static int32_t mndXnodeMergeUpdateTaskSixStar(const SXnodeTaskObj *pOld, const SMUpdateXnodeTaskReq *pUpdate,
                                              char **ppMergedSource, int32_t *pMergedSourceLen, char **ppMergedSink,
                                              int32_t *pMergedSinkLen);
static int32_t mndXnodeMergeUpdateJobSixStar(const SXnodeJobObj *pOld, const SMUpdateXnodeJobReq *pUpdate,
                                             char **ppMergedConfig, int32_t *pMergedConfigLen);

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
static int32_t mndValidateXnodePermissions(SMnode *pMnode, SRpcMsg *pReq, EOperType oper);
static int32_t mndXnodeCheckTaskObjPrivilegeByUser(SMnode *pMnode, SUserObj *curUser,
                                                   SXnodeTaskObj *pTask, EPrivType priv);
static int32_t mndXnodeCheckTaskObjPrivilege(SMnode *pMnode, const char *user, SXnodeTaskObj *pTask,
                                             EPrivType priv);
static int32_t mndXnodeCheckJobObjPrivilegeByUser(SMnode *pMnode, SUserObj *curUser,
                                                  SXnodeJobObj *pJob, EPrivType priv);
static int32_t mndXnodeCheckJobObjPrivilege(SMnode *pMnode, const char *user, SXnodeJobObj *pJob, EPrivType priv);

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
  if (tsEnableAdvancedSecurity == 0 && strcmp(pwd, "taosdata") == 0) {
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
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SXnodeObj xnodeObj = {0};
  xnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE);

  xnodeObj.urlLen = pCreate->urlLen;
  if (xnodeObj.urlLen > TSDB_XNODE_URL_LEN) {
    code = TSDB_CODE_MND_XNODE_TOO_LONG_URL;
    lino = __LINE__;
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
    lino = __LINE__;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode:%s as xnode:%d", pTrans->id, pCreate->url, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeRedoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUndoLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:failed create xnode since %s, line:%d", tstrerror(code), lino);
  }
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
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SXnodeUserPassObj upObj = {0};
  upObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_USER_PASS);

  if (pCreate->user) {
    upObj.userLen = pCreate->userLen;
    if (upObj.userLen > TSDB_USER_LEN) {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
      lino = __LINE__;
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
      lino = __LINE__;
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
    lino = __LINE__;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create xnode:%s as xnode:%d", pTrans->id, pCreate->url, upObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassRedoLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassCommitLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode failed to store userpass since %s, line:%d", tstrerror(code), lino);
  }
  taosMemoryFreeClear(upObj.user);
  taosMemoryFreeClear(upObj.pass);
  taosMemoryFreeClear(upObj.token);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

#ifndef TD_ENTERPRISE

int32_t mndXnodeCreateDefaultToken(SRpcMsg* pReq, char** ppToken) {
  mError("failed to create xnode default token since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

#endif

static int32_t httpCreateXnode(SXnodeObj *pObj) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *pContStr = NULL;

  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/xnode", XNODED_PIPE_SOCKET_URL);
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
  bool             isSuperUser = false;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_CREATE_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for create xnode, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeReq(pReq->pCont, pReq->contLen, &createReq), &lino, _OVER);
  mDebug("xnode:%s, start to create", createReq.url);

  pObj = mndAcquireXnodeByURL(pMnode, createReq.url);
  if (pObj != NULL) {
    code = TSDB_CODE_MND_XNODE_ALREADY_EXIST;
    goto _OVER;
  }

  int32_t numOfRows = sdbGetSize(pMnode->pSdb, SDB_XNODE_USER_PASS);
  if (numOfRows <= 0) {
    if (createReq.token.ptr != NULL) {
      (void)mndCreateXnode(pMnode, NULL, &createReq);
      code = mndStoreXnodeUserPassToken(pMnode, pReq, &createReq);
      if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
        lino = __LINE__;
        goto _OVER;
      }
      mndStartXnoded(pMnode, NULL, NULL, createReq.token.ptr);
    } else if (createReq.user != NULL) {
      TAOS_CHECK_GOTO(xnodeCheckPasswordFmt(createReq.pass), &lino, _OVER);
      (void)mndCreateXnode(pMnode, NULL, &createReq);
      code = mndStoreXnodeUserPassToken(pMnode, pReq, &createReq);
      if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
        lino = __LINE__;
        goto _OVER;
      }
      mndStartXnoded(pMnode, createReq.user, createReq.pass, NULL);
    } else {
      code = mndXnodeCreateDefaultToken(pReq, &pToken);
      if (code == TSDB_CODE_MND_TOKEN_ALREADY_EXIST) {
        code = 0;
      }
      if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
        lino = __LINE__;
        goto _OVER;
      }
      (void)mndCreateXnode(pMnode, NULL, &createReq);
      createReq.token = xCreateCowStr(strlen(pToken), pToken, false);
      (void)mndStoreXnodeUserPassToken(pMnode, NULL, &createReq);
      mndStartXnoded(pMnode, NULL, NULL, createReq.token.ptr);
    }
  } else {
    code = mndCreateXnode(pMnode, pReq, &createReq);
    if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      lino = __LINE__;
      goto _OVER;
    }
  }

  taosMsleep(defaultTimeout);
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
    code = terrno;
    lino = __LINE__;
    goto _OVER;
  }
  mDebug("trans:%d, used to update xnode userpass or token:%d", pTrans->id, upObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeUserPassCommitLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:failed to update xnode %d since %s, line:%d", upObj.id, tstrerror(code), lino);
  }
  taosMemoryFreeClear(upObj.user);
  taosMemoryFreeClear(upObj.pass);
  taosMemoryFreeClear(upObj.token);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = 0;
  SXnodeObj       *pObj = NULL;
  SMUpdateXnodeReq updateReq = {0};
  bool             isSuperUser = false;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_UPDATE_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for update xnode, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);

  if (updateReq.token.ptr != NULL || (updateReq.user.ptr != NULL && updateReq.pass.ptr != NULL)) {
    code = mndUpdateXnodeUserPassToken(pMnode, pReq, &updateReq);
    if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      goto _OVER;
    }
    mndRestartXnoded(pMnode);
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to update since %s", updateReq.id, tstrerror(code));
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

static int32_t mndCheckXnodedResponse(SRpcMsg *pReq, SJson *pJson) {
  if (pJson == NULL) {
    return terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR : terrno;
  }

  SJson *errorJson = tjsonGetObjectItem(pJson, "__inner_error");
  if (errorJson == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char *error = ((cJSON *)errorJson)->valuestring;
  if (error != NULL) {
    mError("xnoded request failed, reason:%s", error);
    if (pReq != NULL) {
      int32_t contLen = strlen(error) + strlen(tstrerror(TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR)) + 32;
      void   *pRsp = rpcMallocCont(contLen);
      if (pRsp == NULL) {
        return terrno;
      }
      pReq->info.rspLen = contLen;
      pReq->info.rsp = pRsp;
      snprintf(pReq->info.rsp, contLen, "%s, since: %s", tstrerror(TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR),
               error);
    }
  }
  return TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
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
  xnodeObj.status = (char *)xnodeDrainStatus;
  xnodeObj.statusLen = strlen(xnodeObj.status) + 1;
  xnodeObj.updateTime = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "drain-xnode");
  TSDB_CHECK_NULL(pTrans, code, lino, _OVER, terrno);

  mndTransSetSerial(pTrans);
  mDebug("trans:%d, to drain xnode:%d", pTrans->id, xnodeObj.id);

  TAOS_CHECK_GOTO(mndSetCreateXnodeCommitLogs(pTrans, &xnodeObj), NULL, _OVER);
  code = mndTransPrepare(pMnode, pTrans);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode:%d, failed to drain since %s", xnodeObj.id, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropXnodeReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SXnodeObj     *pObj = NULL;
  SMDropXnodeReq dropReq = {0};
  SJson         *pJson = NULL;
  bool           isSuperUser = false;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_DROP_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for drop xnode, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);
  mDebug("xnode:%d, start to drop", dropReq.xnodeId);

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
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, migrationTimeout, NULL, 0);
  code = mndCheckXnodedResponse(dropReq.force ? NULL : pReq, pJson);
  if (code != TSDB_CODE_SUCCESS) {
    if (!dropReq.force) goto _OVER;
    mWarn("xnode:%d, force drop continues after xnoded request failed, code:0x%x:%s", pObj->id, code,
          tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    terrno = TSDB_CODE_SUCCESS;
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
  int32_t         code = 0;
  SXnodeObj      *pObj = NULL;
  SMDrainXnodeReq drainReq = {0};
  SJson          *pJson = NULL;
  SJson          *postContent = NULL;
  char           *pContStr = NULL;
  bool            isSuperUser = false;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_DRAIN_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for drain xnode, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMDrainXnodeReq(pReq->pCont, pReq->contLen, &drainReq), NULL, _OVER);
  mDebug("xnode:%d, start to drain", drainReq.xnodeId);

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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/xnode/drain/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, migrationTimeout, pContStr, strlen(pContStr));
  TAOS_CHECK_GOTO(mndCheckXnodedResponse(pReq, pJson), NULL, _OVER);

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

    if (mndGetXnodeShowStatus(pObj, status, TSDB_XNODE_STATUS_LEN) == 0) {
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

int32_t mndGetXnodeShowStatus(SXnodeObj *pObj, char *status, int32_t statusLen) {
  bool usePersistedStatus = false;

  taosRLockLatch(&pObj->lock);
  usePersistedStatus = pObj->status != NULL && strcmp(pObj->status, xnodeDrainStatus) == 0;
  if (usePersistedStatus) {
    tstrncpy(status, pObj->status, statusLen);
  }
  taosRUnLockLatch(&pObj->lock);

  if (usePersistedStatus) {
    return 0;
  }
  return mndGetXnodeStatus(pObj, status, statusLen);
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
      sdbRelease(pSdb, pTask);
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

  int32_t totalStrLen = pObj->nameLen + pObj->sourceDsnLen + pObj->sinkDsnLen + pObj->parserLen + pObj->reasonLen +
                        pObj->statusLen + pObj->createdByLen + pObj->labelsLen;
  int32_t rawDataLen = sizeof(SXnodeTaskObj) + TSDB_XNODE_RESERVE_SIZE + totalStrLen +
                       sizeof(int64_t) + sizeof(int32_t);

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
  SDB_SET_INT64(pRaw, dataPos, pObj->ownerId, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->acctId, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

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
  // Legacy rows rely on zero-filled allocation slack; retain this guard for truncated rows.
  if (dataPos + sizeof(int64_t) + sizeof(int32_t) <= pRaw->dataLen) {
    SDB_GET_INT64(pRaw, dataPos, &pObj->ownerId, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &pObj->acctId, _OVER)
  }

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
  SUserObj *pOperUser = NULL;
  SXnodeTaskObj xnodeObj = {0};

  // Six-star write-queue authority: reject control markers before the final
  // object exists and before any commit log is encoded.
  TAOS_CHECK_GOTO(mndXnodeCheckCreateTaskSixStar(pCreate), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser), NULL, _OVER);
  xnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_TASK);
  xnodeObj.createTime = taosGetTimestampMs();
  xnodeObj.updateTime = xnodeObj.createTime;
  xnodeObj.via = pCreate->options.via;
  xnodeObj.xnodeId = pCreate->xnodeId;
  xnodeObj.ownerId = pOperUser->uid;
  xnodeObj.acctId = pOperUser->acctId;

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

  if (pCreate->options.parser.ptr != NULL) {
    if (pCreate->options.parser.len > TSDB_XNODE_TASK_PARSER_MAX_LEN) {
      code = TSDB_CODE_MND_XNODE_TASK_PARSER_TOO_LONG;
      goto _OVER;
    }
    xnodeObj.parserLen = pCreate->options.parser.len + 1;
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
  mndReleaseUser(pMnode, pOperUser);
  mndFreeXnodeTask(&xnodeObj);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// Helper function to parse and validate the request
static int32_t mndValidateCreateXnodeTaskReq(SRpcMsg *pReq, SMCreateXnodeTaskReq *pCreateReq) {
  int32_t code = 0;
  SJson  *pJson = NULL;
  SJson  *postContent = NULL;
  char   *srcDsn = NULL;
  char   *sinkDsn = NULL;
  char   *pContStr = NULL;

  // from, to check
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

  if (pCreateReq->xnodeId > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "xnode_id", (double)pCreateReq->xnodeId), NULL, _OVER);
  }

  if (pCreateReq->options.via > 0) {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(postContent, "via", (double)pCreateReq->options.via), NULL, _OVER);
  }

  const char *labels = getXTaskOptionByName(&pCreateReq->options, "labels");
  if (labels != NULL) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "labels", labels), NULL, _OVER);
  }

  TAOS_CHECK_GOTO(tjsonAddStringToObject(postContent, "created_by", pReq->info.conn.user), NULL, _OVER);

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
  SJson *errorJson = tjsonGetObjectItem(pJson, "__inner_error");
  if (errorJson != NULL) {
    code = TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR;
    char* pValueString = ((cJSON*)errorJson)->valuestring;
    if (NULL == pValueString) {
      mError("should not failed to get __inner_error message, task name:%s", pCreateReq->name.ptr);
      goto _OVER;
    }
    //handle response
    int32_t contLen = strlen(pValueString) + strlen(tstrerror(code)) + 32;
    void *pRsp = rpcMallocCont(contLen);
    if (pRsp == NULL) {
      TAOS_CHECK_GOTO(terrno, NULL, _OVER);
    }
    pReq->info.rspLen = contLen;
    pReq->info.rsp = pRsp;
    snprintf(pReq->info.rsp, contLen, "%s, since: %s", tstrerror(code), pValueString);
    goto _OVER;
  }

  // todo: only4test
  // (void)mndSendReqRetJson(xnodeUrl, HTTP_TYPE_POST, 60000, pContStr, strlen(pContStr));
  // code = TSDB_CODE_SUCCESS;

_OVER:
  if (srcDsn != NULL) taosMemoryFreeClear(srcDsn);
  if (sinkDsn != NULL) taosMemoryFreeClear(sinkDsn);
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
  int32_t              code = 0;
  SMCreateXnodeTaskReq createReq = {0};

  // Step 1: Validate permissions
  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_CREATE_XNODE_TASK);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for create xnode task, code:%s", tstrerror(code));
    goto _OVER;
  }

  code = tDeserializeSMCreateXnodeTaskReq(pReq->pCont, pReq->contLen, &createReq);
  if (code != 0) {
    mError("failed to deserialize create xnode task request, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  // Step 2: Check if task already exists
  TAOS_CHECK_GOTO(mndCheckXnodeTaskExists(pMnode, createReq.name.ptr), NULL, _OVER);

  // Six-star rejection must happen before the candidate DSN is sent to the
  // xnoded /task/check connector; mndCreateXnodeTask repeats the same check at
  // the write-queue boundary.
  TAOS_CHECK_GOTO(mndXnodeCheckCreateTaskSixStar(&createReq), NULL, _OVER);

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

  snprintf(req.xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/task/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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
  mDebug("start xnode task:%d, request bytes:%zu", pObj->id, strlen(req.pContStr));
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
  int32_t             code = 0;
  SXnodeTaskObj      *pObj = NULL;
  SMStartXnodeTaskReq startReq = {0};
  SXnodeTaskObj      *pObjClone = NULL;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_START_XNODE_TASK);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for start xnode task, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMStartXnodeTaskReq(pReq->pCont, pReq->contLen, &startReq), NULL, _OVER);
  mDebug("xnode start xnode task with tid:%d", startReq.tid);

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
  code = mndXnodeCheckTaskObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode task:%d", pReq->info.conn.user, pObj->id);
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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_STOP_XNODE_TASK);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for stop xnode task, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMStopXnodeTaskReq(pReq->pCont, pReq->contLen, &stopReq), NULL, _OVER);
  mDebug("Stop xnode task with tid:%d", stopReq.tid);

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
  code = mndXnodeCheckTaskObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode task:%d", pReq->info.conn.user, pObj->id);
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/task/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
  pJson = mndSendReqRetJson(xnodeUrl, HTTP_TYPE_DELETE, defaultTimeout, NULL, 0);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task:%d, failed to stop since %s", stopReq.tid, tstrerror(code));
  }
  if (pJson != NULL) {
    tjsonDelete(pJson);
  }
  mndReleaseXnodeTask(pMnode, pObj);
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
  char   *mergedSource = NULL;
  char   *mergedSink = NULL;
  int32_t mergedSourceLen = 0;
  int32_t mergedSinkLen = 0;

  // Six-star KEEP/REPLACE merge against the committed object, before the final
  // object is assembled and the commit log is encoded.
  code = mndXnodeMergeUpdateTaskSixStar(pOld, pUpdate, &mergedSource, &mergedSourceLen, &mergedSink, &mergedSinkLen);
  if (code != TSDB_CODE_SUCCESS) goto _OVER;

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
    if (mergedSource != NULL) {
      taskObj.sourceDsnLen = mergedSourceLen + 1;
      taskObj.sourceDsn = mergedSource;
      mergedSource = NULL;
    } else {
      taskObj.sourceDsnLen = pUpdate->source.cstr.len + 1;
      taskObj.sourceDsn = taosMemoryCalloc(1, taskObj.sourceDsnLen);
      if (taskObj.sourceDsn == NULL) {
        code = terrno;
        goto _OVER;
      }
      (void)memcpy(taskObj.sourceDsn, pUpdate->source.cstr.ptr, pUpdate->source.cstr.len);
    }
    isChange.source = true;
  }
  if (pUpdate->sink.cstr.ptr != NULL) {
    taskObj.sinkType = pUpdate->sink.type;
    if (mergedSink != NULL) {
      taskObj.sinkDsnLen = mergedSinkLen + 1;
      taskObj.sinkDsn = mergedSink;
      mergedSink = NULL;
    } else {
      taskObj.sinkDsnLen = pUpdate->sink.cstr.len + 1;
      taskObj.sinkDsn = taosMemoryCalloc(1, taskObj.sinkDsnLen);
      if (taskObj.sinkDsn == NULL) {
        code = terrno;
        goto _OVER;
      }
      (void)memcpy(taskObj.sinkDsn, pUpdate->sink.cstr.ptr, pUpdate->sink.cstr.len);
    }
    isChange.sink = true;
  }
  if (pUpdate->parser.ptr != NULL) {
    if (pUpdate->parser.len > TSDB_XNODE_TASK_PARSER_MAX_LEN) {
      code = TSDB_CODE_MND_XNODE_TASK_PARSER_TOO_LONG;
      goto _OVER;
    }
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
  taosMemoryFree(mergedSource);
  taosMemoryFree(mergedSink);
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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_UPDATE_XNODE_TASK);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for update xnode task, code:%s", tstrerror(code));
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
  code = mndXnodeCheckTaskObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode task:%d", pReq->info.conn.user, pObj->id);
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

  code = mndSetDropXnodeTaskInfoToTrans(pMnode, pTrans, pTask, false);

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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_DROP_XNODE_TASK);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for drop xnode task, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeTaskReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);
  mDebug("DropXnodeTask with tid:%d, start to drop", dropReq.id);

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
  code = mndXnodeCheckTaskObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_DROP);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode task:%d", pReq->info.conn.user, pObj->id);
    goto _OVER;
  }

  // send request to drop xnode task
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/task/drop/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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
  mndReleaseXnodeTask(pMnode, pObj);
  tFreeSMDropXnodeTaskReq(&dropReq);
  TAOS_RETURN(code);
}
int32_t mndSetXnodeBlobColumn(SColumnInfoData *pColInfo, int32_t row, const char *data, int32_t storedLen) {
  if (data == NULL || storedLen <= 0) {
    colDataSetNULL(pColInfo, row);
    return 0;
  }

  int32_t contentLen = storedLen - 1;
  while (contentLen > 0 && data[contentLen - 1] == '\0') {
    --contentLen;
  }

  return varColSetVarData(pColInfo, row, data, contentLen, false);
}

static int32_t mndXnodeProtectionError(void) { return TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED; }

#define XNODE_SHOW_MASK_VALUE     "******"
#define XNODE_SHOW_MASK_VALUE_LEN 6
// The hand-rolled JSON scanner below replaced the cJSON document parse and must
// keep the same nesting bound the DOM parser used to enforce (CJSON_NESTING_LIMIT).
#define XNODE_SHOW_JSON_MAX_DEPTH 1000

static int32_t mndXnodeHexValue(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

static bool mndXnodePercentValid(const char *value, int32_t len) {
  for (int32_t i = 0; i < len; ++i) {
    if (value[i] == '%' && (i + 2 >= len || mndXnodeHexValue(value[i + 1]) < 0 || mndXnodeHexValue(value[i + 2]) < 0)) {
      return false;
    }
    if (value[i] == '%') i += 2;
  }
  return true;
}

static bool mndXnodeUtf8Valid(const char *value, int32_t len) {
  if (value == NULL || len < 0) return false;
  for (int32_t i = 0; i < len;) {
    const uint8_t first = (uint8_t)value[i++];
    if (first <= 0x7f) continue;
    int32_t extra = 0;
    uint32_t codepoint = 0;
    if (first >= 0xc2 && first <= 0xdf) {
      extra = 1;
      codepoint = first & 0x1f;
    } else if (first >= 0xe0 && first <= 0xef) {
      extra = 2;
      codepoint = first & 0x0f;
    } else if (first >= 0xf0 && first <= 0xf4) {
      extra = 3;
      codepoint = first & 0x07;
    } else {
      return false;
    }
    if (len - i < extra) return false;
    for (int32_t j = 0; j < extra; ++j) {
      const uint8_t next = (uint8_t)value[i++];
      if ((next & 0xc0) != 0x80) return false;
      codepoint = (codepoint << 6) | (next & 0x3f);
    }
    if ((extra == 1 && codepoint < 0x80) || (extra == 2 && codepoint < 0x800) ||
        (extra == 3 && codepoint < 0x10000) || codepoint > 0x10ffff ||
        (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
      return false;
    }
  }
  return true;
}

static int32_t mndXnodePercentDecode(const char *value, int32_t len, char **decoded, int32_t *decodedLen) {
  if (value == NULL || len < 0 || decoded == NULL || decodedLen == NULL || *decoded != NULL) {
    return mndXnodeProtectionError();
  }
  *decodedLen = 0;
  if (!mndXnodePercentValid(value, len)) return mndXnodeProtectionError();
  char *out = taosMemoryMalloc((size_t)len + 1);
  if (out == NULL) return mndXnodeProtectionError();
  int32_t outLen = 0;
  for (int32_t i = 0; i < len; ++i) {
    if (value[i] == '%') {
      out[outLen++] = (char)((mndXnodeHexValue(value[i + 1]) << 4) | mndXnodeHexValue(value[i + 2]));
      i += 2;
    } else {
      out[outLen++] = value[i];
    }
  }
  for (int32_t i = 0; i < outLen; ++i) {
    if ((unsigned char)out[i] == 0) {
      taosMemoryFree(out);
      return mndXnodeProtectionError();
    }
  }
  out[outLen] = 0;
  *decoded = out;
  *decodedLen = outLen;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndXnodeNormalizeSensitiveKey(char *key, int32_t len) {
  int32_t outLen = 0;
  for (int32_t i = 0; i < len; ++i) {
    char c = key[i];
    if (c == '_' || c == '-' || c == '.') continue;
    if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
    key[outLen++] = c;
  }
  key[outLen] = 0;
  return outLen;
}

static bool mndXnodeSensitiveNormalizedMatch(const char *key, int32_t outLen) {
  return strstr(key, "password") != NULL || strstr(key, "passwd") != NULL || strstr(key, "privatekey") != NULL ||
         strstr(key, "secret") != NULL || strstr(key, "apikey") != NULL || strstr(key, "accessid") != NULL ||
         strstr(key, "accesskey") != NULL || strstr(key, "totpcode") != NULL || strcmp(key, "token") == 0 ||
         (outLen >= 5 && strcmp(key + outLen - 5, "token") == 0);
}

static bool mndXnodeSensitiveKey(char *key, int32_t len) {
  return mndXnodeSensitiveNormalizedMatch(key, mndXnodeNormalizeSensitiveKey(key, len));
}

static int32_t mndXnodeMaskDsnInternal(const char *dsn, int32_t dsnLen, char **masked, int32_t *maskedLen) {
  if (dsn == NULL || dsnLen < 0 || masked == NULL || maskedLen == NULL || *masked != NULL) {
    return mndXnodeProtectionError();
  }
  *maskedLen = 0;
  if (!mndXnodeUtf8Valid(dsn, dsnLen)) return mndXnodeProtectionError();
  for (int32_t i = 0; i < dsnLen; ++i) {
    if ((unsigned char)dsn[i] == 0) return mndXnodeProtectionError();
  }

  // The masker mirrors the connector DSN parser (mdsn): there is no fragment
  // concept ('#' is an ordinary data byte), a URL-form DSN (with "://") starts
  // its query at the LAST '?', and a path-like DSN (e.g. csv:/path?x=1) starts
  // it at the FIRST '?'. The query region runs to the end of the DSN.
  int32_t replacements = 0;
  int32_t firstQuestion = -1;
  int32_t lastQuestion = -1;
  for (int32_t i = 0; i < dsnLen; ++i) {
    if (dsn[i] == '?') {
      if (firstQuestion < 0) firstQuestion = i;
      lastQuestion = i;
    }
  }

  int32_t schemeEnd = -1;
  for (int32_t i = 0; i + 2 < dsnLen; ++i) {
    if (dsn[i] == '/' || dsn[i] == '?') break;
    if (dsn[i] == ':' && dsn[i + 1] == '/' && dsn[i + 2] == '/') {
      schemeEnd = i + 3;
      break;
    }
  }
  int32_t queryPos = schemeEnd >= 0 ? lastQuestion : firstQuestion;
  if (schemeEnd >= 0) {
    int32_t authorityEnd = dsnLen;
    for (int32_t i = schemeEnd; i < dsnLen; ++i) {
      if (dsn[i] == '/') {
        authorityEnd = i;
        break;
      }
    }
    if (queryPos >= 0 && queryPos < authorityEnd) authorityEnd = queryPos;
    // The last '@' separates userinfo from host, matching the taosx DSN parser,
    // so passwords containing '@' remain maskable. Authority shape alone never
    // fails closed, matching the write path: an empty authority or an empty
    // userinfo carries no password slot and passes through, and an empty host
    // does not exclude the slot.
    int32_t at = -1;
    for (int32_t i = schemeEnd; i < authorityEnd; ++i) {
      if (dsn[i] == '@') {
        at = i;
      }
    }
    if (at > schemeEnd) {
      int32_t colon = -1;
      for (int32_t i = schemeEnd; i < at; ++i) {
        if (dsn[i] == ':') {
          colon = i;
          break;
        }
      }
      if (colon >= schemeEnd && colon < at) replacements += 1;
    }
  }

  if (queryPos >= 0) {
    int32_t pos = queryPos + 1;
    // Query parameters are separated by '&' alone; ';' is an ordinary value
    // byte (e.g. kafka sasl_password=p1;p2, OPC UA ns=3;i=1004).
    while (pos < dsnLen) {
      int32_t end = pos;
      while (end < dsnLen && dsn[end] != '&') ++end;
      int32_t equal = pos;
      while (equal < end && dsn[equal] != '=') ++equal;
      char *key = NULL;
      int32_t keyLen = 0;
      int32_t code = mndXnodePercentDecode(dsn + pos, equal - pos, &key, &keyLen);
      // A key that cannot be decoded cannot be classified; mask its value
      // conservatively instead of failing the whole DSN.
      bool sensitive = equal < end && (code != TSDB_CODE_SUCCESS || mndXnodeSensitiveKey(key, keyLen));
      taosMemoryFree(key);
      if (sensitive) replacements += 1;
      pos = end + 1;
    }
  }

  int64_t outputLen = (int64_t)dsnLen + (int64_t)replacements * XNODE_SHOW_MASK_VALUE_LEN;
  if (outputLen > INT32_MAX) return mndXnodeProtectionError();
  char *out = taosMemoryMalloc((size_t)outputLen + 1);
  if (out == NULL) return mndXnodeProtectionError();
  int32_t outPos = 0;
  int32_t pos = 0;
  while (pos < dsnLen) {
    if (schemeEnd >= 0 && pos == schemeEnd) {
      int32_t authorityEnd = dsnLen;
      for (int32_t i = schemeEnd; i < dsnLen; ++i) {
        if (dsn[i] == '/') {
          authorityEnd = i;
          break;
        }
      }
      if (queryPos >= 0 && queryPos < authorityEnd) authorityEnd = queryPos;
      int32_t at = -1;
      for (int32_t i = schemeEnd; i < authorityEnd; ++i) if (dsn[i] == '@') at = i;
      if (at > schemeEnd) {
        int32_t colon = -1;
        for (int32_t i = schemeEnd; i < at; ++i) {
          if (dsn[i] == ':') {
            colon = i;
            break;
          }
        }
        if (colon >= schemeEnd && colon < at) {
          memcpy(out + outPos, dsn + pos, (size_t)(colon + 1 - pos));
          outPos += colon + 1 - pos;
          memcpy(out + outPos, XNODE_SHOW_MASK_VALUE, XNODE_SHOW_MASK_VALUE_LEN);
          outPos += XNODE_SHOW_MASK_VALUE_LEN;
          pos = at;
          continue;
        }
      }
    }
    if (queryPos >= 0 && pos == queryPos + 1) {
      int32_t q = pos;
      while (q < dsnLen) {
        int32_t itemEnd = q;
        while (itemEnd < dsnLen && dsn[itemEnd] != '&') ++itemEnd;
        int32_t equal = q;
        while (equal < itemEnd && dsn[equal] != '=') ++equal;
        char *key = NULL;
        int32_t keyLen = 0;
        int32_t code = mndXnodePercentDecode(dsn + q, equal - q, &key, &keyLen);
        bool sensitive = equal < itemEnd && (code != TSDB_CODE_SUCCESS || mndXnodeSensitiveKey(key, keyLen));
        taosMemoryFree(key);
        int32_t valueStart = equal < itemEnd ? equal + 1 : itemEnd;
        memcpy(out + outPos, dsn + q, (size_t)(valueStart - q));
        outPos += valueStart - q;
        if (sensitive) {
          memcpy(out + outPos, XNODE_SHOW_MASK_VALUE, XNODE_SHOW_MASK_VALUE_LEN);
          outPos += XNODE_SHOW_MASK_VALUE_LEN;
        } else {
          memcpy(out + outPos, dsn + valueStart, (size_t)(itemEnd - valueStart));
          outPos += itemEnd - valueStart;
        }
        if (itemEnd < dsnLen) out[outPos++] = dsn[itemEnd];
        q = itemEnd + 1;
      }
      pos = dsnLen;
      continue;
    }
    out[outPos++] = dsn[pos++];
  }
  out[outPos] = 0;
  *masked = out;
  *maskedLen = outPos;
  return TSDB_CODE_SUCCESS;
}

int32_t mndXnodeMaskDsnForShow(const char *dsn, int32_t dsnLen, char **masked, int32_t *maskedLen) {
  return mndXnodeMaskDsnInternal(dsn, dsnLen, masked, maskedLen);
}

int32_t mndXnodeResolveShowMode(int32_t protection, int8_t requestMode, int8_t *effectiveMode) {
  if (effectiveMode == NULL || (requestMode != XNODE_DSN_OUTPUT_DEFAULT && requestMode != XNODE_DSN_OUTPUT_ENCRYPTED)) {
    return mndXnodeProtectionError();
  }
  if (protection == 0) {
    // With protection disabled, even an explicit WITH ENCRYPTED request is
    // intentionally downgraded to plaintext and reported as success.
    *effectiveMode = XNODE_SHOW_OUTPUT_PLAINTEXT;
#ifdef WINDOWS
  } else {
    return TSDB_CODE_NOT_SUPPORTED_IN_WINDOWS;
#else
  } else {
    *effectiveMode = requestMode == XNODE_DSN_OUTPUT_ENCRYPTED ? XNODE_SHOW_OUTPUT_ENCRYPTED : XNODE_SHOW_OUTPUT_MASKED;
#endif
  }
  return TSDB_CODE_SUCCESS;
}

bool mndXnodeShowFilterMatches(int8_t filterType, int64_t filterValue, int32_t taskId, int32_t jobTaskId) {
  if (filterType == XNODE_RETRIEVE_FILTER_NONE) return filterValue == 0;
  if (filterValue < 0 || filterValue > INT32_MAX) return false;
  if (filterType == XNODE_RETRIEVE_FILTER_ID_EQ) return taskId == (int32_t)filterValue;
  if (filterType == XNODE_RETRIEVE_FILTER_TASK_ID_EQ) return jobTaskId == (int32_t)filterValue;
  return false;
}

typedef struct {
  int32_t start;
  int32_t end;
  bool    present;
} SXnodeJsonSpan;

static int32_t mndXnodeSkipJsonString(const char *input, int32_t len, int32_t *pos) {
  if (*pos >= len || input[*pos] != '"') return mndXnodeProtectionError();
  ++(*pos);
  while (*pos < len) {
    unsigned char c = (unsigned char)input[*pos];
    if (c == '"') {
      ++(*pos);
      return TSDB_CODE_SUCCESS;
    }
    if (c < 0x20) return mndXnodeProtectionError();
    if (c == '\\') {
      ++(*pos);
      if (*pos >= len) return mndXnodeProtectionError();
      c = (unsigned char)input[(*pos)++];
      if (c == 'u') {
        if (len - *pos < 4) return mndXnodeProtectionError();
        for (int32_t i = 0; i < 4; ++i) {
          if (mndXnodeHexValue(input[*pos + i]) < 0) return mndXnodeProtectionError();
        }
        if (input[*pos] == '0' && input[*pos + 1] == '0' && input[*pos + 2] == '0' && input[*pos + 3] == '0') {
          return mndXnodeProtectionError();
        }
        *pos += 4;
      } else if (c != '"' && c != '\\' && c != '/' && c != 'b' && c != 'f' && c != 'n' && c != 'r' && c != 't') {
        return mndXnodeProtectionError();
      }
    } else {
      ++(*pos);
    }
  }
  return mndXnodeProtectionError();
}

static void mndXnodeSkipJsonWhitespace(const char *input, int32_t len, int32_t *pos) {
  while (*pos < len &&
         (input[*pos] == ' ' || input[*pos] == '\t' || input[*pos] == '\r' || input[*pos] == '\n')) {
    ++(*pos);
  }
}

// Bare scalars must be strict RFC 8259 literals (true, false, null, or
// -?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?); the removed cJSON document parse used
// to provide this strictness, so the scanner now enforces it itself.
static int32_t mndXnodeSkipJsonLiteral(const char *input, int32_t len, int32_t *pos) {
  const int32_t start = *pos;
  const char    c = input[start];
  if (c == 't' || c == 'f' || c == 'n') {
    const char   *word = c == 't' ? "true" : c == 'f' ? "false" : "null";
    const int32_t wordLen = c == 'f' ? 5 : 4;
    if (len - start < wordLen || memcmp(input + start, word, (size_t)wordLen) != 0) {
      return mndXnodeProtectionError();
    }
    *pos = start + wordLen;
    return TSDB_CODE_SUCCESS;
  }
  int32_t p = start;
  if (input[p] == '-') ++p;
  if (p >= len) return mndXnodeProtectionError();
  if (input[p] == '0') {
    ++p;
  } else if (input[p] >= '1' && input[p] <= '9') {
    while (p < len && input[p] >= '0' && input[p] <= '9') ++p;
  } else {
    return mndXnodeProtectionError();
  }
  if (p < len && input[p] == '.') {
    ++p;
    if (p >= len || input[p] < '0' || input[p] > '9') return mndXnodeProtectionError();
    while (p < len && input[p] >= '0' && input[p] <= '9') ++p;
  }
  if (p < len && (input[p] == 'e' || input[p] == 'E')) {
    ++p;
    if (p < len && (input[p] == '+' || input[p] == '-')) ++p;
    if (p >= len || input[p] < '0' || input[p] > '9') return mndXnodeProtectionError();
    while (p < len && input[p] >= '0' && input[p] <= '9') ++p;
  }
  *pos = p;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndXnodeSkipJsonValue(const char *input, int32_t len, int32_t *pos, int32_t depth) {
  if (depth > XNODE_SHOW_JSON_MAX_DEPTH) return mndXnodeProtectionError();
  if (*pos >= len) return mndXnodeProtectionError();
  if (input[*pos] == '"') return mndXnodeSkipJsonString(input, len, pos);
  if (input[*pos] == '{' || input[*pos] == '[') {
    const char open = input[(*pos)++];
    const char close = open == '{' ? '}' : ']';
    bool first = true;
    while (true) {
      mndXnodeSkipJsonWhitespace(input, len, pos);
      if (*pos >= len) return mndXnodeProtectionError();
      if (input[*pos] == close) {
        ++(*pos);
        return TSDB_CODE_SUCCESS;
      }
      if (!first) {
        if (input[(*pos)++] != ',') return mndXnodeProtectionError();
        mndXnodeSkipJsonWhitespace(input, len, pos);
      }
      if (open == '{') {
        if (mndXnodeSkipJsonString(input, len, pos) != TSDB_CODE_SUCCESS) return mndXnodeProtectionError();
        mndXnodeSkipJsonWhitespace(input, len, pos);
        if (*pos >= len || input[(*pos)++] != ':') return mndXnodeProtectionError();
        mndXnodeSkipJsonWhitespace(input, len, pos);
      }
      if (mndXnodeSkipJsonValue(input, len, pos, depth + 1) != TSDB_CODE_SUCCESS) return mndXnodeProtectionError();
      first = false;
    }
  }
  return mndXnodeSkipJsonLiteral(input, len, pos);
}

static int32_t mndXnodeFindJobConfigSpans(const char *config, int32_t configLen, SXnodeJsonSpan *from,
                                          SXnodeJsonSpan *to) {
  int32_t pos = 0;
  mndXnodeSkipJsonWhitespace(config, configLen, &pos);
  if (pos >= configLen || config[pos++] != '{') return mndXnodeProtectionError();
  bool first = true;
  while (true) {
    mndXnodeSkipJsonWhitespace(config, configLen, &pos);
    if (pos >= configLen) return mndXnodeProtectionError();
    if (config[pos] == '}') {
      ++pos;
      break;
    }
    if (!first) {
      if (config[pos++] != ',') return mndXnodeProtectionError();
      mndXnodeSkipJsonWhitespace(config, configLen, &pos);
    }
    int32_t keyStart = pos;
    if (mndXnodeSkipJsonString(config, configLen, &pos) != TSDB_CODE_SUCCESS) return mndXnodeProtectionError();
    int32_t keyEnd = pos;
    for (int32_t i = keyStart + 1; i + 1 < keyEnd; ++i) {
      if (config[i] == '\\') return mndXnodeProtectionError();
    }
    mndXnodeSkipJsonWhitespace(config, configLen, &pos);
    if (pos >= configLen || config[pos++] != ':') {
      return mndXnodeProtectionError();
    }
    mndXnodeSkipJsonWhitespace(config, configLen, &pos);
    const int32_t keyLen = keyEnd - keyStart - 2;
    bool          isFrom = keyLen == 4 && memcmp(config + keyStart + 1, "from", 4) == 0;
    bool          isTo = keyLen == 2 && memcmp(config + keyStart + 1, "to", 2) == 0;
    int32_t valueStart = pos;
    if (mndXnodeSkipJsonValue(config, configLen, &pos, 1) != TSDB_CODE_SUCCESS) return mndXnodeProtectionError();
    if (isFrom || isTo) {
      if (config[valueStart] != '"') return mndXnodeProtectionError();
      SXnodeJsonSpan *span = isFrom ? from : to;
      if (span->present) return mndXnodeProtectionError();
      span->start = valueStart;
      span->end = pos;
      span->present = true;
    }
    first = false;
  }
  mndXnodeSkipJsonWhitespace(config, configLen, &pos);
  return pos == configLen ? TSDB_CODE_SUCCESS : mndXnodeProtectionError();
}

static int32_t mndXnodeDecodeJsonString(const char *raw, int32_t rawLen, char **decoded, int32_t *decodedLen) {
  if (decoded == NULL || decodedLen == NULL || *decoded != NULL || raw == NULL || rawLen <= 1) {
    return mndXnodeProtectionError();
  }
  *decodedLen = 0;
  for (int32_t i = 0; i < rawLen; ++i) {
    if ((unsigned char)raw[i] == 0) return mndXnodeProtectionError();
    if (raw[i] == '\\' && i + 5 < rawLen && raw[i + 1] == 'u' && raw[i + 2] == '0' && raw[i + 3] == '0' &&
        raw[i + 4] == '0' && raw[i + 5] == '0') {
      int32_t precedingSlashes = 0;
      for (int32_t j = i - 1; j >= 0 && raw[j] == '\\'; --j) ++precedingSlashes;
      if ((precedingSlashes & 1) == 0) return mndXnodeProtectionError();
    }
  }
  cJSON *value = cJSON_ParseWithLength(raw, rawLen);
  if (value == NULL || !cJSON_IsString(value) || value->valuestring == NULL) {
    if (value != NULL) cJSON_Delete(value);
    return mndXnodeProtectionError();
  }
  int32_t len = (int32_t)strlen(value->valuestring);
  char *copy = taosMemoryMalloc((size_t)len + 1);
  if (copy == NULL) {
    cJSON_Delete(value);
    return mndXnodeProtectionError();
  }
  memcpy(copy, value->valuestring, (size_t)len);
  copy[len] = 0;
  cJSON_Delete(value);
  *decoded = copy;
  *decodedLen = len;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndXnodeJsonEscape(const char *value, int32_t valueLen, char **escaped, int32_t *escapedLen) {
  int64_t capacity = (int64_t)valueLen * 6 + 1;
  if (capacity > INT32_MAX || escaped == NULL || escapedLen == NULL || *escaped != NULL) {
    return mndXnodeProtectionError();
  }
  char *out = taosMemoryMalloc((size_t)capacity);
  if (out == NULL) return mndXnodeProtectionError();
  int32_t pos = 0;
  for (int32_t i = 0; i < valueLen; ++i) {
    unsigned char c = (unsigned char)value[i];
    if (c == '"' || c == '\\') {
      out[pos++] = '\\';
      out[pos++] = (char)c;
    } else if (c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t') {
      out[pos++] = '\\';
      out[pos++] = c == '\b' ? 'b' : c == '\f' ? 'f' : c == '\n' ? 'n' : c == '\r' ? 'r' : 't';
    } else if (c < 0x20) {
      static const char hex[] = "0123456789abcdef";
      out[pos++] = '\\';
      out[pos++] = 'u';
      out[pos++] = '0';
      out[pos++] = '0';
      out[pos++] = hex[c >> 4];
      out[pos++] = hex[c & 0xf];
    } else {
      out[pos++] = (char)c;
    }
  }
  out[pos] = 0;
  *escaped = out;
  *escapedLen = pos;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndXnodeJsonQuote(const char *value, int32_t valueLen, char **quoted, int32_t *quotedLen) {
  if (quoted == NULL || quotedLen == NULL || *quoted != NULL || value == NULL || valueLen < 0) {
    return mndXnodeProtectionError();
  }
  *quotedLen = 0;
  if (valueLen > INT32_MAX - 3) return mndXnodeProtectionError();

  char *out = taosMemoryMalloc((size_t)valueLen + 3);
  if (out == NULL) return mndXnodeProtectionError();
  out[0] = '"';
  if (valueLen > 0) memcpy(out + 1, value, (size_t)valueLen);
  out[valueLen + 1] = '"';
  out[valueLen + 2] = 0;
  *quoted = out;
  *quotedLen = valueLen + 2;
  return TSDB_CODE_SUCCESS;
}

static int32_t mndXnodeEncryptDsnForShow(SShowObj *pShow, const char *dsn, int32_t dsnLen, uint32_t maxPlainLen,
                                         char **encoded, int32_t *encodedLen) {
#ifdef WINDOWS
  (void)pShow;
  (void)dsn;
  (void)dsnLen;
  (void)maxPlainLen;
  if (encoded != NULL) *encoded = NULL;
  if (encodedLen != NULL) *encodedLen = 0;
  return mndXnodeProtectionError();
#else
  if (pShow == NULL || encoded == NULL || encodedLen == NULL || *encoded != NULL || dsn == NULL || dsnLen < 0) {
    return mndXnodeProtectionError();
  }
  *encodedLen = 0;
  if (pShow->pXnodeBatch == NULL) {
    const SEnvelopePublicKey *key = taosGetXnodeDsnPublicKey();
    SEnvelopeBatch           *batch = NULL;
    if (key == NULL || envelopeBatchBegin(key, &batch) != ENVELOPE_CRYPTO_SUCCESS) {
      return mndXnodeProtectionError();
    }
    if (pShow->pXnodeBatch != NULL) {
      // A concurrent retrieve on the same show object began a batch first: free
      // this duplicate instead of overwriting the live batch, which would leak
      // its key material. (Concurrent retrieves of one show object race exactly
      // like the pre-existing SShowObj.pIter access; that class is not new.)
      (void)envelopeBatchAbort(&batch);
    } else {
      pShow->pXnodeBatch = batch;
    }
  }
  uint32_t length = 0;
  int32_t code = envelopeBatchEncryptField((SEnvelopeBatch *)pShow->pXnodeBatch, (const uint8_t *)dsn,
                                           (uint32_t)dsnLen, maxPlainLen, encoded, &length);
  if (code != ENVELOPE_CRYPTO_SUCCESS) return mndXnodeProtectionError();
  *encodedLen = (int32_t)length;
  return TSDB_CODE_SUCCESS;
#endif
}

// Decodes one top-level from/to string span and rewrites it for the show output
// mode (masked + re-escaped, or envelope-encrypted). Failures are logged with the
// show id and the failing stage; the config content itself is never logged.
static int32_t mndXnodeProjectConfigDsnValue(SShowObj *pShow, const char *name, const char *raw, int32_t rawLen,
                                             char **replacement, int32_t *replacementLen) {
  const char *stage = "decode";
  char       *decoded = NULL;
  int32_t     decodedLen = 0;
  int32_t     code = mndXnodeDecodeJsonString(raw, rawLen, &decoded, &decodedLen);
  if (code == TSDB_CODE_SUCCESS && pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_MASKED) {
    stage = "mask";
    char   *masked = NULL;
    int32_t maskedLen = 0;
    code = mndXnodeMaskDsnInternal(decoded, decodedLen, &masked, &maskedLen);
    if (code == TSDB_CODE_SUCCESS) {
      stage = "escape";
      char   *escaped = NULL;
      int32_t escapedLen = 0;
      code = mndXnodeJsonEscape(masked, maskedLen, &escaped, &escapedLen);
      if (code == TSDB_CODE_SUCCESS) {
        stage = "quote";
        code = mndXnodeJsonQuote(escaped, escapedLen, replacement, replacementLen);
      }
      taosMemoryFree(escaped);
    }
    taosMemoryFree(masked);
  } else if (code == TSDB_CODE_SUCCESS) {
    stage = "encrypt";
    char   *encrypted = NULL;
    int32_t encryptedLen = 0;
    code = mndXnodeEncryptDsnForShow(pShow, decoded, decodedLen, TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN, &encrypted,
                                     &encryptedLen);
    if (code == TSDB_CODE_SUCCESS) {
      stage = "quote";
      code = mndXnodeJsonQuote(encrypted, encryptedLen, replacement, replacementLen);
    }
    taosMemoryFree(encrypted);
  }
  taosMemoryFree(decoded);
  if (code != TSDB_CODE_SUCCESS) {
    mError("show:0x%" PRIx64 ", xnode job config protection failed at %s-%s, code:%s", pShow->id, name, stage,
           tstrerror(code));
  }
  return code;
}

int32_t mndXnodeProjectJobConfigForShow(SShowObj *pShow, const char *config, int32_t configLen, char **projected,
                                        int32_t *projectedLen) {
  if (pShow == NULL || configLen < 0 || projected == NULL || projectedLen == NULL || *projected != NULL ||
      (config == NULL && configLen != 0)) {
    return mndXnodeProtectionError();
  }
  *projectedLen = 0;
  int32_t contentLen = configLen;
  if (pShow->xnodeOutputMode != XNODE_SHOW_OUTPUT_PLAINTEXT && pShow->xnodeOutputMode != XNODE_SHOW_OUTPUT_MASKED &&
      pShow->xnodeOutputMode != XNODE_SHOW_OUTPUT_ENCRYPTED) {
    return mndXnodeProtectionError();
  }
  if (config == NULL) {
    return pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT ? TSDB_CODE_SUCCESS : mndXnodeProtectionError();
  }
  while (contentLen > 0 && config[contentLen - 1] == '\0') --contentLen;
  if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT) {
    // An empty payload is only a valid BLOB in PLAINTEXT mode; protected modes
    // fail closed below because an empty payload is not a valid JSON object.
    char *copy = taosMemoryMalloc((size_t)contentLen + 1);
    if (copy == NULL) return mndXnodeProtectionError();
    if (contentLen > 0) memcpy(copy, config, (size_t)contentLen);
    copy[contentLen] = 0;
    *projected = copy;
    *projectedLen = contentLen;
    return TSDB_CODE_SUCCESS;
  }

  const char   *stage = "size";
  int32_t       code = TSDB_CODE_SUCCESS;
  SXnodeJsonSpan from = {0};
  SXnodeJsonSpan to = {0};
  char         *fromReplacement = NULL;
  char         *toReplacement = NULL;
  int32_t       fromReplacementLen = 0;
  int32_t       toReplacementLen = 0;

  if (contentLen > TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN) goto _err;
  stage = "content";
  for (int32_t i = 0; i < contentLen; ++i) {
    if ((unsigned char)config[i] == 0) goto _err;
  }
  if (!mndXnodeUtf8Valid(config, contentLen)) goto _err;
  stage = "schema";
  code = mndXnodeFindJobConfigSpans(config, contentLen, &from, &to);
  if (code != TSDB_CODE_SUCCESS) goto _err;
  if (from.present) {
    code = mndXnodeProjectConfigDsnValue(pShow, "from", config + from.start, from.end - from.start, &fromReplacement,
                                         &fromReplacementLen);
    if (code != TSDB_CODE_SUCCESS) goto _err_logged;
  }
  if (to.present) {
    code = mndXnodeProjectConfigDsnValue(pShow, "to", config + to.start, to.end - to.start, &toReplacement,
                                         &toReplacementLen);
    if (code != TSDB_CODE_SUCCESS) goto _err_logged;
  }

  stage = "assemble";
  {
    int64_t resultLen = contentLen;
    if (from.present) resultLen += fromReplacementLen - (from.end - from.start);
    if (to.present) resultLen += toReplacementLen - (to.end - to.start);
    if (resultLen < 0 || resultLen > TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN || resultLen > INT32_MAX) goto _err;
    char *out = taosMemoryMalloc((size_t)resultLen + 1);
    if (out == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    int32_t outPos = 0;
    int32_t inputPos = 0;
    SXnodeJsonSpan spans[2] = {from, to};
    char   *replacements[2] = {fromReplacement, toReplacement};
    int32_t replacementLens[2] = {fromReplacementLen, toReplacementLen};
    if (spans[0].present && spans[1].present && spans[1].start < spans[0].start) {
      SXnodeJsonSpan span = spans[0];
      spans[0] = spans[1];
      spans[1] = span;
      char *replacement = replacements[0];
      replacements[0] = replacements[1];
      replacements[1] = replacement;
      int32_t replacementLen = replacementLens[0];
      replacementLens[0] = replacementLens[1];
      replacementLens[1] = replacementLen;
    }
    for (int32_t i = 0; i < 2; ++i) {
      if (!spans[i].present) continue;
      memcpy(out + outPos, config + inputPos, (size_t)(spans[i].start - inputPos));
      outPos += spans[i].start - inputPos;
      memcpy(out + outPos, replacements[i], (size_t)replacementLens[i]);
      outPos += replacementLens[i];
      inputPos = spans[i].end;
    }
    memcpy(out + outPos, config + inputPos, (size_t)(contentLen - inputPos));
    outPos += contentLen - inputPos;
    out[outPos] = 0;
    taosMemoryFree(fromReplacement);
    taosMemoryFree(toReplacement);
    *projected = out;
    *projectedLen = outPos;
    return TSDB_CODE_SUCCESS;
  }

_err:
  if (code == TSDB_CODE_SUCCESS) code = mndXnodeProtectionError();
  mError("show:0x%" PRIx64 ", failed to project xnode job config at %s, code:%s", pShow->id, stage, tstrerror(code));
_err_logged:
  taosMemoryFree(fromReplacement);
  taosMemoryFree(toReplacement);
  return mndXnodeProtectionError();
}

void mndXnodeCleanupShowProtection(SShowObj *pShow) {
  if (pShow == NULL || pShow->pXnodeBatch == NULL) return;
#ifndef WINDOWS
  (void)envelopeBatchAbort((SEnvelopeBatch **)&pShow->pXnodeBatch);
#else
  pShow->pXnodeBatch = NULL;
#endif
}

int32_t mndXnodeFinishShowProtection(SShowObj *pShow) {
  if (pShow == NULL) return mndXnodeProtectionError();
  if (pShow->pXnodeBatch == NULL) return TSDB_CODE_SUCCESS;
#ifndef WINDOWS
  return envelopeBatchFinish((SEnvelopeBatch **)&pShow->pXnodeBatch) == ENVELOPE_CRYPTO_SUCCESS
             ? TSDB_CODE_SUCCESS
             : mndXnodeProtectionError();
#else
  return mndXnodeProtectionError();
#endif
}

static int32_t mndXnodeSetDsnShowColumn(SShowObj *pShow, SColumnInfoData *pColInfo, int32_t row, const char *dsn,
                                        int32_t dsnLen, uint32_t maxPlainLen) {
  if (dsn == NULL || dsnLen < 0 || pShow == NULL || pColInfo == NULL) return mndXnodeProtectionError();
  const char *stage = "copy";
  char       *output = NULL;
  int32_t     outputLen = 0;
  int32_t     code = TSDB_CODE_SUCCESS;
  if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT) {
    output = taosMemoryMalloc((size_t)dsnLen + 1);
    if (output == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(output, dsn, (size_t)dsnLen);
      output[dsnLen] = 0;
      outputLen = dsnLen;
    }
  } else if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_MASKED) {
    stage = "mask";
    code = mndXnodeMaskDsnForShow(dsn, dsnLen, &output, &outputLen);
  } else if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_ENCRYPTED) {
    stage = "encrypt";
    code = mndXnodeEncryptDsnForShow(pShow, dsn, dsnLen, maxPlainLen, &output, &outputLen);
  } else {
    stage = "mode";
    code = mndXnodeProtectionError();
  }
  if (code == TSDB_CODE_SUCCESS) {
    int32_t maxLen = pColInfo->info.bytes - VARSTR_HEADER_SIZE;
    if (outputLen > maxLen) {
      stage = "length";
      code = mndXnodeProtectionError();
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    stage = "set-column";
    code = varColSetVarData(pColInfo, row, output, outputLen, false);
  }
  if (code != TSDB_CODE_SUCCESS) {
    // Log the stage and the original code before normalizing; the DSN content
    // itself must never be logged.
    mError("show:0x%" PRIx64 ", failed to set xnode dsn show column at %s, code:%s", pShow->id, stage,
           tstrerror(code));
  }
  taosMemoryFree(output);
  return code == TSDB_CODE_SUCCESS ? TSDB_CODE_SUCCESS : mndXnodeProtectionError();
}

// ---------------------------------------------------------------------------
// Six-star write semantics for CREATE/ALTER XNODE TASK/JOB.
//
// When xnodeDsnProtection=1 the mnode applies the structured six-star rules of
// the shared DsnInteropV1 profile to the sensitive fields of candidate DSNs and
// Job configs. The merge runs on the write queue before the final object is
// encoded into the commit log, so no control six-star can reach the commit log,
// a connector, or the runtime object regardless of the client entry point. When
// the protection is disabled the merge is skipped entirely and a six-star value
// is stored as an ordinary credential. The sixStarError out-parameters carry the
// stable cross-language registry mapping (EXnodeSixStarError); the SQL-visible
// error stays TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED and never embeds the
// candidate DSN, the old secret, or the six-star marker itself.
// ---------------------------------------------------------------------------

static int32_t mndXnodeSixStarError(int32_t *pSixStarError, int32_t sixStarCode) {
  if (pSixStarError != NULL) *pSixStarError = sixStarCode;
  return mndXnodeProtectionError();
}

static int32_t mndXnodeSixStarDupBytes(const char *src, int32_t len, char **dst, int32_t *dstLen) {
  if (src == NULL || len < 0 || dst == NULL || dstLen == NULL || *dst != NULL) return mndXnodeProtectionError();
  char *out = taosMemoryMalloc((size_t)len + 1);
  if (out == NULL) return mndXnodeProtectionError();
  if (len > 0) memcpy(out, src, (size_t)len);
  out[len] = 0;
  *dst = out;
  *dstLen = len;
  return TSDB_CODE_SUCCESS;
}

static void mndXnodeSixStarFreeSlot(SXnodeSixStarSlot *pSlot) {
  if (pSlot == NULL) return;
  taosMemoryFree(pSlot->decodedKey);
  taosMemoryFree(pSlot->normalizedKey);
  taosMemoryFree(pSlot->decodedValue);
  pSlot->decodedKey = NULL;
  pSlot->normalizedKey = NULL;
  pSlot->decodedValue = NULL;
}

void mndXnodeSixStarFreeSlots(SXnodeSixStarSlot *pSlots, int32_t numSlots) {
  if (pSlots == NULL) return;
  for (int32_t i = 0; i < numSlots; ++i) mndXnodeSixStarFreeSlot(&pSlots[i]);
  taosMemoryFree(pSlots);
}

int32_t mndXnodeSixStarScanDsn(const char *dsn, int32_t dsnLen, SXnodeSixStarSlot **ppSlots, int32_t *pNumSlots,
                               int32_t *pSixStarError) {
  if (ppSlots == NULL || pNumSlots == NULL || pSixStarError == NULL || *ppSlots != NULL) {
    return mndXnodeProtectionError();
  }
  *pNumSlots = 0;
  *pSixStarError = XNODE_SIXSTAR_ERROR_NONE;
  if (dsn == NULL || dsnLen < 0) {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
  }
  for (int32_t i = 0; i < dsnLen; ++i) {
    if ((unsigned char)dsn[i] == 0) {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
    }
  }
  if (!mndXnodeUtf8Valid(dsn, dsnLen)) {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SArray  *pSlots = taosArrayInit(4, sizeof(SXnodeSixStarSlot));
  if (pSlots == NULL) return mndXnodeProtectionError();
  // Per-normalized-key occurrence counts for duplicate ordinals, so a scan with
  // k sensitive parameters stays O(k) instead of rescanning the slots array.
  SHashObj *pKeyCounts = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pKeyCounts == NULL) {
    taosArrayDestroy(pSlots);
    return mndXnodeProtectionError();
  }

  // The scanner mirrors the connector DSN parser (mdsn): there is no fragment
  // concept ('#' is an ordinary data byte), a URL-form DSN (with "://") starts
  // its query at the LAST '?', and a path-like DSN (e.g. csv:/path?x=1) starts
  // it at the FIRST '?'. The query region runs to the end of the DSN. Percent
  // decoding is strict and happens exactly once per sensitive field (userinfo
  // password, every query key, every sensitive query value); a bare '%'
  // anywhere else (e.g. busy_threshold=30%) passes through untouched.
  int32_t firstQuestion = -1;
  int32_t lastQuestion = -1;
  for (int32_t i = 0; i < dsnLen; ++i) {
    if (dsn[i] == '?') {
      if (firstQuestion < 0) firstQuestion = i;
      lastQuestion = i;
    }
  }

  int32_t schemeEnd = -1;
  for (int32_t i = 0; i + 2 < dsnLen; ++i) {
    if (dsn[i] == '/' || dsn[i] == '?') break;
    if (dsn[i] == ':' && dsn[i + 1] == '/' && dsn[i + 2] == '/') {
      schemeEnd = i + 3;
      break;
    }
  }
  int32_t queryPos = schemeEnd >= 0 ? lastQuestion : firstQuestion;
  if (schemeEnd >= 0) {
    int32_t authorityEnd = dsnLen;
    for (int32_t i = schemeEnd; i < dsnLen; ++i) {
      if (dsn[i] == '/') {
        authorityEnd = i;
        break;
      }
    }
    if (queryPos >= 0 && queryPos < authorityEnd) authorityEnd = queryPos;
    // The userinfo password span is located, not validated: it exists only when a
    // ':' precedes the last '@' of the authority (last-'@' matches the connector
    // DSN parser). Empty authority (csv/orc) or empty userinfo simply have no
    // userinfo slot; an empty host does NOT suppress the slot, so a "******"
    // there stays a control marker. The SHOW masker applies the same slot rules.
    // Structural validity is the connector's call.
    int32_t at = -1;
    for (int32_t i = schemeEnd; i < authorityEnd; ++i) {
      if (dsn[i] == '@') at = i;
    }
    if (at > schemeEnd) {
      int32_t colon = -1;
      for (int32_t i = schemeEnd; i < at; ++i) {
        if (dsn[i] == ':') {
          colon = i;
          break;
        }
      }
      if (colon >= schemeEnd && colon < at) {
        SXnodeSixStarSlot slot = {0};
        slot.role = XNODE_SIXSTAR_ROLE_USERINFO_PASSWORD;
        slot.wireKeyOff = XNODE_SIXSTAR_WIRE_KEY_NONE;
        slot.wireValueOff = colon + 1;
        slot.wireValueLen = at - colon - 1;
        code = mndXnodeSixStarDupBytes("", 0, &slot.decodedKey, &slot.decodedKeyLen);
        if (code == TSDB_CODE_SUCCESS) {
          code = mndXnodeSixStarDupBytes("", 0, &slot.normalizedKey, &slot.normalizedKeyLen);
        }
        if (code == TSDB_CODE_SUCCESS) {
          code = mndXnodePercentDecode(dsn + slot.wireValueOff, slot.wireValueLen, &slot.decodedValue,
                                       &slot.decodedValueLen);
          if (code != TSDB_CODE_SUCCESS) {
            code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_PERCENT_ENCODING_INVALID);
          }
        }
        if (code == TSDB_CODE_SUCCESS) {
          if (taosArrayPush(pSlots, &slot) == NULL) code = mndXnodeProtectionError();
        }
        if (code != TSDB_CODE_SUCCESS) {
          mndXnodeSixStarFreeSlot(&slot);
          goto _OVER;
        }
      }
    }
  }

  if (queryPos >= 0) {
    int32_t pos = queryPos + 1;
    // Query parameters are separated by '&' alone; ';' is an ordinary value
    // byte (e.g. kafka sasl_password=p1;p2, OPC UA ns=3;i=1004).
    while (pos < dsnLen) {
      int32_t end = pos;
      while (end < dsnLen && dsn[end] != '&') ++end;
      int32_t equal = pos;
      while (equal < end && dsn[equal] != '=') ++equal;
      char   *decodedKey = NULL;
      int32_t decodedKeyLen = 0;
      code = mndXnodePercentDecode(dsn + pos, equal - pos, &decodedKey, &decodedKeyLen);
      if (code != TSDB_CODE_SUCCESS) {
        code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_PERCENT_ENCODING_INVALID);
        goto _OVER;
      }
      // The decoded key is normalized in place for the sensitivity test; only a
      // sensitive hit keeps the normalized bytes and re-decodes the wire key to
      // recover decodedKey, so the common non-sensitive parameter pays a single
      // allocation.
      int32_t normalizedKeyLen = mndXnodeNormalizeSensitiveKey(decodedKey, decodedKeyLen);
      if (equal < end && mndXnodeSensitiveNormalizedMatch(decodedKey, normalizedKeyLen)) {
        char *normalizedKey = decodedKey;
        decodedKey = NULL;
        code = mndXnodePercentDecode(dsn + pos, equal - pos, &decodedKey, &decodedKeyLen);
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFree(normalizedKey);
          code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_PERCENT_ENCODING_INVALID);
          goto _OVER;
        }
        SXnodeSixStarSlot slot = {0};
        slot.role = XNODE_SIXSTAR_ROLE_QUERY_SENSITIVE;
        slot.wireKeyOff = pos;
        slot.wireKeyLen = equal - pos;
        slot.wireValueOff = equal + 1;
        slot.wireValueLen = end - equal - 1;
        slot.decodedKey = decodedKey;
        slot.decodedKeyLen = decodedKeyLen;
        slot.normalizedKey = normalizedKey;
        slot.normalizedKeyLen = normalizedKeyLen;
        int32_t *pCount = taosHashGet(pKeyCounts, normalizedKey, (size_t)normalizedKeyLen);
        if (pCount != NULL) {
          slot.duplicateOrdinal = *pCount;
          ++(*pCount);
        } else {
          int32_t first = 1;
          if (taosHashPut(pKeyCounts, normalizedKey, (size_t)normalizedKeyLen, &first, sizeof(first)) != 0) {
            mndXnodeSixStarFreeSlot(&slot);
            code = mndXnodeProtectionError();
            goto _OVER;
          }
        }
        code = mndXnodePercentDecode(dsn + slot.wireValueOff, slot.wireValueLen, &slot.decodedValue,
                                     &slot.decodedValueLen);
        if (code != TSDB_CODE_SUCCESS) {
          mndXnodeSixStarFreeSlot(&slot);
          code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_PERCENT_ENCODING_INVALID);
          goto _OVER;
        }
        if (taosArrayPush(pSlots, &slot) == NULL) {
          mndXnodeSixStarFreeSlot(&slot);
          code = mndXnodeProtectionError();
          goto _OVER;
        }
      } else {
        taosMemoryFree(decodedKey);
      }
      pos = end + 1;
    }
  }

_OVER:
  if (code == TSDB_CODE_SUCCESS) {
    int32_t numSlots = (int32_t)taosArrayGetSize(pSlots);
    if (numSlots > 0) {
      SXnodeSixStarSlot *out = taosMemoryMalloc((size_t)numSlots * sizeof(SXnodeSixStarSlot));
      if (out == NULL) {
        code = mndXnodeProtectionError();
      } else {
        for (int32_t i = 0; i < numSlots; ++i) out[i] = *(SXnodeSixStarSlot *)taosArrayGet(pSlots, i);
        *ppSlots = out;
        *pNumSlots = numSlots;
      }
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pSlots); ++i) {
      mndXnodeSixStarFreeSlot((SXnodeSixStarSlot *)taosArrayGet(pSlots, i));
    }
  }
  taosHashCleanup(pKeyCounts);
  taosArrayDestroy(pSlots);
  return code;
}

static bool mndXnodeSixStarIsMaskedValue(const char *decodedValue, int32_t decodedValueLen) {
  return decodedValueLen == XNODE_SHOW_MASK_VALUE_LEN &&
         memcmp(decodedValue, XNODE_SHOW_MASK_VALUE, XNODE_SHOW_MASK_VALUE_LEN) == 0;
}

// Old slots are matched by (role, normalizedKey, duplicateOrdinal); a userinfo
// password slot carries an empty normalized key and ordinal 0, so one ordering
// covers both roles. The index is a pointer array sorted once per old-slot set,
// turning each KEEP lookup into a binary search instead of a linear rescan.
static int32_t mndXnodeSixStarSlotOrderCmp(const void *pa, const void *pb) {
  const SXnodeSixStarSlot *a = *(SXnodeSixStarSlot *const *)pa;
  const SXnodeSixStarSlot *b = *(SXnodeSixStarSlot *const *)pb;
  if (a->role != b->role) return a->role < b->role ? -1 : 1;
  int32_t minLen = a->normalizedKeyLen < b->normalizedKeyLen ? a->normalizedKeyLen : b->normalizedKeyLen;
  if (minLen > 0) {
    int32_t c = memcmp(a->normalizedKey, b->normalizedKey, (size_t)minLen);
    if (c != 0) return c;
  }
  if (a->normalizedKeyLen != b->normalizedKeyLen) return a->normalizedKeyLen < b->normalizedKeyLen ? -1 : 1;
  if (a->duplicateOrdinal != b->duplicateOrdinal) return a->duplicateOrdinal < b->duplicateOrdinal ? -1 : 1;
  return 0;
}

static int32_t mndXnodeSixStarIndexOldSlots(const SXnodeSixStarSlot *pOldSlots, int32_t numOldSlots,
                                            const SXnodeSixStarSlot ***pppSorted) {
  *pppSorted = NULL;
  if (numOldSlots <= 0) return TSDB_CODE_SUCCESS;
  const SXnodeSixStarSlot **ppSorted = taosMemoryMalloc((size_t)numOldSlots * sizeof(*ppSorted));
  if (ppSorted == NULL) return mndXnodeProtectionError();
  for (int32_t i = 0; i < numOldSlots; ++i) ppSorted[i] = &pOldSlots[i];
  taosSort(ppSorted, numOldSlots, sizeof(*ppSorted), mndXnodeSixStarSlotOrderCmp);
  *pppSorted = ppSorted;
  return TSDB_CODE_SUCCESS;
}

static const SXnodeSixStarSlot *mndXnodeSixStarMatchOldSlot(const SXnodeSixStarSlot *const *ppSortedOld,
                                                            int32_t numOldSlots, const SXnodeSixStarSlot *pKeepSlot) {
  if (ppSortedOld == NULL || numOldSlots <= 0) return NULL;
  const SXnodeSixStarSlot  *key = pKeepSlot;
  const SXnodeSixStarSlot **ppHit =
      taosbsearch(&key, ppSortedOld, numOldSlots, sizeof(*ppSortedOld), mndXnodeSixStarSlotOrderCmp, TD_EQ);
  return ppHit != NULL ? *ppHit : NULL;
}

// Merges one candidate DSN against its old committed value. A sensitive field
// whose decoded value is exactly "******" is a control marker: CREATE rejects it,
// ALTER copies the wire bytes of the corresponding old sensitive value into the
// candidate span (KEEP). Any other value keeps the candidate wire bytes as-is
// (REPLACE), and a sensitive field absent from the candidate stays absent.
static int32_t mndXnodeSixStarMergeDsn(const char *oldDsn, int32_t oldDsnLen, const char *candDsn, int32_t candDsnLen,
                                       bool create, int32_t maxLen, char **ppFinal, int32_t *pFinalLen,
                                       int32_t *pSixStarError) {
  if (ppFinal == NULL || pFinalLen == NULL || *ppFinal != NULL || candDsn == NULL || candDsnLen < 0) {
    return mndXnodeProtectionError();
  }
  *pFinalLen = 0;
  SXnodeSixStarSlot       *candSlots = NULL;
  SXnodeSixStarSlot       *oldSlots = NULL;
  const SXnodeSixStarSlot **ppOldIndex = NULL;
  int32_t                  numCand = 0;
  int32_t                  numOld = 0;
  int32_t          code = mndXnodeSixStarScanDsn(candDsn, candDsnLen, &candSlots, &numCand, pSixStarError);
  if (code != TSDB_CODE_SUCCESS) return code;

  bool hasKeep = false;
  for (int32_t i = 0; i < numCand; ++i) {
    if (!mndXnodeSixStarIsMaskedValue(candSlots[i].decodedValue, candSlots[i].decodedValueLen)) continue;
    if (create) {
      code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_CREATE_MASKED);
      goto _OVER;
    }
    hasKeep = true;
  }
  if (!hasKeep) {
    code = mndXnodeSixStarDupBytes(candDsn, candDsnLen, ppFinal, pFinalLen);
    goto _OVER;
  }
  if (oldDsn == NULL) {
    code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_KEEP_MISSING);
    goto _OVER;
  }
  code = mndXnodeSixStarScanDsn(oldDsn, oldDsnLen, &oldSlots, &numOld, pSixStarError);
  if (code != TSDB_CODE_SUCCESS) goto _OVER;
  code = mndXnodeSixStarIndexOldSlots(oldSlots, numOld, &ppOldIndex);
  if (code != TSDB_CODE_SUCCESS) goto _OVER;

  int64_t finalLen = candDsnLen;
  for (int32_t i = 0; i < numCand; ++i) {
    if (!mndXnodeSixStarIsMaskedValue(candSlots[i].decodedValue, candSlots[i].decodedValueLen)) continue;
    const SXnodeSixStarSlot *pMatch = mndXnodeSixStarMatchOldSlot(ppOldIndex, numOld, &candSlots[i]);
    if (pMatch == NULL) {
      code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_KEEP_MISSING);
      goto _OVER;
    }
    finalLen += (int64_t)pMatch->wireValueLen - candSlots[i].wireValueLen;
  }
  if (finalLen < 0 || finalLen > INT32_MAX || (maxLen > 0 && finalLen > maxLen)) {
    code = mndXnodeProtectionError();
    goto _OVER;
  }
  char *out = taosMemoryMalloc((size_t)finalLen + 1);
  if (out == NULL) {
    code = mndXnodeProtectionError();
    goto _OVER;
  }
  int32_t outPos = 0;
  int32_t cursor = 0;
  for (int32_t i = 0; i < numCand; ++i) {
    if (!mndXnodeSixStarIsMaskedValue(candSlots[i].decodedValue, candSlots[i].decodedValueLen)) continue;
    const SXnodeSixStarSlot *pMatch = mndXnodeSixStarMatchOldSlot(ppOldIndex, numOld, &candSlots[i]);
    memcpy(out + outPos, candDsn + cursor, (size_t)(candSlots[i].wireValueOff - cursor));
    outPos += candSlots[i].wireValueOff - cursor;
    memcpy(out + outPos, oldDsn + pMatch->wireValueOff, (size_t)pMatch->wireValueLen);
    outPos += pMatch->wireValueLen;
    cursor = candSlots[i].wireValueOff + candSlots[i].wireValueLen;
  }
  memcpy(out + outPos, candDsn + cursor, (size_t)(candDsnLen - cursor));
  outPos += candDsnLen - cursor;
  out[outPos] = 0;
  *ppFinal = out;
  *pFinalLen = outPos;

_OVER:
  taosMemoryFree(ppOldIndex);
  mndXnodeSixStarFreeSlots(candSlots, numCand);
  mndXnodeSixStarFreeSlots(oldSlots, numOld);
  return code;
}

// Job config top-level from/to span scanner with six-star registry granularity:
// malformed JSON or an escaped top-level key is SIXSTAR_JOB_JSON_INVALID, a
// non-string from/to value is SIXSTAR_INPUT_TYPE_INVALID, and a duplicate
// top-level from/to is SIXSTAR_FIELD_AMBIGUOUS. Kept separate from the SHOW
// projection scanner, which collapses every failure into one error code.
static int32_t mndXnodeSixStarFindJobConfigSpans(const char *config, int32_t configLen, SXnodeJsonSpan *from,
                                                 SXnodeJsonSpan *to, int32_t *pSixStarError) {
  int32_t pos = 0;
  while (pos < configLen && (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
    ++pos;
  if (pos >= configLen || config[pos++] != '{') {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  bool first = true;
  while (true) {
    while (pos < configLen && (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
      ++pos;
    if (pos >= configLen) return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    if (config[pos] == '}') {
      ++pos;
      break;
    }
    if (!first) {
      if (config[pos++] != ',') return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
      while (pos < configLen &&
             (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
        ++pos;
    }
    int32_t keyStart = pos;
    if (mndXnodeSkipJsonString(config, configLen, &pos) != TSDB_CODE_SUCCESS) {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
    int32_t keyEnd = pos;
    for (int32_t i = keyStart + 1; i + 1 < keyEnd; ++i) {
      if (config[i] == '\\') return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
    while (pos < configLen && (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
      ++pos;
    if (pos >= configLen || config[pos++] != ':') {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
    while (pos < configLen && (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
      ++pos;
    const int32_t keyLen = keyEnd - keyStart - 2;
    bool          isFrom = keyLen == 4 && memcmp(config + keyStart + 1, "from", 4) == 0;
    bool          isTo = keyLen == 2 && memcmp(config + keyStart + 1, "to", 2) == 0;
    int32_t       valueStart = pos;
    if (mndXnodeSkipJsonValue(config, configLen, &pos, 1) != TSDB_CODE_SUCCESS) {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
    if (isFrom || isTo) {
      if (config[valueStart] != '"') {
        return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
      }
      SXnodeJsonSpan *span = isFrom ? from : to;
      if (span->present) return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS);
      span->start = valueStart;
      span->end = pos;
      span->present = true;
    }
    first = false;
  }
  while (pos < configLen && (config[pos] == ' ' || config[pos] == '\t' || config[pos] == '\r' || config[pos] == '\n'))
    ++pos;
  return pos == configLen ? TSDB_CODE_SUCCESS
                          : mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
}

// Decoded per-direction DSNs and sensitive slots of an old committed Job config,
// loaded lazily only when a KEEP must be resolved.
typedef struct {
  bool              loaded;
  char             *fromDsn;
  int32_t           fromDsnLen;
  SXnodeSixStarSlot *fromSlots;
  int32_t           numFromSlots;
  char             *toDsn;
  int32_t           toDsnLen;
  SXnodeSixStarSlot *toSlots;
  int32_t           numToSlots;
} SXnodeSixStarOldJobConfig;

static void mndXnodeSixStarFreeOldJobConfig(SXnodeSixStarOldJobConfig *pOld) {
  if (pOld == NULL) return;
  taosMemoryFree(pOld->fromDsn);
  taosMemoryFree(pOld->toDsn);
  mndXnodeSixStarFreeSlots(pOld->fromSlots, pOld->numFromSlots);
  mndXnodeSixStarFreeSlots(pOld->toSlots, pOld->numToSlots);
  pOld->fromDsn = NULL;
  pOld->toDsn = NULL;
  pOld->fromSlots = NULL;
  pOld->toSlots = NULL;
}

static int32_t mndXnodeSixStarLoadOldJobConfig(const char *oldConfig, int32_t oldConfigLen,
                                               SXnodeSixStarOldJobConfig *pOld, int32_t *pSixStarError) {
  if (pOld->loaded) return TSDB_CODE_SUCCESS;
  if (oldConfig == NULL || oldConfigLen <= 0) {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  for (int32_t i = 0; i < oldConfigLen; ++i) {
    if ((unsigned char)oldConfig[i] == 0) {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
  }
  if (!mndXnodeUtf8Valid(oldConfig, oldConfigLen)) {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  cJSON *root = cJSON_ParseWithLength(oldConfig, oldConfigLen);
  if (root == NULL || !cJSON_IsObject(root)) {
    if (root != NULL) cJSON_Delete(root);
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  SXnodeJsonSpan from = {0};
  SXnodeJsonSpan to = {0};
  int32_t        code = mndXnodeSixStarFindJobConfigSpans(oldConfig, oldConfigLen, &from, &to, pSixStarError);
  if (code == TSDB_CODE_SUCCESS) {
    int32_t fromCount = 0;
    int32_t toCount = 0;
    for (cJSON *child = root->child; child != NULL; child = child->next) {
      if (child->string == NULL) continue;
      if (strcmp(child->string, "from") == 0) {
        if (++fromCount > 1) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS);
        if (!cJSON_IsString(child)) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
      } else if (strcmp(child->string, "to") == 0) {
        if (++toCount > 1) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS);
        if (!cJSON_IsString(child)) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
      }
    }
    if (code == TSDB_CODE_SUCCESS && ((fromCount > 0 && !from.present) || (toCount > 0 && !to.present))) {
      code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
  }
  if (code == TSDB_CODE_SUCCESS && from.present) {
    code = mndXnodeDecodeJsonString(oldConfig + from.start, from.end - from.start, &pOld->fromDsn, &pOld->fromDsnLen);
    if (code != TSDB_CODE_SUCCESS) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    if (code == TSDB_CODE_SUCCESS) {
      code = mndXnodeSixStarScanDsn(pOld->fromDsn, pOld->fromDsnLen, &pOld->fromSlots, &pOld->numFromSlots,
                                    pSixStarError);
    }
  }
  if (code == TSDB_CODE_SUCCESS && to.present) {
    code = mndXnodeDecodeJsonString(oldConfig + to.start, to.end - to.start, &pOld->toDsn, &pOld->toDsnLen);
    if (code != TSDB_CODE_SUCCESS) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    if (code == TSDB_CODE_SUCCESS) {
      code = mndXnodeSixStarScanDsn(pOld->toDsn, pOld->toDsnLen, &pOld->toSlots, &pOld->numToSlots, pSixStarError);
    }
  }
  cJSON_Delete(root);
  if (code == TSDB_CODE_SUCCESS) pOld->loaded = true;
  return code;
}

// JobConfigSpliceV1 write-path profile: the candidate config bytes are validated
// with cJSON and a syntax-aware span scanner cross-checked against it, the
// top-level from/to DSNs are merged with six-star semantics, and only the two
// string literal spans are spliced back. Every other byte of the candidate is
// copied verbatim; a direction without KEEP values keeps its original literal
// untouched, and the result is never re-serialized through tjsonToString.
static int32_t mndXnodeSixStarMergeJobConfig(const char *oldConfig, int32_t oldConfigLen, const char *candConfig,
                                             int32_t candConfigLen, bool create, int32_t maxConfigLen, char **ppFinal,
                                             int32_t *pFinalLen, int32_t *pSixStarError) {
  if (ppFinal == NULL || pFinalLen == NULL || *ppFinal != NULL || candConfig == NULL || candConfigLen < 0) {
    return mndXnodeProtectionError();
  }
  *pFinalLen = 0;
  if (candConfigLen == 0) return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  for (int32_t i = 0; i < candConfigLen; ++i) {
    if ((unsigned char)candConfig[i] == 0) {
      return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
  }
  if (!mndXnodeUtf8Valid(candConfig, candConfigLen)) {
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  cJSON *root = cJSON_ParseWithLength(candConfig, candConfigLen);
  if (root == NULL || !cJSON_IsObject(root)) {
    if (root != NULL) cJSON_Delete(root);
    return mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
  }
  SXnodeJsonSpan from = {0};
  SXnodeJsonSpan to = {0};
  int32_t        code = mndXnodeSixStarFindJobConfigSpans(candConfig, candConfigLen, &from, &to, pSixStarError);
  if (code == TSDB_CODE_SUCCESS) {
    int32_t fromCount = 0;
    int32_t toCount = 0;
    for (cJSON *child = root->child; child != NULL; child = child->next) {
      if (child->string == NULL) continue;
      if (strcmp(child->string, "from") == 0) {
        if (++fromCount > 1) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS);
        if (!cJSON_IsString(child)) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
      } else if (strcmp(child->string, "to") == 0) {
        if (++toCount > 1) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS);
        if (!cJSON_IsString(child)) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID);
      }
    }
    if (code == TSDB_CODE_SUCCESS && ((fromCount > 0 && !from.present) || (toCount > 0 && !to.present))) {
      code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
    }
  }

  char                    *replacements[2] = {NULL, NULL};
  int32_t                  replacementLens[2] = {0, 0};
  SXnodeSixStarOldJobConfig oldJob = {0};
  const SXnodeJsonSpan     *spanFor[2] = {&from, &to};
  // directions in document order: 0 = from, 1 = to
  int32_t order[2] = {0, 1};
  if (to.present && (!from.present || to.start < from.start)) {
    order[0] = 1;
    order[1] = 0;
  }
  for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < 2; ++i) {
    const int32_t         direction = order[i];
    const SXnodeJsonSpan *span = spanFor[direction];
    if (!span->present) continue;
    char   *decoded = NULL;
    int32_t decodedLen = 0;
    code = mndXnodeDecodeJsonString(candConfig + span->start, span->end - span->start, &decoded, &decodedLen);
    if (code != TSDB_CODE_SUCCESS) {
      code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID);
      break;
    }
    SXnodeSixStarSlot *candSlots = NULL;
    int32_t          numCand = 0;
    code = mndXnodeSixStarScanDsn(decoded, decodedLen, &candSlots, &numCand, pSixStarError);
    if (code == TSDB_CODE_SUCCESS) {
      bool hasKeep = false;
      for (int32_t s = 0; s < numCand; ++s) {
        if (!mndXnodeSixStarIsMaskedValue(candSlots[s].decodedValue, candSlots[s].decodedValueLen)) continue;
        if (create) {
          code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_CREATE_MASKED);
          break;
        }
        hasKeep = true;
      }
      if (code == TSDB_CODE_SUCCESS && hasKeep) {
        if (oldConfig == NULL) {
          code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_KEEP_MISSING);
        }
        if (code == TSDB_CODE_SUCCESS) {
          code = mndXnodeSixStarLoadOldJobConfig(oldConfig, oldConfigLen, &oldJob, pSixStarError);
        }
        const char              *oldDsn = NULL;
        int32_t                  oldDsnLen = 0;
        const SXnodeSixStarSlot *oldSlots = NULL;
        int32_t                  numOld = 0;
        if (code == TSDB_CODE_SUCCESS) {
          oldDsn = direction == 0 ? oldJob.fromDsn : oldJob.toDsn;
          oldDsnLen = direction == 0 ? oldJob.fromDsnLen : oldJob.toDsnLen;
          oldSlots = direction == 0 ? oldJob.fromSlots : oldJob.toSlots;
          numOld = direction == 0 ? oldJob.numFromSlots : oldJob.numToSlots;
          if (oldDsn == NULL) code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_KEEP_MISSING);
        }
        const SXnodeSixStarSlot **ppOldIndex = NULL;
        if (code == TSDB_CODE_SUCCESS) {
          code = mndXnodeSixStarIndexOldSlots(oldSlots, numOld, &ppOldIndex);
        }
        char   *merged = NULL;
        int32_t mergedLen = 0;
        if (code == TSDB_CODE_SUCCESS) {
          int64_t mergedCap = decodedLen;
          for (int32_t s = 0; s < numCand; ++s) {
            if (!mndXnodeSixStarIsMaskedValue(candSlots[s].decodedValue, candSlots[s].decodedValueLen)) continue;
            const SXnodeSixStarSlot *pMatch = mndXnodeSixStarMatchOldSlot(ppOldIndex, numOld, &candSlots[s]);
            if (pMatch == NULL) {
              code = mndXnodeSixStarError(pSixStarError, XNODE_SIXSTAR_ERROR_KEEP_MISSING);
              break;
            }
            mergedCap += (int64_t)pMatch->wireValueLen - candSlots[s].wireValueLen;
          }
          if (code == TSDB_CODE_SUCCESS) {
            if (mergedCap < 0 || mergedCap > INT32_MAX) {
              code = mndXnodeProtectionError();
            } else {
              merged = taosMemoryMalloc((size_t)mergedCap + 1);
              if (merged == NULL) code = mndXnodeProtectionError();
            }
          }
        }
        if (code == TSDB_CODE_SUCCESS) {
          int32_t outPos = 0;
          int32_t cursor = 0;
          for (int32_t s = 0; s < numCand; ++s) {
            if (!mndXnodeSixStarIsMaskedValue(candSlots[s].decodedValue, candSlots[s].decodedValueLen)) continue;
            const SXnodeSixStarSlot *pMatch = mndXnodeSixStarMatchOldSlot(ppOldIndex, numOld, &candSlots[s]);
            memcpy(merged + outPos, decoded + cursor, (size_t)(candSlots[s].wireValueOff - cursor));
            outPos += candSlots[s].wireValueOff - cursor;
            memcpy(merged + outPos, oldDsn + pMatch->wireValueOff, (size_t)pMatch->wireValueLen);
            outPos += pMatch->wireValueLen;
            cursor = candSlots[s].wireValueOff + candSlots[s].wireValueLen;
          }
          memcpy(merged + outPos, decoded + cursor, (size_t)(decodedLen - cursor));
          outPos += decodedLen - cursor;
          merged[outPos] = 0;
          mergedLen = outPos;
          char   *escaped = NULL;
          int32_t escapedLen = 0;
          code = mndXnodeJsonEscape(merged, mergedLen, &escaped, &escapedLen);
          if (code == TSDB_CODE_SUCCESS) {
            code = mndXnodeJsonQuote(escaped, escapedLen, &replacements[direction], &replacementLens[direction]);
          }
          taosMemoryFree(escaped);
          taosMemoryFree(merged);
        }
        taosMemoryFree(ppOldIndex);
      }
    }
    mndXnodeSixStarFreeSlots(candSlots, numCand);
    taosMemoryFree(decoded);
  }
  cJSON_Delete(root);
  if (code == TSDB_CODE_SUCCESS) {
    int64_t resultLen = candConfigLen;
    for (int32_t i = 0; i < 2; ++i) {
      if (spanFor[i]->present && replacements[i] != NULL) {
        resultLen += replacementLens[i] - (spanFor[i]->end - spanFor[i]->start);
      }
    }
    if (resultLen < 0 || resultLen > INT32_MAX || (maxConfigLen > 0 && resultLen > maxConfigLen)) {
      code = mndXnodeProtectionError();
    } else {
      char *out = taosMemoryMalloc((size_t)resultLen + 1);
      if (out == NULL) {
        code = mndXnodeProtectionError();
      } else {
        int32_t outPos = 0;
        int32_t inputPos = 0;
        for (int32_t i = 0; i < 2; ++i) {
          const int32_t         direction = order[i];
          const SXnodeJsonSpan *span = spanFor[direction];
          if (!span->present) continue;
          memcpy(out + outPos, candConfig + inputPos, (size_t)(span->start - inputPos));
          outPos += span->start - inputPos;
          if (replacements[direction] != NULL) {
            memcpy(out + outPos, replacements[direction], (size_t)replacementLens[direction]);
            outPos += replacementLens[direction];
          } else {
            memcpy(out + outPos, candConfig + span->start, (size_t)(span->end - span->start));
            outPos += span->end - span->start;
          }
          inputPos = span->end;
        }
        memcpy(out + outPos, candConfig + inputPos, (size_t)(candConfigLen - inputPos));
        outPos += candConfigLen - inputPos;
        out[outPos] = 0;
        *ppFinal = out;
        *pFinalLen = outPos;
      }
    }
  }
  taosMemoryFree(replacements[0]);
  taosMemoryFree(replacements[1]);
  mndXnodeSixStarFreeOldJobConfig(&oldJob);
  return code;
}

static int32_t mndXnodeSixStarDupField(const SXnodeSixStarField *pField, char **ppOut, int32_t *pOutLen) {
  if (pField->ptr == NULL) return TSDB_CODE_SUCCESS;
  return mndXnodeSixStarDupBytes(pField->ptr, pField->len, ppOut, pOutLen);
}

int32_t mndXnodeSixStarMerge(const SXnodeSixStarMergeInput *pInput, SXnodeSixStarMergeOutput *pOutput,
                             int32_t *pSixStarError) {
  if (pInput == NULL || pOutput == NULL || pSixStarError == NULL) return mndXnodeProtectionError();
  memset(pOutput, 0, sizeof(*pOutput));
  *pSixStarError = XNODE_SIXSTAR_ERROR_NONE;
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pInput->protectionEnabled) {
    const SXnodeSixStarField *finalFrom = pInput->candidateFrom.ptr != NULL ? &pInput->candidateFrom : &pInput->oldFrom;
    const SXnodeSixStarField *finalTo = pInput->candidateTo.ptr != NULL ? &pInput->candidateTo : &pInput->oldTo;
    const SXnodeSixStarField *finalConfig =
        pInput->candidateConfig.ptr != NULL ? &pInput->candidateConfig : &pInput->oldConfig;
    code = mndXnodeSixStarDupField(finalFrom, &pOutput->from, &pOutput->fromLen);
    if (code == TSDB_CODE_SUCCESS) code = mndXnodeSixStarDupField(finalTo, &pOutput->to, &pOutput->toLen);
    if (code == TSDB_CODE_SUCCESS) code = mndXnodeSixStarDupField(finalConfig, &pOutput->config, &pOutput->configLen);
  } else if (pInput->isJob) {
    if (pInput->candidateConfig.ptr == NULL) {
      code = mndXnodeSixStarDupField(&pInput->oldConfig, &pOutput->config, &pOutput->configLen);
    } else {
      code = mndXnodeSixStarMergeJobConfig(pInput->oldConfig.ptr, pInput->oldConfig.len, pInput->candidateConfig.ptr,
                                           pInput->candidateConfig.len, pInput->create, pInput->maxConfigLen,
                                           &pOutput->config, &pOutput->configLen, pSixStarError);
    }
  } else {
    if (pInput->candidateFrom.ptr == NULL) {
      code = mndXnodeSixStarDupField(&pInput->oldFrom, &pOutput->from, &pOutput->fromLen);
    } else {
      code = mndXnodeSixStarMergeDsn(pInput->oldFrom.ptr, pInput->oldFrom.len, pInput->candidateFrom.ptr,
                                     pInput->candidateFrom.len, pInput->create, pInput->maxFromLen, &pOutput->from,
                                     &pOutput->fromLen, pSixStarError);
    }
    if (code == TSDB_CODE_SUCCESS) {
      if (pInput->candidateTo.ptr == NULL) {
        code = mndXnodeSixStarDupField(&pInput->oldTo, &pOutput->to, &pOutput->toLen);
      } else {
        code = mndXnodeSixStarMergeDsn(pInput->oldTo.ptr, pInput->oldTo.len, pInput->candidateTo.ptr,
                                       pInput->candidateTo.len, pInput->create, pInput->maxToLen, &pOutput->to,
                                       &pOutput->toLen, pSixStarError);
      }
    }
  }
  if (code != TSDB_CODE_SUCCESS) mndXnodeSixStarMergeOutputFree(pOutput);
  return code;
}

void mndXnodeSixStarMergeOutputFree(SXnodeSixStarMergeOutput *pOutput) {
  if (pOutput == NULL) return;
  taosMemoryFree(pOutput->from);
  taosMemoryFree(pOutput->to);
  taosMemoryFree(pOutput->config);
  pOutput->from = NULL;
  pOutput->to = NULL;
  pOutput->config = NULL;
  pOutput->fromLen = 0;
  pOutput->toLen = 0;
  pOutput->configLen = 0;
}

static int32_t mndXnodeCheckCreateTaskSixStar(const SMCreateXnodeTaskReq *pCreate) {
  if (tsXnodeDsnProtection == 0) return TSDB_CODE_SUCCESS;
  SXnodeSixStarMergeInput  input = {0};
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  input.create = true;
  input.protectionEnabled = true;
  input.isJob = false;
  input.candidateFrom.ptr = pCreate->source.cstr.ptr;
  input.candidateFrom.len = pCreate->source.cstr.len;
  input.candidateTo.ptr = pCreate->sink.cstr.ptr;
  input.candidateTo.len = pCreate->sink.cstr.len;
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  input.maxToLen = TSDB_XNODE_TASK_SINK_LEN;
  int32_t code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
  mndXnodeSixStarMergeOutputFree(&output);
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("xnode task:%s, create rejected by dsn protection, code:0x%x:%s, sixStarError:%d", pCreate->name.ptr, code,
          tstrerror(code), sixStarError);
  }
  return code;
}

static int32_t mndXnodeCheckCreateJobSixStar(const SMCreateXnodeJobReq *pCreate) {
  if (tsXnodeDsnProtection == 0) return TSDB_CODE_SUCCESS;
  SXnodeSixStarMergeInput  input = {0};
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  input.create = true;
  input.protectionEnabled = true;
  input.isJob = true;
  input.candidateConfig.ptr = pCreate->config.ptr;
  input.candidateConfig.len = pCreate->config.len;
  input.maxConfigLen = TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN;
  int32_t code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
  mndXnodeSixStarMergeOutputFree(&output);
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("xnode job on task:%d, create rejected by dsn protection, code:0x%x:%s, sixStarError:%d", pCreate->tid, code,
          tstrerror(code), sixStarError);
  }
  return code;
}

// Merges the ALTER candidate DSNs against the committed object. On accept,
// *ppMergedSource/*ppMergedSink take over the final DSN for each field the
// candidate carries (NULL when the field is not updated); the caller adopts the
// buffers into the new object version.
static int32_t mndXnodeMergeUpdateTaskSixStar(const SXnodeTaskObj *pOld, const SMUpdateXnodeTaskReq *pUpdate,
                                              char **ppMergedSource, int32_t *pMergedSourceLen, char **ppMergedSink,
                                              int32_t *pMergedSinkLen) {
  if (ppMergedSource == NULL || pMergedSourceLen == NULL || ppMergedSink == NULL || pMergedSinkLen == NULL) {
    return mndXnodeProtectionError();
  }
  *ppMergedSource = NULL;
  *pMergedSourceLen = 0;
  *ppMergedSink = NULL;
  *pMergedSinkLen = 0;
  if (tsXnodeDsnProtection == 0) return TSDB_CODE_SUCCESS;
  if (pUpdate->source.cstr.ptr == NULL && pUpdate->sink.cstr.ptr == NULL) return TSDB_CODE_SUCCESS;
  SXnodeSixStarMergeInput  input = {0};
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = false;
  input.oldFrom.ptr = pOld->sourceDsn;
  input.oldFrom.len = pOld->sourceDsn != NULL ? pOld->sourceDsnLen - 1 : 0;
  input.oldTo.ptr = pOld->sinkDsn;
  input.oldTo.len = pOld->sinkDsn != NULL ? pOld->sinkDsnLen - 1 : 0;
  input.candidateFrom.ptr = pUpdate->source.cstr.ptr;
  input.candidateFrom.len = pUpdate->source.cstr.len;
  input.candidateTo.ptr = pUpdate->sink.cstr.ptr;
  input.candidateTo.len = pUpdate->sink.cstr.len;
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  input.maxToLen = TSDB_XNODE_TASK_SINK_LEN;
  int32_t code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
  if (code == TSDB_CODE_SUCCESS) {
    if (pUpdate->source.cstr.ptr != NULL) {
      *ppMergedSource = output.from;
      *pMergedSourceLen = output.fromLen;
      output.from = NULL;
    }
    if (pUpdate->sink.cstr.ptr != NULL) {
      *ppMergedSink = output.to;
      *pMergedSinkLen = output.toLen;
      output.to = NULL;
    }
  }
  mndXnodeSixStarMergeOutputFree(&output);
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("xnode task:%d, alter rejected by dsn protection, code:0x%x:%s, sixStarError:%d", pUpdate->tid, code,
          tstrerror(code), sixStarError);
  }
  return code;
}

static int32_t mndXnodeMergeUpdateJobSixStar(const SXnodeJobObj *pOld, const SMUpdateXnodeJobReq *pUpdate,
                                             char **ppMergedConfig, int32_t *pMergedConfigLen) {
  if (ppMergedConfig == NULL || pMergedConfigLen == NULL) return mndXnodeProtectionError();
  *ppMergedConfig = NULL;
  *pMergedConfigLen = 0;
  if (tsXnodeDsnProtection == 0) return TSDB_CODE_SUCCESS;
  if (pUpdate->config == NULL) return TSDB_CODE_SUCCESS;
  SXnodeSixStarMergeInput  input = {0};
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = true;
  input.oldConfig.ptr = pOld->config;
  input.oldConfig.len = pOld->config != NULL ? pOld->configLen - 1 : 0;
  input.candidateConfig.ptr = pUpdate->config;
  input.candidateConfig.len = pUpdate->configLen;
  input.maxConfigLen = TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN;
  int32_t code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
  if (code == TSDB_CODE_SUCCESS) {
    *ppMergedConfig = output.config;
    *pMergedConfigLen = output.configLen;
    output.config = NULL;
  }
  mndXnodeSixStarMergeOutputFree(&output);
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("xnode job:%d, alter rejected by dsn protection, code:0x%x:%s, sixStarError:%d", pUpdate->jid, code,
          tstrerror(code), sixStarError);
  }
  return code;
}

static int32_t mndRetrieveXnodeTasks(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  int32_t        numOfRows = 0;
  int32_t        cols = 0;
  SXnodeTaskObj *pObj = NULL;
  char           buf[VARSTR_HEADER_SIZE + TSDB_XNODE_TASK_SOURCE_LEN];
  int32_t        code = 0;
  SUserObj      *pOperUser = NULL;
  mDebug("show.type:%d, %s:%d: retrieve xnode tasks with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser), NULL, _end);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_TASK, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    if (!pOperUser->superUser) {
      bool isOwner = pObj->ownerId != 0 && pObj->ownerId == pOperUser->uid;
      // Legacy rows have no immutable owner; preserve the historical createdBy fallback.
      if (pObj->ownerId == 0 && pObj->createdBy != NULL && pObj->createdByLen > 0 &&
          strncmp(pObj->createdBy, pReq->info.conn.user, TSDB_USER_LEN) == 0) {
        isOwner = true;
      }
      if (!isOwner) {
        // Reuse the acquired user to avoid per-task user-table locking.
        bool canShow =
            (0 == mndXnodeCheckTaskObjPrivilegeByUser(pMnode, pOperUser, pObj, PRIV_CM_SHOW));
        if (!canShow) {
          sdbRelease(pSdb, pObj);
          continue;
        }
      }
    }

    if (!mndXnodeShowFilterMatches(pShow->xnodeRetrieveFilterType, pShow->xnodeRetrieveFilterValue, pObj->id, 0)) {
      sdbRelease(pSdb, pObj);
      continue;
    }

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
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pObj->sourceDsnLen > 0 && pObj->sourceDsn != NULL) {
      if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT) {
        // PLAINTEXT keeps the zero-allocation stack path. buf holds at most the
        // stored DSN limit; the show schema is wider (2x) to fit protected output.
        buf[0] = 0;
        STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->sourceDsn, VARSTR_HEADER_SIZE + TSDB_XNODE_TASK_SOURCE_LEN);
        code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      } else {
        code = mndXnodeSetDsnShowColumn(pShow, pColInfo, numOfRows, pObj->sourceDsn, pObj->sourceDsnLen - 1,
                                        TSDB_XNODE_TASK_SOURCE_LEN);
      }
      if (code != 0) goto _end;
    } else {
      colDataSetNULL(pColInfo, numOfRows);
    }

    // to
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pObj->sinkDsnLen > 0 && pObj->sinkDsn != NULL) {
      if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT) {
        buf[0] = 0;
        STR_WITH_MAXSIZE_TO_VARSTR(buf, pObj->sinkDsn, VARSTR_HEADER_SIZE + TSDB_XNODE_TASK_SINK_LEN);
        code = colDataSetVal(pColInfo, numOfRows, (const char *)buf, false);
      } else {
        code = mndXnodeSetDsnShowColumn(pShow, pColInfo, numOfRows, pObj->sinkDsn, pObj->sinkDsnLen - 1,
                                        TSDB_XNODE_TASK_SINK_LEN);
      }
      if (code != 0) goto _end;
    } else {
      colDataSetNULL(pColInfo, numOfRows);
    }

    // parser
    if (pObj->parserLen > 0 && pObj->parser != NULL) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = mndSetXnodeBlobColumn(pColInfo, numOfRows, pObj->parser, pObj->parserLen);
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
  if (code != 0) {
    sdbRelease(pSdb, pObj);
    mError("failed to retrieve xnode tasks, code:%s", tstrerror(code));
  }
  mndReleaseUser(pMnode, pOperUser);

  // Only failures reported by the protection helpers carry the normalized code
  // and turn the show object sticky; unrelated failures keep their original code.
  if (code == TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED) {
    pShow->xnodeProtectionFailed = true;
    return code;
  }

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

  int32_t rawDataLen =
      sizeof(SXnodeJobObj) + TSDB_XNODE_RESERVE_SIZE + pObj->configLen + pObj->reasonLen + pObj->statusLen;

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

  // The cheap length gate runs before the six-star scan so an oversized config
  // is rejected without paying for the full protection scan.
  jobObj.configLen = pCreate->config.len + 1;
  if (jobObj.configLen > TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN + 1) {
    code = TSDB_CODE_MND_XNODE_TASK_JOB_CONFIG_TOO_LONG;
    goto _OVER;
  }

  // Six-star write-queue authority: reject control markers before the final
  // object exists and before any commit log is encoded.
  TAOS_CHECK_GOTO(mndXnodeCheckCreateJobSixStar(pCreate), NULL, _OVER);

  jobObj.id = sdbGetMaxId(pMnode->pSdb, SDB_XNODE_JOB);
  jobObj.taskId = pCreate->tid;
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
    if (jobObj.reasonLen > TSDB_XNODE_TASK_REASON_LEN + 1) {
      code = TSDB_CODE_MND_XNODE_TASK_REASON_TOO_LONG;
      goto _OVER;
    }
    jobObj.reason = taosMemoryCalloc(1, jobObj.reasonLen);
    if (jobObj.reason == NULL) goto _OVER;
    (void)memcpy(jobObj.reason, pCreate->reason.ptr, pCreate->reason.len);
  }

  jobObj.createTime = taosGetTimestampMs();
  jobObj.updateTime = jobObj.createTime;

  mDebug("create xnode job, id:%d, tid:%d, config bytes:%d, time:%" PRId64, jobObj.id, jobObj.taskId,
         jobObj.configLen - 1, jobObj.createTime);

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
  char   *mergedConfig = NULL;
  int32_t mergedConfigLen = 0;

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

  // The cheap length gate runs before the six-star merge so an oversized
  // candidate config is rejected without paying for the merge.
  if (pUpdate->config != NULL && pUpdate->configLen > TSDB_XNODE_TASK_JOB_CONFIG_MAX_LEN) {
    code = TSDB_CODE_MND_XNODE_TASK_JOB_CONFIG_TOO_LONG;
    goto _OVER;
  }

  // Six-star KEEP/REPLACE merge against the committed object, before the final
  // object is assembled and the commit log is encoded.
  code = mndXnodeMergeUpdateJobSixStar(pOld, pUpdate, &mergedConfig, &mergedConfigLen);
  if (code != TSDB_CODE_SUCCESS) goto _OVER;

  if (pUpdate->config != NULL) {
    if (mergedConfig != NULL) {
      jobObj.configLen = mergedConfigLen + 1;
      jobObj.config = mergedConfig;
      mergedConfig = NULL;
    } else {
      jobObj.configLen = pUpdate->configLen + 1;
      jobObj.config = taosMemoryCalloc(1, jobObj.configLen);
      if (jobObj.config == NULL) goto _OVER;
      (void)memcpy(jobObj.config, pUpdate->config, pUpdate->configLen);
    }
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
    code = terrno;
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
  taosMemoryFree(mergedConfig);
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
  SXnodeTaskObj      *pTaskObj = NULL;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_CREATE_XNODE_JOB);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for create xnode job, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMCreateXnodeJobReq(pReq->pCont, pReq->contLen, &createReq), NULL, _OVER);
  mDebug("xnode create job on xnode:%d", createReq.xnodeId);

  // task must exists
  pTaskObj = mndAcquireXnodeTaskById(pMnode, createReq.tid);
  if (pTaskObj == NULL) {
    code = TSDB_CODE_MND_XNODE_TASK_NOT_EXIST;
    goto _OVER;
  }
  // check task privilege
  code = mndXnodeCheckTaskObjPrivilege(pMnode, pReq->info.conn.user, pTaskObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode task:%d", pReq->info.conn.user, pTaskObj->id);
    goto _OVER;
  }

  code = mndCreateXnodeJob(pMnode, pReq, &createReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("xnode task job on task id:%d, failed to create since %s", createReq.tid, tstrerror(code));
  }

  mndReleaseXnodeTask(pMnode, pTaskObj);
  tFreeSMCreateXnodeJobReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessUpdateXnodeJobReq(SRpcMsg *pReq) {
  SMnode             *pMnode = pReq->info.node;
  int32_t             code = -1;
  SXnodeJobObj       *pObj = NULL;
  SMUpdateXnodeJobReq updateReq = {0};

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_UPDATE_XNODE_JOB);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for update xnode job, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMUpdateXnodeJobReq(pReq->pCont, pReq->contLen, &updateReq), NULL, _OVER);

  pObj = mndAcquireXnodeJob(pMnode, updateReq.jid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  code = mndXnodeCheckJobObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode job:%d, task:%d", pReq->info.conn.user, pObj->id, pObj->taskId);
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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_REBALANCE_XNODE_JOB);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for rebalance xnode job, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMRebalanceXnodeJobReq(pReq->pCont, pReq->contLen, &rebalanceReq), NULL, _OVER);
  mDebug("RebalanceXnodeJob with jid:%d, xnode_id:%d, start to rebalance", rebalanceReq.jid, rebalanceReq.xnodeId);

  if (rebalanceReq.jid <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pObj = mndAcquireXnodeJob(pMnode, rebalanceReq.jid);
  if (pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  code = mndXnodeCheckJobObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_ALTER);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode job:%d, task:%d", pReq->info.conn.user, pObj->id, pObj->taskId);
    goto _OVER;
  }

  // send request
  char xnodeUrl[TSDB_XNODE_URL_LEN + 1] = {0};
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/rebalance/manual/%d/%d/%d", XNODED_PIPE_SOCKET_URL, pObj->taskId, pObj->id,
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
  createTime.nd.datum.p =
      formatTimestampLocal(createTime.nd.datum.p, TD_TIME_STR_LEN, pJob->createTime, TSDB_TIME_PRECISION_MILLI);
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
  updateTime.nd.datum.p =
      formatTimestampLocal(updateTime.nd.datum.p, TD_TIME_STR_LEN, pJob->updateTime, TSDB_TIME_PRECISION_MILLI);
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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/rebalance/auto", XNODED_PIPE_SOCKET_URL);
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
  int32_t                      code = 0;
  SMnode                      *pMnode = pReq->info.node;
  SMRebalanceXnodeJobsWhereReq rebalanceReq = {0};
  SNode                       *pWhere = NULL;
  SArray                      *pArray = NULL;
  SArray                      *pResult = NULL;

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_REBALANCE_XNODE_JOB);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for rebalance xnode jobs where, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMRebalanceXnodeJobsWhereReq(pReq->pCont, pReq->contLen, &rebalanceReq), NULL, _OVER);

  TAOS_CHECK_GOTO(mndAcquireXnodeJobsAll(pMnode, &pArray), NULL, _OVER);
  for (int32_t i = 0; i < pArray->size; i++) {
    SXnodeJobObj *pJob = taosArrayGet(pArray, i);
    code = mndXnodeCheckJobObjPrivilege(pMnode, pReq->info.conn.user, pJob, PRIV_CM_ALTER);
    if (code == TSDB_CODE_SUCCESS) {
      continue;
    } else if (code == TSDB_CODE_MND_NO_RIGHTS) {
      taosArrayRemove(pArray, i);
      i--;
      // Reset so filtered-out jobs do not fail the whole batch.
      code = 0;
    } else {
      mError("user:%s rebalance where xnode job:%d, error:%s", pReq->info.conn.user, pJob->id, tstrerror(code));
      goto _OVER;
    }
  }
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
  int32_t        code = 0;
  int32_t        lino = 0;
  SXnodeJobObj  *pObj = NULL;

  pObj = mndAcquireXnodeJob(pMnode, jid);
  if (pObj == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _OVER;
  }
  code = mndXnodeCheckJobObjPrivilege(pMnode, pReq->info.conn.user, pObj, PRIV_CM_DROP);
  if (code != TSDB_CODE_SUCCESS) {
    mError("user:%s has no privilege on xnode job:%d, task:%d", pReq->info.conn.user, pObj->id, pObj->taskId);
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

    for (int32_t i = 0; i < pArray->size; i++) {
      SXnodeJobObj *pJob = taosArrayGet(pArray, i);
      code = mndXnodeCheckJobObjPrivilege(pMnode, pReq->info.conn.user, pJob, PRIV_CM_DROP);
      if (code == TSDB_CODE_SUCCESS) {
        continue;
      } else if (code == TSDB_CODE_MND_NO_RIGHTS) {
        taosArrayRemove(pArray, i);
        i--;
        // Reset so filtered-out jobs do not fail the whole batch.
        code = 0;
      } else {
        mError("user:%s drop where xnode job:%d, error:%s", pReq->info.conn.user, pJob->id, tstrerror(code));
        goto _OVER;
      }
    }
    TAOS_CHECK_GOTO(filterJobsByWhereCond(pWhere, pArray, &pResult), &lino, _OVER);

    for (int32_t i = 0; i < pResult->size; i++) {
      pObj = taosArrayGet(pResult, i);
      TAOS_CHECK_GOTO(mndDropXnodeJob(pMnode, NULL, pObj), &lino, _OVER);
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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_DROP_XNODE_JOB);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for drop xnode jobs, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeJobReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);
  mDebug("Xnode drop job with jid:%d", dropReq.jid);

  if (dropReq.jid <= 0 && dropReq.ast.ptr == NULL) {
    code = TSDB_CODE_MND_XNODE_INVALID_MSG;
    goto _OVER;
  }
  if (dropReq.jid > 0) {
    code = dropXnodeJobById(pMnode, pReq, dropReq.jid);
    if (code == 0) {
      code = TSDB_CODE_ACTION_IN_PROGRESS;
    } else {
      goto _OVER;
    }
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
    {.name = "config",
     .bytes = TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN + BLOBSTR_HEADER_SIZE,
     .type = TSDB_DATA_TYPE_BLOB, .sysInfo = false},
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
  char          buf[VARSTR_HEADER_SIZE + TSDB_XNODE_TASK_REASON_LEN];
  char          status[64] = {0};
  int32_t       code = 0;
  SUserObj     *pOperUser = NULL;
  mDebug("show.type:%d, %s:%d: retrieve xnode jobs with rows: %d", pShow->type, __FILE__, __LINE__, rows);

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser), NULL, _end);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_XNODE_JOB, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    if (!pOperUser->superUser) {
      // Reuse the acquired user to avoid per-job user-table locking.
      bool allowed = (0 == mndXnodeCheckJobObjPrivilegeByUser(pMnode, pOperUser, pObj, PRIV_CM_SHOW));
      if (!allowed) {
        sdbRelease(pSdb, pObj);
        continue;
      }
    }

    if (!mndXnodeShowFilterMatches(pShow->xnodeRetrieveFilterType, pShow->xnodeRetrieveFilterValue, pObj->id,
                                   pObj->taskId)) {
      sdbRelease(pSdb, pObj);
      continue;
    }

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
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (pShow->xnodeOutputMode == XNODE_SHOW_OUTPUT_PLAINTEXT) {
      // PLAINTEXT keeps the zero-allocation direct write into the column buffer.
      code = mndSetXnodeBlobColumn(pColInfo, numOfRows, pObj->config, pObj->configLen);
      if (code != 0) goto _end;
    } else if (pObj->config == NULL) {
      colDataSetNULL(pColInfo, numOfRows);
    } else {
      char   *projectedConfig = NULL;
      int32_t projectedConfigLen = 0;
      code = mndXnodeProjectJobConfigForShow(pShow, pObj->config, pObj->configLen, &projectedConfig,
                                             &projectedConfigLen);
      if (code == TSDB_CODE_SUCCESS) {
        code = varColSetVarData(pColInfo, numOfRows, projectedConfig, projectedConfigLen, false);
      }
      taosMemoryFree(projectedConfig);
      if (code != 0) goto _end;
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
  mndReleaseUser(pMnode, pOperUser);

  // Only failures reported by the protection helpers carry the normalized code
  // and turn the show object sticky; unrelated failures keep their original code.
  if (code == TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED) {
    pShow->xnodeProtectionFailed = true;
    return code;
  }

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

static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp, int32_t timeout, const char *socketPath) {
  CURL   *curl = NULL;
  int32_t code = 0;
  int32_t lino = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  #ifndef WINDOWS
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath), &lino, _OVER);
  #endif
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
  #ifndef WINDOWS
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath), &lino, _OVER);
  #endif
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_URL, url), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POST, 1), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf), &lino, _OVER);
  TAOS_CHECK_GOTO(curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L), &lino, _OVER);
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

  #ifndef WINDOWS
  if (curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, socketPath)) goto _OVER;
  #endif
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

SJson *mndSendReqRetJson(const char *url, EHttpType type, int64_t timeout, const char *buf, int64_t bufLen) {
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};
  char      socketPath[PATH_MAX] = {0};

  getXnodedPipeName(socketPath, sizeof(socketPath));
  #ifndef WINDOWS
  if (!taosCheckExistFile(socketPath)) {
    uError("xnode failed to send request, socket path:%s not exist", socketPath);
    terrno = TSDB_CODE_MND_XNODE_URL_CANT_ACCESS;
    goto _EXIT;
  }
  #endif
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
    goto _EXIT;
  }

_OVER:
  if (terrno == TSDB_CODE_SUCCESS) {
    if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
      pJson = tjsonCreateObject();
      goto _EXIT;
    }
    pJson = tjsonParse(curlRsp.data);
    if (pJson == NULL) {
      terrno = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _EXIT;
    }
  } else if (terrno == TSDB_CODE_MND_XNODE_HTTP_CODE_ERROR) {
    pJson = tjsonCreateObject();
    char *buf = taosMemCalloc(1, curlRsp.dataLen + 1);
    (void)memcpy(buf, curlRsp.data, curlRsp.dataLen);
    if (tjsonAddStringToObject(pJson, "__inner_error", buf) != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(buf);
      goto _EXIT;
    }
    taosMemoryFreeClear(buf);
  }

_EXIT:
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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/xnode/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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

static int32_t mndXnodeCheckTaskObjPrivilegeByUser(SMnode *pMnode, SUserObj *curUser,
                                                   SXnodeTaskObj *pTask, EPrivType priv) {
  if (pMnode == NULL || curUser == NULL || pTask == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SUserObj *tUser = NULL;
  int32_t   code = 0;
  int64_t ownerId = pTask->ownerId;
  int32_t acctId = pTask->acctId;
  if (ownerId == 0) {
    // Legacy rows have no immutable owner; preserve the historical createdBy fallback.
    if (pTask->createdBy != NULL) {
      code = mndAcquireUser(pMnode, pTask->createdBy, &tUser);
    }
    if (code != 0 && code != TSDB_CODE_MND_USER_NOT_EXIST) {
      goto _OVER;
    }
    ownerId = tUser != NULL ? tUser->uid : 0;
    acctId = tUser != NULL ? tUser->acctId : curUser->acctId;
  }

  char objName[32] = {0};
  snprintf(objName, sizeof(objName), "%d", pTask->id);
  code = mndCheckObjPrivilegeRec(pMnode, curUser, priv, PRIV_OBJ_XTASK, ownerId, acctId, objName, NULL);

_OVER:
  mndReleaseUser(pMnode, tUser);
  return code;
}

static int32_t mndXnodeCheckTaskObjPrivilege(SMnode *pMnode, const char *user, SXnodeTaskObj *pTask,
                                             EPrivType priv) {
  if (pMnode == NULL || user == NULL || pTask == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SUserObj *curUser = NULL;
  int32_t   code = mndAcquireUser(pMnode, user, &curUser);
  if (code == 0) {
    code = mndXnodeCheckTaskObjPrivilegeByUser(pMnode, curUser, pTask, priv);
  }
  mndReleaseUser(pMnode, curUser);
  return code;
}

static int32_t mndXnodeCheckJobObjPrivilegeByUser(SMnode *pMnode, SUserObj *curUser,
                                                  SXnodeJobObj *pJob, EPrivType priv) {
  if (pMnode == NULL || curUser == NULL || pJob == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SXnodeTaskObj *pTask = mndAcquireXnodeTaskById(pMnode, pJob->taskId);
  if (pTask != NULL) {
    int32_t code = mndXnodeCheckTaskObjPrivilegeByUser(pMnode, curUser, pTask, priv);
    mndReleaseXnodeTask(pMnode, pTask);
    return code;
  }
  if (!curUser->superUser) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  char objName[32] = {0};
  snprintf(objName, sizeof(objName), "%d", pJob->taskId);
  return mndCheckObjPrivilegeRec(pMnode, curUser, priv, PRIV_OBJ_XTASK, 0, curUser->acctId, objName, NULL);
}

static int32_t mndXnodeCheckJobObjPrivilege(SMnode *pMnode, const char *user, SXnodeJobObj *pJob, EPrivType priv) {
  if (pMnode == NULL || user == NULL || pJob == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SUserObj *curUser = NULL;
  int32_t   code = mndAcquireUser(pMnode, user, &curUser);
  if (code == 0) {
    code = mndXnodeCheckJobObjPrivilegeByUser(pMnode, curUser, pJob, priv);
  }
  mndReleaseUser(pMnode, curUser);
  return code;
}

static int32_t mndValidateXnodePermissions(SMnode *pMnode, SRpcMsg *pReq, EOperType oper) {
  int32_t code = grantCheck(TSDB_GRANT_XNODE);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to validate xnode permissions, code:%s, oper:%d", tstrerror(code), oper);
    return code;
  }

  EPrivType privType;
  switch (oper) {
    case MND_OPER_CREATE_XNODE:        privType = PRIV_NODE_CREATE; break;
    case MND_OPER_UPDATE_XNODE:        privType = PRIV_NODE_ALTER; break;
    case MND_OPER_DRAIN_XNODE:         privType = PRIV_NODE_ALTER; break;
    case MND_OPER_DROP_XNODE:          privType = PRIV_NODE_DROP; break;
    case MND_OPER_CREATE_XNODE_AGENT:  privType = PRIV_NODE_CREATE; break;
    case MND_OPER_UPDATE_XNODE_AGENT:  privType = PRIV_NODE_ALTER; break;
    case MND_OPER_DROP_XNODE_AGENT:    privType = PRIV_NODE_DROP; break;

    case MND_OPER_CREATE_XNODE_TASK:   privType = PRIV_XNODE_TASK_CREATE; break;
    case MND_OPER_START_XNODE_TASK:    return code;
    case MND_OPER_STOP_XNODE_TASK:     return code;
    case MND_OPER_UPDATE_XNODE_TASK:   return code;
    case MND_OPER_DROP_XNODE_TASK:     return code;

    case MND_OPER_CREATE_XNODE_JOB:    privType = PRIV_XNODE_TASK_CREATE; break;
    case MND_OPER_UPDATE_XNODE_JOB:    return code;
    case MND_OPER_REBALANCE_XNODE_JOB: return code;
    case MND_OPER_DROP_XNODE_JOB:      return code;
    default: return TSDB_CODE_MND_NO_RIGHTS;
  }

  SUserObj *pUser = NULL;
  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pUser);
  if (code != 0) return code;

  code = mndCheckSysObjPrivilege(pMnode, pUser, NULL, privType, 0, 0, NULL, NULL);
  mndReleaseUser(pMnode, pUser);
  return code;
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

typedef struct {
  int64_t sub;  // agent ID
  int64_t iat;  // issued at time
} agentTokenField;

const unsigned char MNDXNODE_DEFAULT_SECRET[] = {126, 222, 130, 137, 43,  122, 41,  173, 144, 146, 116,
                                                 138, 153, 244, 251, 99,  50,  55,  140, 238, 218, 232,
                                                 15,  161, 226, 54,  130, 40,  211, 234, 111, 171};

#ifndef WINDOWS

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

  // BIO chain:b64  ->  bio
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
#else

static int32_t mndXnodeHmacSha256Windows(const unsigned char *key, size_t key_len, const unsigned char *message,
                                         size_t message_len, unsigned char *output) {
  unsigned char k_pad[64];
  unsigned char k_ipad[64];
  unsigned char k_opad[64];
  unsigned char inner_hash[SHA256_DIGEST_SIZE];
  sha256_ctx    ctx;

  if (key_len > 64) {
    sha256_init(&ctx);
    sha256_update(&ctx, key, key_len);
    sha256_final(&ctx, k_pad);
    memset(k_pad + SHA256_DIGEST_SIZE, 0, 64 - SHA256_DIGEST_SIZE);
  } else {
    memcpy(k_pad, key, key_len);
    memset(k_pad + key_len, 0, 64 - key_len);
  }

  for (int i = 0; i < 64; i++) {
    k_ipad[i] = k_pad[i] ^ 0x36;
    k_opad[i] = k_pad[i] ^ 0x5c;
  }

  sha256_init(&ctx);
  sha256_update(&ctx, k_ipad, 64);
  sha256_update(&ctx, message, message_len);
  sha256_final(&ctx, inner_hash);

  sha256_init(&ctx);
  sha256_update(&ctx, k_opad, 64);
  sha256_update(&ctx, inner_hash, SHA256_DIGEST_SIZE);
  sha256_final(&ctx, output);

  return 0;
}

static char *mndXnodeBase64UrlEncodeWindows(const unsigned char *input, size_t input_len) {
  char *base64_str = NULL;

  int32_t ret = base64_encode(input, input_len, &base64_str);
  if (ret != 0 || !base64_str) {
    return NULL;
  }

  for (size_t i = 0; base64_str[i] != '\0'; i++) {
    if (base64_str[i] == '+') {
      base64_str[i] = '-';
    } else if (base64_str[i] == '/') {
      base64_str[i] = '_';
    }
  }

  size_t len = strlen(base64_str);
  while (len > 0 && base64_str[len - 1] == '=') {
    base64_str[len - 1] = '\0';
    len--;
  }

  return base64_str;
}

static char *mndXnodeCreateTokenHeaderWindows() {
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
  encoded = mndXnodeBase64UrlEncodeWindows((const unsigned char *)headerJsonStr, strlen(headerJsonStr));
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

static char *mndXnodeCreateTokenPayloadWindows(const agentTokenField *claims) {
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
  encoded = mndXnodeBase64UrlEncodeWindows((const unsigned char *)payloadStr, strlen(payloadStr));
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

static char *mndXnodeCreateTokenSignatureWindows(const char *header_payload, const unsigned char *secret,
                                                  size_t secret_len) {
  int32_t       code = 0, lino = 0;
  unsigned char hash[SHA256_DIGEST_SIZE] = {0};
  char         *encoded = NULL;

  int32_t ret = mndXnodeHmacSha256Windows(secret, secret_len, (const unsigned char *)header_payload,
                                          strlen(header_payload), hash);
  if (ret != 0) {
    code = terrno;
    goto _exit;
  }

  encoded = mndXnodeBase64UrlEncodeWindows(hash, SHA256_DIGEST_SIZE);
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

  header = mndXnodeCreateTokenHeaderWindows();
  if (!header) {
    code = terrno;
    goto _exit;
  }

  payload = mndXnodeCreateTokenPayloadWindows(claims);
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

  signature = mndXnodeCreateTokenSignatureWindows(headerPayload, secret, secret_len);
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

  agentTokenField claims = {
      .iat = pAgent->createTime,
      .sub = pAgent->id,
  };
  // token be like:
  // "<JWT_HEADER>.<JWT_PAYLOAD>.<JWT_SIGNATURE>"
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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/agent", XNODED_PIPE_SOCKET_URL);
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
    mError("failed check permission for create xnode agent, code:%s", tstrerror(code));
    goto _OVER;
  }

  code = tDeserializeSMCreateXnodeAgentReq(pReq->pCont, pReq->contLen, &createReq);
  if (code != 0) {
    mError("failed to deserialize create xnode agent request, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_CHECK_GOTO(mndCheckXnodeAgentExists(pMnode, createReq.name.ptr), NULL, _OVER);

  TAOS_CHECK_GOTO(mndCreateXnodeAgent(pMnode, pReq, &createReq, &pObj), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_UPDATE_XNODE_AGENT);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for update xnode agent, code:%s", tstrerror(code));
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

  code = mndValidateXnodePermissions(pMnode, pReq, MND_OPER_DROP_XNODE_AGENT);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed check permission for drop xnode agent, code:%s", tstrerror(code));
    goto _OVER;
  }
  TAOS_CHECK_GOTO(tDeserializeSMDropXnodeAgentReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);
  mDebug("xnode drop agent with id:%d, start to drop", dropReq.id);

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
  snprintf(xnodeUrl, TSDB_XNODE_URL_LEN + 1, "%s/agent/%d", XNODED_PIPE_SOCKET_URL, pObj->id);
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
    snprintf(pOption.token, sizeof(pOption.token), "%s", token);
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
    mInfo("mndXnode become leader found no xnoded user pass");
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

  taosMsleep(200);
  SXnodeUserPassObj *pObj = mndAcquireFirstXnodeUserPass(pMnode);
  if (pObj == NULL) {
    mInfo("mndXnode restart found no xnoded user pass");
    return;
  }
  mndStartXnoded(pMnode, pObj->user, pObj->pass, pObj->token);
  mndReleaseXnodeUserPass(pMnode, pObj);
  mInfo("mndxnode xnoded restarted");
  return;
}
