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
#include "audit.h"
#include "mndConfig.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "tutil.h"
#include "tcompare.h"

#define CFG_VER_NUMBER    1
#define CFG_RESERVE_SIZE  63
#define CFG_ALTER_TIMEOUT 3 * 1000

// Sync timeout ratio constants
#define SYNC_TIMEOUT_DIVISOR       4
#define SYNC_TIMEOUT_ELECT_DIVISOR 2
#define SYNC_TIMEOUT_SR_DIVISOR    4
#define SYNC_TIMEOUT_HB_DIVISOR    8

static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pInMCfgReq, int32_t optLen, int32_t *pOutValue);
static int32_t mndProcessShowVariablesReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessConfigReq(SRpcMsg *pReq);
static int32_t mndInitWriteCfg(SMnode *pMnode);
static int32_t mndSendRebuildReq(SMnode *pMnode);
static int32_t mndTryRebuildConfigSdbRsp(SRpcMsg *pRsp);
static int32_t initConfigArrayFromSdb(SMnode *pMnode, SArray *array);
static int32_t mndTryRebuildConfigSdb(SRpcMsg *pReq);
static void    cfgArrayCleanUp(SArray *array);
static void    cfgObjArrayCleanUp(SArray *array);
int32_t        compareSConfigItemArrays(SMnode *pMnode, const SArray *dArray, SArray *diffArray);

static int32_t mndConfigUpdateTrans(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype,
                                    int32_t tsmmConfigVersion);
static int32_t mndConfigUpdateTransWithDnode(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype,
                                             int32_t tsmmConfigVersion, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq);
static int32_t mndFindConfigsToAdd(SMnode *pMnode, SArray *addArray);
static int32_t mndFindConfigsToDelete(SMnode *pMnode, SArray *deleteArray);
static int32_t mndExecuteConfigSyncTrans(SMnode *pMnode, SArray *addArray, SArray *deleteArray);

int32_t mndSetCreateConfigCommitLogs(STrans *pTrans, SConfigObj *obj);
int32_t mndSetDeleteConfigCommitLogs(STrans *pTrans, SConfigObj *item);
int32_t mndSetCreateConfigPrepareLogs(STrans *pTrans, SConfigObj *obj);

int32_t mndInitConfig(SMnode *pMnode) {
  int32_t   code = 0;
  SSdbTable table = {.sdbType = SDB_CFG,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mnCfgActionEncode,
                     .decodeFp = (SdbDecodeFp)mndCfgActionDecode,
                     .insertFp = (SdbInsertFp)mndCfgActionInsert,
                     .updateFp = (SdbUpdateFp)mndCfgActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndCfgActionDelete,
                     .deployFp = (SdbDeployFp)mndCfgActionDeploy,
                     .afterRestoredFp = (SdbAfterRestoredFp)mndCfgActionAfterRestored};

  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG, mndProcessConfigReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_DNODE, mndProcessConfigDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CONFIG_DNODE_RSP, mndProcessConfigDnodeRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_SHOW_VARIABLES, mndProcessShowVariablesReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_SDB, mndTryRebuildConfigSdb);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_SDB_RSP, mndTryRebuildConfigSdbRsp);

  return sdbSetTable(pMnode->pSdb, table);
}

SSdbRaw *mnCfgActionEncode(SConfigObj *obj) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if ((code = tEncodeSConfigObj(&encoder, obj)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_CFG, CFG_VER_NUMBER, size);
  TSDB_CHECK_NULL(pRaw, code, lino, _over, terrno);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  tEncoderInit(&encoder, buf, tlen);
  if ((code = tEncodeSConfigObj(&encoder, obj)) < 0) {
    tEncoderClear(&encoder);
    TSDB_CHECK_CODE(code, lino, _over);
  }

  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _over);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _over);
  SDB_SET_DATALEN(pRaw, dataPos, _over);

_over:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    mError("cfg:%s, failed to encode to raw:%p at line:%d since %s", obj->name, pRaw, lino, tstrerror(code));
    sdbFreeRaw(pRaw);
    terrno = code;
    return NULL;
  }

  terrno = 0;
  mTrace("cfg:%s, encode to raw:%p, row:%p", obj->name, pRaw, obj);
  return pRaw;
}

SSdbRow *mndCfgActionDecode(SSdbRaw *pRaw) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSdbRow    *pRow = NULL;
  SConfigObj *pObj = NULL;
  void       *buf = NULL;
  int8_t      sver = 0;
  int32_t     tlen;
  int32_t     dataPos = 0;

  code = sdbGetRawSoftVer(pRaw, &sver);
  TSDB_CHECK_CODE(code, lino, _over);

  if (sver != CFG_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _over;
  }

  pRow = sdbAllocRow(sizeof(SConfigObj));
  TSDB_CHECK_NULL(pRow, code, lino, _over, terrno);

  pObj = sdbGetRowObj(pRow);
  TSDB_CHECK_NULL(pObj, code, lino, _over, terrno);

  SDB_GET_INT32(pRaw, dataPos, &tlen, _over);

  buf = taosMemoryMalloc(tlen + 1);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _over);

  SDecoder decoder;
  tDecoderInit(&decoder, buf, tlen + 1);
  code = tDecodeSConfigObj(&decoder, pObj);
  tDecoderClear(&decoder);

  if (code < 0) {
    tFreeSConfigObj(pObj);
  }

_over:
  taosMemoryFreeClear(buf);

  if (code != TSDB_CODE_SUCCESS) {
    mError("cfg:%s, failed to decode from raw:%p since %s at:%d", pObj->name, pRaw, tstrerror(code), lino);
    taosMemoryFreeClear(pRow);
    terrno = code;
    return NULL;
  } else {
    mTrace("config:%s, decode from raw:%p, row:%p", pObj->name, pRaw, pObj);
    terrno = 0;
    return pRow;
  }
}

static int32_t mndCfgActionInsert(SSdb *pSdb, SConfigObj *obj) {
  mTrace("cfg:%s, perform insert action, row:%p", obj->name, obj);
  return 0;
}

static int32_t mndCfgActionDelete(SSdb *pSdb, SConfigObj *obj) {
  mTrace("cfg:%s, perform delete action, row:%p", obj->name, obj);
  tFreeSConfigObj(obj);
  return 0;
}

static int32_t mndCfgActionUpdate(SSdb *pSdb, SConfigObj *pOld, SConfigObj *pNew) {
  mTrace("cfg:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  switch (pNew->dtype) {
    case CFG_DTYPE_NONE:
      break;
    case CFG_DTYPE_BOOL:
      pOld->bval = pNew->bval;
      break;
    case CFG_DTYPE_INT32:
      pOld->i32 = pNew->i32;
      break;
    case CFG_DTYPE_INT64:
      pOld->i64 = pNew->i64;
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      pOld->fval = pNew->fval;
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      taosMemoryFree(pOld->str);
      pOld->str = taosStrdup(pNew->str);
      if (pOld->str == NULL) {
        return terrno;
      }
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndCfgActionDeploy(SMnode *pMnode) { return mndInitWriteCfg(pMnode); }

static int32_t mndCfgActionAfterRestored(SMnode *pMnode) { return mndSendRebuildReq(pMnode); }

static int32_t mndProcessConfigReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SConfigReq configReq = {0};
  int32_t    code = TSDB_CODE_SUCCESS;
  SArray    *array = NULL;
  bool       needFree = false;
  code = tDeserializeSConfigReq(pReq->pCont, pReq->contLen, &configReq);
  if (code != 0) {
    mError("failed to deserialize config req, since %s", terrstr());
    goto _OVER;
  }

  SConfigObj *vObj = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  if (vObj == NULL) {
    mInfo("failed to acquire mnd config version, since %s", terrstr());
    goto _OVER;
  }

  array = taosArrayInit(16, sizeof(SConfigItem));
  if (array == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  SConfigRsp configRsp = {0};
  configRsp.cver = vObj->i32;

  if (configReq.cver == vObj->i32) {
    configRsp.isVersionVerified = 1;
  } else {
    code = initConfigArrayFromSdb(pMnode, array);
    if (code != 0) {
      mError("failed to init config array from sdb, since %s", tstrerror(code));
      goto _OVER;
    }
    configRsp.array = array;
  }

  int32_t contLen = tSerializeSConfigRsp(NULL, 0, &configRsp);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  void *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }
  contLen = tSerializeSConfigRsp(pHead, contLen, &configRsp);
  if (contLen < 0) {
    rpcFreeCont(pHead);
    code = contLen;
    goto _OVER;
  }
  pReq->info.rspLen = contLen;
  pReq->info.rsp = pHead;

_OVER:
  if (code != 0) {
    mError("failed to process config req, since %s", tstrerror(code));
  }
  sdbRelease(pMnode->pSdb, vObj);
  cfgArrayCleanUp(array);
  tFreeSConfigReq(&configReq);
  return code;
}

int32_t mndInitWriteCfg(SMnode *pMnode) {
  int    code = 0;
  size_t sz = 0;

  mInfo("init write cfg to sdb");
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "init-write-config");
  if (pTrans == NULL) {
    mError("failed to init write cfg in create trans, since %s", terrstr());
    goto _OVER;
  }

  // encode mnd config version
  SConfigObj versionObj = mndInitConfigVersion();
  if ((code = mndSetCreateConfigCommitLogs(pTrans, &versionObj)) != 0) {
    mError("failed to init mnd config version, since %s", tstrerror(code));
    tFreeSConfigObj(&versionObj);
    goto _OVER;
  }
  tFreeSConfigObj(&versionObj);
  sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));

  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
    SConfigObj   obj;
    if ((code = mndInitConfigObj(item, &obj)) != 0) {
      goto _OVER;
    }
    if ((code = mndSetCreateConfigCommitLogs(pTrans, &obj)) != 0) {
      mError("failed to init mnd config:%s, since %s", item->name, tstrerror(code));
      tFreeSConfigObj(&obj);
      goto _OVER;
    }
    tFreeSConfigObj(&obj);
  }
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

_OVER:
  if (code != 0) {
    mError("failed to init write cfg, since %s", tstrerror(code));
  }
  mndTransDrop(pTrans);
  return code;
}

int32_t mndSendRebuildReq(SMnode *pMnode) {
  int32_t code = 0;

  SRpcMsg rpcMsg = {.pCont = NULL,
                    .contLen = 0,
                    .msgType = TDMT_MND_CONFIG_SDB,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1,
                    .info.refId = 0,
                    .info.noResp = 0,
                    .info.handle = 0};
  SEpSet  epSet = {0};

  mndGetMnodeEpSet(pMnode, &epSet);

  code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    mError("failed to send rebuild config req, since %s", tstrerror(code));
  }
  return code;
}

static int32_t mndTryRebuildConfigSdb(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  if (!mndIsLeader(pMnode)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t     code = 0;
  SConfigObj *vObj = NULL;
  SArray     *addArray = NULL;
  SArray     *deleteArray = NULL;

  vObj = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  if (vObj == NULL) {
    code = mndInitWriteCfg(pMnode);
    if (code < 0) {
      mError("failed to init write cfg, since %s", tstrerror(code));
    } else {
      mInfo("failed to acquire mnd config version, try to rebuild config in sdb.");
    }
    goto _exit;
  }

  addArray = taosArrayInit(4, sizeof(SConfigObj));
  deleteArray = taosArrayInit(4, sizeof(SConfigObj));
  if (addArray == NULL || deleteArray == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // Find configs to add and delete
  if ((code = mndFindConfigsToAdd(pMnode, addArray)) != 0) {
    mError("failed to find configs to add, since %s", tstrerror(code));
    goto _exit;
  }

  if ((code = mndFindConfigsToDelete(pMnode, deleteArray)) != 0) {
    mError("failed to find configs to delete, since %s", tstrerror(code));
    goto _exit;
  }

  // Execute the sync transaction
  if ((code = mndExecuteConfigSyncTrans(pMnode, addArray, deleteArray)) != 0) {
    mError("failed to execute config sync transaction, since %s", tstrerror(code));
    goto _exit;
  }

_exit:
  if (code != 0) {
    mError("failed to try rebuild config in sdb, since %s", tstrerror(code));
  }
  sdbRelease(pMnode->pSdb, vObj);
  cfgObjArrayCleanUp(addArray);
  cfgObjArrayCleanUp(deleteArray);
  TAOS_RETURN(code);
}

int32_t mndSetCreateConfigCommitLogs(STrans *pTrans, SConfigObj *item) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mnCfgActionEncode(item);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    taosMemoryFree(pCommitRaw);
    TAOS_RETURN(code);
  }
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) TAOS_RETURN(code);
  return TSDB_CODE_SUCCESS;
}

int32_t mndSetDeleteConfigCommitLogs(STrans *pTrans, SConfigObj *item) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mnCfgActionEncode(item);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    taosMemoryFree(pCommitRaw);
    TAOS_RETURN(code);
  }
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED)) != 0) TAOS_RETURN(code);
  return TSDB_CODE_SUCCESS;
}

int32_t mndSetCreateConfigPrepareLogs(STrans *pTrans, SConfigObj *item) {
  int32_t  code = 0;
  SSdbRaw *pPrepareRaw = mnCfgActionEncode(item);
  if (pPrepareRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pPrepareRaw)) != 0) {
    taosMemoryFree(pPrepareRaw);
    TAOS_RETURN(code);
  }
  if ((code = sdbSetRawStatus(pPrepareRaw, SDB_STATUS_READY)) != 0) TAOS_RETURN(code);
  return TSDB_CODE_SUCCESS;
}

static int32_t mndFindConfigsToAdd(SMnode *pMnode, SArray *addArray) {
  int32_t code = 0;
  int32_t sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));

  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
    SConfigObj  *obj = sdbAcquire(pMnode->pSdb, SDB_CFG, item->name);
    if (obj == NULL) {
      mInfo("config:%s, not exist in sdb, will add it", item->name);
      SConfigObj newObj;
      if ((code = mndInitConfigObj(item, &newObj)) != 0) {
        TAOS_RETURN(code);
      }
      if (NULL == taosArrayPush(addArray, &newObj)) {
        tFreeSConfigObj(&newObj);
        TAOS_RETURN(terrno);
      }
    } else {
      sdbRelease(pMnode->pSdb, obj);
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t mndFindConfigsToDelete(SMnode *pMnode, SArray *deleteArray) {
  int32_t     code = 0;
  int32_t     sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SConfigObj *obj = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_CFG, pIter, (void **)&obj);
    if (pIter == NULL) break;
    if (obj == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    // Skip the version config
    if (strcasecmp(obj->name, "tsmmConfigVersion") == 0) {
      sdbRelease(pSdb, obj);
      continue;
    }

    // Check if this config exists in global config
    bool existsInGlobal = false;
    for (int i = 0; i < sz; ++i) {
      SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
      if (strcasecmp(obj->name, item->name) == 0) {
        existsInGlobal = true;
        break;
      }
    }

    if (!existsInGlobal) {
      mInfo("config:%s, not exist in global config, will delete it from sdb", obj->name);
      SConfigObj deleteObj = {0};
      tstrncpy(deleteObj.name, obj->name, CFG_NAME_MAX_LEN);
      deleteObj.dtype = obj->dtype;

      // Copy the value based on type
      switch (obj->dtype) {
        case CFG_DTYPE_BOOL:
          deleteObj.bval = obj->bval;
          break;
        case CFG_DTYPE_INT32:
          deleteObj.i32 = obj->i32;
          break;
        case CFG_DTYPE_INT64:
          deleteObj.i64 = obj->i64;
          break;
        case CFG_DTYPE_FLOAT:
        case CFG_DTYPE_DOUBLE:
          deleteObj.fval = obj->fval;
          break;
        case CFG_DTYPE_STRING:
        case CFG_DTYPE_DIR:
        case CFG_DTYPE_LOCALE:
        case CFG_DTYPE_CHARSET:
        case CFG_DTYPE_TIMEZONE:
          deleteObj.str = taosStrdup(obj->str);
          if (deleteObj.str == NULL) {
            sdbCancelFetch(pSdb, pIter);
            sdbRelease(pSdb, obj);
            TAOS_RETURN(terrno);
          }
          break;
        default:
          break;
      }

      if (NULL == taosArrayPush(deleteArray, &deleteObj)) {
        tFreeSConfigObj(&deleteObj);
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, obj);
        TAOS_RETURN(terrno);
      }
    }

    sdbRelease(pSdb, obj);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t mndExecuteConfigSyncTrans(SMnode *pMnode, SArray *addArray, SArray *deleteArray) {
  int32_t addSize = taosArrayGetSize(addArray);
  int32_t deleteSize = taosArrayGetSize(deleteArray);

  if (addSize == 0 && deleteSize == 0) {
    return TSDB_CODE_SUCCESS;
  }

  const char *transName = "sync-config";
  if (addSize > 0 && deleteSize > 0) {
    transName = "sync-config";
  } else if (addSize > 0) {
    transName = "add-config";
  } else {
    transName = "delete-config";
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, transName);
  if (pTrans == NULL) {
    TAOS_RETURN(terrno);
  }

  int32_t code = 0;

  // Add new configs
  for (int i = 0; i < addSize; ++i) {
    SConfigObj *AddObj = taosArrayGet(addArray, i);
    if ((code = mndSetCreateConfigCommitLogs(pTrans, AddObj)) != 0) {
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
  }

  // Delete obsolete configs
  for (int i = 0; i < deleteSize; ++i) {
    SConfigObj *DelObj = taosArrayGet(deleteArray, i);
    if ((code = mndSetDeleteConfigCommitLogs(pTrans, DelObj)) != 0) {
      mndTransDrop(pTrans);
      TAOS_RETURN(code);
    }
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mInfo("sync config to sdb, add nums:%d, delete nums:%d", addSize, deleteSize);
  mndTransDrop(pTrans);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t mndMCfg2DCfg(SMCfgDnodeReq *pMCfgReq, SDCfgDnodeReq *pDCfgReq) {
  int32_t code = 0;
  char   *p = pMCfgReq->config;
  while (*p) {
    if (*p == ' ') {
      break;
    }
    p++;
  }

  size_t optLen = p - pMCfgReq->config;
  tstrncpy(pDCfgReq->config, pMCfgReq->config, sizeof(pDCfgReq->config));
  pDCfgReq->config[optLen] = 0;

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    tstrncpy(pDCfgReq->value, p + 1, sizeof(pDCfgReq->value));
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    tstrncpy(pDCfgReq->value, pMCfgReq->value, sizeof(pDCfgReq->value));
  }

  TAOS_RETURN(code);

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  code = TSDB_CODE_INVALID_CFG;
  TAOS_RETURN(code);
}

static int32_t mndBuildCfgDnodeRedoAction(STrans *pTrans, SDnodeObj *pDnode, SDCfgDnodeReq *pDcfgReq) {
  int32_t code = 0;
  SEpSet  epSet = mndGetDnodeEpset(pDnode);
  int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, pDcfgReq);
  void   *pBuf = taosMemoryMalloc(bufLen);

  if (pBuf == NULL) {
    code = terrno;
    return code;
  }

  if ((bufLen = tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq)) <= 0) {
    code = bufLen;
    taosMemoryFree(pBuf);
    return code;
  }

  STransAction action = {
      .epSet = epSet,
      .pCont = pBuf,
      .contLen = bufLen,
      .msgType = TDMT_DND_CONFIG_DNODE,
      .acceptableCode = 0,
      .groupId = -1,
  };

  mInfo("dnode:%d, append redo action to trans, config:%s value:%s", pDnode->id, pDcfgReq->config, pDcfgReq->value);

  if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
    taosMemoryFree(pBuf);
    return code;
  }

  return code;
}

static int32_t mndSendCfgDnodeReq(SMnode *pMnode, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;

  int64_t curMs = taosGetTimestampMs();

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (pDnode->id == dnodeId || dnodeId == -1 || dnodeId == 0) {
      bool online = mndIsDnodeOnline(pDnode, curMs);
      if (!online) {
        mWarn("dnode:%d, is offline, skip to send config req", pDnode->id);
        continue;
      }
      SEpSet  epSet = mndGetDnodeEpset(pDnode);
      int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, pDcfgReq);
      void   *pBuf = rpcMallocCont(bufLen);

      if (pBuf == NULL) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDnode);
        code = TSDB_CODE_OUT_OF_MEMORY;
        return code;
      }

      if ((bufLen = tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq)) <= 0) {
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDnode);
        code = bufLen;
        rpcFreeCont(pBuf);
        return code;
      }

      mInfo("dnode:%d, send config req to dnode, config:%s value:%s", pDnode->id, pDcfgReq->config, pDcfgReq->value);
      SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen};
      SRpcMsg rpcRsp = {0};

      code = rpcSendRecvWithTimeout(pMnode->msgCb.statusRpc, &epSet, &rpcMsg, &rpcRsp, NULL, CFG_ALTER_TIMEOUT);
      if (code != 0) {
        mError("failed to send config req to dnode:%d, since %s", pDnode->id, tstrerror(code));
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDnode);
        return code;
      }

      code = rpcRsp.code;
      if (code != 0) {
        mError("failed to alter config %s,on dnode:%d, since %s", pDcfgReq->config, pDnode->id, tstrerror(code));
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDnode);
        return code;
      }
      rpcFreeCont(rpcRsp.pCont);
    }
    sdbRelease(pSdb, pDnode);
  }

  if (code == -1) {
    code = TSDB_CODE_MND_DNODE_NOT_EXIST;
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  int32_t       lino = -1;
  SMnode       *pMnode = pReq->info.node;
  SMCfgDnodeReq cfgReq = {0};
  SConfigObj   *vObj = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  if (vObj == NULL) {
    code = TSDB_CODE_SDB_OBJ_NOT_THERE;
    mInfo("failed to acquire mnd config version, since %s", tstrerror(code));
    goto _err_out;
  }

  TAOS_CHECK_RETURN(tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq));
  int8_t updateIpWhiteList = 0;
  mInfo("dnode:%d, start to config, option:%s, value:%s", cfgReq.dnodeId, cfgReq.config, cfgReq.value);
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE)) != 0) {
    goto _err_out;
  }

  SDCfgDnodeReq dcfgReq = {0};
  if (strcasecmp(cfgReq.config, "resetlog") == 0) {
    tstrncpy(dcfgReq.config, "resetlog", 9);
    goto _send_req;
#ifdef TD_ENTERPRISE
  } else if (strncasecmp(cfgReq.config, "ssblocksize", 12) == 0) {
    int32_t optLen = strlen("ssblocksize");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) {
      goto _err_out;
    }

    if (flag > 1024 * 1024 || (flag > -1 && flag < 1024) || flag < -1) {
      mError("dnode:%d, failed to config ssblocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]",
             cfgReq.dnodeId, flag);
      code = TSDB_CODE_INVALID_CFG;
      goto _err_out;
    }

    tstrncpy(dcfgReq.config, "ssblocksize", 12);
    snprintf(dcfgReq.value, TSDB_DNODE_VALUE_LEN, "%d", flag);
#endif
  } else {
    TAOS_CHECK_GOTO(mndMCfg2DCfg(&cfgReq, &dcfgReq), &lino, _err_out);
    if (strlen(dcfgReq.config) > TSDB_DNODE_CONFIG_LEN) {
      mError("dnode:%d, failed to config since config is too long", cfgReq.dnodeId);
      code = TSDB_CODE_INVALID_CFG;
      goto _err_out;
    }
    if (strncasecmp(dcfgReq.config, "enableWhiteList", strlen("enableWhiteList")) == 0) {
      updateIpWhiteList = 1;
    }

    CfgAlterType alterType = (cfgReq.dnodeId == 0 || cfgReq.dnodeId == -1) ? CFG_ALTER_ALL_DNODES : CFG_ALTER_DNODE;
    TAOS_CHECK_GOTO(cfgCheckRangeForDynUpdate(taosGetCfg(), dcfgReq.config, dcfgReq.value, true, alterType), &lino,
                    _err_out);
  }
  SConfigItem *pItem = cfgGetItem(taosGetCfg(), dcfgReq.config);
  // Update config in sdb.
  if (pItem == NULL) {
    mError("failed to find config:%s while process config dnode req", cfgReq.config);
    code = TSDB_CODE_CFG_NOT_FOUND;
    goto _err_out;
  }

  // Audit log
  {
    char obj[50] = {0};
    (void)tsnprintf(obj, sizeof(obj), "%d", cfgReq.dnodeId);
    auditRecord(pReq, pMnode->clusterId, "alterDnode", obj, "", cfgReq.sql, cfgReq.sqlLen);
  }

  dcfgReq.version = vObj->i32 + 1;

  if (pItem->category == CFG_CATEGORY_GLOBAL) {
    // Use transaction to update SDB and send to dnode atomically
    TAOS_CHECK_GOTO(mndConfigUpdateTransWithDnode(pMnode, dcfgReq.config, dcfgReq.value, pItem->dtype, dcfgReq.version,
                                                  cfgReq.dnodeId, &dcfgReq),
                    &lino, _err_out);
  } else {
    // For local config, still use the old method (only send to dnode)
    goto _send_req;
  }

  // For global config, transaction has handled everything, go to success
  goto _success;

_send_req:
  dcfgReq.version = vObj->i32;
  code = mndSendCfgDnodeReq(pMnode, cfgReq.dnodeId, &dcfgReq);
  if (code != 0) {
    mError("failed to send config req to dnode:%d, since %s", cfgReq.dnodeId, tstrerror(code));
    goto _err_out;
  }

_success:
  // dont care suss or succ;
  if (updateIpWhiteList) mndRefreshUserIpWhiteList(pMnode);
  tFreeSMCfgDnodeReq(&cfgReq);
  sdbRelease(pMnode->pSdb, vObj);
  TAOS_RETURN(code);

_err_out:
  mError("failed to process config dnode req, since %s", tstrerror(code));
  tFreeSMCfgDnodeReq(&cfgReq);
  sdbRelease(pMnode->pSdb, vObj);
  TAOS_RETURN(code);
}

static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from dnode");
  return 0;
}

static int32_t mndTryRebuildConfigSdbRsp(SRpcMsg *pRsp) {
  mInfo("rebuild config sdb rsp");
  return 0;
}

// Helper function to create and commit a config object
static int32_t mndCreateAndCommitConfigObj(STrans *pTrans, const char *srcName, const char *cfgName, char *value,
                                           int32_t *lino) {
  int32_t     code = 0;
  SConfigObj *pTmp = taosMemoryMalloc(sizeof(SConfigObj));
  if (pTmp == NULL) {
    code = terrno;
    return code;
  }

  pTmp->dtype = CFG_DTYPE_INT32;
  tstrncpy(pTmp->name, cfgName, CFG_NAME_MAX_LEN);
  code = mndUpdateObj(pTmp, srcName, value);
  if (code != 0) {
    tFreeSConfigObj(pTmp);
    taosMemoryFree(pTmp);
    return code;
  }

  code = mndSetCreateConfigCommitLogs(pTrans, pTmp);
  tFreeSConfigObj(pTmp);
  taosMemoryFree(pTmp);
  return code;
}

// Helper function to handle syncTimeout related config updates
static int32_t mndHandleSyncTimeoutConfigs(STrans *pTrans, const char *srcName, const char *pValue, int32_t *lino) {
  int32_t code = 0;
  int32_t syncTimeout = 0;
  char    tmp[10] = {0};

  if (sscanf(pValue, "%d", &syncTimeout) != 1) {
    syncTimeout = 0;
  }

  int32_t baseTimeout = syncTimeout - syncTimeout / SYNC_TIMEOUT_DIVISOR;

  // arbSetAssignedTimeoutMs = syncTimeout
  sprintf(tmp, "%d", syncTimeout);
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "arbSetAssignedTimeoutMs", tmp, lino), lino, _OVER);

  // arbHeartBeatIntervalMs = syncTimeout / 4
  sprintf(tmp, "%d", syncTimeout / SYNC_TIMEOUT_DIVISOR);
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "arbHeartBeatIntervalMs", tmp, lino), lino, _OVER);

  // arbCheckSyncIntervalMs = syncTimeout / 4
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "arbCheckSyncIntervalMs", tmp, lino), lino, _OVER);

  // syncVnodeElectIntervalMs = (syncTimeout - syncTimeout / 4) / 2
  sprintf(tmp, "%d", baseTimeout / SYNC_TIMEOUT_ELECT_DIVISOR);
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "syncVnodeElectIntervalMs", tmp, lino), lino, _OVER);

  // syncMnodeElectIntervalMs = (syncTimeout - syncTimeout / 4) / 2
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "syncMnodeElectIntervalMs", tmp, lino), lino, _OVER);

  // statusTimeoutMs = (syncTimeout - syncTimeout / 4) / 2
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "statusTimeoutMs", tmp, lino), lino, _OVER);

  // statusSRTimeoutMs = (syncTimeout - syncTimeout / 4) / 4
  sprintf(tmp, "%d", baseTimeout / SYNC_TIMEOUT_SR_DIVISOR);
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "statusSRTimeoutMs", tmp, lino), lino, _OVER);

  // syncVnodeHeartbeatIntervalMs = (syncTimeout - syncTimeout / 4) / 8
  sprintf(tmp, "%d", baseTimeout / SYNC_TIMEOUT_HB_DIVISOR);
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "syncVnodeHeartbeatIntervalMs", tmp, lino), lino, _OVER);

  // syncMnodeHeartbeatIntervalMs = (syncTimeout - syncTimeout / 4) / 8
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "syncMnodeHeartbeatIntervalMs", tmp, lino), lino, _OVER);

  // statusIntervalMs = (syncTimeout - syncTimeout / 4) / 8
  TAOS_CHECK_GOTO(mndCreateAndCommitConfigObj(pTrans, srcName, "statusIntervalMs", tmp, lino), lino, _OVER);

_OVER:
  return code;
}

// get int32_t value from 'SMCfgDnodeReq'
static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pMCfgReq, int32_t optLen, int32_t *pOutValue) {
  int32_t code = 0;
  if (' ' != pMCfgReq->config[optLen] && 0 != pMCfgReq->config[optLen]) {
    goto _err;
  }

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    *pOutValue = taosStr2Int32(pMCfgReq->config + optLen + 1, NULL, 10);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    *pOutValue = taosStr2Int32(pMCfgReq->value, NULL, 10);
  }

  TAOS_RETURN(code);

_err:
  mError(" failed to set config since:%s", tstrerror(code));
  TAOS_RETURN(code);
}

static int32_t mndConfigUpdateTrans(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype,
                                    int32_t tsmmConfigVersion) {
  int32_t     code = -1;
  int32_t     lino = -1;
  SConfigObj *pVersion = taosMemoryMalloc(sizeof(SConfigObj)), *pObj = taosMemoryMalloc(sizeof(SConfigObj));
  if (pVersion == NULL || pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  tstrncpy(pVersion->name, "tsmmConfigVersion", CFG_NAME_MAX_LEN);
  pVersion->dtype = CFG_DTYPE_INT32;
  pVersion->i32 = tsmmConfigVersion;

  pObj->dtype = dtype;
  tstrncpy(pObj->name, name, CFG_NAME_MAX_LEN);

  TAOS_CHECK_GOTO(mndUpdateObj(pObj, name, pValue), &lino, _OVER);
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-config");
  if (pTrans == NULL) {
    code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update config:%s to value:%s", pTrans->id, name, pValue);
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, pVersion), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, pObj), &lino, _OVER);

  if (taosStrncasecmp(name, "syncTimeout", CFG_NAME_MAX_LEN) == 0) {
    TAOS_CHECK_GOTO(mndHandleSyncTimeoutConfigs(pTrans, name, pValue, &lino), &lino, _OVER);
  }
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;
  code = 0;
_OVER:
  if (code != 0) {
    mError("failed to update config:%s to value:%s, since %s", name, pValue, tstrerror(code));
  }
  mndTransDrop(pTrans);
  tFreeSConfigObj(pVersion);
  taosMemoryFree(pVersion);
  tFreeSConfigObj(pObj);
  taosMemoryFree(pObj);
  return code;
}

static int32_t mndConfigUpdateTransWithDnode(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype,
                                             int32_t tsmmConfigVersion, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq) {
  int32_t     code = -1;
  int32_t     lino = -1;
  SConfigObj *pVersion = taosMemoryMalloc(sizeof(SConfigObj)), *pObj = taosMemoryMalloc(sizeof(SConfigObj));
  if (pVersion == NULL || pObj == NULL) {
    code = terrno;
    goto _OVER;
  }
  tstrncpy(pVersion->name, "tsmmConfigVersion", CFG_NAME_MAX_LEN);
  pVersion->dtype = CFG_DTYPE_INT32;
  pVersion->i32 = tsmmConfigVersion;

  pObj->dtype = dtype;
  tstrncpy(pObj->name, name, CFG_NAME_MAX_LEN);

  TAOS_CHECK_GOTO(mndUpdateObj(pObj, name, pValue), &lino, _OVER);
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "update-config-with-dnode");
  if (pTrans == NULL) {
    code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update config:%s to value:%s and send to dnode", pTrans->id, name, pValue);

  // Add prepare logs for SDB config updates (execute in PREPARE stage, before redo actions)
  TAOS_CHECK_GOTO(mndSetCreateConfigPrepareLogs(pTrans, pVersion), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateConfigPrepareLogs(pTrans, pObj), &lino, _OVER);

  // Add commit logs for transaction persistence
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, pVersion), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, pObj), &lino, _OVER);

  if (taosStrncasecmp(name, "syncTimeout", CFG_NAME_MAX_LEN) == 0) {
    TAOS_CHECK_GOTO(mndHandleSyncTimeoutConfigs(pTrans, name, pValue, &lino), &lino, _OVER);
  }

  // Add redo actions to send config to dnodes
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int64_t curMs = taosGetTimestampMs();

  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (pDnode->id == dnodeId || dnodeId == -1 || dnodeId == 0) {
      bool online = mndIsDnodeOnline(pDnode, curMs);
      if (!online) {
        mWarn("dnode:%d, is offline, still add to trans for retry", pDnode->id);
      }

      code = mndBuildCfgDnodeRedoAction(pTrans, pDnode, pDcfgReq);
      if (code != 0) {
        mError("failed to build config redo action for dnode:%d, since %s", pDnode->id, tstrerror(code));
        sdbCancelFetch(pMnode->pSdb, pIter);
        sdbRelease(pMnode->pSdb, pDnode);
        goto _OVER;
      }
    }
    sdbRelease(pSdb, pDnode);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;
  code = 0;

_OVER:
  if (code != 0) {
    mError("failed to update config:%s to value:%s and send to dnode, since %s", name, pValue, tstrerror(code));
  }
  mndTransDrop(pTrans);
  tFreeSConfigObj(pVersion);
  taosMemoryFree(pVersion);
  tFreeSConfigObj(pObj);
  taosMemoryFree(pObj);
  return code;
}

static int32_t initConfigArrayFromSdb(SMnode *pMnode, SArray *array) {
  int32_t     code = 0;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SConfigObj *obj = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_CFG, pIter, (void **)&obj);
    if (pIter == NULL) break;
    if (obj == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    if (strcasecmp(obj->name, "tsmmConfigVersion") == 0) {
      sdbRelease(pSdb, obj);
      continue;
    }
    SConfigItem item = {0};
    item.dtype = obj->dtype;
    item.name = taosStrdup(obj->name);
    if (item.name == NULL) {
      code = terrno;
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, obj);
      goto _exit;
    }
    switch (obj->dtype) {
      case CFG_DTYPE_NONE:
        break;
      case CFG_DTYPE_BOOL:
        item.bval = obj->bval;
        break;
      case CFG_DTYPE_INT32:
        item.i32 = obj->i32;
        break;
      case CFG_DTYPE_INT64:
        item.i64 = obj->i64;
        break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE:
        item.fval = obj->fval;
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
        item.str = taosStrdup(obj->str);
        if (item.str == NULL) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, obj);
          code = terrno;
          goto _exit;
        }
        break;
    }
    if (taosArrayPush(array, &item) == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, obj);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
      break;
    }
    sdbRelease(pSdb, obj);
  }
_exit:
  if (code != 0) {
    mError("failed to init config array from sdb, since %s", tstrerror(code));
  }
  return code;
}

static void cfgArrayCleanUp(SArray *array) {
  if (array == NULL) {
    return;
  }

  int32_t sz = taosArrayGetSize(array);
  for (int32_t i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(array, i);
    if (item->dtype == CFG_DTYPE_STRING || item->dtype == CFG_DTYPE_DIR || item->dtype == CFG_DTYPE_LOCALE ||
        item->dtype == CFG_DTYPE_CHARSET || item->dtype == CFG_DTYPE_TIMEZONE) {
      taosMemoryFreeClear(item->str);
    }
    taosMemoryFreeClear(item->name);
  }

  taosArrayDestroy(array);
}

static void cfgObjArrayCleanUp(SArray *array) {
  if (array == NULL) {
    return;
  }
  int32_t sz = taosArrayGetSize(array);
  for (int32_t i = 0; i < sz; ++i) {
    SConfigObj *obj = taosArrayGet(array, i);
    tFreeSConfigObj(obj);
  }
  taosArrayDestroy(array);
}

static SArray *initVariablesFromItems(SArray *pItems, const char* likePattern) {
  if (pItems == NULL) {
    return NULL;
  }

  int32_t sz = taosArrayGetSize(pItems);

  SArray *pInfos = taosArrayInit(sz, sizeof(SVariablesInfo));
  if (pInfos == NULL) {
    mError("failed to init array while init variables from items, since %s", tstrerror(terrno));
    return NULL;
  }
  for (int32_t i = 0; i < sz; ++i) {
    SConfigItem   *pItem = taosArrayGet(pItems, i);
    SVariablesInfo info = {0};
    tstrncpy(info.name, pItem->name, sizeof(info.name));
    if (likePattern != NULL && rawStrPatternMatch(pItem->name, likePattern) != TSDB_PATTERN_MATCH) {
      continue;
    }

    // init info value
    switch (pItem->dtype) {
      case CFG_DTYPE_NONE:
        break;
      case CFG_DTYPE_BOOL:
        tsnprintf(info.value, sizeof(info.value), "%d", pItem->bval);
        break;
      case CFG_DTYPE_INT32:
        tsnprintf(info.value, sizeof(info.value), "%d", pItem->i32);
        break;
      case CFG_DTYPE_INT64:
        tsnprintf(info.value, sizeof(info.value), "%" PRId64, pItem->i64);
        break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE:
        tsnprintf(info.value, sizeof(info.value), "%f", pItem->fval);
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
        tsnprintf(info.value, sizeof(info.value), "%s", pItem->str);
        break;
    }

    // init info scope
    switch (pItem->scope) {
      case CFG_SCOPE_SERVER:
        tstrncpy(info.scope, "server", sizeof(info.scope));
        break;
      case CFG_SCOPE_CLIENT:
        tstrncpy(info.scope, "client", sizeof(info.scope));
        break;
      case CFG_SCOPE_BOTH:
        tstrncpy(info.scope, "both", sizeof(info.scope));
        break;
      default:
        tstrncpy(info.scope, "unknown", sizeof(info.scope));
        break;
    }
    // init info category
    switch (pItem->category) {
      case CFG_CATEGORY_GLOBAL:
        tstrncpy(info.category, "global", sizeof(info.category));
        break;
      case CFG_CATEGORY_LOCAL:
        tstrncpy(info.category, "local", sizeof(info.category));
        break;
      default:
        tstrncpy(info.category, "unknown", sizeof(info.category));
        break;
    }
    if (NULL == taosArrayPush(pInfos, &info)) {
      mError("failed to push info to array while init variables from items,since %s", tstrerror(terrno));
      taosArrayDestroy(pInfos);
      return NULL;
    }
  }

  return pInfos;
}

static int32_t mndProcessShowVariablesReq(SRpcMsg *pReq) {
  SShowVariablesRsp rsp = {0};
  int32_t           code = TSDB_CODE_SUCCESS;
  SShowVariablesReq req = {0};
  SArray           *array = NULL;

  code = tDeserializeSShowVariablesReq(pReq->pCont, pReq->contLen, &req);
  if (code != 0) {
    mError("failed to deserialize config req, since %s", terrstr());
    goto _OVER;
  }

  if ((code = mndCheckOperPrivilege(pReq->info.node, pReq->info.conn.user, MND_OPER_SHOW_VARIABLES)) != 0) {
    goto _OVER;
  }

  SVariablesInfo info = {0};
  char          *likePattern = req.opType == OP_TYPE_LIKE ? req.val : NULL;
  rsp.variables = initVariablesFromItems(taosGetGlobalCfg(tsCfg), likePattern);
  if (rsp.variables == NULL) {
    code = terrno;
    goto _OVER;
  }
  int32_t rspLen = tSerializeSShowVariablesRsp(NULL, 0, &rsp);
  void   *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    code = terrno;
    goto _OVER;
  }

  if ((rspLen = tSerializeSShowVariablesRsp(pRsp, rspLen, &rsp)) <= 0) {
    rpcFreeCont(pRsp);
    code = rspLen;
    goto _OVER;
  }

  pReq->info.rspLen = rspLen;
  pReq->info.rsp = pRsp;
  code = 0;

_OVER:

  if (code != 0) {
    mError("failed to get show variables info since %s", tstrerror(code));
  }
  tFreeSShowVariablesReq(&req);
  tFreeSShowVariablesRsp(&rsp);
  TAOS_RETURN(code);
}

int32_t compareSConfigItem(const SConfigObj *item1, SConfigItem *item2, bool *compare) {
  *compare = true;
  switch (item1->dtype) {
    case CFG_DTYPE_BOOL:
      if (item1->bval != item2->bval) {
        item2->bval = item1->bval;
        *compare = false;
      }
      break;
    case CFG_DTYPE_FLOAT:
      if (item1->fval != item2->fval) {
        item2->fval = item1->fval;
        *compare = false;
      }
      break;
    case CFG_DTYPE_INT32:
      if (item1->i32 != item2->i32) {
        item2->i32 = item1->i32;
        *compare = false;
      }
      break;
    case CFG_DTYPE_INT64:
      if (item1->i64 != item2->i64) {
        item2->i64 = item1->i64;
        *compare = false;
      }
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      if (strcmp(item1->str, item2->str) != 0) {
        item2->str = taosStrdup(item1->str);
        if (item2->str == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        *compare = false;
      }
      break;
    default:
      *compare = false;
      return TSDB_CODE_INVALID_CFG;
  }
  return TSDB_CODE_SUCCESS;
}