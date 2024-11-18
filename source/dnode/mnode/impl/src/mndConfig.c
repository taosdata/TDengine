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
#include "mndConfig.h"
#include "mndDnode.h"
#include "mndTrans.h"

#define CFG_VER_NUMBER   1
#define CFG_RESERVE_SIZE 63

static int32_t cfgUpdateItem(SConfigItem *pItem, SConfigObj *obj);
static int32_t mndProcessConfigReq(SRpcMsg *pReq);
static int32_t mndInitWriteCfg(SMnode *pMnode);
static int32_t mndInitReadCfg(SMnode *pMnode);

int32_t mndSetCreateConfigCommitLogs(STrans *pTrans, SConfigObj *obj);

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
                     .prepareFp = (SdbPrepareFp)mndCfgActionPrepare};

  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG, mndProcessConfigReq);

  return sdbSetTable(pMnode->pSdb, table);
}

SSdbRaw *mnCfgActionEncode(SConfigObj *obj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  char buf[30];

  int32_t  size = sizeof(SConfigObj);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CFG, CFG_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  char    name[CFG_NAME_MAX_LEN] = {0};
  strncpy(name, obj->name, CFG_NAME_MAX_LEN);
  SDB_SET_BINARY(pRaw, dataPos, name, CFG_NAME_MAX_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, obj->dtype, _OVER)
  switch (obj->dtype) {
    case CFG_DTYPE_NONE:
      break;
    case CFG_DTYPE_BOOL:
      SDB_SET_BOOL(pRaw, dataPos, obj->bval, _OVER)
      break;
    case CFG_DTYPE_INT32:
      SDB_SET_INT32(pRaw, dataPos, obj->i32, _OVER);
      break;
    case CFG_DTYPE_INT64:
      SDB_SET_INT64(pRaw, dataPos, obj->i64, _OVER);
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      (void)sprintf(buf, "%f", obj->fval);
      SDB_SET_INT32(pRaw, dataPos, strlen(buf), _OVER)
      SDB_SET_BINARY(pRaw, dataPos, buf, strlen(buf), _OVER)
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      SDB_SET_INT32(pRaw, dataPos, strlen(obj->str), _OVER)
      SDB_SET_BINARY(pRaw, dataPos, obj->str, strlen(obj->str), _OVER)
      break;
  }

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("cfg failed to encode to raw:%p since %s", pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }
  mTrace("cfg encode to raw:%p, row:%p", pRaw, obj);
  return pRaw;
}

SSdbRow *mndCfgActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t len = -1;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow    *pRow = NULL;
  SConfigObj *obj = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != CFG_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SConfigObj));
  if (pRow == NULL) goto _OVER;

  obj = sdbGetRowObj(pRow);
  if (obj == NULL) goto _OVER;
  int32_t dataPos = 0;
  // TODO(beryl):free it.
  SDB_GET_BINARY(pRaw, dataPos, obj->name, CFG_NAME_MAX_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, (int32_t *)&obj->dtype, _OVER)
  switch (obj->dtype) {
    case CFG_DTYPE_NONE:
      break;
    case CFG_DTYPE_BOOL:
      SDB_GET_BOOL(pRaw, dataPos, &obj->bval, _OVER)
      break;
    case CFG_DTYPE_INT32:
      SDB_GET_INT32(pRaw, dataPos, &obj->i32, _OVER);
      break;
    case CFG_DTYPE_INT64:
      SDB_GET_INT64(pRaw, dataPos, &obj->i64, _OVER);
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER)
      char *buf = taosMemoryMalloc(len + 1);
      SDB_GET_BINARY(pRaw, dataPos, buf, len, _OVER)
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER)
      SDB_GET_BINARY(pRaw, dataPos, obj->str, len, _OVER)
      break;
  }
  terrno = TSDB_CODE_SUCCESS;

_OVER:
  if (terrno != 0) {
    mError("cfg failed to decode from raw:%p since %s", pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("cfg decode from raw:%p, row:%p", pRaw, obj);
  return pRow;
}

static int32_t mndCfgActionInsert(SSdb *pSdb, SConfigObj *obj) {
  mTrace("cfg:%s, perform insert action, row:%p", obj->name, obj);
  return 0;
}

static int32_t mndCfgActionDelete(SSdb *pSdb, SConfigObj *obj) {
  mTrace("cfg:%s, perform delete action, row:%p", obj->name, obj);
  return 0;
}

static int32_t mndCfgActionUpdate(SSdb *pSdb, SConfigObj *pOld, SConfigObj *pNew) {
  mTrace("cfg:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  return 0;
}

static int32_t mndCfgActionDeploy(SMnode *pMnode) { return mndInitWriteCfg(pMnode); }

static int32_t mndCfgActionPrepare(SMnode *pMnode) { return mndInitReadCfg(pMnode); }

static int32_t mndProcessConfigReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SConfigReq configReq = {0};
  SDnodeObj *pDnode = NULL;
  int32_t    code = -1;

  tDeserializeSConfigReq(pReq->pCont, pReq->contLen, &configReq);
  SArray    *diffArray = taosArrayInit(16, sizeof(SConfigItem));
  SConfigRsp configRsp = {0};
  configRsp.forceReadConfig = configReq.forceReadConfig;
  configRsp.cver = tsmmConfigVersion;
  if (configRsp.forceReadConfig) {
    // compare config array from configReq with current config array
    if (compareSConfigItemArrays(getGlobalCfg(tsCfg), configReq.array, diffArray)) {
      configRsp.array = diffArray;
    } else {
      configRsp.isConifgVerified = 1;
      taosArrayDestroy(diffArray);
    }
  } else {
    configRsp.array = getGlobalCfg(tsCfg);
    if (configReq.cver == tsmmConfigVersion) {
      configRsp.isVersionVerified = 1;
    } else {
      configRsp.array = getGlobalCfg(tsCfg);
    }
  }

  int32_t contLen = tSerializeSConfigRsp(NULL, 0, &configRsp);
  void   *pHead = rpcMallocCont(contLen);
  contLen = tSerializeSConfigRsp(pHead, contLen, &configRsp);
  taosArrayDestroy(diffArray);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  pReq->info.rspLen = contLen;
  pReq->info.rsp = pHead;
_OVER:

  mndReleaseDnode(pMnode, pDnode);
  return TSDB_CODE_SUCCESS;
}

int32_t mndInitWriteCfg(SMnode *pMnode) {
  int    code = -1;
  size_t sz = 0;

  SConfigObj obj = {0};
  STrans    *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "init-write-config");
  if (pTrans == NULL) {
    mError("failed to init write cfg in create trans, since %s", terrstr());
    goto _OVER;
  }

  // encode mnd config version
  obj = (SConfigObj){.name = "tsmmConfigVersion", .dtype = CFG_DTYPE_INT32, .i32 = tsmmConfigVersion};
  if ((code = mndSetCreateConfigCommitLogs(pTrans, &obj)) != 0) {
    mError("failed to init mnd config version, since %s", terrstr());
  }
  sz = taosArrayGetSize(getGlobalCfg(tsCfg));

  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(getGlobalCfg(tsCfg), i);
    SConfigObj  *obj = mndInitConfigObj(item);
    if ((code = mndSetCreateConfigCommitLogs(pTrans, obj)) != 0) {
      mError("failed to init mnd config:%s, since %s", item->name, terrstr());
    }
    taosMemoryFree(obj);
  }
  if ((code = mndTransCheckConflict(pMnode, pTrans)) != 0) goto _OVER;
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

_OVER:
  mndTransDrop(pTrans);
  return TSDB_CODE_SUCCESS;
}

int32_t mndInitReadCfg(SMnode *pMnode) {
  int32_t code = 0;
  int32_t sz = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "init-read-config");
  if (pTrans == NULL) {
    mError("failed to init read cfg in create trans, since %s", terrstr());
    goto _OVER;
  }
  SConfigObj *obj = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  if (obj == NULL) {
    mInfo("failed to acquire mnd config version, since %s", terrstr());
    goto _OVER;
  } else {
    tsmmConfigVersion = obj->i32;
    sdbRelease(pMnode->pSdb, obj);
  }

  sz = taosArrayGetSize(getGlobalCfg(tsCfg));
  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(getGlobalCfg(tsCfg), i);
    SConfigObj  *newObj = sdbAcquire(pMnode->pSdb, SDB_CFG, item->name);
    if (newObj == NULL) {
      mInfo("failed to acquire mnd config:%s, since %s", item->name, terrstr());
      continue;
    }
    cfgUpdateItem(item, newObj);
    sdbRelease(pMnode->pSdb, newObj);
  }
_OVER:
  mndTransDrop(pTrans);
  return TSDB_CODE_SUCCESS;
}

int32_t mndSetCreateConfigCommitLogs(STrans *pTrans, SConfigObj *item) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mnCfgActionEncode(item);
  if (pCommitRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw) != 0)) TAOS_RETURN(code);
  if ((code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY)) != 0) TAOS_RETURN(code);
  return TSDB_CODE_SUCCESS;
}

int32_t cfgUpdateItem(SConfigItem *pItem, SConfigObj *obj) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pItem == NULL || obj == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL: {
      pItem->bval = obj->bval;
      break;
    }
    case CFG_DTYPE_INT32: {
      pItem->i32 = obj->i32;
      break;
    }
    case CFG_DTYPE_INT64: {
      pItem->i64 = obj->i64;
      break;
    }
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      pItem->fval = obj->fval;
      break;
    }
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_TIMEZONE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_NONE:
    case CFG_DTYPE_STRING: {
      pItem->str = obj->str;
      break;
    }
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }

  TAOS_RETURN(code);
}