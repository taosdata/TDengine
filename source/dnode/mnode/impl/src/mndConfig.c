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
#include "mndPrivilege.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "tutil.h"

#define CFG_VER_NUMBER   1
#define CFG_RESERVE_SIZE 63

enum CfgAlterType {
  CFG_ALTER_DNODE,
  CFG_ALTER_ALL_DNODES,
};

static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pInMCfgReq, int32_t optLen, int32_t *pOutValue);
static int32_t cfgUpdateItem(SConfigItem *pItem, SConfigObj *obj);
static int32_t mndConfigUpdateTrans(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype);
static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp);
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
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_DNODE, mndProcessConfigDnodeReq);
  mndSetMsgHandle(pMnode, TDMT_DND_CONFIG_DNODE_RSP, mndProcessConfigDnodeRsp);

  return sdbSetTable(pMnode->pSdb, table);
}

SSdbRaw *mnCfgActionEncode(SConfigObj *obj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  char buf[30];

  int32_t  sz = sizeof(SConfigObj) + CFG_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CFG, CFG_VER_NUMBER, sz);
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
      SDB_SET_FLOAT(pRaw, dataPos, obj->fval, _OVER)
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      SDB_SET_BINARY(pRaw, dataPos, obj->str, TSDB_CONFIG_VALUE_LEN, _OVER)
      break;
  }
  SDB_SET_RESERVE(pRaw, dataPos, CFG_RESERVE_SIZE, _OVER)

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
      SDB_GET_FLOAT(pRaw, dataPos, &obj->fval, _OVER)
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
      SDB_GET_BINARY(pRaw, dataPos, obj->str, TSDB_CONFIG_VALUE_LEN, _OVER)
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
      tstrncpy(pOld->str, pNew->str, TSDB_CONFIG_VALUE_LEN);
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndCfgActionDeploy(SMnode *pMnode) { return mndInitWriteCfg(pMnode); }

static int32_t mndCfgActionPrepare(SMnode *pMnode) { return mndInitReadCfg(pMnode); }

static int32_t mndProcessConfigReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SConfigReq configReq = {0};
  SDnodeObj *pDnode = NULL;
  int32_t    code = -1;

  code = tDeserializeSConfigReq(pReq->pCont, pReq->contLen, &configReq);
  if (code != 0) {
    mError("failed to deserialize config req, since %s", terrstr());
    goto _OVER;
  }
  SArray    *diffArray = taosArrayInit(16, sizeof(SConfigItem));
  SConfigRsp configRsp = {0};
  configRsp.forceReadConfig = configReq.forceReadConfig;
  configRsp.cver = tsmmConfigVersion;
  if (configRsp.forceReadConfig) {
    // compare config array from configReq with current config array
    if (compareSConfigItemArrays(taosGetGlobalCfg(tsCfg), configReq.array, diffArray)) {
      configRsp.array = diffArray;
    } else {
      configRsp.isConifgVerified = 1;
    }
  } else {
    configRsp.array = taosGetGlobalCfg(tsCfg);
    if (configReq.cver == tsmmConfigVersion) {
      configRsp.isVersionVerified = 1;
    } else {
      configRsp.array = taosGetGlobalCfg(tsCfg);
    }
  }

  int32_t contLen = tSerializeSConfigRsp(NULL, 0, &configRsp);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  void *pHead = rpcMallocCont(contLen);
  contLen = tSerializeSConfigRsp(pHead, contLen, &configRsp);
  if (contLen < 0) {
    code = contLen;
    goto _OVER;
  }
  pReq->info.rspLen = contLen;
  pReq->info.rsp = pHead;

_OVER:
  taosArrayDestroy(diffArray);
  mndReleaseDnode(pMnode, pDnode);
  return TSDB_CODE_SUCCESS;
}

int32_t mndInitWriteCfg(SMnode *pMnode) {
  int    code = -1;
  size_t sz = 0;

  mInfo("init write cfg to sdb");
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "init-write-config");
  if (pTrans == NULL) {
    mError("failed to init write cfg in create trans, since %s", terrstr());
    goto _OVER;
  }

  // encode mnd config version
  SConfigObj *versionObj = mndInitConfigVersion();
  if ((code = mndSetCreateConfigCommitLogs(pTrans, versionObj)) != 0) {
    mError("failed to init mnd config version, since %s", terrstr());
    taosMemoryFree(versionObj->str);
    taosMemoryFree(versionObj);
    goto _OVER;
  }
  taosMemoryFree(versionObj);
  sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));

  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
    SConfigObj  *obj = mndInitConfigObj(item);
    if ((code = mndSetCreateConfigCommitLogs(pTrans, obj)) != 0) {
      mError("failed to init mnd config:%s, since %s", item->name, terrstr());
    }
    taosMemoryFree(obj);
  }
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

_OVER:
  mndTransDrop(pTrans);
  return TSDB_CODE_SUCCESS;
}

int32_t mndInitReadCfg(SMnode *pMnode) {
  int32_t code = 0;
  int32_t sz = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "init-read-config");
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

  sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));
  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
    SConfigObj  *newObj = sdbAcquire(pMnode->pSdb, SDB_CFG, item->name);
    if (newObj == NULL) {
      mInfo("failed to acquire mnd config:%s, since %s", item->name, terrstr());
      continue;
    }
    code = cfgUpdateItem(item, newObj);
    if (code != 0) {
      mError("failed to update mnd config:%s, since %s", item->name, terrstr());
    }
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
      if (obj->str != NULL) {
        taosMemoryFree(pItem->str);
        pItem->str = taosMemoryMalloc(strlen(obj->str) + 1);
        tstrncpy(pItem->str, obj->str, strlen(obj->str) + 1);
      }
      break;
    }
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }

  TAOS_RETURN(code);
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
  (void)strncpy(pDCfgReq->config, pMCfgReq->config, optLen);
  pDCfgReq->config[optLen] = 0;

  if (' ' == pMCfgReq->config[optLen]) {
    // 'key value'
    if (strlen(pMCfgReq->value) != 0) goto _err;
    (void)strcpy(pDCfgReq->value, p + 1);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    (void)strcpy(pDCfgReq->value, pMCfgReq->value);
  }

  TAOS_RETURN(code);

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  code = TSDB_CODE_INVALID_CFG;
  TAOS_RETURN(code);
}

static int32_t mndSendCfgDnodeReq(SMnode *pMnode, int32_t dnodeId, SDCfgDnodeReq *pDcfgReq) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    if (pDnode->id == dnodeId || dnodeId == -1 || dnodeId == 0) {
      SEpSet  epSet = mndGetDnodeEpset(pDnode);
      int32_t bufLen = tSerializeSDCfgDnodeReq(NULL, 0, pDcfgReq);
      void   *pBuf = rpcMallocCont(bufLen);

      if (pBuf != NULL) {
        if ((bufLen = tSerializeSDCfgDnodeReq(pBuf, bufLen, pDcfgReq)) <= 0) {
          code = bufLen;
          return code;
        }
        mInfo("dnode:%d, send config req to dnode, config:%s value:%s", dnodeId, pDcfgReq->config, pDcfgReq->value);
        SRpcMsg rpcMsg = {.msgType = TDMT_DND_CONFIG_DNODE, .pCont = pBuf, .contLen = bufLen};
        code = tmsgSendReq(&epSet, &rpcMsg);
      }
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
  TAOS_CHECK_RETURN(tDeserializeSMCfgDnodeReq(pReq->pCont, pReq->contLen, &cfgReq));
  int8_t updateIpWhiteList = 0;
  mInfo("dnode:%d, start to config, option:%s, value:%s", cfgReq.dnodeId, cfgReq.config, cfgReq.value);
  if ((code = mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CONFIG_DNODE)) != 0) {
    tFreeSMCfgDnodeReq(&cfgReq);
    TAOS_RETURN(code);
  }

  SDCfgDnodeReq dcfgReq = {0};
  if (strcasecmp(cfgReq.config, "resetlog") == 0) {
    (void)strcpy(dcfgReq.config, "resetlog");
    goto _send_req;
#ifdef TD_ENTERPRISE
  } else if (strncasecmp(cfgReq.config, "s3blocksize", 11) == 0) {
    int32_t optLen = strlen("s3blocksize");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) return code;

    if (flag > 1024 * 1024 || (flag > -1 && flag < 1024) || flag < -1) {
      mError("dnode:%d, failed to config s3blocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]",
             cfgReq.dnodeId, flag);
      code = TSDB_CODE_INVALID_CFG;
      tFreeSMCfgDnodeReq(&cfgReq);
      TAOS_RETURN(code);
    }

    strcpy(dcfgReq.config, "s3blocksize");
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

    bool isUpdateAll = (cfgReq.dnodeId == 0 || cfgReq.dnodeId == -1) ? true : false;
    TAOS_CHECK_GOTO(cfgCheckRangeForDynUpdate(taosGetCfg(), dcfgReq.config, dcfgReq.value, true, isUpdateAll), &lino,
                    _err_out);
  }
  SConfigItem *pItem = cfgGetItem(taosGetCfg(), dcfgReq.config);
  // Update config in sdb.
  if (pItem == NULL) {
    mError("failed to find config:%s while process config dnode req", cfgReq.config);
    code = TSDB_CODE_CFG_NOT_FOUND;
    goto _err_out;
  }
  if (pItem->category == CFG_CATEGORY_GLOBAL) {
    TAOS_CHECK_GOTO(mndConfigUpdateTrans(pMnode, dcfgReq.config, dcfgReq.value, pItem->dtype), &lino, _err_out);
  }
_send_req :

{  // audit
  char obj[50] = {0};
  (void)sprintf(obj, "%d", cfgReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "alterDnode", obj, "", cfgReq.sql, cfgReq.sqlLen);
}

  tFreeSMCfgDnodeReq(&cfgReq);

  dcfgReq.version = tsmmConfigVersion;
  code = mndSendCfgDnodeReq(pMnode, cfgReq.dnodeId, &dcfgReq);

  // dont care suss or succ;
  if (updateIpWhiteList) mndRefreshUserIpWhiteList(pMnode);
  TAOS_RETURN(code);

_err_out:
  tFreeSMCfgDnodeReq(&cfgReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from dnode");
  return 0;
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
    *pOutValue = atoi(pMCfgReq->config + optLen + 1);
  } else {
    // 'key' 'value'
    if (strlen(pMCfgReq->value) == 0) goto _err;
    *pOutValue = atoi(pMCfgReq->value);
  }

  TAOS_RETURN(code);

_err:
  mError("dnode:%d, failed to config since invalid conf:%s", pMCfgReq->dnodeId, pMCfgReq->config);
  code = TSDB_CODE_INVALID_CFG;
  TAOS_RETURN(code);
}

static int32_t mndConfigUpdateTrans(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype) {
  int32_t code = -1;
  int32_t lino = -1;
  // SConfigObj *pVersion = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  // if (pVersion == NULL) {
  //   mError("failed to acquire tsmmConfigVersion while update config, since %s", terrstr());
  //   code = terrno;
  //   goto _OVER;
  // }
  // pVersion->i32 = ++tsmmConfigVersion;
  // SConfigObj *pObj = sdbAcquire(pMnode->pSdb, SDB_CFG, name);
  // if (pObj == NULL) {
  //   mError("failed to acquire mnd config:%s while update config, since %s", name, terrstr());
  //   code = terrno;
  //   goto _OVER;
  // }
  SConfigObj pVersion = {0}, pObj = {0};
  pVersion.i32 = ++tsmmConfigVersion;
  strncpy(pVersion.name, "tsmmConfigVersion", CFG_NAME_MAX_LEN);

  pObj.dtype = dtype;
  strncpy(pObj.name, name, CFG_NAME_MAX_LEN);

  TAOS_CHECK_GOTO(mndUpdateObj(&pObj, name, pValue), &lino, _OVER);
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ARBGROUP, NULL, "update-config");
  if (pTrans == NULL) {
    if (terrno != 0) code = terrno;
    goto _OVER;
  }
  mInfo("trans:%d, used to update config:%s to value:%s", pTrans->id, name, pValue);
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, &pVersion), &lino, _OVER);
  TAOS_CHECK_GOTO(mndSetCreateConfigCommitLogs(pTrans, &pObj), &lino, _OVER);
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;
  code = 0;
_OVER:
  if (code != 0) {
    --tsmmConfigVersion;
    mError("failed to update config:%s to value:%s, since %s", name, pValue, tstrerror(code));
  }
  mndTransDrop(pTrans);
  return code;
}