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

#define CFG_VER_NUMBER    1
#define CFG_RESERVE_SIZE  63
#define CFG_ALTER_TIMEOUT 3 * 1000

static int32_t mndMCfgGetValInt32(SMCfgDnodeReq *pInMCfgReq, int32_t optLen, int32_t *pOutValue);
static int32_t mndProcessShowVariablesReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeReq(SRpcMsg *pReq);
static int32_t mndProcessConfigDnodeRsp(SRpcMsg *pRsp);
static int32_t mndProcessConfigReq(SRpcMsg *pReq);
static int32_t mndInitWriteCfg(SMnode *pMnode);
static int32_t mndTryRebuildCfg(SMnode *pMnode);
static int32_t initConfigArrayFromSdb(SMnode *pMnode, SArray *array);
static void    cfgArrayCleanUp(SArray *array);
static void    cfgObjArrayCleanUp(SArray *array);

static int32_t mndConfigUpdateTrans(SMnode *pMnode, const char *name, char *pValue, ECfgDataType dtype,
                                    int32_t tsmmConfigVersion);

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
  mndSetMsgHandle(pMnode, TDMT_MND_SHOW_VARIABLES, mndProcessShowVariablesReq);

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

static int32_t mndCfgActionPrepare(SMnode *pMnode) { return mndTryRebuildCfg(pMnode); }

static int32_t mndProcessConfigReq(SRpcMsg *pReq) {
  SMnode    *pMnode = pReq->info.node;
  SConfigReq configReq = {0};
  int32_t    code = TSDB_CODE_SUCCESS;
  SArray    *array = NULL;

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
  configRsp.forceReadConfig = configReq.forceReadConfig;

  configRsp.cver = vObj->i32;
  if (configRsp.forceReadConfig) {
    // compare config array from configReq with current config array
    if (compareSConfigItemArrays(taosGetGlobalCfg(tsCfg), configReq.array, array)) {
      configRsp.array = array;
    } else {
      configRsp.isConifgVerified = 1;
    }
  } else {
    if (configReq.cver == vObj->i32) {
      configRsp.isVersionVerified = 1;
    } else {
      code = initConfigArrayFromSdb(pMnode, array);
      if (code != 0) {
        mError("failed to init config array from sdb, since %s", terrstr());
        goto _OVER;
      }
      configRsp.array = array;
    }
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
  return TSDB_CODE_SUCCESS;
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
  SConfigObj *versionObj = mndInitConfigVersion();
  if ((code = mndSetCreateConfigCommitLogs(pTrans, versionObj)) != 0) {
    mError("failed to init mnd config version, since %s", tstrerror(code));
    tFreeSConfigObj(versionObj);
    taosMemoryFree(versionObj);
    goto _OVER;
  }
  tFreeSConfigObj(versionObj);
  taosMemoryFree(versionObj);
  sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));

  for (int i = 0; i < sz; ++i) {
    SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
    SConfigObj  *obj = mndInitConfigObj(item);
    if (obj == NULL) {
      code = terrno;
      goto _OVER;
    }
    if ((code = mndSetCreateConfigCommitLogs(pTrans, obj)) != 0) {
      mError("failed to init mnd config:%s, since %s", item->name, tstrerror(code));
      tFreeSConfigObj(obj);
      taosMemoryFree(obj);
      goto _OVER;
    }
    tFreeSConfigObj(obj);
    taosMemoryFree(obj);
  }
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _OVER;

_OVER:
  if (code != 0) {
    mError("failed to init write cfg, since %s", tstrerror(code));
  }
  mndTransDrop(pTrans);
  return code;
}

int32_t mndTryRebuildCfg(SMnode *pMnode) {
  int32_t   code = 0;
  int32_t   sz = -1;
  STrans   *pTrans = NULL;
  SAcctObj *vObj = NULL, *obj = NULL;
  SArray   *addArray = NULL;
  vObj = sdbAcquire(pMnode->pSdb, SDB_CFG, "tsmmConfigVersion");
  if (vObj == NULL) {
    if ((code = mndInitWriteCfg(pMnode)) < 0) goto _exit;
    mInfo("failed to acquire mnd config version, try to rebuild config in sdb.");
  } else {
    sz = taosArrayGetSize(taosGetGlobalCfg(tsCfg));
    addArray = taosArrayInit(4, sizeof(SConfigObj));
    for (int i = 0; i < sz; ++i) {
      SConfigItem *item = taosArrayGet(taosGetGlobalCfg(tsCfg), i);
      obj = sdbAcquire(pMnode->pSdb, SDB_CFG, item->name);
      if (obj == NULL) {
        SConfigObj *newObj = mndInitConfigObj(item);
        if (newObj == NULL) {
          code = terrno;
          goto _exit;
        }
        if (NULL == taosArrayPush(addArray, newObj)) {
          code = terrno;
          goto _exit;
        }
      } else {
        sdbRelease(pMnode->pSdb, obj);
      }
    }
    int32_t addSize = taosArrayGetSize(addArray);
    if (addSize > 0) {
      pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "add-config");
      if (pTrans == NULL) {
        code = terrno;
        goto _exit;
      }
      for (int i = 0; i < addSize; ++i) {
        SConfigObj *AddObj = taosArrayGet(addArray, i);
        if ((code = mndSetCreateConfigCommitLogs(pTrans, AddObj)) != 0) goto _exit;
      }
      if ((code = mndTransPrepare(pMnode, pTrans)) != 0) goto _exit;
      mInfo("add new config to sdb, nums:%d", addSize);
    }
  }
_exit:
  if (code != 0) {
    mError("failed to try rebuild config in sdb, since %s", tstrerror(code));
  }
  sdbRelease(pMnode->pSdb, vObj);
  sdbRelease(pMnode->pSdb, obj);
  cfgObjArrayCleanUp(addArray);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
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
  strncpy(pDCfgReq->config, pMCfgReq->config, optLen);
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
  } else if (strncasecmp(cfgReq.config, "s3blocksize", 12) == 0) {
    int32_t optLen = strlen("s3blocksize");
    int32_t flag = -1;
    int32_t code = mndMCfgGetValInt32(&cfgReq, optLen, &flag);
    if (code < 0) {
      goto _err_out;
    }

    if (flag > 1024 * 1024 || (flag > -1 && flag < 1024) || flag < -1) {
      mError("dnode:%d, failed to config s3blocksize since value:%d. Valid range: -1 or [1024, 1024 * 1024]",
             cfgReq.dnodeId, flag);
      code = TSDB_CODE_INVALID_CFG;
      goto _err_out;
    }

    tstrncpy(dcfgReq.config, "s3blocksize", 12);
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
  if (pItem->category == CFG_CATEGORY_GLOBAL) {
    TAOS_CHECK_GOTO(mndConfigUpdateTrans(pMnode, dcfgReq.config, dcfgReq.value, pItem->dtype, ++vObj->i32), &lino,
                    _err_out);
  }
_send_req :

{  // audit
  char obj[50] = {0};
  (void)sprintf(obj, "%d", cfgReq.dnodeId);

  auditRecord(pReq, pMnode->clusterId, "alterDnode", obj, "", cfgReq.sql, cfgReq.sqlLen);
}
  dcfgReq.version = vObj->i32;
  code = mndSendCfgDnodeReq(pMnode, cfgReq.dnodeId, &dcfgReq);
  if (code != 0) {
    mError("failed to send config req to dnode:%d, since %s", cfgReq.dnodeId, tstrerror(code));
    goto _err_out;
  }
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
    taosMemoryFree(obj);
  }
  taosArrayDestroy(array);
}

SArray *initVariablesFromItems(SArray *pItems) {
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
    strcpy(info.name, pItem->name);

    // init info value
    switch (pItem->dtype) {
      case CFG_DTYPE_NONE:
        break;
      case CFG_DTYPE_BOOL:
        sprintf(info.value, "%d", pItem->bval);
        break;
      case CFG_DTYPE_INT32:
        sprintf(info.value, "%d", pItem->i32);
        break;
      case CFG_DTYPE_INT64:
        sprintf(info.value, "%" PRId64, pItem->i64);
        break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE:
        sprintf(info.value, "%f", pItem->fval);
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
        sprintf(info.value, "%s", pItem->str);
        break;
    }

    // init info scope
    switch (pItem->scope) {
      case CFG_SCOPE_SERVER:
        strcpy(info.scope, "server");
        break;
      case CFG_SCOPE_CLIENT:
        strcpy(info.scope, "client");
        break;
      case CFG_SCOPE_BOTH:
        strcpy(info.scope, "both");
        break;
      default:
        strcpy(info.scope, "unknown");
        break;
    }
    // init info category
    switch (pItem->category) {
      case CFG_CATEGORY_GLOBAL:
        strcpy(info.category, "global");
        break;
      case CFG_CATEGORY_LOCAL:
        strcpy(info.category, "local");
        break;
      default:
        strcpy(info.category, "unknown");
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
  int32_t           code = -1;

  if ((code = mndCheckOperPrivilege(pReq->info.node, pReq->info.conn.user, MND_OPER_SHOW_VARIABLES)) != 0) {
    goto _OVER;
  }

  SVariablesInfo info = {0};

  rsp.variables = initVariablesFromItems(taosGetGlobalCfg(tsCfg));
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

  tFreeSShowVariablesRsp(&rsp);
  TAOS_RETURN(code);
}