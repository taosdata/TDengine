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

#include "mndGrant.h"
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndTrans.h"
#include "mndUser.h"

#define MND_GRANT_VER_NUMBER 1

void tFreeGrantObj(SGrantObj *pGrant) {
  taosArrayDestroy(pGrant->pMachines);
  taosMemoryFree(pGrant->active);
}

 SGrantObj *mndAcquireGrant(SMnode *pMnode, void **ppIter) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SGrantObj *pGrant = NULL;
    pIter = sdbFetch(pSdb, SDB_GRANT, pIter, (void **)&pGrant);
    if (pIter == NULL) break;

    *ppIter = pIter;
    return pGrant;
  }

  return NULL;
}

 void mndReleaseGrant(SMnode *pMnode, SGrantObj *pGrant, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
  sdbRelease(pSdb, pGrant);
}

static int32_t mndGrantObjAppendActive(SGrantObj *pObj, const char *active) {
  int8_t idx = pObj->nActives;
  if (idx >= GRANT_ACTIVE_NUM) {
    memmove(&pObj->actives[0], &pObj->actives[1], sizeof(pObj->actives) - sizeof(pObj->actives[0]));
    idx = GRANT_ACTIVE_NUM - 1;
  } else {
    ++pObj->nActives;
  }
  pObj->actives[idx].ts = taosGetTimestampMs() / 1000;
  tstrncpy(pObj->actives[idx].active, active, GRANT_ACTIVE_HEAD_LEN);
  return 0;
}

static int32_t mndGrantObjAppendMachine(SGrantObj *pObj, const char *active) { return 0; }

static int32_t mndGrantObjAppendState(SGrantObj *pObj, SGrantState *pState) {
  int8_t idx = pObj->nStates;

  if (pState->lastState == GRANT_STATE_INIT) {
    if (idx > 0) return 0;
    pObj->createTime = pState->ts;
    pObj->updateTime = pObj->createTime;
  }
  if (idx >= GRANT_STATE_NUM) {
    memmove(&pObj->states[0], &pObj->states[1], sizeof(pObj->states) - sizeof(pObj->states[0]));
    idx = GRANT_STATE_NUM - 1;
  } else {
    ++pObj->nStates;
  }
  pObj->states[idx] = *pState;
  return 0;
}

int32_t mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t   code = 0;
  SGrantObj grantObj = {0};

  if (strlen(pCfg->config) >= TSDB_DNODE_CONFIG_LEN) {
    code = TSDB_CODE_INVALID_CFG;
    goto _exit;
  }

  int32_t valLen = strlen(pCfg->value);
  if (valLen <= 0 || valLen >= TSDB_CLUSTER_VALUE_LEN) {
    code = TSDB_CODE_INVALID_CFG_VALUE;
    goto _exit;
  }

  void      *pIter = NULL;
  SGrantObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant || pGrant->id <= 0) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);
    goto _exit;
  }
  memcpy(&grantObj, pGrant, sizeof(SGrantObj));
  grantObj.pMachines = NULL;
  grantObj.active = NULL;
  if (pGrant->active) {
    int32_t activeLen = strlen(pGrant->active);
    if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
  }
  int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
  if (nMachines > 0) {
    if (!(grantObj.pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    taosArrayAddAll(grantObj.pMachines, pGrant->pMachines);
  }
  mndReleaseGrant(pMnode, pGrant, pIter);

  char *newActive = NULL;
  if ((code = grantAlterActiveCode(pCfg->value, &newActive)) != 0) {
    goto _exit;
  }
  if (newActive) {
    tstrncpy(pCfg->value, newActive, TSDB_CLUSTER_VALUE_LEN);
    taosMemoryFreeClear(newActive);
  }
  mndGrantObjAppendActive(&grantObj, pCfg->value);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-cluster-active");
  if (pTrans == NULL) {
    code = terrno;
    return -1;
  }

  SSdbRaw *pCommitRaw = mndGrantActionEncode(&grantObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }

  mndTransDrop(pTrans);
_exit:
  tFreeGrantObj(&grantObj);
  return code;
}

int32_t mndProcessUpdMachineReq(SMnode *pMnode, SRpcMsg *pReq, SArray *pMachines) {
  int32_t   code = 0;
  SGrantObj grantObj = {0};
  int32_t   nMachines = taosArrayGetSize(pMachines);

  if (nMachines <= 0) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }

  void      *pIter = NULL;
  SGrantObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant || pGrant->id <= 0) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);
    goto _exit;
  }
  memcpy(&grantObj, pGrant, sizeof(SGrantObj));
  grantObj.pMachines = NULL;
  grantObj.active = NULL;
  if (pGrant->active) {
    int32_t activeLen = strlen(pGrant->active);
    if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
  }
  int32_t totalMachines = taosArrayGetSize(pGrant->pMachines) + nMachines;
  if (totalMachines > 0) {
    if (!(grantObj.pMachines = taosArrayInit(totalMachines, sizeof(SGrantMachine)))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    taosArrayAddAll(grantObj.pMachines, pGrant->pMachines);
    taosArrayAddAll(grantObj.pMachines, pMachines);
  }
  mndReleaseGrant(pMnode, pGrant, pIter);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-grant-machine");
  if (pTrans == NULL) {
    code = terrno;
    return -1;
  }

  SSdbRaw *pCommitRaw = mndGrantActionEncode(&grantObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }

  mndTransDrop(pTrans);
_exit:
  return code;
}

int32_t mndProcessUpdStateReq(SMnode *pMnode, SRpcMsg *pReq, SGrantState *pState) {
  int32_t   code = 0;
  SGrantObj grantObj = {0};
  void     *pIter = NULL;

  SGrantObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant && pState->lastState != GRANT_STATE_INIT) {
    code = TSDB_CODE_APP_IS_STARTING;
    goto _exit;
  }

  if (pGrant) {
    memcpy(&grantObj, pGrant, sizeof(SGrantObj));
    grantObj.pMachines = NULL;
    grantObj.active = NULL;
    if (pGrant->active) {
      int32_t activeLen = strlen(pGrant->active);
      if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
    }
    int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
    if (nMachines > 0) {
      if (!(grantObj.pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      taosArrayAddAll(grantObj.pMachines, pGrant->pMachines);
    }

    mndReleaseGrant(pMnode, pGrant, pIter);
  }

  mndGrantObjAppendState(&grantObj, pState);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-grant-state");
  if (pTrans == NULL) {
    code = terrno;
    return -1;
  }

  SSdbRaw *pCommitRaw = mndGrantActionEncode(&grantObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    goto _exit;
  }

  mndTransDrop(pTrans);
_exit:
  tFreeGrantObj(&grantObj);
  return code;
}

int32_t mndGrantGetLastState(SMnode *pMnode, SGrantState *pState) {
  int32_t    code = 0;
  void      *pIter = NULL;
  SGrantObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant || pGrant->id <= 0) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);
    goto _exit;
  }

  if (pGrant->nStates > 0) {
    *pState = pGrant->states[pGrant->nStates - 1];
  } else {
    code = TSDB_CODE_GRANT_OBJ_NOT_EXIST;
  }
  mndReleaseGrant(pMnode, pGrant, pIter);
_exit:
  return code;
}

int32_t mndRetrieveGrantLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
#if 0
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pWrite = NULL;
  char    tmp[192] = {0};
  char    tmp1[192] = {0};
  char    ts[GRANT_TS_SEC_LEN] = {0};

  if (pShow->numOfRows < 1) {
    SGrantDataIns *pDataIn = NULL;
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = GRANT_VERSION;
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_EXPIRE_SHOW(gStatus.basicExpireSec);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = gStatus.basicExpired || (gStatus.multiTierExpired && tsDiskCfgNum > 1) ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_ITEM_SHOW(gStatus.curTimeSeries, gStatus.limitTimeSeries, 64);
    GRANT_ITEM_SHOW(gStatus.curDnodes, gStatus.limitDnodes, 16);
    GRANT_ITEM_SHOW(gStatus.curStreams, gStatus.limitStreams, 16);
    GRANT_ITEM_SHOW(gStatus.curSubscriptions, gStatus.limitSubscriptions, 16);
    GRANT_ITEM_SHOW(gStatus.curCpuCores, gStatus.limitCpuCores, 32);

    GRANT_EXPIRE_SHOW(gStatus.multiTierExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.streamExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.subscriptionExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.auditExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.csvExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.bakRstExpireSec);

    // connectors
    // for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    //   GRANT_DATA_IN_SHOW(i);
    // }

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
#endif
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

void mndCancelGetNextGrantLog(SMnode *pMnode, void *pIter) {
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t tSerializeSGrantObj(void *buf, int32_t bufLen, const SGrantObj *pObj) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) goto _exit;

  if (tEncodeI32v(&encoder, pObj->id) < 0) goto _exit;
  if (tEncodeI8(&encoder, pObj->nStates) < 0) goto _exit;
  if (tEncodeI8(&encoder, pObj->nActives) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pObj->createTime) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pObj->updateTime) < 0) goto _exit;
  for (int32_t i = 0; i < GRANT_STATE_NUM; ++i) {
    if (tEncodeI64v(&encoder, pObj->states[i].u0) < 0) goto _exit;
  }
  for (int32_t i = 0; i < GRANT_ACTIVE_NUM; ++i) {
    if (tEncodeI64v(&encoder, pObj->actives[i].u0) < 0) goto _exit;
    if (tEncodeCStr(&encoder, pObj->actives[i].active) < 0) goto _exit;
  }
  int32_t activeLen = 0;
  if (pObj->active) {
    activeLen = strlen(pObj->active);
  }
  if (tEncodeI32v(&encoder, activeLen) < 0) goto _exit;
  if (activeLen > 0) {
    if (tEncodeBinary(&encoder, pObj->active, activeLen) < 0) goto _exit;
  }

  int32_t nMachines = taosArrayGetSize(pObj->pMachines);
  if (tEncodeI32v(&encoder, activeLen) < 0) goto _exit;
  for (int32_t i = 0; i < nMachines; ++i) {
    SGrantMachine *pMachine = TARRAY_GET_ELEM(pObj->pMachines, i);
    if (tEncodeI64v(&encoder, pMachine->u0) < 0) goto _exit;
    if (tEncodeBinary(&encoder, pMachine->machine, TSDB_MACHINE_ID_LEN) < 0) goto _exit;
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
  code = 0;
_exit:
  tEncoderClear(&encoder);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return code == 0 ? tlen : code;
}

int32_t tDeserializeSGrantObj(void *buf, int32_t bufLen, SGrantObj *pObj) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) goto _exit;

  if (tDecodeI32v(&decoder, &pObj->id) < 0) goto _exit;
  if (tDecodeI8(&decoder, &pObj->nStates) < 0) goto _exit;
  if (tDecodeI8(&decoder, &pObj->nActives) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pObj->createTime) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pObj->updateTime) < 0) goto _exit;
  for (int32_t i = 0; i < GRANT_STATE_NUM; ++i) {
    SGrantState *state = &pObj->states[i];
    if (tDecodeI64v(&decoder, &state->u0) < 0) goto _exit;
  }
  for (int32_t i = 0; i < GRANT_ACTIVE_NUM; ++i) {
    SGrantActive *active = &pObj->actives[i];
    if (tDecodeI64v(&decoder, &active->u0) < 0) goto _exit;
    char *pGrantActive = &active->active[0];
    if (tDecodeBinary(&decoder, (uint8_t **)&pGrantActive, NULL) < 0) return -1;
  }
  int32_t activeLen = 0;
  if (tDecodeI32v(&decoder, &activeLen) < 0) goto _exit;
  if (activeLen > 0) {
    if (!(pObj->active = taosMemoryMalloc(activeLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    if (tDecodeCStrTo(&decoder, pObj->active) < 0) return -1;
  }
  int32_t nMachines = 0;
  if (tDecodeI32v(&decoder, &nMachines) < 0) goto _exit;
  if (nMachines > 0) {
    if (!(pObj->pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    if (!taosArrayPush(pObj->pMachines, &(SGrantMachine){0})) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    SGrantMachine *pLast = taosArrayGetLast(pObj->pMachines);
    if (tDecodeI64v(&decoder, &pLast->u0) < 0) goto _exit;
    char *pGrantMachine = &pLast->machine[0];
    if (tDecodeBinary(&decoder, (uint8_t **)&pGrantMachine, NULL) < 0) goto _exit;
  }

  code = 0;
_exit:
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code != 0) {
    tFreeGrantObj(pObj);
    mError("grant, %s failed since %s, row:%p", __func__, tstrerror(code), pObj);
  }
  return code;
}

SSdbRaw *mndGrantActionEncode(SGrantObj *pGrant) {
  terrno = TSDB_CODE_SUCCESS;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSGrantObj(NULL, 0, pGrant);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_GRANT, MND_GRANT_VER_NUMBER, size);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  tlen = tSerializeSGrantObj(buf, tlen, pGrant);
  if (tlen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);

_exit:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("grant, failed to encode to raw:%p since %s", pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("grant, encode to raw:%p, row:%p", pRaw, pGrant);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return pRaw;
}

SSdbRow *mndGrantActionDecode(SSdbRaw *pRaw) {
  SSdbRow   *pRow = NULL;
  SGrantObj *pGrant = NULL;
  void      *buf = NULL;
  terrno = TSDB_CODE_SUCCESS;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_GRANT_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("grant read invalid ver, data ver: %d, curr ver: %d", sver, MND_GRANT_VER_NUMBER);
    goto _exit;
  }

  if (!(pRow = sdbAllocRow(sizeof(SGrantObj)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (!(pGrant = sdbGetRowObj(pRow))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSGrantObj(buf, tlen, pGrant) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosInitRWLatch(&pGrant->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("view, failed to decode from raw:%p since %s", pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  mTrace("view, decode from raw:%p, row:%p", pRaw, pGrant);
  return pRow;
}

int32_t mndGrantActionInsert(SSdb *pSdb, SGrantObj *pGrant) {
  mTrace("grant:%d, perform insert action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t mndGrantActionDelete(SSdb *pSdb, SGrantObj *pGrant) {
  mTrace("grant:%d, perform delete action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  tFreeGrantObj(pGrant);
  return 0;
}

int32_t mndGrantActionUpdate(SSdb *pSdb, SGrantObj *pOldGrant, SGrantObj *pNewGrant) {
  taosWLockLatch(&pOldGrant->lock);

  mTrace("grant:%d, perform update action, old row:%p new row:%p", pOldGrant->id, pOldGrant, pNewGrant);

  pOldGrant->id = pNewGrant->id;
  pOldGrant->createTime = pNewGrant->createTime;
  pOldGrant->updateTime = pNewGrant->updateTime;
  TSWAP(pOldGrant->states, pNewGrant->states);
  TSWAP(pOldGrant->actives, pNewGrant->actives);
  TSWAP(pOldGrant->active, pNewGrant->active);
  taosArrayClear(pOldGrant->pMachines);
  taosArrayAddAll(pOldGrant->pMachines, pNewGrant->pMachines);

  taosWUnLockLatch(&pOldGrant->lock);

  return 0;
}

// SGrantObj *mndAcquireGrant(SMnode *pMnode, int32_t id) {
//   SSdb       *pSdb = pMnode->pSdb;
//   SGrantObj   *pObj = sdbAcquire(pSdb, SDB_GRANT, &id);
//   if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
//     terrno = TSDB_CODE_SUCCESS;
//   }
//   return pObj;
// }

// void mndReleaseGrant(SMnode *pMnode, SGrantObj *pGrant) {
//   SSdb *pSdb = pMnode->pSdb;
//   sdbRelease(pSdb, pGrant);
// }