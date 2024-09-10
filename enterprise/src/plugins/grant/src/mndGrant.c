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
#include "machine.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndTrans.h"
#include "mndUser.h"

#define MND_GRANT_VER_NUMBER 1

void tDestroyGrantObj(SGrantLogObj *pGrant) {
  taosArrayDestroy(pGrant->pMachines);
  taosMemoryFree(pGrant->active);
}

SGrantLogObj *mndAcquireGrant(SMnode *pMnode, void **ppIter) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SGrantLogObj *pGrant = NULL;
    pIter = sdbFetch(pSdb, SDB_GRANT, pIter, (void **)&pGrant);
    if (pIter == NULL) break;

    *ppIter = pIter;
    return pGrant;
  }

  return NULL;
}

void mndReleaseGrant(SMnode *pMnode, SGrantLogObj *pGrant, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
  sdbRelease(pSdb, pGrant);
}

static void mndGrantObjAppendActive(SGrantLogObj *pObj, const char *active) {
  int8_t idx = pObj->nActives;
  if (idx >= GRANT_ACTIVE_NUM) {
    (void)memmove(&pObj->actives[0], &pObj->actives[1], sizeof(pObj->actives) - sizeof(pObj->actives[0]));
    idx = GRANT_ACTIVE_NUM - 1;
  } else {
    ++pObj->nActives;
  }
  pObj->actives[idx].ts = taosGetTimestampMs() / 1000;
  tstrncpy(pObj->actives[idx].active, active, GRANT_ACTIVE_HEAD_LEN + 1);
}

static int32_t mndGrantObjAppendMachine(SGrantLogObj *pObj, const char *active) { return 0; }

static void mndGrantObjAppendState(SGrantLogObj *pObj, SGrantState *pState) {
  int8_t idx = pObj->nStates;

  int64_t ts = taosGetTimestampMs() / 1000;
  if (idx == 0) {
    pObj->states[0].lastState = GRANT_STATE_INIT;
    pObj->states[0].state = GRANT_STATE_UNGRANTED;
    pObj->states[0].reason = GRANT_STATE_REASON_INIT;
    pObj->states[0].ts = ts;
    pObj->createTime = ts;
    pObj->updateTime = ts;
    ++pObj->nStates;
    ++idx;
  }

  if (pState->state != GRANT_STATE_UNGRANTED) {
    pState->lastState = pObj->states[idx - 1].state;
    pState->ts = ts;

    if (idx >= GRANT_STATE_NUM) {
      (void)memmove(&pObj->states[0], &pObj->states[1], sizeof(pObj->states) - sizeof(pObj->states[0]));
      idx = GRANT_STATE_NUM - 1;
      pObj->nStates = GRANT_STATE_NUM;
    } else {
      ++pObj->nStates;
    }
    pObj->states[idx] = *pState;
  }
}

int32_t mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SGrantLogObj grantObj = {0};
  bool         revoked = false;

  int32_t valLen = strlen(pCfg->value);
  if (valLen < GRANT_ACTIVE_HEAD_LEN || valLen >= TSDB_CLUSTER_VALUE_LEN) {
    if (strncasecmp(pCfg->value, "revoked", 8) == 0) {
      revoked = true;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
    }
  }

  void         *pIter = NULL;
  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (revoked) {
    // duplicated operation, return 0 directly
    if (pGrant->nStates > 0 && pGrant->states[pGrant->nStates - 1].state == GRANT_STATE_REVOKED) goto _exit;
  }

  (void)memcpy(&grantObj, pGrant, sizeof(SGrantLogObj));
  grantObj.pMachines = NULL;
  grantObj.active = NULL;
  if (pGrant->active) {
    int32_t activeLen = strlen(pGrant->active);
    if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
  }

  int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
  if (nMachines > 0) {
    if (!(grantObj.pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if(!taosArrayAddAll(grantObj.pMachines, pGrant->pMachines)){
      mndReleaseGrant(pMnode, pGrant, pIter);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  mndReleaseGrant(pMnode, pGrant, pIter);

  if (revoked) {
    mndGrantObjAppendState(&grantObj, &(SGrantState){.state = GRANT_STATE_REVOKED, .reason = GRANT_STATE_REASON_ALTER});
  } else {
    char *mergeActive = NULL;
    TAOS_CHECK_EXIT(grantAlterActiveCode(pMnode, &grantObj, grantObj.active, pCfg->value, &mergeActive));

    mndGrantObjAppendState(&grantObj, &(SGrantState){.state = GRANT_STATE_GRANTED, .reason = GRANT_STATE_REASON_ALTER});

    // merge or newActive utilized
    char   *finalActive = NULL;
    int32_t finalActiveLen = 0;
    if (mergeActive) {
      finalActive = mergeActive;
      finalActiveLen = strlen(mergeActive);
    } else {
      finalActive = pCfg->value;
      finalActiveLen = valLen;
    }

    if (finalActiveLen > 0) {
      char *tmpBuf = taosMemoryRealloc(grantObj.active, finalActiveLen + 1);
      if (!tmpBuf) {
        taosMemoryFreeClear(mergeActive);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      grantObj.active = tmpBuf;
    }

    tstrncpy(grantObj.active, finalActive, finalActiveLen + 1);
    taosMemoryFreeClear(mergeActive);

    mndGrantObjAppendActive(&grantObj, pCfg->value);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-cluster-active");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  SSdbRaw *pCommitRaw = mndGrantActionEncode(&grantObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    code = terrno;
    mError("trans:%d, failed to append commit log since %s", pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    TAOS_CHECK_EXIT(code);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    code = terrno;
    mError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    TAOS_CHECK_EXIT(code);
  }

  mndTransDrop(pTrans);
  tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
_exit:
  tDestroyGrantObj(&grantObj);
  TAOS_RETURN(code);
}

int32_t mndProcessUpdGrantLog(SMnode *pMnode, SRpcMsg *pReq, SArray *pMachines, SGrantState *pState) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SGrantLogObj grantObj = {0};
  int32_t      nMachines = taosArrayGetSize(pMachines);

  if (nMachines <= 0 && !pState) {
    goto _exit;
  }

  void         *pIter = NULL;
  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant && (!pState || pState->reason != GRANT_STATE_REASON_INIT)) {
    if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (pGrant) {
    (void)memcpy(&grantObj, pGrant, sizeof(SGrantLogObj));
    grantObj.pMachines = NULL;
    grantObj.active = NULL;
    if (pGrant->active) {
      int32_t activeLen = strlen(pGrant->active);
      if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
        mndReleaseGrant(pMnode, pGrant, pIter);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
    }
    int32_t totalMachines = taosArrayGetSize(pGrant->pMachines) + nMachines;
    if (totalMachines > 0) {
      if (!(grantObj.pMachines = taosArrayInit(totalMachines, sizeof(SGrantMachine)))) {
        mndReleaseGrant(pMnode, pGrant, pIter);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      if (taosArrayGetSize(pGrant->pMachines) > 0 && !taosArrayAddAll(grantObj.pMachines, pGrant->pMachines)) {
        mndReleaseGrant(pMnode, pGrant, pIter);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
      if (nMachines > 0 && !taosArrayAddAll(grantObj.pMachines, pMachines)) {
        mndReleaseGrant(pMnode, pGrant, pIter);
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
  }
  if (pState) {
    mndGrantObjAppendState(&grantObj, pState);
  }
  if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-grant-log");
  if (pTrans == NULL) {
    code = terrno;
    TAOS_CHECK_EXIT(code);
  }

  SSdbRaw *pCommitRaw = mndGrantActionEncode(&grantObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    code = terrno;
    mError("trans:%d, failed to append commit log since %s", pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    TAOS_CHECK_EXIT(code);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    code = terrno;
    mError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    mndTransDrop(pTrans);
    TAOS_CHECK_EXIT(code);
  }

  mndTransDrop(pTrans);
_exit:
  if (code < 0) {
    mError("grant, %s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  tDestroyGrantObj(&grantObj);
  TAOS_RETURN(code);
}

int32_t mndGrantGetLastState(SMnode *pMnode, SGrantState *pState) {
  int32_t       code = 0;
  void         *pIter = NULL;
  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
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
  TAOS_RETURN(code);
}

int32_t tSerializeSGrantObj(void *buf, int32_t bufLen, const SGrantLogObj *pObj) {
  int32_t  code = 0;
  int32_t  lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->nStates));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->nActives));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->upgradeTime));
  for (int8_t i = 0; i < pObj->nStates; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->states[i].u0));
  }
  for (int8_t i = 0; i < pObj->nActives; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->actives[i].u0));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->actives[i].active));
  }

  int32_t activeLen = 0;
  if (pObj->active) {
    activeLen = strlen(pObj->active);
  }
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, activeLen));
  if (activeLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pObj->active, activeLen + 1));
  }

  int32_t nMachines = taosArrayGetSize(pObj->pMachines);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nMachines));
  for (int32_t i = 0; i < nMachines; ++i) {
    SGrantMachine *pMachine = TARRAY_GET_ELEM(pObj->pMachines, i);
    TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pMachine->u0));
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pMachine->machine, TSDB_MACHINE_ID_LEN));
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("grant, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

int32_t tDeserializeSGrantObj(void *buf, int32_t bufLen, SGrantLogObj *pObj) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->nStates));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->nActives));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->upgradeTime));

  for (int8_t i = 0; i < pObj->nStates; ++i) {
    SGrantState *state = &pObj->states[i];
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &state->u0));
  }
  for (int8_t i = 0; i < pObj->nActives; ++i) {
    SGrantActive *active = &pObj->actives[i];
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &active->u0));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, &active->active[0]));
  }
  int32_t activeLen = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &activeLen));
  if (activeLen > 0) {
    if (!(pObj->active = taosMemoryMalloc(activeLen + 1))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->active));
  }
  int32_t nMachines = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nMachines));
  if (nMachines > 0) {
    if (!pObj->pMachines && !(pObj->pMachines = taosArrayInit_s(sizeof(SGrantMachine), nMachines))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int32_t i = 0; i < nMachines; ++i) {
      SGrantMachine *pMachine = TARRAY_GET_ELEM(pObj->pMachines, i);
      TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pMachine->u0));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, &pMachine->machine[0]));
    }
  }
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    tDestroyGrantObj(pObj);
    mError("grant, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

SSdbRaw *mndGrantActionEncode(SGrantLogObj *pGrant) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSGrantObj(NULL, 0, pGrant);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_GRANT, MND_GRANT_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  tlen = tSerializeSGrantObj(buf, tlen, pGrant);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("grant, failed to encode to raw:%p since %s", pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("grant, encode to raw:%p, row:%p", pRaw, pGrant);
  return pRaw;
}

SSdbRow *mndGrantActionDecode(SSdbRaw *pRaw) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SSdbRow      *pRow = NULL;
  SGrantLogObj *pGrant = NULL;
  void         *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_GRANT_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("grant read invalid ver, data ver: %d, curr ver: %d", sver, MND_GRANT_VER_NUMBER);
    goto _exit;
  }

  if (!(pRow = sdbAllocRow(sizeof(SGrantLogObj)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (!(pGrant = sdbGetRowObj(pRow))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSGrantObj(buf, tlen, pGrant) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosInitRWLatch(&pGrant->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("grant, failed to decode from raw:%p since %s", pRaw, tstrerror(code));
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("grant, decode from raw:%p, row:%p", pRaw, pGrant);
  return pRow;
}

int32_t mndGrantActionInsert(SSdb *pSdb, SGrantLogObj *pGrant) {
  mTrace("grant:%d, perform insert action", pGrant->id);
  return 0;
}

int32_t mndGrantActionDelete(SSdb *pSdb, SGrantLogObj *pGrant) {
  mTrace("grant:%d, perform delete action", pGrant->id);
  tDestroyGrantObj(pGrant);
  return 0;
}

int32_t mndGrantActionUpdate(SSdb *pSdb, SGrantLogObj *pOldGrant, SGrantLogObj *pNewGrant) {
  taosWLockLatch(&pOldGrant->lock);

  mTrace("grant:%d, perform update action, old row:%p new row:%p", pOldGrant->id, pOldGrant, pNewGrant);

  pOldGrant->id = pNewGrant->id;
  pOldGrant->createTime = pNewGrant->createTime;
  pOldGrant->updateTime = taosGetTimestampMs() / 1000;
  pOldGrant->upgradeTime = pNewGrant->upgradeTime;
  pOldGrant->nStates = pNewGrant->nStates;
  pOldGrant->nActives = pNewGrant->nActives;
  (void)memcpy(pOldGrant->states, pNewGrant->states, sizeof(pNewGrant->states));
  (void)memcpy(pOldGrant->actives, pNewGrant->actives, sizeof(pNewGrant->actives));
  TSWAP(pOldGrant->active, pNewGrant->active);
  TSWAP(pOldGrant->pMachines, pNewGrant->pMachines);

  taosWUnLockLatch(&pOldGrant->lock);

  return 0;
}