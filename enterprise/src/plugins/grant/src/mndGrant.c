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

extern int8_t       grantHbLock;
extern SGrantStatus gStatus;

#define MND_GRANT_VER_NUMBER 1

#define RETURN_WITH_CODE(cond, v) \
  do {                            \
    if (cond) {                   \
      code = (v);                 \
      lino = __LINE__;            \
      goto _return;               \
    }                             \
  } while (0)

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

static int32_t mndGrantObjAppendActive(SGrantLogObj *pObj, const char *active) {
  int8_t idx = pObj->nActives;
  if (idx >= GRANT_ACTIVE_NUM) {
    memmove(&pObj->actives[0], &pObj->actives[1], sizeof(pObj->actives) - sizeof(pObj->actives[0]));
    idx = GRANT_ACTIVE_NUM - 1;
  } else {
    ++pObj->nActives;
  }
  pObj->actives[idx].ts = taosGetTimestampMs() / 1000;
  tstrncpy(pObj->actives[idx].active, active, GRANT_ACTIVE_HEAD_LEN + 1);
  return 0;
}

static int32_t mndGrantObjAppendMachine(SGrantLogObj *pObj, const char *active) { return 0; }

static int32_t mndGrantObjAppendState(SGrantLogObj *pObj, SGrantState *pState) {
  int8_t idx = pObj->nStates;

  int64_t ts = taosGetTimestampMs() / 1000;
  if (idx == 0) {
    pObj->states[0].lastState = GRANT_STATE_INIT;
    pObj->states[0].state = GRANT_STATE_UNGRANTED;
    pObj->states[0].reason = GRANT_STATE_REASON_INIT;
    pObj->states[0].ts = ts;
    pObj->createTime = pState->ts;
    pObj->updateTime = pObj->createTime;
    ++pObj->nStates;
    ++idx;
  }

  if (pState->state != GRANT_STATE_UNGRANTED) {
    pState->lastState = pObj->states[idx - 1].state;
    pState->ts = ts;

    if (idx >= GRANT_STATE_NUM) {
      memmove(&pObj->states[0], &pObj->states[1], sizeof(pObj->states) - sizeof(pObj->states[0]));
      idx = GRANT_STATE_NUM - 1;
      pObj->nStates = GRANT_STATE_NUM;
    } else {
      ++pObj->nStates;
    }
    pObj->states[idx] = *pState;
  }

  return 0;
}

int32_t mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t      code = 0;
  SGrantLogObj grantObj = {0};
  bool         revoked = false;

  int32_t valLen = strlen(pCfg->value);
  if (valLen < GRANT_ACTIVE_HEAD_LEN || valLen >= TSDB_CLUSTER_VALUE_LEN) {
    if (strncasecmp(pCfg->value, "revoked", 8) == 0) {
      revoked = true;
    } else {
      code = TSDB_CODE_INVALID_CFG_VALUE;
      goto _exit;
    }
  }

  void         *pIter = NULL;
  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    code = TSDB_CODE_APP_IS_STARTING;
    goto _exit;
  }

  if (revoked) {
    // duplicated operation, return 0 directly
    if (pGrant->nStates > 0 && pGrant->states[pGrant->nStates - 1].state == GRANT_STATE_REVOKED) goto _exit;
  }

  memcpy(&grantObj, pGrant, sizeof(SGrantLogObj));
  grantObj.pMachines = NULL;
  grantObj.active = NULL;
  if (pGrant->active) {
    int32_t activeLen = strlen(pGrant->active);
    if (!(grantObj.active = taosMemoryMalloc(activeLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      mndReleaseGrant(pMnode, pGrant, pIter);
      goto _exit;
    }
    tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
  }

  int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
  if (nMachines > 0) {
    if (!(grantObj.pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      mndReleaseGrant(pMnode, pGrant, pIter);
      goto _exit;
    }
    taosArrayAddAll(grantObj.pMachines, pGrant->pMachines);
  }
  mndReleaseGrant(pMnode, pGrant, pIter);

  if (revoked) {
    mndGrantObjAppendState(&grantObj, &(SGrantState){.state = GRANT_STATE_REVOKED, .reason = GRANT_STATE_REASON_ALTER});
    gStatus.grantState = GRANT_STATE_REVOKED;
  } else {
    char *mergeActive = NULL;
    if ((code = grantAlterActiveCode(pMnode, &grantObj, grantObj.active, pCfg->value, &mergeActive)) != 0) {
      goto _exit;
    }

    SGrantState state = {0};
    if (gStatus.expired == 0) {
      state.state = GRANT_STATE_GRANTED;
      state.reason = GRANT_STATE_REASON_ALTER;
    } else {
      state.state = GRANT_STATE_EXPIRED;
      state.reason = GRANT_STATE_REASON_EXPIRE;
    }

    if (pGrant->nStates == 0 || (pGrant->nStates > 0 && pGrant->states[pGrant->nStates - 1].state != state.state)) {
      mndGrantObjAppendState(&grantObj, &state);
      gStatus.grantState = state.state;
    }

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
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
      grantObj.active = tmpBuf;
    }

    tstrncpy(grantObj.active, finalActive, finalActiveLen + 1);
    taosMemoryFreeClear(mergeActive);

    mndGrantObjAppendActive(&grantObj, pCfg->value);
  }

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
  tDestroyGrantObj(&grantObj);
  return code;
}

int32_t mndProcessUpdGrantLog(SMnode *pMnode, SRpcMsg *pReq, SArray *pMachines, SGrantState *pState) {
  int32_t      code = 0;
  SGrantLogObj grantObj = {0};
  int32_t      nMachines = taosArrayGetSize(pMachines);

  if (nMachines <= 0 && !pState) {
    goto _exit;
  }

  void         *pIter = NULL;
  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant && (!pState || pState->reason != GRANT_STATE_REASON_INIT)) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);
    goto _exit;
  }

  if (pGrant) {
    memcpy(&grantObj, pGrant, sizeof(SGrantLogObj));
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
  }
  if (pState) {
    mndGrantObjAppendState(&grantObj, pState);
    gStatus.grantState = pState->state;
    if (pState->state = GRANT_STATE_REVOKED) {
      gStatus.revokedExpireSec = pState->ts + GRANT_CHK_TOLERENCE;
    }
  }
  if (pGrant) mndReleaseGrant(pMnode, pGrant, pIter);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-grant-log");
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
  return code;
}

int32_t tSerializeSGrantObj(void *buf, int32_t bufLen, const SGrantLogObj *pObj) {
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
  for (int8_t i = 0; i < pObj->nStates; ++i) {
    if (tEncodeI64v(&encoder, pObj->states[i].u0) < 0) goto _exit;
  }
  for (int8_t i = 0; i < pObj->nActives; ++i) {
    if (tEncodeI64v(&encoder, pObj->actives[i].u0) < 0) goto _exit;
    if (tEncodeCStr(&encoder, pObj->actives[i].active) < 0) goto _exit;
  }
  if (pObj->nActives > 0) {
    assert(strlen(pObj->actives[0].active) == 30);
  }
  int32_t activeLen = 0;
  if (pObj->active) {
    activeLen = strlen(pObj->active);
  }
  if (tEncodeI32v(&encoder, activeLen) < 0) goto _exit;
  if (activeLen > 0) {
    if (tEncodeBinary(&encoder, pObj->active, activeLen + 1) < 0) goto _exit;
  }

  int32_t nMachines = taosArrayGetSize(pObj->pMachines);
  if (tEncodeI32v(&encoder, nMachines) < 0) goto _exit;
  uInfo("%s:%d nMachines = %d\n\n", __func__, __LINE__, nMachines);
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

int32_t tDeserializeSGrantObj(void *buf, int32_t bufLen, SGrantLogObj *pObj) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  RETURN_WITH_CODE(tStartDecode(&decoder) < 0, code);

  RETURN_WITH_CODE(tDecodeI32v(&decoder, &pObj->id) < 0, code);
  RETURN_WITH_CODE(tDecodeI8(&decoder, &pObj->nStates) < 0, code);
  RETURN_WITH_CODE(tDecodeI8(&decoder, &pObj->nActives), code);
  RETURN_WITH_CODE(tDecodeI64v(&decoder, &pObj->createTime), code);
  RETURN_WITH_CODE(tDecodeI64v(&decoder, &pObj->updateTime), code);

  for (int8_t i = 0; i < pObj->nStates; ++i) {
    SGrantState *state = &pObj->states[i];
    RETURN_WITH_CODE(tDecodeI64v(&decoder, &state->u0) < 0, code);
  }
  for (int8_t i = 0; i < pObj->nActives; ++i) {
    SGrantActive *active = &pObj->actives[i];
    RETURN_WITH_CODE(tDecodeI64v(&decoder, &active->u0) < 0, code);
    RETURN_WITH_CODE(tDecodeCStrTo(&decoder, &active->active[0]) < 0, code);
  }
  if (pObj->nActives > 0) {
    assert(strlen(pObj->actives[0].active) == 30);
  }
  int32_t activeLen = 0;
  RETURN_WITH_CODE(tDecodeI32v(&decoder, &activeLen) < 0, code);
  if (activeLen > 0) {
    if (!(pObj->active = taosMemoryMalloc(activeLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
    RETURN_WITH_CODE(tDecodeCStrTo(&decoder, pObj->active) < 0, code);
  }
  int32_t nMachines = 0;
  if (tDecodeI32v(&decoder, &nMachines) < 0) {
    goto _return;
  }
  uInfo("%s:%d nMachines = %d\n\n", __func__, __LINE__, nMachines);
  if (nMachines > 0) {
    if (!(pObj->pMachines = taosArrayInit(nMachines, sizeof(SGrantMachine)))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
    if (!taosArrayPush(pObj->pMachines, &(SGrantMachine){0})) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
    SGrantMachine *pLast = taosArrayGetLast(pObj->pMachines);
    if (tDecodeI64v(&decoder, &pLast->u0) < 0) {
      goto _return;
    };
    RETURN_WITH_CODE(tDecodeCStrTo(&decoder, &pLast->machine[0]) < 0, code);
  }

  code = 0;
_return:
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code != 0) {
    tDestroyGrantObj(pObj);
    mError("grant, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  return code;
}

SSdbRaw *mndGrantActionEncode(SGrantLogObj *pGrant) {
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
  SSdbRow      *pRow = NULL;
  SGrantLogObj *pGrant = NULL;
  void         *buf = NULL;
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

  if (!(pRow = sdbAllocRow(sizeof(SGrantLogObj)))) {
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

int32_t mndGrantActionInsert(SSdb *pSdb, SGrantLogObj *pGrant) {
  mTrace("grant:%d, perform insert action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  return 0;
}

int32_t mndGrantActionDelete(SSdb *pSdb, SGrantLogObj *pGrant) {
  mTrace("grant:%d, perform delete action", pGrant->id);
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
  tDestroyGrantObj(pGrant);
  return 0;
}

int32_t mndGrantActionUpdate(SSdb *pSdb, SGrantLogObj *pOldGrant, SGrantLogObj *pNewGrant) {
  taosWLockLatch(&pOldGrant->lock);

  mTrace("grant:%d, perform update action, old row:%p new row:%p", pOldGrant->id, pOldGrant, pNewGrant);

  pOldGrant->id = pNewGrant->id;
  pOldGrant->createTime = pNewGrant->createTime;
  pOldGrant->updateTime = taosGetTimestampMs() / 1000;
  pOldGrant->nStates = pNewGrant->nStates;
  pOldGrant->nActives = pNewGrant->nActives;
  TSWAP(pOldGrant->states, pNewGrant->states);
  TSWAP(pOldGrant->actives, pNewGrant->actives);
  TSWAP(pOldGrant->active, pNewGrant->active);
  TSWAP(pOldGrant->pMachines, pNewGrant->pMachines);

  taosWUnLockLatch(&pOldGrant->lock);

  return 0;
}