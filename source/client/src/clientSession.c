
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

#include "clientSession.h"
#include "clientInt.h"
#include "clientLog.h"

static SSessionMgt sessMgt = {0};

#define HANDLE_SESSION_CONTROL() \
  do {                           \
    if (sessMgt.inited == 0) {   \
      return TSDB_CODE_SUCCESS;  \
    }                            \
  } while (0)

static int32_t tscCheckConnStatus(STscObj *pTsc);

static int32_t sessPerUserCheckFn(int64_t *value, int64_t *limit) {
  int32_t code = 0;
  int64_t cValue = atomic_load_64(value);
  int64_t cLimit = atomic_load_64(limit);
  if (cLimit == -1) {
    return 0;
  }

  if (cValue > cLimit) {
    code = TSDB_CODE_TSC_SESS_PER_USER_LIMIT;
  }

  return code;
}

static int32_t sessPerUserUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  atomic_add_fetch_64(value, limit);
  return code;
}

static int32_t sessConnTimeCheckFn(int64_t *value, int64_t *limit) {
  int32_t code = 0;

  int64_t cValue = atomic_load_64(value);
  int64_t cLimit = atomic_load_64(limit);
  if (cLimit == -1) {
    return code;
  }
  int64_t currentTime = taosGetTimestampMs();
  if ((cValue + cLimit * 1000) < currentTime) {
    code = TSDB_CODE_TSC_SESS_CONN_TIMEOUT;
  }

  return code;
}

static int32_t sessConnTimeUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  int64_t now = taosGetTimestampMs();
  atomic_store_64(value, now);
  return code;
}

static int32_t sessConnIdleTimeCheckFn(int64_t *value, int64_t *limit) {
  int32_t code = 0;

  int64_t currentTime = taosGetTimestampMs();
  int64_t cValue = atomic_load_64(value);
  int64_t cLimit = atomic_load_64(limit);
  if (cLimit == -1) {
    return 0;
  }

  if ((cValue + cLimit * 1000) < currentTime) {
    code = TSDB_CODE_TSC_SESS_CONN_IDLE_TIMEOUT;
  }
  return code;
}

static int32_t sessConnIdleTimeUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  int64_t now = taosGetTimestampMs();
  atomic_store_64(value, now);

  return code;
}

static int32_t sessMaxConnCurrencyCheckFn(int64_t *value, int64_t *limit) {
  int32_t code = 0;
  int64_t cValue = atomic_load_64(value);
  int64_t cLimit = atomic_load_64(limit);
  if (cLimit == -1) {
    return code;
  }
  return code;
}

static int32_t sessMaxConnCurrencyUpdateFn(int64_t *value, int64_t delta) {
  int32_t code = 0;
  if (delta == -1) {
    return code;
  }
  return code;
}

static int32_t sessVnodeCallCheckFn(int64_t *value, int64_t *limit) {
  int32_t code = 0;
  int64_t cValue = atomic_load_64(value);
  int64_t cLimit = atomic_load_64(limit);
  if (cLimit == -1) {
    return code;
  }

  if (value > limit) {
    code = TSDB_CODE_TSC_SESS_MAX_CALL_VNODE_LIMIT;
  }
  return code;
}

static int32_t sessVnodeCallNumUpdateFn(int64_t *value, int64_t delta) {
  int32_t code = 0;
  atomic_fetch_add_64(value, delta);
  return code;
}
static int32_t sessSetValueLimitFn(int64_t *pLimit, int64_t src) {
  int32_t code = 0;
  atomic_store_64(pLimit, src);
  return code;
}

static SSessionError sessFnSet[] = {
    {SESSION_PER_USER, sessPerUserCheckFn, sessPerUserUpdateFn, sessSetValueLimitFn},
    {SESSION_CONN_TIME, sessConnTimeCheckFn, sessConnTimeUpdateFn, sessSetValueLimitFn},
    {SESSION_CONN_IDLE_TIME, sessConnIdleTimeCheckFn, sessConnIdleTimeUpdateFn, sessSetValueLimitFn},
    {SESSION_MAX_CONCURRENCY, sessMaxConnCurrencyCheckFn, sessMaxConnCurrencyUpdateFn, sessSetValueLimitFn},
    {SESSION_MAX_CALL_VNODE_NUM, sessVnodeCallCheckFn, sessVnodeCallNumUpdateFn, sessSetValueLimitFn},
};

int32_t sessMetricCreate(SSessMetric **ppMetric) {
  HANDLE_SESSION_CONTROL();
  int32_t      code = 0;
  SSessMetric *pMetric = (SSessMetric *)taosMemoryMalloc(sizeof(SSessMetric));
  if (pMetric == NULL) {
    code = terrno;
    return code;
  }

  memset(pMetric->value, 0, sizeof(pMetric->value));

  pMetric->limit[SESSION_PER_USER] = sessionPerUser;
  pMetric->limit[SESSION_CONN_TIME] = sessionConnTime;
  pMetric->limit[SESSION_CONN_IDLE_TIME] = sessionConnIdleTime;
  pMetric->limit[SESSION_MAX_CONCURRENCY] = sessionMaxConcurrency;
  pMetric->limit[SESSION_MAX_CALL_VNODE_NUM] = sessionMaxCallVnodeNum;

  *ppMetric = pMetric;
  return code;
}

int32_t sessMetricUpdateLimit(SSessMetric *pMetric, ESessionType type, int32_t value) {
  int32_t code = 0;

  code = sessFnSet[type].limitFn(&pMetric->limit[type], value);
  return code;
}

int32_t sessMetricCheckImpl(SSessMetric *pMetric) {
  int32_t code = 0;

  for (int32_t i = 0; i < sizeof(pMetric->limit) / sizeof(pMetric->limit[0]); i++) {
    code = sessFnSet[i].checkFn(&pMetric->value[i], &pMetric->limit[i]);
    if (code != 0) {
      break;
    }
  }

  return code;
}
int32_t sessMetricCheckByTypeImpl(SSessMetric *pMetric, ESessionType type) {
  return sessFnSet[type].checkFn(&pMetric->value[type], &pMetric->limit[type]);
}

int32_t sessMetricUpdate(SSessMetric *pMetric, SSessParam *p) {
  int32_t code = 0;
  int32_t lino = 0;

  code = sessMetricCheckByTypeImpl(pMetric, p->type);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = sessFnSet[p->type].updateFn(&pMetric->value[p->type], p->value);
_error:

  return code;
}

int32_t sessMetricCheckValue(SSessMetric *pMetric, ESessionType type, int64_t value) {
  int32_t code = 0;
  code = sessFnSet[type].checkFn(&value, &pMetric->limit[type]);
  return code;
}
void    sessMetricDestroy(SSessMetric *pMetric) { taosMemoryFree(pMetric); }
void   sessMetricRef(SSessMetric *pMetric) { (void)atomic_add_fetch_32(&pMetric->refCnt, 1); }
int32_t sessMetricUnref(SSessMetric *pMetric) {
  int32_t ref = atomic_sub_fetch_32(&pMetric->refCnt, 1);
  if (ref == 0) {
    sessMetricDestroy(pMetric);
  }
  return ref;
}

int32_t sessMgtInit() {
  int32_t code = 0;
  int32_t lino = 0;

  sessMgt.pSessMetricMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (sessMgt.pSessMetricMap == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }
  sessMgt.inited = 1;

_error:
  if (code != 0) {
    tscError("failed to init session mgt, line:%d, code:%d", lino, code);
  }
  return code;
}

int32_t sessMgtGetOrCreateUserMetric(char *user, SSessMetric **pMetric) {
  HANDLE_SESSION_CONTROL();
  int32_t lino = 0;
  int32_t code = 0;

  SSessMetric *p = NULL;

  (void)taosThreadRwlockWrlock(&sessMgt.lock);
  SSessMetric **ppMetric = taosHashGet(sessMgt.pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = sessMetricCreate(&p);
    TAOS_CHECK_GOTO(code, &lino, _error);

    code = taosHashPut(sessMgt.pSessMetricMap, user, strlen(user), &pMetric, sizeof(SSessMetric *));
    TAOS_CHECK_GOTO(code, &lino, _error);
  } else {
    p = *ppMetric;
  }

  *pMetric = p;

_error:
  (void)taosThreadRwlockUnlock(&sessMgt.lock);
  return code;
}

int32_t sessMgtUpdataLimit(char *user, ESessionType type, int32_t value) {
  HANDLE_SESSION_CONTROL();
  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;

  if (type >= SESSION_MAX_TYPE || type < SESSION_PER_USER) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSessMetric *pMetric = NULL;
  (void)taosThreadRwlockWrlock(&pMgt->lock);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = sessMetricCreate(&pMetric);
    TAOS_CHECK_GOTO(code, &lino, _error);

    code = taosHashPut(pMgt->pSessMetricMap, user, strlen(user), &pMetric, sizeof(SSessMetric *));
    TAOS_CHECK_GOTO(code, &lino, _error);

  } else {
    pMetric = *ppMetric;
  }

  code = sessMetricUpdateLimit(pMetric, type, value);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  if (code != 0) {
    uError("failed to update session mgt type:%d, line:%d, code:%d", type, lino, code);
  }

  TAOS_UNUSED(taosThreadRwlockUnlock(&pMgt->lock));

  return code;
}

int32_t sessMgtUpdateUserMetric(char *user, SSessParam *pPara) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  int32_t lino = 0;

  SSessionMgt *pMgt = &sessMgt;

  SSessMetric *pMetric = NULL;
  (void)taosThreadRwlockWrlock(&pMgt->lock);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = sessMetricCreate(&pMetric);
    TAOS_CHECK_GOTO(code, &lino, _error);

    code = taosHashPut(pMgt->pSessMetricMap, user, strlen(user), &pMetric, sizeof(SSessMetric *));
    TAOS_CHECK_GOTO(code, &lino, _error);
  } else {
    pMetric = *ppMetric;
  }

  code = sessMetricUpdate(pMetric, pPara);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  if (code != 0) {
    uError("failed to update user session metric, line:%d, code:%d", lino, code);
  }

  TAOS_UNUSED(taosThreadRwlockUnlock(&pMgt->lock));
  return code;
}

int32_t sessMgtRemoveUser(char *user) {
  HANDLE_SESSION_CONTROL();

  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;

  code = taosThreadRwlockWrlock(&pMgt->lock);
  TAOS_CHECK_GOTO(code, &lino, _error);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric != NULL && *ppMetric != NULL) {
    if (*ppMetric != NULL) {
      int32_t ref = sessMetricUnref(*ppMetric);
      if (ref == 0) {
        taosHashRemove(pMgt->pSessMetricMap, user, strlen(user));
      }
    }
  }
_error:
  TAOS_UNUSED(taosThreadRwlockUnlock(&pMgt->lock));
  return code;
}

void sessMgtDestroy() {
  SSessionMgt *pMgt = &sessMgt;
  int32_t      code = 0;

  if (pMgt->pSessMetricMap == NULL) {
    return;
  }

  void *p = taosHashIterate(pMgt->pSessMetricMap, NULL);
  while (p) {
    SSessMetric *pMetric = *(SSessMetric **)p;
    sessMetricDestroy(pMetric);
    p = taosHashIterate(pMgt->pSessMetricMap, p);
  }

  code = taosThreadRwlockDestroy(&pMgt->lock);
  if (code != 0) {
    uError("failed to destroy session mgt, code:%d", code);
  }
  taosHashCleanup(pMgt->pSessMetricMap);

  pMgt->pSessMetricMap = NULL;
}
int32_t tscCheckConnStatus(STscObj *pTsc) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  int32_t lino = 0;

  SConnAccessInfo *p = &pTsc->sessInfo;
  SSessMetric     *pMetric = (SSessMetric *)pTsc->pSessMetric;

  code = sessMetricCheckValue(pMetric, SESSION_CONN_TIME, p->startTime);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = sessMetricCheckValue(pMetric, SESSION_CONN_IDLE_TIME, p->lastAccessTime);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  if (code != 0) {
    uError("failed to check connection status line:%d, code:%d", lino, code);
  }
  return code;
}

int32_t connCheckAndUpateMetric(int64_t connId) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  int32_t lino = 0;

  STscObj *pTscObj = acquireTscObj(connId);
  if (pTscObj == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  code = tscCheckConnStatus(pTscObj);
  TAOS_CHECK_GOTO(code, &lino, _error);

  updateConnAccessInfo(&pTscObj->sessInfo);

  code =
      sessMetricUpdate((SSessMetric *)pTscObj->pSessMetric, &(SSessParam){.type = SESSION_MAX_CONCURRENCY, .value = 1});
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  if (code != 0) {
    tscError("conn:0x%" PRIx64 ", check and update metric failed at line:%d, code:%s", connId, lino, tstrerror(code));
  }

  releaseTscObj(connId);
  return code;
}

int32_t tscUpdateSessMetric(STscObj *pTscObj, SSessParam *pParam) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  int32_t lino = 0;

  if (pTscObj == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  SSessMetric *pMetric = (SSessMetric *)pTscObj->pSessMetric;
  code = sessMetricUpdate(pMetric, pParam);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:

  return code;
}

int32_t tscCheckConnSessionMetric(STscObj *pTscObj) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  if (pTscObj == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }
  code = tscCheckConnStatus(pTscObj);

_error:
  if (code != 0) {
    uError("failed to check connection session metric, code:%d", code);
  }
  return code;
}

int32_t tscRefSessMetric(STscObj *pTscObj) {
  HANDLE_SESSION_CONTROL();

  int32_t code = 0;
  int32_t lino = 0;

  SSessMetric *pMetric = NULL;
  code = sessMgtGetOrCreateUserMetric((char *)pTscObj->user, &pMetric);
  TAOS_CHECK_GOTO(code, &lino, _error);

  pTscObj->pSessMetric = pMetric;
_error:
  return code;
}
int32_t tscUnrefSessMetric(STscObj *pTscObj) {
  HANDLE_SESSION_CONTROL();
  int32_t code = 0;

  SSessMetric *pMetric = (SSessMetric *)pTscObj->pSessMetric;
  if (pMetric != NULL) {
    sessMetricUnref(pMetric);
    pTscObj->pSessMetric = NULL;
  }
  return code;
}