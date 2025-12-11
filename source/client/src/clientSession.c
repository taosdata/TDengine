
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

static SSessionMgt sessMgt = {0};

static int32_t sessPerUserCheckFn(int64_t value, int64_t limit) {
  int32_t code = 0;
  if (limit == -1) {
    return 0;
  }

  if (value > limit) {
    code = TSDB_CODE_TSC_SESS_PER_USER_LIMIT;
  }

  return code;
}

static int32_t sessPerUserUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  *value += limit;
  return code;
}

static int32_t sessConnTimeCheckFn(int64_t value, int64_t limit) {
  int32_t code = 0;
  if (limit == -1) {
    return code;
  }
  int64_t currentTime = taosGetTimestampMs();
  if ((value + limit) < currentTime) {
    code = TSDB_CODE_TSC_SESS_CONN_TIMEOUT;
  }

  return code;
}

static int32_t sessConnTimeUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  *value = taosGetTimestampMs();
  return code;
}

static int32_t sessConnIdleTimeCheckFn(int64_t value, int64_t limit) {
  if (limit == -1) {
    return 0;
  }
  int32_t code = 0;
  int64_t currentTime = taosGetTimestampMs();
  if ((value + limit) < currentTime) {
    code = TSDB_CODE_TSC_SESS_CONN_IDLE_TIMEOUT;
  }
  return code;
}

static int32_t sessConnIdleTimeUpdateFn(int64_t *value, int64_t limit) {
  int32_t code = 0;
  *value = taosGetTimestampMs();
  return code;
}

static int32_t sessMaxConnCurrencyCheckFn(int64_t value, int64_t limit) {
  int32_t code = 0;
  if (limit == -1) {
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

static int32_t sessVnodeCallCheckFn(int64_t value, int64_t limit) {
  int32_t code = 0;
  if (limit == -1) {
    return code;
  }

  if (value > limit) {
    code = TSDB_CODE_TSC_SESS_MAX_CALL_VNODE_LIMIT;
  }
  return code;
}

static int32_t sessVnodeCallNumUpdateFn(int64_t *value, int64_t delta) {
  int32_t code = 0;
  *value += delta;
  return code;
}
static int32_t sessSetValueLimitFn(int64_t *pLimit, int64_t src) {
  int32_t code = 0;
  *pLimit = src;
  return code;
}

static SSessionError sessFnSet[] = {
    {SESSION_PER_USER, sessPerUserCheckFn, sessPerUserUpdateFn, sessSetValueLimitFn},
    {SESSION_CONN_TIME, sessConnTimeCheckFn, sessConnTimeUpdateFn, sessSetValueLimitFn},
    {SESSION_CONN_IDLE_TIME, sessConnIdleTimeCheckFn, sessMaxConnCurrencyUpdateFn, sessSetValueLimitFn},
    {SESSION_MAX_CONCURRENCY, sessMaxConnCurrencyCheckFn, sessMaxConnCurrencyUpdateFn, sessSetValueLimitFn},
    {SESSION_MAX_CALL_VNODE_NUM, sessVnodeCallCheckFn, sessVnodeCallNumUpdateFn, sessSetValueLimitFn},
};

int32_t sessMetricCreate(SSessMetric **ppMetric) {
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

  code = taosThreadRwlockInit(&pMetric->lock, NULL);
  if (code != 0) {
    taosMemoryFree(pMetric);
    return code;
  }

  *ppMetric = pMetric;
  return code;
}

int32_t sessMetricUpdateLimit(SSessMetric *pMetric, ESessionType type, int32_t value) {
  int32_t code = 0;

  (void)taosThreadRwlockWrlock(&pMetric->lock);
  code = sessFnSet[type].limitFn(&pMetric->limit[type], value);
  (void)taosThreadRwlockUnlock(&pMetric->lock);
  return code;
}

int32_t sessMetricCheckImpl(SSessMetric *pMetric) {
  int32_t code = 0;

  for (int32_t i = 0; i < sizeof(pMetric->limit) / sizeof(pMetric->limit[0]); i++) {
    code = sessFnSet[i].checkFn(pMetric->value[i], pMetric->limit[i]);
    if (code != 0) {
      break;
    }
  }

  return code;
}
int32_t sessMetricCheckByTypeImpl(SSessMetric *pMetric, ESessionType type) {
  return sessFnSet[type].checkFn(pMetric->value[type], pMetric->limit[type]);
}

int32_t sessMetricCheck(SSessMetric *pMetric) {
  int32_t code = 0;

  (void)taosThreadRwlockRdlock(&pMetric->lock);
  code = sessMetricCheckImpl(pMetric);

  (void)taosThreadRwlockUnlock(&pMetric->lock);

  return code;
}

int32_t sessMetricCheckByType(SSessMetric *pMetric, ESessionType type) {
  int32_t code = 0;

  (void)taosThreadRwlockRdlock(&pMetric->lock);
  code = sessMetricCheckByTypeImpl(pMetric, type);
  (void)taosThreadRwlockUnlock(&pMetric->lock);

  return code;
}

int32_t sessMetricGet(SSessMetric *pMetric, ESessionType type, int32_t *pValue) {
  int32_t code = 0;

  (void)taosThreadRwlockRdlock(&pMetric->lock);
  *pValue = pMetric->limit[type];
  (void)taosThreadRwlockUnlock(&pMetric->lock);

  return code;
}

int32_t sessMetricUpdate(SSessMetric *pMetric, SSessParam *p) {
  int32_t code = 0;
  int32_t lino = 0;
  (void)taosThreadRwlockWrlock(&pMetric->lock);

  code = sessMetricCheckByTypeImpl(pMetric, p->type);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = sessFnSet[p->type].updateFn(&pMetric->value[p->type], p->value);
_error:

  (void)taosThreadRwlockUnlock(&pMetric->lock);
  return code;
}
void sessMetricDestroy(SSessMetric *pMetric) {
  TAOS_UNUSED(taosThreadRwlockDestroy(&pMetric->lock));
  taosMemoryFree(pMetric);
}

int32_t sessMgtInit() {
  int32_t code = 0;
  int32_t lino = 0;

  sessMgt.pSessMetricMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (sessMgt.pSessMetricMap == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

_error:

  return code;
}

int32_t sessMgtUpdataLimit(char *user, ESessionType type, int32_t value) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;
  if (type >= SESSION_MAX_TYPE) {
    return TSDB_CODE_INVALID_PARA;
  }

  (void)taosThreadRwlockWrlock(&pMgt->lock);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = sessMetricUpdateLimit(*ppMetric, type, value);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  if (code != 0) {
    uError("failed to update session mgt type:%d, line:%d, code:%d", type, lino, code);
  }

  code = taosThreadRwlockUnlock(&pMgt->lock);
  TAOS_CHECK_GOTO(code, &lino, _error);

  return code;
}

int32_t sessMgtUpdateUserMetric(char *user, SSessParam *pPara) {
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

  (void)taosThreadRwlockUnlock(&pMgt->lock);
  return code;
}

int32_t sessMgtGet(char *user, ESessionType type, int32_t *pValue) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;

  if (type >= SESSION_MAX_TYPE) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = taosThreadRwlockRdlock(&pMgt->lock);
  TAOS_CHECK_GOTO(code, &lino, _error);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = sessMetricGet(*ppMetric, type, pValue);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  code = taosThreadRwlockUnlock(&pMgt->lock);

  if (code != 0) {
    uError("failed to get session mgt type:%d, line:%d, code:%d", type, lino, code);
  }
  return code;
}
int32_t sessMgtCheckUser(char *user, ESessionType type) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;
  code = taosThreadRwlockRdlock(&pMgt->lock);
  TAOS_CHECK_GOTO(code, &lino, _error);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric == NULL || *ppMetric == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = sessMetricCheckByType(*ppMetric, type);

_error:
  code = taosThreadRwlockUnlock(&pMgt->lock);
  if (code != 0) {
    uError("failed to check user session, line:%d, code:%d", lino, code);
  }
  return code;
}

int32_t sessMgtRemoveUser(char *user) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSessionMgt *pMgt = &sessMgt;

  code = taosThreadRwlockWrlock(&pMgt->lock);
  TAOS_CHECK_GOTO(code, &lino, _error);

  SSessMetric **ppMetric = taosHashGet(pMgt->pSessMetricMap, user, strlen(user));
  if (ppMetric != NULL && *ppMetric != NULL) {
    sessMetricDestroy(*ppMetric);
    code = taosHashRemove(pMgt->pSessMetricMap, user, strlen(user));
    TAOS_CHECK_GOTO(code, &lino, _error);
  }
_error:
  return code;
}

void sessMgtDestroy() {
  SSessionMgt *pMgt = &sessMgt;
  int32_t      code = 0;

  void *p = taosHashIterate(pMgt->pSessMetricMap, NULL);
  while (p) {
    SSessMetric *pMetric = *(SSessMetric **)p;
    p = taosHashIterate(pMgt->pSessMetricMap, p);
    sessMetricDestroy(pMetric);
  }

  code = taosThreadRwlockDestroy(&pMgt->lock);
  if (code != 0) {
    uError("failed to destroy session mgt, code:%d", code);
  }
  taosHashCleanup(pMgt->pSessMetricMap);
}
