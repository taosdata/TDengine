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
#ifndef TDENGINE_SESSION_H
#define TDENGINE_SESSION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "clientInt.h"
#include "tdef.h"
#include "tglobal.h"
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"

typedef enum {
  SESSION_PER_USER = 0,
  SESSION_CONN_TIME = 1,
  SESSION_CONN_IDLE_TIME = 2,
  SESSION_MAX_CONCURRENCY = 3,
  SESSION_MAX_CALL_VNODE_NUM = 4,
  SESSION_MAX_TYPE = 5
} ESessionType;

typedef int32_t (*sessCheckFn)(int64_t* pValue, int64_t* limit);
typedef int32_t (*sessUpdateValueFn)(int64_t* pValue, int64_t delta);
typedef int32_t (*sessUpdateLimitFn)(int64_t* pValue, int64_t limit);
typedef struct {
  ESessionType      type;
  sessCheckFn       checkFn;
  sessUpdateValueFn updateFn;
  sessUpdateLimitFn limitFn;
} SSessionError;

typedef struct SSessMetric {
  int32_t refCnt;
  int64_t accessTime;
  int64_t lastAccessTime;

  int64_t value[SESSION_MAX_TYPE];
  int64_t limit[SESSION_MAX_TYPE];
  char    user[TSDB_USER_LEN];
} SSessMetric;

typedef struct {
  ESessionType type;
  int64_t      value;
} SSessParam;

typedef struct SSessionMgt {
  TdThreadRwlock lock;
  SHashObj*      pSessMetricMap;
  int8_t         inited;
} SSessionMgt;

int32_t sessMgtInit();

int32_t sessMgtUpdataLimit(char* user, ESessionType type, int32_t value);
int32_t sessMgtUpdateUserMetric(char* user, SSessParam* pPara);
int32_t sessMgtRemoveUser(char* user);
void    sessMgtDestroy();

int32_t sessMetricCreate(const char* user, SSessMetric** ppMetric);
void    sessMetricRef(SSessMetric* pMetric);
int32_t sessMetricUnref(SSessMetric* pMetric);
int32_t sessMetricUpdateLimit(SSessMetric* pMetric, ESessionType type, int32_t value);
int32_t sessMetricUpdate(SSessMetric* pMetric, SSessParam* p);
int32_t sessMetricCheckValue(SSessMetric* pMetric, ESessionType type, int64_t value);
void    sessMetricDestroy(SSessMetric* pMetric);

int32_t connCheckAndUpateMetric(int64_t connId);
int32_t tscUpdateSessMetric(STscObj* pTscObj, SSessParam* pParam);
int32_t tscCheckConnSessionMetric(STscObj* pTscObj);
int32_t tscRefSessMetric(STscObj* pTscObj);
int32_t tscUnrefSessMetric(STscObj* pTscObj);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SESSION_H
