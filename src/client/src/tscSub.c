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

#include "os.h"

#include "shash.h"
#include "taos.h"
#include "tlog.h"
#include "trpc.h"
#include "tsclient.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "tscUtil.h"

typedef struct SSubscriptionProgress {
  int64_t uid;
  TSKEY key;
} SSubscriptionProgress;

typedef struct SSub {
  void *                  signature;
  TAOS *                  taos;
  void *                  pTimer;
  SSqlObj *               pSql;
  int                     interval;
  TAOS_SUBSCRIBE_CALLBACK fp;
  void *                  param;
  int                     numOfMeters;
  SSubscriptionProgress * progress;
} SSub;


static int tscCompareSubscriptionProgress(const void* a, const void* b) {
  return ((const SSubscriptionProgress*)a)->uid - ((const SSubscriptionProgress*)b)->uid;
}

TSKEY tscGetSubscriptionProgress(SSqlObj* pSql, int64_t uid) {
  if( pSql == NULL || pSql->pSubscription == NULL)
    return 0;

  SSub* pSub = (SSub*)pSql->pSubscription;
  for (int s = 0, e = pSub->numOfMeters; s < e;) {
    int m = (s + e) / 2;
    SSubscriptionProgress* p = pSub->progress + m;
    if (p->uid > uid)
      e = m;
    else if (p->uid < uid)
      s = m + 1;
    else
      return p->key;
  }

  return 0;
}

void tscUpdateSubscriptionProgress(SSqlObj* pSql, int64_t uid, TSKEY ts) {
  if( pSql == NULL || pSql->pSubscription == NULL)
    return;

  SSub* pSub = (SSub*)pSql->pSubscription;
  for (int s = 0, e = pSub->numOfMeters; s < e;) {
    int m = (s + e) / 2;
    SSubscriptionProgress* p = pSub->progress + m;
    if (p->uid > uid)
      e = m;
    else if (p->uid < uid)
      s = m + 1;
    else {
      p->key = ts + 1;
      break;
    }
  }
}


static SSub* tscCreateSubscription(STscObj* pObj, const char* sql) {
  SSub* pSub = calloc(1, sizeof(SSub));
  if (pSub == NULL) {
    globalCode = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("failed to allocate memory for subscription");
    return NULL;
  }

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    globalCode = TSDB_CODE_CLI_OUT_OF_MEMORY;
    tscError("failed to allocate SSqlObj for subscription");
    goto failed;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pObj;

  char* sqlstr = (char*)malloc(strlen(sql) + 1);
  if (sqlstr == NULL) {
    tscError("failed to allocate sql string for subscription");
    goto failed;
  }
  strcpy(sqlstr, sql);
  strtolower(sqlstr, sqlstr);
  pSql->sqlstr = sqlstr;

  tsem_init(&pSql->rspSem, 0, 0);
  tsem_init(&pSql->emptyRspSem, 0, 1);

  SSqlRes *pRes = &pSql->res;
  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;

  pSql->pSubscription = pSub;
  pSub->pSql = pSql;
  pSub->signature = pSub;
  return pSub;

failed:
  if (sqlstr != NULL) {
    free(sqlstr);
  }
  if (pSql != NULL) {
    free(pSql);
  }
  free(pSub);
  return NULL;
}


static void tscProcessSubscribeTimer(void *handle, void *tmrId) {
  SSub *pSub = (SSub *)handle;
  if (pSub == NULL || pSub->pTimer != tmrId) return;

  TAOS_RES* res = taos_consume(pSub);
  if (res != NULL) {
    pSub->fp(pSub->param, res, 0);
    taos_free_result(res);
  }

  taosTmrReset(tscProcessSubscribeTimer, pSub->interval, pSub, tscTmr, &pSub->pTimer);
}


TAOS_SUB *taos_subscribe(TAOS *taos, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval) {
  STscObj* pObj = (STscObj*)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  SSub* pSub = tscCreateSubscription(pObj, sql);
  if (pSub == NULL) {
    return NULL;
  }

  int code = (uint8_t)tsParseSql(pSub->pSql, pObj->acctId, pObj->db, false);
  if (code != TSDB_CODE_SUCCESS) {
    taos_unsubscribe(pSub);
    return NULL;
  }

// ??? if there's more than one vnode
  SSqlCmd* pCmd = &pSub->pSql->cmd;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
    pSub->numOfMeters = 1;
    pSub->progress = calloc(1, sizeof(SSubscriptionProgress));
    pSub->progress[0].uid = pMeterMetaInfo->pMeterMeta->uid;
  } else {
    SMetricMeta* pMetricMeta = pMeterMetaInfo->pMetricMeta;
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);
    pSub->numOfMeters = pVnodeSidList->numOfSids;
    pSub->progress = calloc(pSub->numOfMeters, sizeof(SSubscriptionProgress));
    for (int32_t i = 0; i < pSub->numOfMeters; ++i) {
      SMeterSidExtInfo *pMeterInfo = tscGetMeterSidInfo(pVnodeSidList, i);
      pSub->progress[i].uid = pMeterInfo->uid;
    }
    qsort(pSub->progress, pSub->numOfMeters, sizeof(SSubscriptionProgress), tscCompareSubscriptionProgress);
  }

  // timestamp must in the output column
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  tscFieldInfoSetValue(pFieldInfo, pFieldInfo->numOfOutputCols, TSDB_DATA_TYPE_TIMESTAMP, "_c0", TSDB_KEYSIZE);
  tscSqlExprInsertEmpty(pCmd, pFieldInfo->numOfOutputCols - 1, TSDB_FUNC_PRJ);
  tscFieldInfoUpdateVisible(pFieldInfo, pFieldInfo->numOfOutputCols - 1, false);
  tscFieldInfoCalOffset(pCmd);

  if (fp != NULL) {
    pSub->fp = fp;
    pSub->interval = interval;
    pSub->param = param;
    taosTmrReset(tscProcessSubscribeTimer, 0, pSub, tscTmr, &pSub->pTimer);
  }

  return pSub;
}

TAOS_RES *taos_consume(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;
  if (pSub == NULL) return NULL;

  SSqlObj* pSql = pSub->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;
  pRes->qhandle = 0;
  pSql->thandle = NULL;

  tscDoQuery(pSql);
  if (pRes->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }
  return pSql;
}

void taos_unsubscribe(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;
  if (pSub == NULL || pSub->signature != pSub) return;

  if (pSub->pTimer != NULL) {
    taosTmrStop(pSub->pTimer);
  }
  tscFreeSqlObj(pSub->pSql);
  free(pSub->progress);
  memset(pSub, 0, sizeof(*pSub));
  free(pSub);
}

int taos_subfields_count(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;

  return taos_num_fields(pSub->pSql);
}

TAOS_FIELD *taos_fetch_subfields(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;

  return pSub->pSql->cmd.fieldsInfo.pFields;
}
