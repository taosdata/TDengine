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
  char                    topic[32];
  int64_t                 lastSyncTime;
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

TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid) {
  if (sub == NULL)
    return 0;

  SSub* pSub = (SSub*)sub;
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

void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts) {
  if( sub == NULL)
    return;

  SSub* pSub = (SSub*)sub;
  for (int s = 0, e = pSub->numOfMeters; s < e;) {
    int m = (s + e) / 2;
    SSubscriptionProgress* p = pSub->progress + m;
    if (p->uid > uid)
      e = m;
    else if (p->uid < uid)
      s = m + 1;
    else {
      if (ts >= p->key) p->key = ts + 1;
      break;
    }
  }
}


static SSub* tscCreateSubscription(STscObj* pObj, const char* topic, const char* sql) {
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
  strncpy(pSub->topic, topic, sizeof(pSub->topic));
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


static void tscProcessSubscriptionTimer(void *handle, void *tmrId) {
  SSub *pSub = (SSub *)handle;
  if (pSub == NULL || pSub->pTimer != tmrId) return;

  TAOS_RES* res = taos_consume(pSub);
  if (res != NULL) {
    pSub->fp(pSub, res, pSub->param, 0);
    // TODO: memory leak
  }

  taosTmrReset(tscProcessSubscriptionTimer, pSub->interval, pSub, tscTmr, &pSub->pTimer);
}


bool tscUpdateSubscription(STscObj* pObj, SSub* pSub) {
  int code = (uint8_t)tsParseSql(pSub->pSql, pObj->acctId, pObj->db, false);
  if (code != TSDB_CODE_SUCCESS) {
    taos_unsubscribe(pSub);
    return false;
  }

  int numOfMeters = 0;
  SSubscriptionProgress* progress = NULL;

// ??? if there's more than one vnode
  SSqlCmd* pCmd = &pSub->pSql->cmd;

  SMeterMetaInfo *pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
    numOfMeters = 1;
    progress = calloc(1, sizeof(SSubscriptionProgress));
    int64_t uid = pMeterMetaInfo->pMeterMeta->uid;
    progress[0].uid = uid;
    progress[0].key = tscGetSubscriptionProgress(pSub, uid);
  } else {
    SMetricMeta* pMetricMeta = pMeterMetaInfo->pMetricMeta;
    SVnodeSidList *pVnodeSidList = tscGetVnodeSidList(pMetricMeta, pMeterMetaInfo->vnodeIndex);
    numOfMeters = pVnodeSidList->numOfSids;
    progress = calloc(numOfMeters, sizeof(SSubscriptionProgress));
    for (int32_t i = 0; i < numOfMeters; ++i) {
      SMeterSidExtInfo *pMeterInfo = tscGetMeterSidInfo(pVnodeSidList, i);
      int64_t uid = pMeterInfo->uid;
      progress[i].uid = uid;
      progress[i].key = tscGetSubscriptionProgress(pSub, uid);
    }
    qsort(progress, numOfMeters, sizeof(SSubscriptionProgress), tscCompareSubscriptionProgress);
  }

  free(pSub->progress);
  pSub->numOfMeters = numOfMeters;
  pSub->progress = progress;

  // timestamp must in the output column
  SFieldInfo* pFieldInfo = &pCmd->fieldsInfo;
  tscFieldInfoSetValue(pFieldInfo, pFieldInfo->numOfOutputCols, TSDB_DATA_TYPE_TIMESTAMP, "_c0", TSDB_KEYSIZE);
  tscSqlExprInsertEmpty(pCmd, pFieldInfo->numOfOutputCols - 1, TSDB_FUNC_PRJ);
  tscFieldInfoUpdateVisible(pFieldInfo, pFieldInfo->numOfOutputCols - 1, false);
  tscFieldInfoCalOffset(pCmd);

  pSub->lastSyncTime = taosGetTimestampMs();

  return true;
}


static void tscLoadSubscriptionProgress(SSub* pSub) {
  char buf[TSDB_MAX_SQL_LEN];
  sprintf(buf, "%s/subscribe/%s", dataDir, pSub->topic);

  FILE* fp = fopen(buf, "r");
  if (fp == NULL) {
    tscTrace("subscription progress file does not exist: %s", pSub->topic);
    return true;
  }

  if (fgets(buf, sizeof(buf), fp) == NULL) {
    tscTrace("invalid subscription progress file: %s", pSub->topic);
    fclose(fp);
    return false;
  }

  for (int i = 0; i < sizeof(buf); i++) {
    if (buf[i] == 0)
      break;
    if (buf[i] == '\r' || buf[i] == '\n') {
      buf[i] = 0;
      break;
    }
  }
  if (strcmp(buf, pSub->pSql->sqlstr) != 0) {
    tscTrace("subscription sql statement mismatch: %s", pSub->topic);
    fclose(fp);
    return false;
  }

  if (fgets(buf, sizeof(buf), fp) == NULL || atoi(buf) < 0) {
    tscTrace("invalid subscription progress file: %s", pSub->topic);
    fclose(fp);
    return false;
  }

  int numOfMeters = atoi(buf);
  SSubscriptionProgress* progress = calloc(numOfMeters, sizeof(SSubscriptionProgress));
  for (int i = 0; i < numOfMeters; i++) {
    if (fgets(buf, sizeof(buf), fp) == NULL) {
      fclose(fp);
      free(progress);
      return false;
    }
    int64_t uid, key;
    sscanf(buf, "uid=%" SCNd64 ",progress=%" SCNd64, &uid, &key);
    progress[i].uid = uid;
    progress[i].key = key;
  }

  fclose(fp);

  qsort(progress, numOfMeters, sizeof(SSubscriptionProgress), tscCompareSubscriptionProgress);
  pSub->numOfMeters = numOfMeters;
  pSub->progress = progress;
  return true;
}

void tscSaveSubscriptionProgress(void* sub) {
  SSub* pSub = (SSub*)sub;

  char path[256];
  sprintf(path, "%s/subscribe", dataDir);
  if (access(path, 0) != 0) {
    mkdir(path, 0777);
  }

  sprintf(path, "%s/subscribe/%s", dataDir, pSub->topic);
  FILE* fp = fopen(path, "w+");
  if (fp == NULL) {
    tscError("failed to create progress file for subscription: %s", pSub->topic);
    return;
  }

  fputs(pSub->pSql->sqlstr, fp);
  fprintf(fp, "\n%d\n", pSub->numOfMeters);
  for (int i = 0; i < pSub->numOfMeters; i++) {
    int64_t uid = pSub->progress[i].uid;
    TSKEY key = pSub->progress[i].key;
    fprintf(fp, "uid=%" PRId64 ",progress=%" PRId64 "\n", uid, key);
  }

  fclose(fp);
}


TAOS_SUB *taos_subscribe(const char* topic, int restart, TAOS *taos, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval) {
  STscObj* pObj = (STscObj*)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    globalCode = TSDB_CODE_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  SSub* pSub = tscCreateSubscription(pObj, topic, sql);
  if (pSub == NULL) {
    return NULL;
  }
  pSub->taos = taos;

  if (restart) {
    tscTrace("restart subscription: %s", topic);
  } else {
    tscLoadSubscriptionProgress(pSub);
  }

  if (!tscUpdateSubscription(pObj, pSub)) {
    return NULL;
  }

  if (fp != NULL) {
    pSub->fp = fp;
    pSub->interval = interval;
    pSub->param = param;
    taosTmrReset(tscProcessSubscriptionTimer, 0, pSub, tscTmr, &pSub->pTimer);
  }

  return pSub;
}

TAOS_RES *taos_consume(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;
  if (pSub == NULL) return NULL;

  if (taosGetTimestampMs() - pSub->lastSyncTime > 30 * 60 * 1000) {
    taos_query(pSub->taos, "reset query cache;");
    // TODO: clear memory
    if (!tscUpdateSubscription(pSub->taos, pSub)) return NULL;
  }

  SSqlObj* pSql = pSub->pSql;
  SSqlRes *pRes = &pSql->res;

  pRes->numOfRows = 1;
  pRes->numOfTotal = 0;
  pRes->qhandle = 0;
  pSql->thandle = NULL;
  pSql->cmd.command = TSDB_SQL_SELECT;

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
