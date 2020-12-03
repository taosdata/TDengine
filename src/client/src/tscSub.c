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
#include "taos.h"
#include "trpc.h"
#include "tsclient.h"
#include "tsocket.h"
#include "ttimer.h"
#include "tutil.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tcache.h"
#include "tscProfile.h"

typedef struct SSubscriptionProgress {
  int64_t uid;
  TSKEY key;
} SSubscriptionProgress;

typedef struct SSub {
  void *                  signature;
  char                    topic[32];
  tsem_t                  sem;
  int64_t                 lastSyncTime;
  int64_t                 lastConsumeTime;
  TAOS *                  taos;
  void *                  pTimer;
  SSqlObj *               pSql;
  int                     interval;
  TAOS_SUBSCRIBE_CALLBACK fp;
  void *                  param;
  SArray* progress;
} SSub;


static int tscCompareSubscriptionProgress(const void* a, const void* b) {
  const SSubscriptionProgress* x = (const SSubscriptionProgress*)a;
  const SSubscriptionProgress* y = (const SSubscriptionProgress*)b;
  if (x->uid > y->uid) return 1;
  if (x->uid < y->uid) return -1;
  return 0;
}

TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid, TSKEY dflt) {
  if (sub == NULL) {
    return dflt;
  }
  SSub* pSub = (SSub*)sub;

  SSubscriptionProgress target = {.uid = uid, .key = 0};
  SSubscriptionProgress* p = taosArraySearch(pSub->progress, &target, tscCompareSubscriptionProgress, TD_EQ);
  if (p == NULL) {
    return dflt;
  }
  return p->key;
}

void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts) {
  if( sub == NULL)
    return;
  SSub* pSub = (SSub*)sub;

  SSubscriptionProgress target = {.uid = uid, .key = ts};
  SSubscriptionProgress* p = taosArraySearch(pSub->progress, &target, tscCompareSubscriptionProgress, TD_EQ);
  if (p != NULL) {
    p->key = ts;
  }
}


static void asyncCallback(void *param, TAOS_RES *tres, int code) {
  assert(param != NULL);
  SSub *pSub = ((SSub *)param);
  pSub->pSql->res.code = code;
  tsem_post(&pSub->sem);
}


static SSub* tscCreateSubscription(STscObj* pObj, const char* topic, const char* sql) {
  int code = TSDB_CODE_SUCCESS, line = __LINE__;
  SSqlObj* pSql = NULL;

  SSub* pSub = calloc(1, sizeof(SSub));
  if (pSub == NULL) {
    line = __LINE__;
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto fail;
  }
  pSub->signature = pSub;
  if (tsem_init(&pSub->sem, 0, 0) == -1) {
    line = __LINE__;
    code = TAOS_SYSTEM_ERROR(errno);
    goto fail;
  }

  tstrncpy(pSub->topic, topic, sizeof(pSub->topic));
  pSub->progress = taosArrayInit(32, sizeof(SSubscriptionProgress));
  if (pSub->progress == NULL) {
    line = __LINE__;
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto fail;
  }

  pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    line = __LINE__;
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto fail;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pObj;
  pSql->pSubscription = pSub;
  pSub->pSql = pSql;

  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  if (tsem_init(&pSql->rspSem, 0, 0) == -1) {
    line = __LINE__;
    code = TAOS_SYSTEM_ERROR(errno);
    goto fail;
  }

  pSql->param = pSub;
  pSql->maxRetry = TSDB_MAX_REPLICA;
  pSql->fp = asyncCallback;
  pSql->fetchFp = asyncCallback;
  pSql->sqlstr = strdup(sql);
  if (pSql->sqlstr == NULL) {
    line = __LINE__;
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto fail;
  }

  strtolower(pSql->sqlstr, pSql->sqlstr);
  pRes->qhandle = 0;
  pRes->numOfRows = 1;

  code = tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (code != TSDB_CODE_SUCCESS) {
    line = __LINE__;
    goto fail;
  }

  registerSqlObj(pSql);

  code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    tsem_wait(&pSub->sem);
    code = pSql->res.code;
  }

  if (code != TSDB_CODE_SUCCESS) {
    line = __LINE__;
    goto fail;
  }

  if (pSql->cmd.command != TSDB_SQL_SELECT && pSql->cmd.command != TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    line = __LINE__;
    code = TSDB_CODE_TSC_INVALID_SQL;
    goto fail;
  }

  return pSub;

fail:
  tscError("tscCreateSubscription failed at line %d, reason: %s", line, tstrerror(code));
  if (pSql != NULL) {
    if (pSql->self != 0) {
      taosReleaseRef(tscObjRef, pSql->self);
    } else {
    tscFreeSqlObj(pSql);
    }

    pSql = NULL;
  }

  if (pSub != NULL) {
    taosArrayDestroy(pSub->progress);
    tsem_destroy(&pSub->sem);
    free(pSub);
    pSub = NULL;
  }

  terrno = code;
  return NULL;
}


static void tscProcessSubscriptionTimer(void *handle, void *tmrId) {
  SSub *pSub = (SSub *)handle;
  if (pSub == NULL || pSub->pTimer != tmrId) return;

  TAOS_RES* res = taos_consume(pSub);
  if (res != NULL) {
    pSub->fp(pSub, res, pSub->param, 0);
  }

  taosTmrReset(tscProcessSubscriptionTimer, pSub->interval, pSub, tscTmr, &pSub->pTimer);
}


static SArray* getTableList( SSqlObj* pSql ) {
  const char* p = strstr( pSql->sqlstr, " from " );
  assert(p != NULL); // we are sure this is a 'select' statement
  char* sql = alloca(strlen(p) + 32);
  sprintf(sql, "select tbid(tbname)%s", p);
  
  SSqlObj* pNew = taos_query(pSql->pTscObj, sql);
  if (pNew == NULL) {
    tscError("failed to retrieve table id: cannot create new sql object.");
    return NULL;

  } else if (taos_errno(pNew) != TSDB_CODE_SUCCESS) {
    tscError("failed to retrieve table id: %s", tstrerror(taos_errno(pNew)));
    return NULL;
  }

  TAOS_ROW row;
  SArray* result = taosArrayInit( 128, sizeof(STidTags) );
  while ((row = taos_fetch_row(pNew))) {
    STidTags tags;
    memcpy(&tags, row[0], sizeof(tags));
    taosArrayPush(result, &tags);
  }

  taos_free_result(pNew);
  
  return result;
}

static int32_t compareTidTag(const void* p1, const void* p2) {
  const STidTags* t1 = (const STidTags*)p1;
  const STidTags* t2 = (const STidTags*)p2;
  
  if (t1->vgId != t2->vgId) {
    return (t1->vgId > t2->vgId) ? 1 : -1;
  }
  if (t1->tid != t2->tid) {
    return (t1->tid > t2->tid) ? 1 : -1;
  }
  return 0;
}


static int tscUpdateSubscription(STscObj* pObj, SSub* pSub) {
  SSqlObj* pSql = pSub->pSql;

  SSqlCmd* pCmd = &pSql->cmd;

  pSub->lastSyncTime = taosGetTimestampMs();

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
    STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
    SSubscriptionProgress target = {.uid = pTableMeta->id.uid, .key = 0};
    SSubscriptionProgress* p = taosArraySearch(pSub->progress, &target, tscCompareSubscriptionProgress, TD_EQ);
    if (p == NULL) {
      taosArrayClear(pSub->progress);
      taosArrayPush(pSub->progress, &target);
    }
    return 1;
  }

  SArray* tables = getTableList(pSql);
  if (tables == NULL) {
    return 0;
  }
  size_t numOfTables = taosArrayGetSize(tables);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SArray* progress = taosArrayInit(numOfTables, sizeof(SSubscriptionProgress));
  for( size_t i = 0; i < numOfTables; i++ ) {
    STidTags* tt = taosArrayGet( tables, i );
    SSubscriptionProgress p = { .uid = tt->uid };
    p.key = tscGetSubscriptionProgress(pSub, tt->uid, pQueryInfo->window.skey);
    taosArrayPush(progress, &p);
  }
  taosArraySort(progress, tscCompareSubscriptionProgress);

  taosArrayDestroy(pSub->progress);
  pSub->progress = progress;

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    taosArraySort( tables, compareTidTag );
    tscFreeVgroupTableInfo(pTableMetaInfo->pVgroupTables);
    tscBuildVgroupTableInfo(pSql, pTableMetaInfo, tables);
  }
  taosArrayDestroy(tables);

  TSDB_QUERY_SET_TYPE(tscGetQueryInfoDetail(pCmd, 0)->type, TSDB_QUERY_TYPE_MULTITABLE_QUERY);
  return 1;
}


static int tscLoadSubscriptionProgress(SSub* pSub) {
  char buf[TSDB_MAX_SQL_LEN];
  sprintf(buf, "%s/subscribe/%s", tsDataDir, pSub->topic);

  FILE* fp = fopen(buf, "r");
  if (fp == NULL) {
    tscDebug("subscription progress file does not exist: %s", pSub->topic);
    return 1;
  }

  if (fgets(buf, sizeof(buf), fp) == NULL) {
    tscDebug("invalid subscription progress file: %s", pSub->topic);
    fclose(fp);
    return 0;
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
    tscDebug("subscription sql statement mismatch: %s", pSub->topic);
    fclose(fp);
    return 0;
  }

  SArray* progress = pSub->progress;
  taosArrayClear(progress);
  while( 1 ) {
    if (fgets(buf, sizeof(buf), fp) == NULL) {
      fclose(fp);
      return 0;
    }
    SSubscriptionProgress p;
    sscanf(buf, "%" SCNd64 ":%" SCNd64, &p.uid, &p.key);
    taosArrayPush(progress, &p);
  }

  fclose(fp);

  taosArraySort(progress, tscCompareSubscriptionProgress);
  tscDebug("subscription progress loaded, %" PRIzu " tables: %s", taosArrayGetSize(progress), pSub->topic);
  return 1;
}

void tscSaveSubscriptionProgress(void* sub) {
  SSub* pSub = (SSub*)sub;

  char path[256];
  sprintf(path, "%s/subscribe", tsDataDir);
  if (taosMkDir(path, 0777) != 0) {
    tscError("failed to create subscribe dir: %s", path);
  }

  sprintf(path, "%s/subscribe/%s", tsDataDir, pSub->topic);
  FILE* fp = fopen(path, "w+");
  if (fp == NULL) {
    tscError("failed to create progress file for subscription: %s", pSub->topic);
    return;
  }

  fputs(pSub->pSql->sqlstr, fp);
  fprintf(fp, "\n");
  for(size_t i = 0; i < taosArrayGetSize(pSub->progress); i++) {
    SSubscriptionProgress* p = taosArrayGet(pSub->progress, i);
    fprintf(fp, "%" PRId64 ":%" PRId64 "\n", p->uid, p->key);
  }

  fclose(fp);
}

TAOS_SUB *taos_subscribe(TAOS *taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval) {
  STscObj* pObj = (STscObj*)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("connection disconnected");
    return NULL;
  }

  SSub* pSub = tscCreateSubscription(pObj, topic, sql);
  if (pSub == NULL) {
    return NULL;
  }
  pSub->taos = taos;

  if (restart) {
    tscDebug("restart subscription: %s", topic);
  } else {
    tscLoadSubscriptionProgress(pSub);
  }

  if (pSub->pSql->cmd.command == TSDB_SQL_SELECT) {
    if (!tscUpdateSubscription(pObj, pSub)) {
      taos_unsubscribe(pSub, 1);
      return NULL;
    }
  }

  pSub->interval = interval;
  if (fp != NULL) {
    tscDebug("asynchronize subscription, create new timer: %s", topic);
    pSub->fp = fp;
    pSub->param = param;
    taosTmrReset(tscProcessSubscriptionTimer, interval, pSub, tscTmr, &pSub->pTimer);
  }

  return pSub;
}

SSqlObj* recreateSqlObj(SSub* pSub) {
  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    return NULL;
  }

  pSql->signature = pSql;
  pSql->pTscObj = pSub->taos;

  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  if (tsem_init(&pSql->rspSem, 0, 0) == -1) {
    tscFreeSqlObj(pSql);
    return NULL;
  }

  pSql->param = pSub;
  pSql->maxRetry = TSDB_MAX_REPLICA;
  pSql->fp = asyncCallback;
  pSql->fetchFp = asyncCallback;
  pSql->sqlstr = strdup(pSub->pSql->sqlstr);
  if (pSql->sqlstr == NULL) {
    tscFreeSqlObj(pSql);
    return NULL;
  }

  pRes->qhandle = 0;
  pRes->numOfRows = 1;

  int code = tscAllocPayload(pCmd, TSDB_DEFAULT_PAYLOAD_SIZE);
  if (code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pSql);
    return NULL;
  }

  registerSqlObj(pSql);

  code = tsParseSql(pSql, true);
  if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    tsem_wait(&pSub->sem);
    code = pSql->res.code;
  }

  if (code != TSDB_CODE_SUCCESS) {
    taosReleaseRef(tscObjRef, pSql->self);
    return NULL;
  }

  if (pSql->cmd.command != TSDB_SQL_SELECT) {
    taosReleaseRef(tscObjRef, pSql->self);
    return NULL;
  }

  return pSql;
}

TAOS_RES *taos_consume(TAOS_SUB *tsub) {
  SSub *pSub = (SSub *)tsub;
  if (pSub == NULL) return NULL;

  if (pSub->pSql->cmd.command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    SSqlObj* pSql = recreateSqlObj(pSub);
    if (pSql == NULL) {
      return NULL;
    }
    if (pSub->pSql->self != 0) {
      taosReleaseRef(tscObjRef, pSub->pSql->self);
    } else {
      tscFreeSqlObj(pSub->pSql);
    }
    pSub->pSql = pSql;
    pSql->pSubscription = pSub;
  }

  tscSaveSubscriptionProgress(pSub);

  SSqlObj *pSql = pSub->pSql;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  if (taosArrayGetSize(pSub->progress) > 0) { // fix crash in single tabel subscription
    pQueryInfo->window.skey = ((SSubscriptionProgress*)taosArrayGet(pSub->progress, 0))->key;
  }

  if (pSub->pTimer == NULL) {
    int64_t duration = taosGetTimestampMs() - pSub->lastConsumeTime;
    if (duration < (int64_t)(pSub->interval)) {
      tscDebug("subscription consume too frequently, blocking...");
      taosMsleep(pSub->interval - (int32_t)duration);
    }
  }

  size_t size = taosArrayGetSize(pSub->progress) * sizeof(STableIdInfo);
  size += sizeof(SQueryTableMsg) + 4096;
  int code = tscAllocPayload(&pSql->cmd, (int)size);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("failed to alloc payload");
    return NULL;
  }

  for (int retry = 0; retry < 3; retry++) {
    tscRemoveFromSqlList(pSql);

    if (taosGetTimestampMs() - pSub->lastSyncTime > 10 * 60 * 1000) {
      tscDebug("begin table synchronization");
      if (!tscUpdateSubscription(pSub->taos, pSub)) return NULL;
      tscDebug("table synchronization completed");
    }

    uint32_t type = pQueryInfo->type;
    tscFreeSqlResult(pSql);
    pRes->numOfRows = 1;
    pRes->qhandle = 0;
    pSql->cmd.command = TSDB_SQL_SELECT;
    pQueryInfo->type = type;

    pTableMetaInfo->vgroupIndex = 0;

    pSql->fp = asyncCallback;
    pSql->fetchFp = asyncCallback;
    pSql->param = pSub;
    tscDoQuery(pSql);
    tsem_wait(&pSub->sem);

    if (pRes->code != TSDB_CODE_SUCCESS) {
      continue;
    }
    // meter was removed, make sync time zero, so that next retry will
    // do synchronization first
    pSub->lastSyncTime = 0;
    break;
  }

  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscError("failed to query data: %s", tstrerror(pRes->code));
    tscRemoveFromSqlList(pSql);
    return NULL;
  }

  pSub->lastConsumeTime = taosGetTimestampMs();
  return pSql;
}

void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress) {
  SSub *pSub = (SSub *)tsub;
  if (pSub == NULL || pSub->signature != pSub) return;

  if (pSub->pTimer != NULL) {
    taosTmrStop(pSub->pTimer);
  }

  if (keepProgress) {
    if (pSub->progress != NULL) {
      tscSaveSubscriptionProgress(pSub);
    }
  } else {
    char path[256];
    sprintf(path, "%s/subscribe/%s", tsDataDir, pSub->topic);
    if (remove(path) != 0) {
      tscError("failed to remove progress file, topic = %s, error = %s", pSub->topic, strerror(errno));
    }
  }

  if (pSub->pSql != NULL) {
    if (pSub->pSql->self != 0) {
      taosReleaseRef(tscObjRef, pSub->pSql->self);
    } else {
      tscFreeSqlObj(pSub->pSql);
    }
  }

  taosArrayDestroy(pSub->progress);
  tsem_destroy(&pSub->sem);
  memset(pSub, 0, sizeof(*pSub));
  free(pSub);
}
