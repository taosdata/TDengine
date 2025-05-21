

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
#include "tlruplancache.h"
#include "os.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "types.h"

typedef struct {
  char user[TSDB_USER_LEN];
  char query[QUERY_STRING_MAX_LEN];
} PlanCacheData;

// struct SPlanCacheEntry;
typedef struct SPlanCacheEntry {
  PlanCacheData           data;
  struct SPlanCacheEntry* prev;
  struct SPlanCacheEntry* next;
} SPlanCacheEntry;

typedef struct {
  TdThreadMutex    lock;
  SPlanCacheEntry* head;
  SPlanCacheEntry* tail;
  int              size;
} PlanCacheList;

typedef struct {
  int64_t          cache_hit;
  int64_t          created_at;
  int64_t          last_accessed_at;
  void*            plan;
  SPlanCacheEntry* entry;
} PlanCacheValue;

typedef struct {
  int32_t   quota;
  int64_t   last_updated_at;
  SHashObj* hash;
} UserCacheValue;

int32_t       totalPlanCacheSize = 0;
SHashObj*     planCacheObj = NULL;
PlanCacheList planCacheList[PLAN_CACHE_PRIORITY_NUM] = {0};

TdThreadMutex cacheLock;

int32_t clientSendAuditLog(void* pTrans, SEpSet* epset, char* operation, char* detail) {
  char*        msg = NULL;
  int32_t      msgLen = 0;
  int32_t      reqType = TDMT_MND_AUDIT_LOG;
  SAuditLogReq req = {operation, detail};

  int32_t len = tSerializeAuditLogReq(NULL, 0, &req);
  void*   pBuf = rpcMallocCont(len);

  tSerializeAuditLogReq(pBuf, len, &req);

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pTrans, epset, &rpcMsg, &rpcRsp);

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

static void planCacheListInit(PlanCacheList* list) {
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
  taosThreadMutexInit(&list->lock, NULL);
}

static int32_t newPlanCacheEntry(SPlanCacheEntry** entry, const char* key, const char* query) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SPlanCacheEntry* newEntry = (SPlanCacheEntry*)taosMemoryCalloc(1, sizeof(SPlanCacheEntry));
  CACHE_CHECK_NULL_GOTO(newEntry, terrno);
  tstrncpy(newEntry->data.user, key, sizeof(newEntry->data.user));
  tstrncpy(newEntry->data.query, query, sizeof(newEntry->data.query));
  newEntry->data.query[sizeof(newEntry->data.query) - 1] = '\0';
  newEntry->prev = NULL;
  newEntry->next = NULL;
  *entry = newEntry;
end:
  PRINT_LOG_END(code, lino);
  return code;
}

static void planCacheListDestroy(PlanCacheList* list) {
  SPlanCacheEntry* entry = list->head;
  while (entry) {
    SPlanCacheEntry* next = entry->next;
    taosMemoryFree(entry);
    entry = next;
  }
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
}

static void planCacheListPushToHead(PlanCacheList* list, SPlanCacheEntry* entry) {
  taosThreadMutexLock(&list->lock);
  if (list->head == NULL) {
    list->head = entry;
    list->tail = entry;
  } else {
    entry->next = list->head;
    list->head->prev = entry;
    list->head = entry;
  }
  list->size++;
  taosThreadMutexUnlock(&list->lock);
}

static SPlanCacheEntry* planCacheListPopFromTail(PlanCacheList* list) {
  taosThreadMutexLock(&list->lock);
  if (list->tail == NULL) {
    taosThreadMutexUnlock(&list->lock);
    return NULL;
  }
  SPlanCacheEntry* entry = list->tail;
  if (entry->prev) {
    list->tail = entry->prev;
    list->tail->next = NULL;
  } else {
    list->head = NULL;
    list->tail = NULL;
  }
  list->size--;
  taosThreadMutexUnlock(&list->lock);

  return entry;
}

static void planCacheListMoveToHead(PlanCacheList* list, SPlanCacheEntry* entry) {
  taosThreadMutexLock(&list->lock);

  if (list->head == entry) {
    taosThreadMutexUnlock(&list->lock);
    return;
  }
  if (entry->prev) {
    entry->prev->next = entry->next;
  } else {
    list->head = entry->next;
  }
  if (entry->next) {
    entry->next->prev = entry->prev;
  } else {
    list->tail = entry->prev;
  }

  entry->next = list->head;
  if (list->head) {
    list->head->prev = entry;
  }
  list->head = entry;
  list->head->prev = NULL;
  if (list->tail == NULL) {
    list->tail = entry;
  }
  taosThreadMutexUnlock(&list->lock);
}

static void erasePlanCache() {
  // from low to high
  for(int i = PLAN_CACHE_PRIORITY_LOW; i >= PLAN_CACHE_PRIORITY_HIGH; i--) {
    PlanCacheList *list = &planCacheList[i];
    while (list->size > 0 && totalPlanCacheSize > MAX_PLAN_CACHE_SIZE_LOW_LEVEL) {
      SPlanCacheEntry* entry = planCacheListPopFromTail(list);
      if (entry == NULL) {
        break;
      }
      void* data = taosHashGet(planCacheObj, entry->data.user, strlen(entry->data.user) + 1);
      if (data != NULL) {
        UserCacheValue* uCache = (UserCacheValue*)data;
        int32_t ret = taosHashRemove(uCache->hash, entry->data.query, strlen(entry->data.query) + 1);
        if (ret != 0) {
          uError("Failed to remove plan cache entry, ret: %d", ret);
        }
      }
      taosMemoryFree(entry);

      totalPlanCacheSize--;
    }
    if (totalPlanCacheSize <= MAX_PLAN_CACHE_SIZE_LOW_LEVEL) {
      break;
    }
  }
}

static char* generateSummary(){
  cJSON* users = cJSON_CreateArray();
  void* pIter = taosHashIterate(planCacheObj, NULL);
  while (pIter != NULL) {
    char*            user = taosHashGetKey(pIter, NULL);
    SHashObj* pHashObj = ((UserCacheValue*)pIter)->hash;

    cJSON* json = cJSON_CreateObject();
    cJSON* userName = cJSON_CreateString(user);
    cJSON_AddItemToObject(json, "user_name", userName);
    cJSON* num = cJSON_CreateNumber(taosHashGetSize(pHashObj));
    cJSON_AddItemToObject(json, "plan_num", num);

    cJSON_AddItemToArray(users, json);
  }
  char* string = cJSON_PrintUnformatted(users);
  cJSON_Delete(users);
  return string;
}

int32_t putToPlanCache(char* user, UserPriority priority, int32_t max, char* query, void* value, void* pTrans,
                       SEpSet* epset) {
  int32_t code = 0;
  int32_t lino = 0;
  CACHE_CHECK_CONDITION_GOTO(priority < PLAN_CACHE_PRIORITY_HIGH || priority > PLAN_CACHE_PRIORITY_LOW,
                             TSDB_CODE_INVALID_PARA);

  taosThreadMutexLock(&cacheLock);
  if (priority == PLAN_CACHE_PRIORITY_LOW && totalPlanCacheSize >= MAX_PLAN_CACHE_SIZE_MEDIUM_LEVEL) {
    char* summary = generateSummary();
    clientSendAuditLog(pTrans, epset, "plan_cache_70_percent", summary);
    taosMemoryFree(summary);
    taosThreadMutexUnlock(&cacheLock);
    goto end;
  }
  if (planCacheObj == NULL) {
    planCacheObj = taosHashInit(4, MurmurHash3_32, false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(planCacheObj, terrno);
  }
  taosThreadMutexUnlock(&cacheLock);

  void* data = taosHashGet(planCacheObj, user, strlen(user) + 1);
  if (data == NULL) {
    data = taosHashInit(4, MurmurHash3_32, false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(data, terrno);
    UserCacheValue uCache = {.quota = max, .last_updated_at = taosGetTimestampMs(), .hash = data};
    CACHE_CHECK_RET_GOTO(taosHashPut(planCacheObj, user, strlen(user) + 1, &uCache, sizeof(uCache)));
  } else {
    UserCacheValue* uCache = (UserCacheValue*)data;
    uCache->last_updated_at = taosGetTimestampMs();
    data = uCache->hash;
  }

  int32_t size = taosHashGetSize(data);
  if (size >= max) {
    goto end;
  }

  SPlanCacheEntry* pValue = taosHashGet(data, query, strlen(query) + 1);
  if (pValue != NULL) {
    goto end;
  }

  SPlanCacheEntry* entry = NULL;
  CACHE_CHECK_RET_GOTO(newPlanCacheEntry(&entry, user, query));
  PlanCacheValue v = {.plan = value,
                      .entry = entry,
                      .cache_hit = 0,
                      .created_at = taosGetTimestampMs(),
                      .last_accessed_at = taosGetTimestampMs()};

  CACHE_CHECK_RET_GOTO(taosHashPut(data, query, strlen(query) + 1, &v, sizeof(v)));
  PlanCacheList* list = &planCacheList[priority];
  planCacheListPushToHead(list, entry);

  taosThreadMutexLock(&cacheLock);
  totalPlanCacheSize++;
  if (totalPlanCacheSize >= MAX_PLAN_CACHE_SIZE_HIGH_LEVEL) {
    char* summary = generateSummary();
    clientSendAuditLog(pTrans, epset, "plan_cache_90_percent", summary);
    taosMemoryFree(summary);
    erasePlanCache();
    summary = generateSummary();
    clientSendAuditLog(pTrans, epset, "plan_cache_50_percent", summary);
    taosMemoryFree(summary);
  }
  taosThreadMutexUnlock(&cacheLock);

end:
  PRINT_LOG_END(code, lino);
  return code;
}

int32_t getFromPlanCache(char* user, UserPriority priority, char* query, void** value) {
  int32_t code = 0;
  int32_t lino = 0;
  CACHE_CHECK_CONDITION_GOTO(priority < PLAN_CACHE_PRIORITY_HIGH || priority > PLAN_CACHE_PRIORITY_LOW,
                             TSDB_CODE_INVALID_PARA);

  taosThreadMutexLock(&cacheLock);
  if (planCacheObj == NULL) {
    planCacheObj = taosHashInit(4, MurmurHash3_32, false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(planCacheObj, terrno);
  }
  taosThreadMutexUnlock(&cacheLock);

  void* data = taosHashGet(planCacheObj, user, strlen(user) + 1);
  if (data == NULL) {
    *value = NULL;
    goto end;
  }
  UserCacheValue* uCache = (UserCacheValue*)data;
  PlanCacheValue* pValue = taosHashGet(uCache->hash, query, strlen(query) + 1);
  if (pValue == NULL) {
    *value = NULL;
    goto end;
  }
  *value = pValue->plan;
  SPlanCacheEntry* entry = pValue->entry;
  pValue->cache_hit++;
  pValue->last_accessed_at = taosGetTimestampMs();
  planCacheListMoveToHead(&planCacheList[priority], entry);

end:
  PRINT_LOG_END(code, lino);
  return code;
}

void formatTimestamp(int64_t timestamp, char* buf, int32_t size) {
  time_t    tt = (time_t)(timestamp / 1000);
  struct tm ptm = {0};
  if (taosLocalTime(&tt, &ptm, NULL) == NULL) {
    return;
  }
  strftime(buf, size, "%Y-%m-%d %H:%M:%S", &ptm);
}

int32_t clientRetrieveCachedPlans(SArray** ppRes) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pRes = taosArrayInit(totalPlanCacheSize, sizeof(SCachedPlan));
  CACHE_CHECK_NULL_GOTO(pRes, terrno);
  void* pIter = taosHashIterate(planCacheObj, NULL);
  while (pIter != NULL) {
    SHashObj* pHashObj = ((UserCacheValue*)pIter)->hash;
    char*     user = taosHashGetKey(pIter, NULL);
    void*     pIterInner = taosHashIterate(pHashObj, NULL);
    while (pIterInner != NULL) {
      PlanCacheValue* pValue = (PlanCacheValue*)pIterInner;
      SCachedPlan*    value = taosArrayReserve(pRes, 1);
      if (value == NULL) {
        code = terrno;
        taosHashCancelIterate(pHashObj, pIterInner);
        taosHashCancelIterate(planCacheObj, pIter);
        goto end;
      }
      char* query = taosHashGetKey(pIterInner, NULL);

      value->cache_hit = pValue->cache_hit;
      formatTimestamp(pValue->created_at, varDataVal(value->created_at), sizeof(value->created_at));
      formatTimestamp(pValue->last_accessed_at, varDataVal(value->last_accessed_at), sizeof(value->last_accessed_at));
      varDataLen(value->created_at) = strlen(varDataVal(value->created_at));
      varDataLen(value->last_accessed_at) = strlen(varDataVal(value->last_accessed_at));

      tstrncpy(varDataVal(value->user), user, strlen(user) + 1);
      varDataLen(value->user) = strlen(user);
      tstrncpy(varDataVal(value->sql), query, strlen(query) + 1);
      varDataLen(value->sql) = strlen(query);

      pIterInner = taosHashIterate(pHashObj, pIterInner);
    }
    pIter = taosHashIterate(planCacheObj, pIter);
  }
  *ppRes = pRes;
  pRes = NULL;

end:
  taosArrayDestroy(pRes);
  PRINT_LOG_END(code, lino);
  return code;
}

int32_t clientRetrieveUserCachedPlans(SArray** ppRes) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pRes = taosArrayInit(taosHashGetSize(planCacheObj), sizeof(SUserCachedPlan));
  CACHE_CHECK_NULL_GOTO(pRes, terrno);
  void* pIter = taosHashIterate(planCacheObj, NULL);
  while (pIter != NULL) {
    char*            user = taosHashGetKey(pIter, NULL);
    SUserCachedPlan* value = taosArrayReserve(pRes, 1);
    if (value == NULL) {
      code = terrno;
      taosHashCancelIterate(planCacheObj, pIter);
      goto end;
    }
    UserCacheValue* uCache = (UserCacheValue*)pIter;
    SHashObj* pHashObj = uCache->hash;

    value->plans = taosHashGetSize(pHashObj);
    value->quota = uCache->quota;
    formatTimestamp(uCache->last_updated_at, varDataVal(value->last_updated_at), sizeof(value->last_updated_at));
    varDataLen(value->last_updated_at) = strlen(varDataVal(value->last_updated_at));

    tstrncpy(varDataVal(value->user), user, strlen(user) + 1);
    varDataLen(value->user) = strlen(user);
    pIter = taosHashIterate(planCacheObj, pIter);
  }
  *ppRes = pRes;
  pRes = NULL;

end:
  taosArrayDestroy(pRes);
  PRINT_LOG_END(code, lino);
  return code;
}
