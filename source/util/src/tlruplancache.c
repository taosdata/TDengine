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

#define MAX_PLAN_CACHE_SIZE 10
#define MAX_PLAN_CACHE_SIZE_LOW_LEVEL ((int32_t)(0.5 * MAX_PLAN_CACHE_SIZE))
#define MAX_PLAN_CACHE_SIZE_MEDIUM_LEVEL ((int32_t)(0.7 * MAX_PLAN_CACHE_SIZE))
#define MAX_PLAN_CACHE_SIZE_HIGH_LEVEL ((int32_t)(0.9 * MAX_PLAN_CACHE_SIZE))

typedef enum { 
  PLAN_CACHE_PRIORITY_HIGH = 0, 
  PLAN_CACHE_PRIORITY_MEDIUM = 1,
  PLAN_CACHE_PRIORITY_LOW = 2,
  PLAN_CACHE_PRIORITY_NUM = 3,
 } UserPriority;

typedef struct {
  char user[128];
  char query[128];
} PlanCacheData;

typedef struct SPlanCacheEntry{
  PlanCacheData    data;
  SPlanCacheEntry *prev;
  SPlanCacheEntry *next;
} SPlanCacheEntry;

typedef struct PlanCacheList {
  TdThreadMutex    lock;
  SPlanCacheEntry *head;
  SPlanCacheEntry *tail;
  int              size;
}

typedef struct {
  void*               value;
  SPlanCacheEntry*    entry;
} PlanCacheValue;

int32_t  totalPlanCacheSize = 0;
SHashObj *planCacheObj = NULL;
PlanCacheList planCacheList[PLAN_CACHE_PRIORITY_NUM] = {0};

TdThreadMutex       cacheLock;

static void planCacheListInit(PlanCacheList *list) {
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
  list->Lock = taosThreadMutexInit();
}

static int32_t newPlanCacheEntry(SPlanCacheEntry **entry, const char *key, const char *query) {
  int32_t code = 0;
  int32_t line = 0;
  SPlanCacheEntry *newEntry = (SPlanCacheEntry *)taosMemoryCalloc(1, sizeof(SPlanCacheEntry));
  CACHE_CHECK_NULL_GOTO(newEntry, terrno);
  tstrncpy(newEntry->data.key, key, sizeof(newEntry->data.key));
  tstrncpy(newEntry->data.query, query, sizeof(newEntry->data.query));
  newEntry->data.query[sizeof(newEntry->data.query) - 1] = '\0';
  newEntry->prev = NULL;
  newEntry->next = NULL;
  *entry = newEntry;
end:
  PRINT_LOG_END(code, line);
  return code;
}

static void planCacheListDestroy(PlanCacheList *list) {
  SPlanCacheEntry *entry = list->head;
  while (entry) {
    SPlanCacheEntry *next = entry->next;
    taosMemoryFree(entry);
    entry = next;
  }
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
}

static void planCacheListPushToHead(PlanCacheList *list, SPlanCacheEntry *entry) {
  taosThreadMutexLock(&list->Lock);
  if (list->head == NULL) {
    list->head = entry;
    list->tail = entry;
  } else {
    entry->next = list->head;
    list->head->prev = entry;
    list->head = entry;
  }
  list->size++;
  taosThreadMutexUnLock(&list->Lock);
}

static SPlanCacheEntry* planCacheListPopFromTail(PlanCacheList *list) {
  taosThreadMutexLock(&list->Lock);
  if (list->tail == NULL) {
    taosThreadMutexUnLock(&list->Lock);
    return NULL;
  }
  SPlanCacheEntry *entry = list->tail;
  if (entry->prev) {
    list->tail = entry->prev;
    list->tail->next = NULL;
  } else {
    list->head = NULL;
    list->tail = NULL;
  }
  list->size--;
  taosThreadMutexUnLock(&list->Lock);

  return entry;
}

static void planCacheListMoveToHead(PlanCacheList *list, SPlanCacheEntry *entry) {
  taosThreadMutexLock(&list->Lock);

  if (list->head == entry) {
    taosThreadMutexUnLock(&list->Lock);
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
  taosThreadMutexUnLock(&list->Lock);
}

static int32_t erasePlanCache(){
  // from low to high
  for(int i = PLAN_CACHE_PRIORITY_LOW; i >= PLAN_CACHE_PRIORITY_HIGH; i++) {
    PlanCacheList *list = &planCacheList[i];
    while (list->size > 0 && totalPlanCacheSize > MAX_PLAN_CACHE_SIZE_LOW_LEVEL) {
      SPlanCacheEntry* entry = planCacheListPopFromTail(list);
      if(entry == NULL) {
        break;
      }
      void* data = taosHashGet(planCacheObj, entry->data.user, strlen(entry->data.user));
      if (data != NULL) {
        int32_t ret = taosHashRemove(data, entry->data.query, strlen(entry->data.query));
        if (ret != 0) {
          tscError("Failed to remove plan cache entry, ret: %d", ret);
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

int32_t putToPlanCache(char* user, UserPriority priority, int32_t max, char* query, void* value) {
  int32_t code = 0;
  int32_t line = 0;
  CACHE_CHECK_CONDITION_GOTO(priority >= PLAN_CACHE_PRIORITY_HIGH && priority < PLAN_CACHE_PRIORITY_LOW, TSDB_CODE_INVALID_PARA);

  taosThreadMutexLock(&cacheLock);
  if (priority == PLAN_CACHE_PRIORITY_LOW && totalPlanCacheSize >= MAX_PLAN_CACHE_SIZE_LOW_LEVEL)) {
    taosThreadMutexUnLock(&cacheLock);
    goto end;
  }
  if (planCacheObj == NULL) {
    planCacheObj = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(planCacheObj, terrno);
  }
  taosThreadMutexUnLock(&cacheLock);
 
  void* data = taosHashGet(planCacheObj, user, strlen(user));
  if (data == NULL) {
    data = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(data, terrno);
    CACHE_CHECK_RET_GOTO(taosHashPut(planObj, user, strlen(user), &data, POINTER_BYTES));
  } else {
    data = *(void**)data;
  }
  int32_t size = taosHashGetSize(data);
  if (size >= max) {
    goto end;
  }

  SPlanCacheEntry* pValue = taosHashGet(data, query, strlen(query));
  if (pValue != NULL) {
    goto end;
  }

  SPlanCacheEntry *entry = NULL;
  CACHE_CHECK_RET_GOTO(newPlanCacheEntry(&entry, user, query));
  PlanCacheValue v = {.value = value, .entry = entry};

  CACHE_CHECK_RET_GOTO(taosHashPut(data, query, strlen(query), &v, sizeof(v)));
  PlanCacheList *list = &planCacheList[priority];
  planCacheListPushToHead(list, entry);

  taosThreadMutexLock(&cacheLock);
  totalPlanCacheSize ++;
  if (totalPlanCacheSize >= MAX_PLAN_CACHE_SIZE_HIGH_LEVEL) {
    erasePlanCache();
  }
  taosThreadMutexUnLock(&cacheLock);
  
end:
  PRINT_LOG_END(code, line);
  return code;
}

int32_t getFromPlanCache(char* user, UserPriority priority, char* query, void** value) {
  int32_t code = 0;
  int32_t line = 0;
  CACHE_CHECK_CONDITION_GOTO(priority >= PLAN_CACHE_PRIORITY_HIGH && priority < PLAN_CACHE_PRIORITY_LOW, TSDB_CODE_INVALID_PARA);

  taosThreadMutexLock(&cacheLock);
  if (planCacheObj == NULL) {
    planCacheObj = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    CACHE_CHECK_NULL_GOTO(planCacheObj, terrno);
  }
  taosThreadMutexUnLock(&cacheLock);
 
  void* data = taosHashGet(planCacheObj, user, strlen(user));
  if (data == NULL) {
    *value = NULL;
    goto end;
  } 
  PlanCacheValue* pValue = taosHashGet(data, query, strlen(query);
  if (pValue == NULL) {
    *value = NULL;
    goto end;
  }
  *value = pValue->value;
  SPlanCacheEntry* entry = = pValue->entry;
  
  planCacheListMoveToHead(&planCacheList[priority], entry);
  
end:
  PRINT_LOG_END(code, line);
  return code;
}

