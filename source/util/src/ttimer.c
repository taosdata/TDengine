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
#include "ttimer.h"
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "tsched.h"

#define tmrFatal(...)                                                     \
  {                                                                       \
    if (tmrDebugFlag & DEBUG_FATAL) {                                     \
      taosPrintLog("TMR FATAL ", DEBUG_FATAL, tmrDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define tmrError(...)                                                     \
  {                                                                       \
    if (tmrDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("TMR ERROR ", DEBUG_ERROR, tmrDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define tmrWarn(...)                                                    \
  {                                                                     \
    if (tmrDebugFlag & DEBUG_WARN) {                                    \
      taosPrintLog("TMR WARN ", DEBUG_WARN, tmrDebugFlag, __VA_ARGS__); \
    }                                                                   \
  }
#define tmrInfo(...)                                               \
  {                                                                \
    if (tmrDebugFlag & DEBUG_INFO) {                               \
      taosPrintLog("TMR ", DEBUG_INFO, tmrDebugFlag, __VA_ARGS__); \
    }                                                              \
  }
#define tmrDebug(...)                                               \
  {                                                                 \
    if (tmrDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("TMR ", DEBUG_DEBUG, tmrDebugFlag, __VA_ARGS__); \
    }                                                               \
  }
#define tmrTrace(...)                                               \
  {                                                                 \
    if (tmrDebugFlag & DEBUG_TRACE) {                               \
      taosPrintLog("TMR ", DEBUG_TRACE, tmrDebugFlag, __VA_ARGS__); \
    }                                                               \
  }

#define TIMER_STATE_WAITING  0
#define TIMER_STATE_EXPIRED  1
#define TIMER_STATE_STOPPED  2
#define TIMER_STATE_CANCELED 3

typedef union _tmr_ctrl_t {
  char label[16];
  struct {
    // pad to ensure 'next' is the end of this union
    char               padding[16 - sizeof(union _tmr_ctrl_t*)];
    union _tmr_ctrl_t* next;
  };
} tmr_ctrl_t;

typedef struct tmr_obj_t {
  uintptr_t         id;
  tmr_ctrl_t*       ctrl;
  struct tmr_obj_t* mnext;
  struct tmr_obj_t* prev;
  struct tmr_obj_t* next;
  uint16_t          slot;
  uint8_t           wheel;
  uint8_t           state;
  uint8_t           refCount;
  uint8_t           reserved1;
  uint16_t          reserved2;
  union {
    int64_t expireAt;
    int64_t executedBy;
  };
  TAOS_TMR_CALLBACK fp;
  void*             param;
} tmr_obj_t;

typedef struct timer_list_t {
  int64_t    lockedBy;
  tmr_obj_t* timers;
} timer_list_t;

typedef struct timer_map_t {
  uint32_t      size;
  uint32_t      count;
  timer_list_t* slots;
} timer_map_t;

typedef struct time_wheel_t {
  TdThreadMutex mutex;
  int64_t       nextScanAt;
  uint32_t      resolution;
  uint16_t      size;
  uint16_t      index;
  tmr_obj_t**   slots;
} time_wheel_t;

static int32_t tsMaxTmrCtrl = TSDB_MAX_VNODES_PER_DB + 100;

static int32_t       tmrModuleInit = 0;
static TdThreadMutex tmrCtrlMutex;
static tmr_ctrl_t*   tmrCtrls;
static tmr_ctrl_t*   unusedTmrCtrl = NULL;
static void*         tmrQhandle;
static int32_t       numOfTmrCtrl = 0;

int32_t          taosTmrThreads = 1;
static uintptr_t nextTimerId = 0;

static time_wheel_t wheels[] = {
    {.resolution = MSECONDS_PER_TICK, .size = 4096},
    {.resolution = 1000, .size = 1024},
    {.resolution = 60000, .size = 1024},
};
static timer_map_t timerMap;

static uintptr_t getNextTimerId() {
  uintptr_t id;
  do {
    id = (uintptr_t)atomic_add_fetch_ptr((void**)&nextTimerId, 1);
  } while (id == 0);
  return id;
}

static void timerAddRef(tmr_obj_t* timer) { atomic_add_fetch_8(&timer->refCount, 1); }

static void timerDecRef(tmr_obj_t* timer) {
  if (atomic_sub_fetch_8(&timer->refCount, 1) == 0) {
    taosMemoryFree(timer);
  }
}

static void lockTimerList(timer_list_t* list) {
  int64_t tid = taosGetSelfPthreadId();
  int32_t i = 0;
  while (atomic_val_compare_exchange_64(&(list->lockedBy), 0, tid) != 0) {
    if (++i % 1000 == 0) {
      sched_yield();
    }
  }
}

static void unlockTimerList(timer_list_t* list) {
  int64_t tid = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(&(list->lockedBy), tid, 0) != tid) {
    ASSERTS(false, "%" PRId64 " trying to unlock a timer list not locked by current thread.", tid);
  }
}

static void addTimer(tmr_obj_t* timer) {
  timerAddRef(timer);
  timer->wheel = tListLen(wheels);

  uint32_t      idx = (uint32_t)(timer->id % timerMap.size);
  timer_list_t* list = timerMap.slots + idx;

  lockTimerList(list);
  timer->mnext = list->timers;
  list->timers = timer;
  unlockTimerList(list);
}

static tmr_obj_t* findTimer(uintptr_t id) {
  tmr_obj_t* timer = NULL;
  if (id > 0) {
    uint32_t      idx = (uint32_t)(id % timerMap.size);
    timer_list_t* list = timerMap.slots + idx;
    lockTimerList(list);
    for (timer = list->timers; timer != NULL; timer = timer->mnext) {
      if (timer->id == id) {
        timerAddRef(timer);
        break;
      }
    }
    unlockTimerList(list);
  }
  return timer;
}

static void removeTimer(uintptr_t id) {
  tmr_obj_t*    prev = NULL;
  uint32_t      idx = (uint32_t)(id % timerMap.size);
  timer_list_t* list = timerMap.slots + idx;
  lockTimerList(list);
  for (tmr_obj_t* p = list->timers; p != NULL; p = p->mnext) {
    if (p->id == id) {
      if (prev == NULL) {
        list->timers = p->mnext;
      } else {
        prev->mnext = p->mnext;
      }
      timerDecRef(p);
      break;
    }
    prev = p;
  }
  unlockTimerList(list);
}

static void addToWheel(tmr_obj_t* timer, uint32_t delay) {
  timerAddRef(timer);
  // select a wheel for the timer, we are not an accurate timer,
  // but the inaccuracy should not be too large.
  timer->wheel = tListLen(wheels) - 1;
  for (uint8_t i = 0; i < tListLen(wheels); i++) {
    time_wheel_t* wheel = wheels + i;
    if (delay < wheel->resolution * wheel->size) {
      timer->wheel = i;
      break;
    }
  }

  time_wheel_t* wheel = wheels + timer->wheel;
  timer->prev = NULL;
  timer->expireAt = taosGetMonotonicMs() + delay;

  taosThreadMutexLock(&wheel->mutex);

  uint32_t idx = 0;
  if (timer->expireAt > wheel->nextScanAt) {
    // adjust delay according to next scan time of this wheel
    // so that the timer is not fired earlier than desired.
    delay = (uint32_t)(timer->expireAt - wheel->nextScanAt);
    idx = (delay + wheel->resolution - 1) / wheel->resolution;
  }

  timer->slot = (uint16_t)((wheel->index + idx + 1) % wheel->size);
  tmr_obj_t* p = wheel->slots[timer->slot];
  wheel->slots[timer->slot] = timer;
  timer->next = p;
  if (p != NULL) {
    p->prev = timer;
  }

  taosThreadMutexUnlock(&wheel->mutex);
}

static bool removeFromWheel(tmr_obj_t* timer) {
  uint8_t wheelIdx = timer->wheel;
  if (wheelIdx >= tListLen(wheels)) {
    return false;
  }
  time_wheel_t* wheel = wheels + wheelIdx;

  bool removed = false;
  taosThreadMutexLock(&wheel->mutex);
  // other thread may modify timer->wheel, check again.
  if (timer->wheel < tListLen(wheels)) {
    if (timer->prev != NULL) {
      timer->prev->next = timer->next;
    }
    if (timer->next != NULL) {
      timer->next->prev = timer->prev;
    }
    if (timer == wheel->slots[timer->slot]) {
      wheel->slots[timer->slot] = timer->next;
    }
    timer->wheel = tListLen(wheels);
    timer->next = NULL;
    timer->prev = NULL;
    timerDecRef(timer);
    removed = true;
  }
  taosThreadMutexUnlock(&wheel->mutex);

  return removed;
}

static void processExpiredTimer(void* handle, void* arg) {
  tmr_obj_t* timer = (tmr_obj_t*)handle;
  timer->executedBy = taosGetSelfPthreadId();
  uint8_t state = atomic_val_compare_exchange_8(&timer->state, TIMER_STATE_WAITING, TIMER_STATE_EXPIRED);
  if (state == TIMER_STATE_WAITING) {
    const char* fmt = "%s timer[id=%" PRIuPTR ", fp=%p, param=%p] execution start.";
    tmrDebug(fmt, timer->ctrl->label, timer->id, timer->fp, timer->param);

    (*timer->fp)(timer->param, (tmr_h)timer->id);
    atomic_store_8(&timer->state, TIMER_STATE_STOPPED);

    fmt = "%s timer[id=%" PRIuPTR ", fp=%p, param=%p] execution end.";
    tmrDebug(fmt, timer->ctrl->label, timer->id, timer->fp, timer->param);
  }
  removeTimer(timer->id);
  timerDecRef(timer);
}

static void addToExpired(tmr_obj_t* head) {
  const char* fmt = "%s adding expired timer[id=%" PRIuPTR ", fp=%p, param=%p] to queue.";

  while (head != NULL) {
    uintptr_t  id = head->id;
    tmr_obj_t* next = head->next;
    tmrDebug(fmt, head->ctrl->label, id, head->fp, head->param);

    SSchedMsg schedMsg;
    schedMsg.fp = NULL;
    schedMsg.tfp = processExpiredTimer;
    schedMsg.msg = NULL;
    schedMsg.ahandle = head;
    schedMsg.thandle = NULL;
    taosScheduleTask(tmrQhandle, &schedMsg);

    tmrDebug("timer[id=%" PRIuPTR "] has been added to queue.", id);
    head = next;
  }
}

static uintptr_t doStartTimer(tmr_obj_t* timer, TAOS_TMR_CALLBACK fp, int32_t mseconds, void* param, tmr_ctrl_t* ctrl) {
  uintptr_t id = getNextTimerId();
  timer->id = id;
  timer->state = TIMER_STATE_WAITING;
  timer->fp = fp;
  timer->param = param;
  timer->ctrl = ctrl;
  addTimer(timer);

  const char* fmt = "%s timer[id=%" PRIuPTR ", fp=%p, param=%p] started";
  tmrDebug(fmt, ctrl->label, timer->id, timer->fp, timer->param);

  if (mseconds == 0) {
    timer->wheel = tListLen(wheels);
    timerAddRef(timer);
    addToExpired(timer);
  } else {
    addToWheel(timer, mseconds);
  }

  // note: use `timer->id` here is unsafe as `timer` may already be freed
  return id;
}

tmr_h taosTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* param, void* handle) {
  tmr_ctrl_t* ctrl = (tmr_ctrl_t*)handle;
  if (ctrl == NULL || ctrl->label[0] == 0) {
    return NULL;
  }

  tmr_obj_t* timer = (tmr_obj_t*)taosMemoryCalloc(1, sizeof(tmr_obj_t));
  if (timer == NULL) {
    tmrError("%s failed to allocated memory for new timer object.", ctrl->label);
    return NULL;
  }

  return (tmr_h)doStartTimer(timer, fp, mseconds, param, ctrl);
}

static void taosTimerLoopFunc(int32_t signo) {
  int64_t now = taosGetMonotonicMs();

  for (int32_t i = 0; i < tListLen(wheels); i++) {
    // `expried` is a temporary expire list.
    // expired timers are first add to this list, then move
    // to expired queue as a batch to improve performance.
    // note this list is used as a stack in this function.
    tmr_obj_t* expired = NULL;

    time_wheel_t* wheel = wheels + i;
    while (now >= wheel->nextScanAt) {
      taosThreadMutexLock(&wheel->mutex);
      wheel->index = (wheel->index + 1) % wheel->size;
      tmr_obj_t* timer = wheel->slots[wheel->index];
      while (timer != NULL) {
        tmr_obj_t* next = timer->next;
        if (now < timer->expireAt) {
          timer = next;
          continue;
        }

        // remove from the wheel
        if (timer->prev == NULL) {
          wheel->slots[wheel->index] = next;
          if (next != NULL) {
            next->prev = NULL;
          }
        } else {
          timer->prev->next = next;
          if (next != NULL) {
            next->prev = timer->prev;
          }
        }
        timer->wheel = tListLen(wheels);

        // add to temporary expire list
        timer->next = expired;
        timer->prev = NULL;
        if (expired != NULL) {
          expired->prev = timer;
        }
        expired = timer;

        timer = next;
      }
      wheel->nextScanAt += wheel->resolution;
      taosThreadMutexUnlock(&wheel->mutex);
    }

    addToExpired(expired);
  }
}

static bool doStopTimer(tmr_obj_t* timer, uint8_t state) {
  if (state == TIMER_STATE_WAITING) {
    bool reusable = false;
    if (removeFromWheel(timer)) {
      removeTimer(timer->id);
      // only safe to reuse the timer when timer is removed from the wheel.
      // we cannot guarantee the thread safety of the timr in all other cases.
      reusable = true;
    }
    const char* fmt = "%s timer[id=%" PRIuPTR ", fp=%p, param=%p] is cancelled.";
    tmrDebug(fmt, timer->ctrl->label, timer->id, timer->fp, timer->param);
    return reusable;
  }

  if (state != TIMER_STATE_EXPIRED) {
    // timer already stopped or cancelled, has nothing to do in this case
    return false;
  }

  if (timer->executedBy == taosGetSelfPthreadId()) {
    // taosTmrReset is called in the timer callback, should do nothing in this
    // case to avoid dead lock. note taosTmrReset must be the last statement
    // of the callback funtion, will be a bug otherwise.
    return false;
  }

  // timer callback is executing in another thread, we SHOULD wait it to stop,
  // BUT this may result in dead lock if current thread are holding a lock which
  // the timer callback need to acquire. so, we HAVE TO return directly.
  const char* fmt = "%s timer[id=%" PRIuPTR ", fp=%p, param=%p] is executing and cannot be stopped.";
  tmrDebug(fmt, timer->ctrl->label, timer->id, timer->fp, timer->param);
  return false;
}

bool taosTmrStop(tmr_h timerId) {
  uintptr_t id = (uintptr_t)timerId;

  tmr_obj_t* timer = findTimer(id);
  if (timer == NULL) {
    tmrDebug("timer[id=%" PRIuPTR "] does not exist", id);
    return false;
  }

  uint8_t state = atomic_val_compare_exchange_8(&timer->state, TIMER_STATE_WAITING, TIMER_STATE_CANCELED);
  doStopTimer(timer, state);
  timerDecRef(timer);

  return state == TIMER_STATE_WAITING;
}

bool taosTmrStopA(tmr_h* timerId) {
  bool ret = taosTmrStop(*timerId);
  *timerId = NULL;
  return ret;
}

bool taosTmrReset(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* param, void* handle, tmr_h* pTmrId) {
  tmr_ctrl_t* ctrl = (tmr_ctrl_t*)handle;
  if (ctrl == NULL || ctrl->label[0] == 0) {
    return false;
  }

  uintptr_t  id = (uintptr_t)*pTmrId;
  bool       stopped = false;
  tmr_obj_t* timer = findTimer(id);
  if (timer == NULL) {
    tmrDebug("%s timer[id=%" PRIuPTR "] does not exist", ctrl->label, id);
  } else {
    uint8_t state = atomic_val_compare_exchange_8(&timer->state, TIMER_STATE_WAITING, TIMER_STATE_CANCELED);
    if (!doStopTimer(timer, state)) {
      timerDecRef(timer);
      timer = NULL;
    }
    stopped = state == TIMER_STATE_WAITING;
  }

  if (timer == NULL) {
    *pTmrId = taosTmrStart(fp, mseconds, param, handle);
    return stopped;
  }

  tmrDebug("%s timer[id=%" PRIuPTR "] is reused", ctrl->label, timer->id);

  // wait until there's no other reference to this timer,
  // so that we can reuse this timer safely.
  for (int32_t i = 1; atomic_load_8(&timer->refCount) > 1; ++i) {
    if (i % 1000 == 0) {
      sched_yield();
    }
  }

  ASSERTS(timer->refCount == 1, "timer refCount=%d not expected 1", timer->refCount);
  memset(timer, 0, sizeof(*timer));
  *pTmrId = (tmr_h)doStartTimer(timer, fp, mseconds, param, ctrl);

  return stopped;
}

static int32_t taosTmrModuleInit(void) {
  tmrCtrls = taosMemoryMalloc(sizeof(tmr_ctrl_t) * tsMaxTmrCtrl);
  if (tmrCtrls == NULL) {
    tmrError("failed to allocate memory for timer controllers.");
    return -1;
  }

  memset(&timerMap, 0, sizeof(timerMap));

  for (uint32_t i = 0; i < tsMaxTmrCtrl - 1; ++i) {
    tmr_ctrl_t* ctrl = tmrCtrls + i;
    ctrl->next = ctrl + 1;
  }
  (tmrCtrls + tsMaxTmrCtrl - 1)->next = NULL;
  unusedTmrCtrl = tmrCtrls;

  taosThreadMutexInit(&tmrCtrlMutex, NULL);

  int64_t now = taosGetMonotonicMs();
  for (int32_t i = 0; i < tListLen(wheels); i++) {
    time_wheel_t* wheel = wheels + i;
    if (taosThreadMutexInit(&wheel->mutex, NULL) != 0) {
      tmrError("failed to create the mutex for wheel, reason:%s", strerror(errno));
      return -1;
    }
    wheel->nextScanAt = now + wheel->resolution;
    wheel->index = 0;
    wheel->slots = (tmr_obj_t**)taosMemoryCalloc(wheel->size, sizeof(tmr_obj_t*));
    if (wheel->slots == NULL) {
      tmrError("failed to allocate wheel slots");
      return -1;
    }
    timerMap.size += wheel->size;
  }

  timerMap.count = 0;
  timerMap.slots = (timer_list_t*)taosMemoryCalloc(timerMap.size, sizeof(timer_list_t));
  if (timerMap.slots == NULL) {
    tmrError("failed to allocate hash map");
    return -1;
  }

  tmrQhandle = taosInitScheduler(10000, taosTmrThreads, "tmr", NULL);
  taosInitTimer(taosTimerLoopFunc, MSECONDS_PER_TICK);

  tmrDebug("timer module is initialized, number of threads: %d", taosTmrThreads);

  return 2;
}

static int32_t taosTmrInitModule(void) {
  if (atomic_load_32(&tmrModuleInit) == 2) {
    return 0;
  }

  if (atomic_load_32(&tmrModuleInit) < 0) {
    return -1;
  }
  
  while (true) {
    if (0 == atomic_val_compare_exchange_32(&tmrModuleInit, 0, 1)) {
      atomic_store_32(&tmrModuleInit, taosTmrModuleInit());
    } else if (atomic_load_32(&tmrModuleInit) < 0) {
      return -1;
    } else if (atomic_load_32(&tmrModuleInit) == 2) {
      return 0;
    } else {
      taosMsleep(1);
    }
  }

  return -1;
}

void* taosTmrInit(int32_t maxNumOfTmrs, int32_t resolution, int32_t longest, const char* label) {
  const char* ret = taosMonotonicInit();
  tmrDebug("ttimer monotonic clock source:%s", ret);

  if (taosTmrInitModule() < 0) {
    return NULL;
  }

  taosThreadMutexLock(&tmrCtrlMutex);
  tmr_ctrl_t* ctrl = unusedTmrCtrl;
  if (ctrl != NULL) {
    unusedTmrCtrl = ctrl->next;
    numOfTmrCtrl++;
  }
  taosThreadMutexUnlock(&tmrCtrlMutex);

  if (ctrl == NULL) {
    tmrError("%s too many timer controllers, failed to create timer controller.", label);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tstrncpy(ctrl->label, label, sizeof(ctrl->label));
  
  tmrDebug("%s timer controller is initialized, number of timer controllers: %d.", label, numOfTmrCtrl);
  return ctrl;
}

void taosTmrCleanUp(void* handle) {
  tmr_ctrl_t* ctrl = (tmr_ctrl_t*)handle;
  if (ctrl == NULL || ctrl->label[0] == 0) {
    return;
  }

  tmrDebug("%s timer controller is cleaned up.", ctrl->label);
  ctrl->label[0] = 0;

  taosThreadMutexLock(&tmrCtrlMutex);
  ctrl->next = unusedTmrCtrl;
  numOfTmrCtrl--;
  unusedTmrCtrl = ctrl;
  taosThreadMutexUnlock(&tmrCtrlMutex);

  tmrDebug("time controller's tmr ctrl size:  %d", numOfTmrCtrl);
  if (numOfTmrCtrl <= 0) {
    taosUninitTimer();

    taosCleanUpScheduler(tmrQhandle);
    taosMemoryFreeClear(tmrQhandle);

    for (int32_t i = 0; i < tListLen(wheels); i++) {
      time_wheel_t* wheel = wheels + i;
      taosThreadMutexDestroy(&wheel->mutex);
      taosMemoryFree(wheel->slots);
    }

    taosThreadMutexDestroy(&tmrCtrlMutex);

    for (size_t i = 0; i < timerMap.size; i++) {
      timer_list_t* list = timerMap.slots + i;
      tmr_obj_t*    t = list->timers;
      while (t != NULL) {
        tmr_obj_t* next = t->mnext;
        taosMemoryFree(t);
        t = next;
      }
    }
    taosMemoryFree(timerMap.slots);
    taosMemoryFree(tmrCtrls);

    tmrCtrls = NULL;
    unusedTmrCtrl = NULL;
    atomic_store_32(&tmrModuleInit, 0);
  }
}
