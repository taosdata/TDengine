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

#include "vnd.h"
#include "vnodeHash.h"

typedef struct SVAsync    SVAsync;
typedef struct SVATask    SVATask;
typedef struct SVAChannel SVAChannel;

#define VNODE_ASYNC_DEFAULT_WORKERS 4
#define VNODE_ASYNC_MAX_WORKERS     256

// priority

#define EVA_PRIORITY_MAX (EVA_PRIORITY_LOW + 1)

// worker
typedef enum {
  EVA_WORKER_STATE_UINIT = 0,
  EVA_WORKER_STATE_ACTIVE,
  EVA_WORKER_STATE_IDLE,
  EVA_WORKER_STATE_STOP,
} EVWorkerState;

typedef struct {
  SVAsync      *async;
  int32_t       workerId;
  EVWorkerState state;
  TdThread      thread;
  SVATask      *runningTask;
} SVWorker;

// task
typedef enum {
  EVA_TASK_STATE_WAITTING = 0,
  EVA_TASK_STATE_RUNNING,
} EVATaskState;

struct SVATask {
  int64_t     taskId;
  EVAPriority priority;
  int32_t     priorScore;
  SVAChannel *channel;
  int32_t (*execute)(void *);
  void (*cancel)(void *);
  void        *arg;
  EVATaskState state;

  // wait
  int32_t      numWait;
  TdThreadCond waitCond;

  // queue
  struct SVATask *prev;
  struct SVATask *next;
};

typedef struct {
  void (*cancel)(void *);
  void *arg;
} SVATaskCancelInfo;

#define VATASK_PIORITY(task_) ((task_)->priority - ((task_)->priorScore / 4))

// async channel
typedef enum {
  EVA_CHANNEL_STATE_OPEN = 0,
  EVA_CHANNEL_STATE_CLOSE,
} EVAChannelState;

struct SVAChannel {
  int64_t         channelId;
  EVAChannelState state;
  SVATask         queue[EVA_PRIORITY_MAX];
  SVATask        *scheduled;

  SVAChannel *prev;
  SVAChannel *next;
};

// async handle
struct SVAsync {
  const char *label;

  TdThreadMutex mutex;
  TdThreadCond  hasTask;
  bool          stop;

  // worker
  int32_t  numWorkers;
  int32_t  numLaunchWorkers;
  int32_t  numIdleWorkers;
  SVWorker workers[VNODE_ASYNC_MAX_WORKERS];

  // channel
  int64_t      nextChannelId;
  int32_t      numChannels;
  SVAChannel   chList;
  SVHashTable *channelTable;

  // task
  int64_t      nextTaskId;
  int32_t      numTasks;
  SVATask      queue[EVA_PRIORITY_MAX];
  SVHashTable *taskTable;
};

struct {
  const char *label;
  SVAsync    *async;
} GVnodeAsyncs[] = {
    [0] = {NULL, NULL},
    [1] = {"vnode-commit", NULL},
    [2] = {"vnode-merge", NULL},
    [3] = {"vnode-compact", NULL},
    [4] = {"vnode-retention", NULL},
};

#define MIN_ASYNC_ID 1
#define MAX_ASYNC_ID (sizeof(GVnodeAsyncs) / sizeof(GVnodeAsyncs[0]) - 1)

static void vnodeAsyncTaskDone(SVAsync *async, SVATask *task) {
  int32_t ret;

  if (task->channel != NULL && task->channel->scheduled == task) {
    task->channel->scheduled = NULL;
    if (task->channel->state == EVA_CHANNEL_STATE_CLOSE) {
      taosMemoryFree(task->channel);
    } else {
      for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
        SVATask *nextTask = task->channel->queue[i].next;
        if (nextTask != &task->channel->queue[i]) {
          if (task->channel->scheduled == NULL) {
            task->channel->scheduled = nextTask;
            nextTask->next->prev = nextTask->prev;
            nextTask->prev->next = nextTask->next;
          } else {
            nextTask->priorScore++;
            int32_t newPriority = VATASK_PIORITY(nextTask);
            if (newPriority != i) {
              // remove from current priority queue
              nextTask->prev->next = nextTask->next;
              nextTask->next->prev = nextTask->prev;
              // add to new priority queue
              nextTask->next = &task->channel->queue[newPriority];
              nextTask->prev = task->channel->queue[newPriority].prev;
              nextTask->next->prev = nextTask;
              nextTask->prev->next = nextTask;
            }
          }
        }
      }

      if (task->channel->scheduled != NULL) {
        int32_t priority = VATASK_PIORITY(task->channel->scheduled);
        task->channel->scheduled->next = &async->queue[priority];
        task->channel->scheduled->prev = async->queue[priority].prev;
        task->channel->scheduled->next->prev = task->channel->scheduled;
        task->channel->scheduled->prev->next = task->channel->scheduled;
      }
    }
  }

  ret = vHashDrop(async->taskTable, task);
  TAOS_UNUSED(ret);
  async->numTasks--;

  if (task->numWait == 0) {
    (void)taosThreadCondDestroy(&task->waitCond);
    taosMemoryFree(task);
  } else if (task->numWait == 1) {
    (void)taosThreadCondSignal(&task->waitCond);
  } else {
    (void)taosThreadCondBroadcast(&task->waitCond);
  }
}

static void vnodeAsyncCancelAllTasks(SVAsync *async, SArray *cancelArray) {
  while (async->queue[0].next != &async->queue[0] || async->queue[1].next != &async->queue[1] ||
         async->queue[2].next != &async->queue[2]) {
    for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
      while (async->queue[i].next != &async->queue[i]) {
        SVATask *task = async->queue[i].next;
        task->prev->next = task->next;
        task->next->prev = task->prev;
        if (task->cancel) {
          if (taosArrayPush(cancelArray, &(SVATaskCancelInfo){
                                             .cancel = task->cancel,
                                             .arg = task->arg,
                                         }) == NULL) {
            vError("failed to push cancel task into array");
          };
        }
        vnodeAsyncTaskDone(async, task);
      }
    }
  }
}

static void *vnodeAsyncLoop(void *arg) {
  SVWorker *worker = (SVWorker *)arg;
  SVAsync  *async = worker->async;
  SArray   *cancelArray = taosArrayInit(0, sizeof(SVATaskCancelInfo));
  if (cancelArray == NULL) {
    return NULL;
  }

  setThreadName(async->label);

  for (;;) {
    (void)taosThreadMutexLock(&async->mutex);

    // finish last running task
    if (worker->runningTask != NULL) {
      vnodeAsyncTaskDone(async, worker->runningTask);
      worker->runningTask = NULL;
    }

    for (;;) {
      if (async->stop || worker->workerId >= async->numWorkers) {
        if (async->stop) {  // cancel all tasks
          vnodeAsyncCancelAllTasks(async, cancelArray);
        }
        worker->state = EVA_WORKER_STATE_STOP;
        async->numLaunchWorkers--;
        (void)taosThreadMutexUnlock(&async->mutex);
        goto _exit;
      }

      for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
        SVATask *task = async->queue[i].next;
        if (task != &async->queue[i]) {
          if (worker->runningTask == NULL) {
            worker->runningTask = task;
            task->prev->next = task->next;
            task->next->prev = task->prev;
          } else {  // promote priority
            task->priorScore++;
            int32_t priority = VATASK_PIORITY(task);
            if (priority != i) {
              // remove from current priority queue
              task->prev->next = task->next;
              task->next->prev = task->prev;
              // add to new priority queue
              task->next = &async->queue[priority];
              task->prev = async->queue[priority].prev;
              task->next->prev = task;
              task->prev->next = task;
            }
          }
        }
      }

      if (worker->runningTask == NULL) {
        worker->state = EVA_WORKER_STATE_IDLE;
        async->numIdleWorkers++;
        (void)taosThreadCondWait(&async->hasTask, &async->mutex);
        async->numIdleWorkers--;
        worker->state = EVA_WORKER_STATE_ACTIVE;
      } else {
        worker->runningTask->state = EVA_TASK_STATE_RUNNING;
        break;
      }
    }

    (void)taosThreadMutexUnlock(&async->mutex);

    // do run the task
    int32_t code = worker->runningTask->execute(worker->runningTask->arg);
    TAOS_UNUSED(code);
  }

_exit:
  for (int32_t i = 0; i < taosArrayGetSize(cancelArray); i++) {
    SVATaskCancelInfo *cancel = (SVATaskCancelInfo *)taosArrayGet(cancelArray, i);
    cancel->cancel(cancel->arg);
  }
  taosArrayDestroy(cancelArray);
  return NULL;
}

static uint32_t vnodeAsyncTaskHash(const void *obj) {
  SVATask *task = (SVATask *)obj;
  return MurmurHash3_32((const char *)(&task->taskId), sizeof(task->taskId));
}

static int32_t vnodeAsyncTaskCompare(const void *obj1, const void *obj2) {
  SVATask *task1 = (SVATask *)obj1;
  SVATask *task2 = (SVATask *)obj2;
  if (task1->taskId < task2->taskId) {
    return -1;
  } else if (task1->taskId > task2->taskId) {
    return 1;
  }
  return 0;
}

static uint32_t vnodeAsyncChannelHash(const void *obj) {
  SVAChannel *channel = (SVAChannel *)obj;
  return MurmurHash3_32((const char *)(&channel->channelId), sizeof(channel->channelId));
}

static int32_t vnodeAsyncChannelCompare(const void *obj1, const void *obj2) {
  SVAChannel *channel1 = (SVAChannel *)obj1;
  SVAChannel *channel2 = (SVAChannel *)obj2;
  if (channel1->channelId < channel2->channelId) {
    return -1;
  } else if (channel1->channelId > channel2->channelId) {
    return 1;
  }
  return 0;
}

static int32_t vnodeAsyncInit(SVAsync **async, const char *label) {
  int32_t ret;

  if (async == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (label == NULL) {
    label = "anonymous";
  }

  (*async) = (SVAsync *)taosMemoryCalloc(1, sizeof(SVAsync) + strlen(label) + 1);
  if ((*async) == NULL) {
    return terrno;
  }

  memcpy((char *)((*async) + 1), label, strlen(label) + 1);
  (*async)->label = (const char *)((*async) + 1);

  (void)taosThreadMutexInit(&(*async)->mutex, NULL);
  (void)taosThreadCondInit(&(*async)->hasTask, NULL);
  (*async)->stop = false;

  // worker
  (*async)->numWorkers = VNODE_ASYNC_DEFAULT_WORKERS;
  (*async)->numLaunchWorkers = 0;
  (*async)->numIdleWorkers = 0;
  for (int32_t i = 0; i < VNODE_ASYNC_MAX_WORKERS; i++) {
    (*async)->workers[i].async = (*async);
    (*async)->workers[i].workerId = i;
    (*async)->workers[i].state = EVA_WORKER_STATE_UINIT;
    (*async)->workers[i].runningTask = NULL;
  }

  // channel
  (*async)->nextChannelId = 0;
  (*async)->numChannels = 0;
  (*async)->chList.prev = &(*async)->chList;
  (*async)->chList.next = &(*async)->chList;
  ret = vHashInit(&(*async)->channelTable, vnodeAsyncChannelHash, vnodeAsyncChannelCompare);
  if (ret != 0) {
    (void)taosThreadMutexDestroy(&(*async)->mutex);
    (void)taosThreadCondDestroy(&(*async)->hasTask);
    taosMemoryFree(*async);
    return ret;
  }

  // task
  (*async)->nextTaskId = 0;
  (*async)->numTasks = 0;
  for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
    (*async)->queue[i].next = &(*async)->queue[i];
    (*async)->queue[i].prev = &(*async)->queue[i];
  }
  ret = vHashInit(&(*async)->taskTable, vnodeAsyncTaskHash, vnodeAsyncTaskCompare);
  if (ret != 0) {
    vHashDestroy(&(*async)->channelTable);
    (void)taosThreadMutexDestroy(&(*async)->mutex);
    (void)taosThreadCondDestroy(&(*async)->hasTask);
    taosMemoryFree(*async);
    return ret;
  }

  return 0;
}

static int32_t vnodeAsyncDestroy(SVAsync **async) {
  if ((*async) == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // set stop and broadcast
  (void)taosThreadMutexLock(&(*async)->mutex);
  (*async)->stop = true;
  (void)taosThreadCondBroadcast(&(*async)->hasTask);
  (void)taosThreadMutexUnlock(&(*async)->mutex);

  // join all workers
  for (int32_t i = 0; i < VNODE_ASYNC_MAX_WORKERS; i++) {
    (void)taosThreadMutexLock(&(*async)->mutex);
    EVWorkerState state = (*async)->workers[i].state;
    (void)taosThreadMutexUnlock(&(*async)->mutex);

    if (state == EVA_WORKER_STATE_UINIT) {
      continue;
    }

    (void)taosThreadJoin((*async)->workers[i].thread, NULL);
    (*async)->workers[i].state = EVA_WORKER_STATE_UINIT;
  }

  // close all channels
  for (SVAChannel *channel = (*async)->chList.next; channel != &(*async)->chList; channel = (*async)->chList.next) {
    channel->next->prev = channel->prev;
    channel->prev->next = channel->next;

    int32_t ret = vHashDrop((*async)->channelTable, channel);
    TAOS_UNUSED(ret);
    (*async)->numChannels--;
    taosMemoryFree(channel);
  }

  (void)taosThreadMutexDestroy(&(*async)->mutex);
  (void)taosThreadCondDestroy(&(*async)->hasTask);

  vHashDestroy(&(*async)->channelTable);
  vHashDestroy(&(*async)->taskTable);
  taosMemoryFree(*async);
  *async = NULL;

  return 0;
}

static void vnodeAsyncLaunchWorker(SVAsync *async) {
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
  for (int32_t i = 0; i < async->numWorkers; i++) {
    if (async->workers[i].state == EVA_WORKER_STATE_ACTIVE) {
      continue;
    } else if (async->workers[i].state == EVA_WORKER_STATE_STOP) {
      int32_t ret = taosThreadJoin(async->workers[i].thread, NULL);
      async->workers[i].state = EVA_WORKER_STATE_UINIT;
    }

    int32_t ret = taosThreadCreate(&async->workers[i].thread, &thAttr, vnodeAsyncLoop, &async->workers[i]);
    if (ret) {
      vError("failed to create worker thread since %s", tstrerror(ret));
    } else {
      async->workers[i].state = EVA_WORKER_STATE_ACTIVE;
      async->numLaunchWorkers++;
    }
    break;
  }
#ifdef TD_ASTRA
  (void)taosThreadAttrDestroy(&thAttr);
#endif
}

int32_t vnodeAsyncOpen() {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t numOfThreads[] = {
      0,                        //
      tsNumOfCommitThreads,     // vnode-commit
      tsNumOfCommitThreads,     // vnode-merge
      tsNumOfCompactThreads,    // vnode-compact
      tsNumOfRetentionThreads,  // vnode-retention
  };

  for (int32_t i = 1; i < sizeof(GVnodeAsyncs) / sizeof(GVnodeAsyncs[0]); i++) {
    code = vnodeAsyncInit(&GVnodeAsyncs[i].async, GVnodeAsyncs[i].label);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = vnodeAsyncSetWorkers(i, numOfThreads[i]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  return code;
}

void vnodeAsyncClose() {
  for (int32_t i = 1; i < sizeof(GVnodeAsyncs) / sizeof(GVnodeAsyncs[0]); i++) {
    int32_t ret = vnodeAsyncDestroy(&GVnodeAsyncs[i].async);
  }
}

int32_t vnodeAsync(int64_t async, EVAPriority priority, int32_t (*execute)(void *), void (*complete)(void *), void *arg,
                   SVATaskID *taskID) {
  SVAChannelID channelID = {
      .async = async,
      .id = 0,
  };
  return vnodeAsyncC(&channelID, priority, execute, complete, arg, taskID);
}

int32_t vnodeAsyncC(SVAChannelID *channelID, EVAPriority priority, int32_t (*execute)(void *), void (*cancel)(void *),
                    void *arg, SVATaskID *taskID) {
  if (channelID == NULL || channelID->async < MIN_ASYNC_ID || channelID->async > MAX_ASYNC_ID || execute == NULL ||
      channelID->id < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t  ret;
  int64_t  id;
  SVAsync *async = GVnodeAsyncs[channelID->async].async;

  // create task object
  SVATask *task = (SVATask *)taosMemoryCalloc(1, sizeof(SVATask));
  if (task == NULL) {
    return terrno;
  }

  task->priority = priority;
  task->priorScore = 0;
  task->execute = execute;
  task->cancel = cancel;
  task->arg = arg;
  task->state = EVA_TASK_STATE_WAITTING;
  task->numWait = 0;
  (void)taosThreadCondInit(&task->waitCond, NULL);

  // schedule task
  (void)taosThreadMutexLock(&async->mutex);

  if (channelID->id == 0) {
    task->channel = NULL;
  } else {
    SVAChannel channel = {
        .channelId = channelID->id,
    };
    ret = vHashGet(async->channelTable, &channel, (void **)&task->channel);
    TAOS_UNUSED(ret);
    if (task->channel == NULL) {
      (void)taosThreadMutexUnlock(&async->mutex);
      (void)taosThreadCondDestroy(&task->waitCond);
      taosMemoryFree(task);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  task->taskId = id = ++async->nextTaskId;

  // add task to hash table
  ret = vHashPut(async->taskTable, task);
  if (ret != 0) {
    (void)taosThreadMutexUnlock(&async->mutex);
    (void)taosThreadCondDestroy(&task->waitCond);
    taosMemoryFree(task);
    return ret;
  }

  async->numTasks++;

  // add task to queue
  if (task->channel == NULL || task->channel->scheduled == NULL) {
    // add task to async->queue
    if (task->channel) {
      task->channel->scheduled = task;
    }

    task->next = &async->queue[priority];
    task->prev = async->queue[priority].prev;
    task->next->prev = task;
    task->prev->next = task;

    // signal worker or launch new worker
    if (async->numIdleWorkers > 0) {
      (void)taosThreadCondSignal(&(async->hasTask));
    } else if (async->numLaunchWorkers < async->numWorkers) {
      vnodeAsyncLaunchWorker(async);
    }
  } else if (task->channel->scheduled->state == EVA_TASK_STATE_RUNNING ||
             priority >= VATASK_PIORITY(task->channel->scheduled)) {
    // add task to task->channel->queue
    task->next = &task->channel->queue[priority];
    task->prev = task->channel->queue[priority].prev;
    task->next->prev = task;
    task->prev->next = task;
  } else {
    // remove task->channel->scheduled from queue
    task->channel->scheduled->prev->next = task->channel->scheduled->next;
    task->channel->scheduled->next->prev = task->channel->scheduled->prev;

    // promote priority and add task->channel->scheduled to task->channel->queue
    task->channel->scheduled->priorScore++;
    int32_t newPriority = VATASK_PIORITY(task->channel->scheduled);
    task->channel->scheduled->next = &task->channel->queue[newPriority];
    task->channel->scheduled->prev = task->channel->queue[newPriority].prev;
    task->channel->scheduled->next->prev = task->channel->scheduled;
    task->channel->scheduled->prev->next = task->channel->scheduled;

    // add task to queue
    task->channel->scheduled = task;
    task->next = &async->queue[priority];
    task->prev = async->queue[priority].prev;
    task->next->prev = task;
    task->prev->next = task;
  }

  (void)taosThreadMutexUnlock(&async->mutex);

  if (taskID != NULL) {
    taskID->async = channelID->async;
    taskID->id = id;
  }

  return 0;
}

void vnodeAWait(SVATaskID *taskID) {
  if (taskID == NULL || taskID->async < MIN_ASYNC_ID || taskID->async > MAX_ASYNC_ID || taskID->id <= 0) {
    return;
  }

  SVAsync *async = GVnodeAsyncs[taskID->async].async;
  SVATask *task = NULL;
  SVATask  task2 = {
       .taskId = taskID->id,
  };

  (void)taosThreadMutexLock(&async->mutex);

  int32_t ret = vHashGet(async->taskTable, &task2, (void **)&task);
  if (task) {
    task->numWait++;
    (void)taosThreadCondWait(&task->waitCond, &async->mutex);
    task->numWait--;

    if (task->numWait == 0) {
      (void)taosThreadCondDestroy(&task->waitCond);
      taosMemoryFree(task);
    }
  }

  (void)taosThreadMutexUnlock(&async->mutex);
}

int32_t vnodeACancel(SVATaskID *taskID) {
  if (taskID == NULL || taskID->async < MIN_ASYNC_ID || taskID->async > MAX_ASYNC_ID || taskID->id <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t  ret = 0;
  SVAsync *async = GVnodeAsyncs[taskID->async].async;
  SVATask *task = NULL;
  SVATask  task2 = {
       .taskId = taskID->id,
  };
  void (*cancel)(void *) = NULL;
  void *arg = NULL;

  (void)taosThreadMutexLock(&async->mutex);

  ret = vHashGet(async->taskTable, &task2, (void **)&task);
  if (task) {
    if (task->state == EVA_TASK_STATE_WAITTING) {
      cancel = task->cancel;
      arg = task->arg;
      task->next->prev = task->prev;
      task->prev->next = task->next;
      vnodeAsyncTaskDone(async, task);
    } else {
      ret = TSDB_CODE_FAILED;
    }
  }

  (void)taosThreadMutexUnlock(&async->mutex);

  if (cancel) {
    cancel(arg);
  }

  return ret;
}

int32_t vnodeAsyncSetWorkers(int64_t asyncID, int32_t numWorkers) {
  if (asyncID < MIN_ASYNC_ID || asyncID > MAX_ASYNC_ID || numWorkers <= 0 || numWorkers > VNODE_ASYNC_MAX_WORKERS) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t  ret;
  SVAsync *async = GVnodeAsyncs[asyncID].async;
  (void)taosThreadMutexLock(&async->mutex);
  async->numWorkers = numWorkers;
  if (async->numIdleWorkers > 0) {
    (void)taosThreadCondBroadcast(&async->hasTask);
  }
  (void)taosThreadMutexUnlock(&async->mutex);

  return 0;
}

int32_t vnodeAChannelInit(int64_t asyncID, SVAChannelID *channelID) {
  if (channelID == NULL || asyncID < MIN_ASYNC_ID || asyncID > MAX_ASYNC_ID) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVAsync *async = GVnodeAsyncs[asyncID].async;

  // create channel object
  SVAChannel *channel = (SVAChannel *)taosMemoryMalloc(sizeof(SVAChannel));
  if (channel == NULL) {
    return terrno;
  }
  channel->state = EVA_CHANNEL_STATE_OPEN;
  for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
    channel->queue[i].next = &channel->queue[i];
    channel->queue[i].prev = &channel->queue[i];
  }
  channel->scheduled = NULL;

  // register channel
  (void)taosThreadMutexLock(&async->mutex);

  channel->channelId = channelID->id = ++async->nextChannelId;

  // add to hash table
  int32_t ret = vHashPut(async->channelTable, channel);
  if (ret != 0) {
    (void)taosThreadMutexUnlock(&async->mutex);
    taosMemoryFree(channel);
    return ret;
  }

  // add to list
  channel->next = &async->chList;
  channel->prev = async->chList.prev;
  channel->next->prev = channel;
  channel->prev->next = channel;

  async->numChannels++;

  (void)taosThreadMutexUnlock(&async->mutex);

  channelID->async = asyncID;
  return 0;
}

int32_t vnodeAChannelDestroy(SVAChannelID *channelID, bool waitRunning) {
  if (channelID == NULL || channelID->async < MIN_ASYNC_ID || channelID->async > MAX_ASYNC_ID || channelID->id <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVAsync    *async = GVnodeAsyncs[channelID->async].async;
  SVAChannel *channel = NULL;
  SVAChannel  channel2 = {
       .channelId = channelID->id,
  };
  SArray *cancelArray = taosArrayInit(0, sizeof(SVATaskCancelInfo));
  if (cancelArray == NULL) {
    return terrno;
  }

  (void)taosThreadMutexLock(&async->mutex);

  int32_t ret = vHashGet(async->channelTable, &channel2, (void **)&channel);
  TAOS_UNUSED(ret);
  if (channel) {
    // unregister channel
    channel->next->prev = channel->prev;
    channel->prev->next = channel->next;
    ret = vHashDrop(async->channelTable, channel);
    async->numChannels--;

    // cancel all waiting tasks
    for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
      while (channel->queue[i].next != &channel->queue[i]) {
        SVATask *task = channel->queue[i].next;
        task->prev->next = task->next;
        task->next->prev = task->prev;
        if (task->cancel) {
          if (taosArrayPush(cancelArray, &(SVATaskCancelInfo){
                                             .cancel = task->cancel,
                                             .arg = task->arg,
                                         }) == NULL) {
            vError("failed to push cancel info");
          };
        }
        vnodeAsyncTaskDone(async, task);
      }
    }

    // cancel or wait the scheduled task
    if (channel->scheduled == NULL || channel->scheduled->state == EVA_TASK_STATE_WAITTING) {
      if (channel->scheduled) {
        channel->scheduled->prev->next = channel->scheduled->next;
        channel->scheduled->next->prev = channel->scheduled->prev;
        if (channel->scheduled->cancel) {
          if (taosArrayPush(cancelArray, &(SVATaskCancelInfo){
                                             .cancel = channel->scheduled->cancel,
                                             .arg = channel->scheduled->arg,
                                         }) == NULL) {
            vError("failed to push cancel info");
          }
        }
        vnodeAsyncTaskDone(async, channel->scheduled);
      }
      taosMemoryFree(channel);
    } else {
      if (waitRunning) {
        // wait task
        SVATask *task = channel->scheduled;
        task->numWait++;
        (void)taosThreadCondWait(&task->waitCond, &async->mutex);
        task->numWait--;
        if (task->numWait == 0) {
          (void)taosThreadCondDestroy(&task->waitCond);
          taosMemoryFree(task);
        }

        taosMemoryFree(channel);
      } else {
        channel->state = EVA_CHANNEL_STATE_CLOSE;
      }
    }
  }

  (void)taosThreadMutexUnlock(&async->mutex);
  for (int32_t i = 0; i < taosArrayGetSize(cancelArray); i++) {
    SVATaskCancelInfo *cancel = (SVATaskCancelInfo *)taosArrayGet(cancelArray, i);
    cancel->cancel(cancel->arg);
  }
  taosArrayDestroy(cancelArray);

  channelID->async = 0;
  channelID->id = 0;
  return 0;
}

const char *vnodeGetATaskName(EVATaskT taskType) {
  switch (taskType) {
    case EVA_TASK_COMMIT:
      return "vnode-commit";
    case EVA_TASK_MERGE:
      return "vnode-merge";
    case EVA_TASK_COMPACT:
      return "vnode-compact";
    case EVA_TASK_RETENTION:
      return "vnode-retention";
    default:
      return "unknown";
  }
}

bool vnodeATaskValid(SVATaskID *taskID) {
  if (taskID == NULL || taskID->async < MIN_ASYNC_ID || taskID->async > MAX_ASYNC_ID || taskID->id <= 0) {
    return false;
  }

  SVAsync *async = GVnodeAsyncs[taskID->async].async;
  SVATask  task2 = {
       .taskId = taskID->id,
  };
  SVATask *task = NULL;

  (void)taosThreadMutexLock(&async->mutex);
  int32_t ret = vHashGet(async->taskTable, &task2, (void **)&task);
  (void)taosThreadMutexUnlock(&async->mutex);

  return ret == 0 && task != NULL;
}