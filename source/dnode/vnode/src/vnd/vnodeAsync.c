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
  void (*complete)(void *);
  void        *arg;
  EVATaskState state;

  // wait
  int32_t      numWait;
  TdThreadCond waitCond;

  // queue
  struct SVATask *prev;
  struct SVATask *next;
};

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

static int32_t vnodeAsyncTaskDone(SVAsync *async, SVATask *task) {
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
  if (ret != 0) {
    ASSERT(0);
  }
  async->numTasks--;

  // call complete callback
  if (task->complete) {
    task->complete(task->arg);
  }

  if (task->numWait == 0) {
    taosThreadCondDestroy(&task->waitCond);
    taosMemoryFree(task);
  } else if (task->numWait == 1) {
    taosThreadCondSignal(&task->waitCond);
  } else {
    taosThreadCondBroadcast(&task->waitCond);
  }
  return 0;
}

static int32_t vnodeAsyncCancelAllTasks(SVAsync *async) {
  while (async->queue[0].next != &async->queue[0] || async->queue[1].next != &async->queue[1] ||
         async->queue[2].next != &async->queue[2]) {
    for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
      while (async->queue[i].next != &async->queue[i]) {
        SVATask *task = async->queue[i].next;
        task->prev->next = task->next;
        task->next->prev = task->prev;
        vnodeAsyncTaskDone(async, task);
      }
    }
  }
  return 0;
}

static void *vnodeAsyncLoop(void *arg) {
  SVWorker *worker = (SVWorker *)arg;
  SVAsync  *async = worker->async;

  setThreadName(async->label);

  for (;;) {
    taosThreadMutexLock(&async->mutex);

    // finish last running task
    if (worker->runningTask != NULL) {
      vnodeAsyncTaskDone(async, worker->runningTask);
      worker->runningTask = NULL;
    }

    for (;;) {
      if (async->stop || worker->workerId >= async->numWorkers) {
        if (async->stop) {  // cancel all tasks
          vnodeAsyncCancelAllTasks(async);
        }
        worker->state = EVA_WORKER_STATE_STOP;
        async->numLaunchWorkers--;
        taosThreadMutexUnlock(&async->mutex);
        return NULL;
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
        taosThreadCondWait(&async->hasTask, &async->mutex);
        async->numIdleWorkers--;
        worker->state = EVA_WORKER_STATE_ACTIVE;
      } else {
        worker->runningTask->state = EVA_TASK_STATE_RUNNING;
        break;
      }
    }

    taosThreadMutexUnlock(&async->mutex);

    // do run the task
    worker->runningTask->execute(worker->runningTask->arg);
  }

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

int32_t vnodeAsyncInit(SVAsync **async, char *label) {
  int32_t ret;

  if (async == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (label == NULL) {
    label = "anonymous";
  }

  (*async) = (SVAsync *)taosMemoryCalloc(1, sizeof(SVAsync) + strlen(label) + 1);
  if ((*async) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  strcpy((char *)((*async) + 1), label);
  (*async)->label = (const char *)((*async) + 1);

  taosThreadMutexInit(&(*async)->mutex, NULL);
  taosThreadCondInit(&(*async)->hasTask, NULL);
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
    taosThreadMutexDestroy(&(*async)->mutex);
    taosThreadCondDestroy(&(*async)->hasTask);
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
    taosThreadMutexDestroy(&(*async)->mutex);
    taosThreadCondDestroy(&(*async)->hasTask);
    taosMemoryFree(*async);
    return ret;
  }

  return 0;
}

int32_t vnodeAsyncDestroy(SVAsync **async) {
  if ((*async) == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // set stop and broadcast
  taosThreadMutexLock(&(*async)->mutex);
  (*async)->stop = true;
  taosThreadCondBroadcast(&(*async)->hasTask);
  taosThreadMutexUnlock(&(*async)->mutex);

  // join all workers
  for (int32_t i = 0; i < VNODE_ASYNC_MAX_WORKERS; i++) {
    taosThreadMutexLock(&(*async)->mutex);
    EVWorkerState state = (*async)->workers[i].state;
    taosThreadMutexUnlock(&(*async)->mutex);

    if (state == EVA_WORKER_STATE_UINIT) {
      continue;
    }

    taosThreadJoin((*async)->workers[i].thread, NULL);
    ASSERT((*async)->workers[i].state == EVA_WORKER_STATE_STOP);
    (*async)->workers[i].state = EVA_WORKER_STATE_UINIT;
  }

  // close all channels
  for (SVAChannel *channel = (*async)->chList.next; channel != &(*async)->chList; channel = (*async)->chList.next) {
    channel->next->prev = channel->prev;
    channel->prev->next = channel->next;

    int32_t ret = vHashDrop((*async)->channelTable, channel);
    if (ret) {
      ASSERT(0);
    }
    (*async)->numChannels--;
    taosMemoryFree(channel);
  }

  ASSERT((*async)->numLaunchWorkers == 0);
  ASSERT((*async)->numIdleWorkers == 0);
  ASSERT((*async)->numChannels == 0);
  ASSERT((*async)->numTasks == 0);

  taosThreadMutexDestroy(&(*async)->mutex);
  taosThreadCondDestroy(&(*async)->hasTask);

  vHashDestroy(&(*async)->channelTable);
  vHashDestroy(&(*async)->taskTable);
  taosMemoryFree(*async);
  *async = NULL;

  return 0;
}

static int32_t vnodeAsyncLaunchWorker(SVAsync *async) {
  for (int32_t i = 0; i < async->numWorkers; i++) {
    ASSERT(async->workers[i].state != EVA_WORKER_STATE_IDLE);
    if (async->workers[i].state == EVA_WORKER_STATE_ACTIVE) {
      continue;
    } else if (async->workers[i].state == EVA_WORKER_STATE_STOP) {
      taosThreadJoin(async->workers[i].thread, NULL);
      async->workers[i].state = EVA_WORKER_STATE_UINIT;
    }

    taosThreadCreate(&async->workers[i].thread, NULL, vnodeAsyncLoop, &async->workers[i]);
    async->workers[i].state = EVA_WORKER_STATE_ACTIVE;
    async->numLaunchWorkers++;
    break;
  }
  return 0;
}

#ifdef BUILD_NO_CALL
int32_t vnodeAsync(SVAsync *async, EVAPriority priority, int32_t (*execute)(void *), void (*complete)(void *),
                   void *arg, int64_t *taskId) {
  return vnodeAsyncC(async, 0, priority, execute, complete, arg, taskId);
}
#endif

int32_t vnodeAsyncC(SVAsync *async, int64_t channelId, EVAPriority priority, int32_t (*execute)(void *),
                    void (*complete)(void *), void *arg, int64_t *taskId) {
  if (async == NULL || execute == NULL || channelId < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int64_t id;

  // create task object
  SVATask *task = (SVATask *)taosMemoryCalloc(1, sizeof(SVATask));
  if (task == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  task->priority = priority;
  task->priorScore = 0;
  task->execute = execute;
  task->complete = complete;
  task->arg = arg;
  task->state = EVA_TASK_STATE_WAITTING;
  task->numWait = 0;
  taosThreadCondInit(&task->waitCond, NULL);

  // schedule task
  taosThreadMutexLock(&async->mutex);

  if (channelId == 0) {
    task->channel = NULL;
  } else {
    SVAChannel channel = {.channelId = channelId};
    vHashGet(async->channelTable, &channel, (void **)&task->channel);
    if (task->channel == NULL) {
      taosThreadMutexUnlock(&async->mutex);
      taosThreadCondDestroy(&task->waitCond);
      taosMemoryFree(task);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  task->taskId = id = ++async->nextTaskId;

  // add task to hash table
  int32_t ret = vHashPut(async->taskTable, task);
  if (ret != 0) {
    taosThreadMutexUnlock(&async->mutex);
    taosThreadCondDestroy(&task->waitCond);
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
      taosThreadCondSignal(&(async->hasTask));
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

  taosThreadMutexUnlock(&async->mutex);

  if (taskId != NULL) {
    *taskId = id;
  }

  return 0;
}

int32_t vnodeAWait(SVAsync *async, int64_t taskId) {
  if (async == NULL || taskId <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVATask *task = NULL;
  SVATask  task2 = {.taskId = taskId};

  taosThreadMutexLock(&async->mutex);

  vHashGet(async->taskTable, &task2, (void **)&task);
  if (task) {
    task->numWait++;
    taosThreadCondWait(&task->waitCond, &async->mutex);
    task->numWait--;

    if (task->numWait == 0) {
      taosThreadCondDestroy(&task->waitCond);
      taosMemoryFree(task);
    }
  }

  taosThreadMutexUnlock(&async->mutex);

  return 0;
}

int32_t vnodeACancel(SVAsync *async, int64_t taskId) {
  if (async == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t  ret = 0;
  SVATask *task = NULL;
  SVATask  task2 = {.taskId = taskId};

  taosThreadMutexLock(&async->mutex);

  vHashGet(async->taskTable, &task2, (void **)&task);
  if (task) {
    if (task->state == EVA_TASK_STATE_WAITTING) {
      // remove from queue
      task->next->prev = task->prev;
      task->prev->next = task->next;
      vnodeAsyncTaskDone(async, task);
    } else {
      ret = TSDB_CODE_FAILED;
    }
  }

  taosThreadMutexUnlock(&async->mutex);

  return ret;
}

int32_t vnodeAsyncSetWorkers(SVAsync *async, int32_t numWorkers) {
  if (async == NULL || numWorkers <= 0 || numWorkers > VNODE_ASYNC_MAX_WORKERS) {
    return TSDB_CODE_INVALID_PARA;
  }

  taosThreadMutexLock(&async->mutex);
  async->numWorkers = numWorkers;
  if (async->numIdleWorkers > 0) {
    taosThreadCondBroadcast(&async->hasTask);
  }
  taosThreadMutexUnlock(&async->mutex);

  return 0;
}

int32_t vnodeAChannelInit(SVAsync *async, int64_t *channelId) {
  if (async == NULL || channelId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // create channel object
  SVAChannel *channel = (SVAChannel *)taosMemoryMalloc(sizeof(SVAChannel));
  if (channel == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  channel->state = EVA_CHANNEL_STATE_OPEN;
  for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
    channel->queue[i].next = &channel->queue[i];
    channel->queue[i].prev = &channel->queue[i];
  }
  channel->scheduled = NULL;

  // register channel
  taosThreadMutexLock(&async->mutex);

  channel->channelId = *channelId = ++async->nextChannelId;

  // add to hash table
  int32_t ret = vHashPut(async->channelTable, channel);
  if (ret != 0) {
    taosThreadMutexUnlock(&async->mutex);
    taosMemoryFree(channel);
    return ret;
  }

  // add to list
  channel->next = &async->chList;
  channel->prev = async->chList.prev;
  channel->next->prev = channel;
  channel->prev->next = channel;

  async->numChannels++;

  taosThreadMutexUnlock(&async->mutex);

  return 0;
}

int32_t vnodeAChannelDestroy(SVAsync *async, int64_t channelId, bool waitRunning) {
  if (async == NULL || channelId <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVAChannel *channel = NULL;
  SVAChannel  channel2 = {.channelId = channelId};

  taosThreadMutexLock(&async->mutex);

  vHashGet(async->channelTable, &channel2, (void **)&channel);
  if (channel) {
    // unregister channel
    channel->next->prev = channel->prev;
    channel->prev->next = channel->next;
    vHashDrop(async->channelTable, channel);
    async->numChannels--;

    // cancel all waiting tasks
    for (int32_t i = 0; i < EVA_PRIORITY_MAX; i++) {
      while (channel->queue[i].next != &channel->queue[i]) {
        SVATask *task = channel->queue[i].next;
        task->prev->next = task->next;
        task->next->prev = task->prev;
        vnodeAsyncTaskDone(async, task);
      }
    }

    // cancel or wait the scheduled task
    if (channel->scheduled == NULL || channel->scheduled->state == EVA_TASK_STATE_WAITTING) {
      if (channel->scheduled) {
        channel->scheduled->prev->next = channel->scheduled->next;
        channel->scheduled->next->prev = channel->scheduled->prev;
        vnodeAsyncTaskDone(async, channel->scheduled);
      }
      taosMemoryFree(channel);
    } else {
      if (waitRunning) {
        // wait task
        SVATask *task = channel->scheduled;
        task->numWait++;
        taosThreadCondWait(&task->waitCond, &async->mutex);
        task->numWait--;
        if (task->numWait == 0) {
          taosThreadCondDestroy(&task->waitCond);
          taosMemoryFree(task);
        }

        taosMemoryFree(channel);
      } else {
        channel->state = EVA_CHANNEL_STATE_CLOSE;
      }
    }
  } else {
    taosThreadMutexUnlock(&async->mutex);
    return TSDB_CODE_INVALID_PARA;
  }

  taosThreadMutexUnlock(&async->mutex);

  return 0;
}