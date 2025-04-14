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

#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#include <addr_any.h>


#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif

#include "os.h"

#include "thash.h"
#include "theap.h"
#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "stub.h"
#include "../inc/tmempoolInt.h"
#include "tglobal.h"

namespace {

#define MPT_PRINTF(param, ...) (void)printf("[%" PRId64 ",%" PRId64 "] " param, mptCaseLoop, mptExecLoop, __VA_ARGS__)
#define MPT_EPRINTF(param, ...) (void)printf(param, __VA_ARGS__)

#define MPT_MAX_MEM_ACT_TIMES 300
#define MPT_MAX_SESSION_NUM 100
#define MPT_MAX_JOB_NUM     100
#define MPT_MAX_THREAD_NUM  100
#define MPT_MAX_JOB_LOOP_TIMES 100

#define MPT_DEFAULT_RESERVE_MEM_PERCENT 20
#define MPT_MIN_RESERVE_MEM_SIZE        (512 * 1048576UL)
#define MPT_MIN_MEM_POOL_SIZE           (1048576UL)
#define MPT_MAX_RETIRE_JOB_NUM          10000
#define MPT_DEFAULT_TASK_RUN_TIMES      10
#define MPT_NON_POOL_ALLOC_UNIT         (1048576UL)
#define MPT_NON_POOL_KEEP_ALLOC_UNIT    (10485760UL * 8)
#define MPT_MAX_NON_POOL_ALLOC_TIMES    30000

enum {
  MPT_READ = 1,
  MPT_WRITE,
};

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000



threadlocal void* mptThreadPoolHandle = NULL;
threadlocal void* mptThreadPoolSession = NULL;
threadlocal int32_t mptJobNum = 0;
threadlocal int32_t mptExecNum = 0;
threadlocal int32_t mptExecLoop = 0;
threadlocal int64_t mptCaseLoop = 0;





#define MPT_SET_TEID(id, tId, eId)                              \
    do {                                                              \
      *(uint64_t *)(id) = (tId);                                      \
      *(uint32_t *)((char *)(id) + sizeof(tId)) = (eId);              \
    } while (0)

#define MPT_SET_QCID(id, qId, cId)                                                 \
      do {                                                                            \
        *(uint64_t *)(id) = (qId);                                                    \
        *(uint64_t *)((char *)(id) + sizeof(qId)) = (cId);                            \
      } while (0)


#define mptEnableMemoryPoolUsage(_pool, _session) do { mptThreadPoolHandle = _pool; mptThreadPoolSession = _session; } while (0) 
#define mptDisableMemoryPoolUsage() (mptThreadPoolHandle = NULL, mptThreadPoolSession = NULL) 
#define mptSaveDisableMemoryPoolUsage(_handle) do { (_handle) = mptThreadPoolHandle; mptThreadPoolHandle = NULL; } while (0)
#define mptRestoreEnableMemoryPoolUsage(_handle) (mptThreadPoolHandle = (_handle))

#define mptMemoryMalloc(_size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolMalloc(mptThreadPoolHandle, mptThreadPoolSession, _size, __FILE__, __LINE__)) : (taosMemMalloc(_size)))
#define mptMemoryCalloc(_num, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolCalloc(mptThreadPoolHandle, mptThreadPoolSession, _num, _size, __FILE__, __LINE__)) : (taosMemCalloc(_num, _size)))
#define mptMemoryRealloc(_ptr, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolRealloc(mptThreadPoolHandle, mptThreadPoolSession, _ptr, _size, __FILE__, __LINE__)) : (taosMemRealloc(_ptr, _size)))
#define mptStrdup(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolStrdup(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosStrdupi(_ptr)))
#define mptStrndup(_ptr, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolStrndup(mptThreadPoolHandle, mptThreadPoolSession, _ptr, _size, (char*)__FILE__, __LINE__)) : (taosStrndupi(_ptr, _size)))
#define mptMemoryFree(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolFree(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemFree(_ptr)))
#define mptMemorySize(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolGetMemorySize(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemSize(_ptr)))
#define mptMemoryTrim(_size, _trimed) ((NULL != mptThreadPoolHandle) ? (taosMemPoolTrim(mptThreadPoolHandle, mptThreadPoolSession, _size, __FILE__, __LINE__, _trimed)) : (taosMemTrim(_size, _trimed)))
#define mptMemoryMallocAlign(_alignment, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolMallocAlign(mptThreadPoolHandle, mptThreadPoolSession, _alignment, _size, __FILE__, __LINE__)) : (taosMemMallocAlign(_alignment, _size)))

enum {
  MPT_SMALL_MSIZE = 0,
  MPT_BIG_MSIZE,
};

typedef struct {
  int32_t jobNum;
  int32_t sessionNum;
  bool    memSize[2];
  bool    jobQuotaRetire;
  bool    poolRetire;
} SMPTCaseParam;

typedef struct SMPTJobInfo {
  int8_t              retired;
  int32_t             errCode;
  SMemPoolJob*        memInfo;
  void*               pCtx;

  SRWLatch            lock;
  int8_t              destroyed;
  SHashObj*           pSessions;
  int8_t              initDone;
} SMPTJobInfo;


typedef struct {
  int32_t taskActTimes;
  int32_t caseLoopTimes;
  int32_t jobExecTimes;
  int32_t jobNum;
  int32_t jobTaskNum;
  int64_t maxSingleAllocSize;
  bool    printExecDetail;
  bool    printInputRow;

  bool    lockDbg;
} SMPTestCtrl;

typedef struct {
  void*   p;
  int64_t size;
} SMPTestMemInfo;

typedef struct {
  uint64_t  taskId;
  SRWLatch  taskExecLock;
  bool      destoryed;
  
  int64_t poolMaxUsedSize;
  int64_t poolTotalUsedSize;

  SMPStatDetail   stat;
  
  int32_t         memIdx;
  SMPTestMemInfo* pMemList;

  bool    taskFreed;
  int32_t lastAct;
} SMPTestTaskCtx;

typedef struct {
  SRWLatch       jobExecLock;

  int32_t        jobIdx;
  int64_t        jobId;
  int32_t        initTimes;
  void*          pSessions[MPT_MAX_SESSION_NUM];
  int32_t        taskNum;
  SMPTestTaskCtx taskCtxs[MPT_MAX_SESSION_NUM];

  int32_t        taskRunningNum;
  SMPTJobInfo*   pJob;
  int32_t        jobStatus;
} SMPTestJobCtx;

typedef struct {
  int32_t        jobQuota;
  bool           enableMemPool;
  bool           reserveMode;
  int64_t        upperLimitSize;
  int32_t        reserveSize;          //MB
  int32_t        threadNum;
  int32_t        randTask;
} SMPTestParam;

typedef struct {
  int64_t        initNum;
  int64_t        retireNum;
  int64_t        destoryNum;
} SMPTestJobStat;

typedef struct {
  int32_t  idx;
  TdThread threadFp;
  bool     allJobs;
  bool     autoJob;
} SMPTestThread;

typedef struct SMPTestCtx {
  int64_t        qId;
  int64_t        tId;
  SHashObj*      pJobs;
  BoundedQueue*  pJobQueue;
  SMPTestThread  threadCtxs[MPT_MAX_THREAD_NUM];
  TdThread       dropThreadFp;
  TdThread       nPoolThreadFp;
  int32_t        jobNum;
  int64_t        totalTaskNum;
  SMPTestJobCtx* jobCtxs;
  SMPTestParam   param;
  SMPTestJobStat runStat;

  SRWLatch       stringLock;
  char*          pSrcString;

  bool           initDone;
  int8_t         testDone;
  int64_t        jobLoop;

  int32_t         npIdx;
  SMPTestMemInfo* npMemList;
} SMPTestCtx;

SMPTestCtx mptCtx = {0};
SMPTestCtrl mptCtrl = {0};

static int32_t MPT_TRY_LOCK(int32_t type, SRWLatch *_lock) {
  int32_t code = -1;
  
  if (MPT_READ == (type)) {     
    if (mptCtrl.lockDbg) {
      if (atomic_load_32((_lock)) < 0) {                  
        uError("invalid lock value before try read lock");                          
        return -1;                                                               
      }                                                                      
      uDebug("MPT TRY RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); 
    }
    code = taosRTryLockLatch(_lock);                                                               
    if (mptCtrl.lockDbg) {
      uDebug("MPT TRY RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);
      if (atomic_load_32((_lock)) <= 0) {                                               
        uError("invalid lock value after try read lock");                           
        return -1;                                                                               
      }                                                                                        
    }
  } else {                                                                                   
    if (mptCtrl.lockDbg) {
      if (atomic_load_32((_lock)) < 0) {                                                      
        uError("invalid lock value before try write lock");                                 
        return -1;                                                               
      }                                                                                           
      uDebug("MPT TRY WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); 
    }
    code = taosWTryLockLatch(_lock);                                                                   
    if (mptCtrl.lockDbg) {
      uDebug("MPT TRY WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); 
      if (atomic_load_32((_lock)) != TD_RWLATCH_WRITE_FLAG_COPY) {                        
        uError("invalid lock value after try write lock");                          
        return -1;                                                               
      }    
    }
  }                                                                          

  return code;
}

#define MPT_LOCK(type, _lock)                                                                       \
  do {                                                                                             \
    if (MPT_READ == (type)) {                                                                       \
      if (mptCtrl.lockDbg) {                                                                        \
        if (atomic_load_32((_lock)) < 0) {                                                           \
          uError("invalid lock value before read lock");                                             \
          break;                                                                                     \
        }                                                                                            \
        uDebug("MPT RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      }                                                                                           \
      taosRLockLatch(_lock);                                                                       \
      if (mptCtrl.lockDbg) {                                                                        \
        uDebug("MPT RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
        if (atomic_load_32((_lock)) <= 0) {                                                          \
          uError("invalid lock value after read lock");                                              \
          break;                                                                                     \
        }                                                                                            \
      }                                                                                           \
    } else {                                                                                       \
      if (mptCtrl.lockDbg) {                                                                        \
        if (atomic_load_32((_lock)) < 0) {                                                           \
          uError("invalid lock value before write lock");                                            \
          break;                                                                                     \
        }                                                                                            \
        uDebug("MPT WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      }                                                                                           \
      taosWLockLatch(_lock);                                                                       \
      if (mptCtrl.lockDbg) {                                                                        \
        uDebug("MPT WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
        if (atomic_load_32((_lock)) != TD_RWLATCH_WRITE_FLAG_COPY) {                                 \
          uError("invalid lock value after write lock");                                             \
          break;                                                                                     \
        }                                                                                            \
      }                                                                                           \
    }                                                                                              \
  } while (0)

#define MPT_UNLOCK(type, _lock)                                                                      \
  do {                                                                                              \
    if (MPT_READ == (type)) {                                                                        \
      if (mptCtrl.lockDbg) {                                                                        \
        if (atomic_load_32((_lock)) <= 0) {                                                           \
          uError("invalid lock value before read unlock");                                            \
          break;                                                                                      \
        }                                                                                             \
        uDebug("MPT RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      }                                                                                           \
      taosRUnLockLatch(_lock);                                                                      \
      if (mptCtrl.lockDbg) {                                                                        \
        uDebug("MPT RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
        if (atomic_load_32((_lock)) < 0) {                                                            \
          uError("invalid lock value after read unlock");                                             \
          break;                                                                                      \
        }                                                                                             \
      }                                                                                           \
    } else {                                                                                        \
      if (mptCtrl.lockDbg) {                                                                        \
        if (atomic_load_32((_lock)) != TD_RWLATCH_WRITE_FLAG_COPY) {                                  \
          uError("invalid lock value before write unlock");                                           \
          break;                                                                                      \
        }                                                                                             \
        uDebug("MPT WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
      }                                                                                           \
      taosWUnLockLatch(_lock);                                                                      \
      if (mptCtrl.lockDbg) {                                                                        \
        uDebug("MPT WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
        if (atomic_load_32((_lock)) < 0) {                                                            \
          uError("invalid lock value after write unlock");                                            \
          break;                                                                                      \
        }                                                                                             \
      }                                                                                           \
    }                                                                                               \
  } while (0)




#if 0
void joinTestReplaceRetrieveFp() {
  static Stub stub;
  stub.set(getNextBlockFromDownstreamRemain, getDummyInputBlock);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("getNextBlockFromDownstreamRemain", result);
    for (const auto &f : result) {
      stub.set(f.second, getDummyInputBlock);
    }
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^getNextBlockFromDownstreamRemain$", result);
    for (const auto &f : result) {
      stub.set(f.second, getDummyInputBlock);
    }
#endif
  }
}
#endif

void mptInitLogFile() {
  const char   *defaultLogFileNamePrefix = "mplog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  uDebugFlag = 159;
  tsNumOfLogLines = INT32_MAX;
  tsLogKeepDays = 10;
  TAOS_STRCPY(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    MPT_PRINTF("failed to open log file in directory:%s\n", tsLogDir);
  }
  taosSetNoNewFile();
}

static bool mptJobMemSizeCompFn(void* l, void* r, void* param) {
  SMPTJobInfo* left = (SMPTJobInfo*)l;
  SMPTJobInfo* right = (SMPTJobInfo*)r;
  if (atomic_load_8(&right->retired)) {
    return true;
  }
  
  return atomic_load_64(&right->memInfo->allocMemSize) < atomic_load_64(&left->memInfo->allocMemSize);
}

void mptDeleteJobQueueData(void* pData) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)pData;
  taosHashRelease(mptCtx.pJobs, pJob);
}


void mptDestroyJobInfo(void* job) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)job;

  taosMemFree(pJob->memInfo);
  taosHashCleanup(pJob->pSessions);
}


void mptWriteMem(void* pStart, int64_t size) {
  char* pEnd = (char*)pStart + size - 1;
  char* p = (char*)pStart;
  while (p <= pEnd) {
    *p = 'a' + taosRand() % 26;
    p += 4096;
  }
}


void mptInit() {
  osDefaultInit();
  mptInitLogFile();
  
  mptCtrl.caseLoopTimes = 100000;
  mptCtrl.taskActTimes = 0;
  mptCtrl.maxSingleAllocSize = 104857600 * 5;
  mptCtrl.jobNum = 100;
  mptCtrl.jobExecTimes = 10;
  mptCtrl.jobTaskNum = 0;

  mptCtx.pJobs = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  ASSERT_TRUE(NULL != mptCtx.pJobs);

  taosHashSetFreeFp(mptCtx.pJobs, mptDestroyJobInfo);

  mptCtx.pJobQueue = createBoundedQueue(10000, mptJobMemSizeCompFn, mptDeleteJobQueueData, NULL);
  ASSERT_TRUE(NULL != mptCtx.pJobQueue);
  mptCtx.jobCtxs = (SMPTestJobCtx*)taosMemoryCalloc(MPT_MAX_JOB_NUM, sizeof(*mptCtx.jobCtxs));
  ASSERT_TRUE(NULL != mptCtx.jobCtxs);
  
  mptCtx.pSrcString = (char*)taosMemoryMalloc(mptCtrl.maxSingleAllocSize);
  ASSERT_TRUE(NULL != mptCtx.pSrcString);
  memset(mptCtx.pSrcString, 'P', mptCtrl.maxSingleAllocSize - 1);
  mptCtx.pSrcString[mptCtrl.maxSingleAllocSize - 1] = 0;

}

void mptDestroySession(uint64_t qId, int64_t tId, int32_t eId, int32_t taskIdx, SMPTestJobCtx* pJobCtx, void* session) {
  SMPTJobInfo *pJobInfo = pJobCtx->pJob;
  char id[sizeof(tId) + sizeof(eId) + 1] = {0};
  MPT_SET_TEID(id, tId, eId);
  int32_t remainSessions = atomic_sub_fetch_32(&pJobInfo->memInfo->remainSession, 1);

  (void)taosHashRemove(pJobInfo->pSessions, id, sizeof(id));

  taosMemPoolDestroySession(gMemPoolHandle, session);

  if (0 == remainSessions) {
    if (0 == taosHashGetSize(pJobInfo->pSessions)) {
      atomic_store_8(&pJobInfo->destroyed, 1);

      uDebug("JOB:0x%x idx:%d destroyed, code:0x%x", pJobCtx->jobId, pJobCtx->jobIdx, pJobInfo->errCode);

      atomic_add_fetch_64(&mptCtx.runStat.destoryNum, 1);
      
      (void)taosHashRemove(mptCtx.pJobs, &qId, sizeof(qId));

      pJobCtx->pJob = NULL;
      uInfo("the whole query job removed");
    }
  }
}


void mptDestroyTaskCtx(SMPTestJobCtx* pJobCtx, int32_t taskIdx) {
  TD_ALWAYS_ASSERT(gMemPoolHandle);

  SMPTestTaskCtx* pTask = &pJobCtx->taskCtxs[taskIdx];

  if (mptCtx.param.enableMemPool) {
    mptEnableMemoryPoolUsage(gMemPoolHandle, pJobCtx->pSessions[taskIdx]);  
  }
  
  for (int32_t i = 0; i < pTask->memIdx; ++i) {
    pTask->stat.times.memFree.exec++;
    pTask->stat.bytes.memFree.exec+=mptMemorySize(pTask->pMemList[i].p);        
    pTask->stat.bytes.memFree.succ+=mptMemorySize(pTask->pMemList[i].p);        
    mptMemoryFree(pTask->pMemList[i].p);
    pTask->pMemList[i].p = NULL;
  }
  
  if (mptCtx.param.enableMemPool) {
    mptDisableMemoryPoolUsage();
  }
  
  mptDestroySession(pJobCtx->jobId, pJobCtx->taskCtxs[taskIdx].taskId, 0, taskIdx, pJobCtx, pJobCtx->pSessions[taskIdx]);
  pJobCtx->pSessions[taskIdx] = NULL;
  
  taosMemFreeClear(pTask->pMemList);

  pTask->destoryed = true;
}


int32_t mptInitJobInfo(uint64_t qId, SMPTJobInfo* pJob) {
  pJob->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->pSessions) {
    uError("fail to init session hash, code: 0x%x", terrno);
    return terrno;
  }

  int32_t code = taosMemPoolCallocJob(qId, 0, (void**)&pJob->memInfo);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pJob->pSessions);
    pJob->pSessions = NULL;
    return code;
  }

  return code;
}


int32_t mptInitSession(uint64_t qId, uint64_t tId, int32_t eId, SMPTestJobCtx* pJobCtx, void** ppSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMPTJobInfo* pJob = NULL;
  
  while (true) {
    pJob = (SMPTJobInfo*)taosHashAcquire(mptCtx.pJobs, &qId, sizeof(qId));
    if (NULL == pJob) {
      SMPTJobInfo jobInfo = {0};
      code = mptInitJobInfo(qId, &jobInfo);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      
      code = taosHashPut(mptCtx.pJobs, &qId, sizeof(qId), &jobInfo, sizeof(jobInfo));
      if (TSDB_CODE_SUCCESS != code) {
        mptDestroyJobInfo(&jobInfo);
        if (TSDB_CODE_DUP_KEY == code) {
          code = TSDB_CODE_SUCCESS;
          continue;
        }
        
        return code;
      }

      pJob = (SMPTJobInfo*)taosHashAcquire(mptCtx.pJobs, &qId, sizeof(qId));
      if (NULL == pJob) {
        uError("QID:0x%" PRIx64 " not in joj hash, may be dropped", qId);
        return TSDB_CODE_QRY_JOB_NOT_EXIST;
      }
    }

    break;
  }

  atomic_store_ptr(&pJobCtx->pJob, pJob);
  pJob->pCtx = pJobCtx;

  char id[sizeof(tId) + sizeof(eId) + 1] = {0};
  MPT_SET_TEID(id, tId, eId);

  TD_ALWAYS_ASSERT(0 == taosMemPoolInitSession(gMemPoolHandle, ppSession, pJob->memInfo, id));

  atomic_add_fetch_32(&pJob->memInfo->remainSession, 1);

  TD_ALWAYS_ASSERT(0 == taosHashPut(pJob->pSessions, id, sizeof(id), ppSession, POINTER_BYTES));

  atomic_store_8(&pJob->initDone, 1);

_return:

  if (NULL != pJob) {
    taosHashRelease(mptCtx.pJobs, pJob);
  }

  return code;
}



void mptInitTask(int32_t idx, int32_t eId, SMPTestJobCtx* pJob) {
  pJob->taskCtxs[idx].taskId = atomic_add_fetch_64(&mptCtx.tId, 1);

  ASSERT_TRUE(0 == mptInitSession(pJob->jobId, pJob->taskCtxs[idx].taskId, eId, pJob, &pJob->pSessions[idx]));

  pJob->taskCtxs[idx].pMemList = (SMPTestMemInfo*)taosMemoryCalloc(MPT_MAX_MEM_ACT_TIMES, sizeof(*pJob->taskCtxs[idx].pMemList));
  ASSERT_TRUE(NULL != pJob->taskCtxs[idx].pMemList);

  pJob->taskCtxs[idx].destoryed = false;
  
  uDebug("JOB:0x%x TASK:0x%x idx:%d initialized", pJob->jobId, pJob->taskCtxs[idx].taskId, idx);
}

void mptInitJob(int32_t idx) {
  SMPTestJobCtx* pJobCtx = &mptCtx.jobCtxs[idx];

  pJobCtx->jobIdx = idx;
  pJobCtx->jobId = atomic_add_fetch_64(&mptCtx.qId, 1);
  pJobCtx->taskNum = (mptCtrl.jobTaskNum) ? mptCtrl.jobTaskNum : ((taosRand() % 10) ? (taosRand() % (MPT_MAX_SESSION_NUM/10)) : (taosRand() % MPT_MAX_SESSION_NUM)) + 1;
  pJobCtx->initTimes++;

  if (!mptCtx.initDone) {
    atomic_add_fetch_64(&mptCtx.totalTaskNum, pJobCtx->taskNum);
  }
  
  for (int32_t i = 0; i < pJobCtx->taskNum; ++i) {
    mptInitTask(i, 0, pJobCtx);
    TD_ALWAYS_ASSERT(pJobCtx->pJob);
  }

  atomic_add_fetch_64(&mptCtx.runStat.initNum, 1);

  uDebug("JOB:0x%x idx:%d initialized, total times:%d, taskNum:%d", pJobCtx->jobId, idx, pJobCtx->initTimes, pJobCtx->taskNum);
}


void mptDestroyTask(SMPTestJobCtx* pJobCtx, int32_t taskIdx) {
  if (mptCtx.param.enableMemPool && tsMemPoolFullFunc) {
    SMPStatDetail* pStat = NULL;
    int64_t allocSize = 0;
    taosMemPoolGetSessionStat(pJobCtx->pSessions[taskIdx], &pStat, &allocSize, NULL);
    int64_t usedSize = MEMPOOL_GET_USED_SIZE(pStat);
    
    TD_ALWAYS_ASSERT(allocSize == usedSize);
    TD_ALWAYS_ASSERT(0 == memcmp(pStat, &pJobCtx->taskCtxs[taskIdx].stat, sizeof(*pStat)));
  }
  
  mptDestroyTaskCtx(pJobCtx, taskIdx);
}

int32_t mptDestroyJob(SMPTestJobCtx* pJobCtx, bool reset) {
  uint64_t jobId = pJobCtx->jobId;
  for (int32_t i = 0; i < pJobCtx->taskNum; ++i) {
    if (!pJobCtx->taskCtxs[i].destoryed) {
      mptDestroyTask(pJobCtx, i);
    }
  }
  
  //mptDestroyJobInfo(pJobCtx->pJob);
  //(void)taosHashRemove(mptCtx.pJobs, &pJobCtx->jobId, sizeof(pJobCtx->jobId));

  if (reset) {
    int32_t jobIdx = pJobCtx->jobIdx;
    memset((char*)pJobCtx + sizeof(pJobCtx->jobExecLock), 0, sizeof(SMPTestJobCtx) - sizeof(pJobCtx->jobExecLock));
    mptInitJob(jobIdx);
  }
  
  MPT_PRINTF("    JOB:0x%x retired\n", jobId);

  return 0;
}

void mptCheckCompareJobInfo(SMPTestJobCtx* pJobCtx) {

}

int32_t mptResetJob(SMPTestJobCtx* pJobCtx) {
  if (MPT_TRY_LOCK(MPT_WRITE, &pJobCtx->jobExecLock)) {
    return -1;
  }

  if (NULL == atomic_load_ptr(&pJobCtx->pJob)) {
    int32_t jobIdx = pJobCtx->jobIdx;
    memset((char*)pJobCtx + sizeof(pJobCtx->jobExecLock), 0, sizeof(SMPTestJobCtx) - sizeof(pJobCtx->jobExecLock));
    mptInitJob(jobIdx);

    MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);
    return 0;
  }

  int32_t code = 0;
  if (atomic_load_8(&pJobCtx->pJob->retired)) {
    int32_t taskRunning = atomic_load_32(&pJobCtx->taskRunningNum);
    if (0 == taskRunning) {
      code = mptDestroyJob(pJobCtx, true);
    } else {
      uDebug("JOB:0x%x retired but will not destroy cause of task running, num:%d", pJobCtx->jobId, taskRunning);
      code = -1;
    }
  }

  MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);

  return 0;
}

bool mptRetireJob(SMPTJobInfo* pJob) {
  SMPTestJobCtx* pCtx = (SMPTestJobCtx*)pJob->pCtx;

  if (MPT_TRY_LOCK(MPT_WRITE, &pCtx->jobExecLock)) {
    return false;
  }

  bool retired = false;
  int32_t taskRunning = atomic_load_32(&pCtx->taskRunningNum);
  if (0 == taskRunning) {
    mptDestroyJob(pCtx, false);
    retired = true;
  } else {
    uDebug("JOB:0x%x retired but will not destroy cause of task running, num:%d", pCtx->jobId, taskRunning);
  }

  MPT_UNLOCK(MPT_WRITE, &pCtx->jobExecLock);

  return retired;
}

int32_t mptGetMemPoolMaxMemSize(void* pHandle, int64_t* maxSize) {
  int64_t freeSize = 0;
  int64_t usedSize = 0;
  bool needEnd = false;

  taosMemPoolGetUsedSizeBegin(pHandle, &usedSize, &needEnd);
  int32_t code = taosGetSysAvailMemory(&freeSize);
  if (needEnd) {
    taosMemPoolGetUsedSizeEnd(pHandle);
  }
  
  if (TSDB_CODE_SUCCESS != code) {
    uError("get system available memory size failed, error: 0x%x", code);
    return code;
  }

  int64_t totalSize = freeSize + usedSize;
  int64_t reserveSize = TMAX(totalSize * MPT_DEFAULT_RESERVE_MEM_PERCENT / 100 / 1048576UL * 1048576UL, MPT_MIN_RESERVE_MEM_SIZE);
  int64_t availSize = (totalSize - reserveSize) / 1048576UL * 1048576UL;
  if (availSize < MPT_MIN_MEM_POOL_SIZE) {
    uError("too little available query memory, totalAvailable: %" PRId64 ", reserveSize: %" PRId64, totalSize, reserveSize);
    //return TSDB_CODE_QRY_TOO_FEW_AVAILBLE_MEM;
  }

  uDebug("new pool maxSize:%" PRId64 ", usedSize:%" PRId64 ", freeSize:%" PRId64, availSize, usedSize, freeSize);

  *maxSize = availSize;

  return TSDB_CODE_SUCCESS;
}

void mptRetireJobsCb(int64_t retireSize, int32_t errCode) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, NULL);
  int32_t jobNum = 0;
  uint64_t jobId = 0;
  int64_t retiredSize = 0;
  while (retiredSize < retireSize && NULL != pJob) {
    if (atomic_load_8(&pJob->retired) || 0 == atomic_load_8(&pJob->initDone)) {
      pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, pJob);
      continue;
    }

    if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);
      jobId = pJob->memInfo->jobId;

      atomic_add_fetch_64(&mptCtx.runStat.retireNum, 1);

      bool retired = mptRetireJob(pJob);
      if (retired) {
        retiredSize += aSize;    
      }
      
      jobNum++;

      uDebug("QID:0x%" PRIx64 " job mark retired cause of limit reached, retired:%d, usedSize:%" PRId64 ", retireSize:%" PRId64 ", retiredSize:%" PRId64, 
          jobId, retired, aSize, retireSize, retiredSize);
    }

    pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, pJob);
  }

  taosHashCancelIterate(mptCtx.pJobs, pJob);

  uDebug("total %d jobs mark retired, retiredSize:%" PRId64 " targetRetireSize:%" PRId64, jobNum, retiredSize, retireSize);
}


void mptRetireJobCb(uint64_t jobId, uint64_t clientId, int32_t errCode) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)taosHashGet(mptCtx.pJobs, &jobId, sizeof(jobId));
  if (NULL == pJob) {
    uError("QID:0x%" PRIx64 " fail to get job from job hash", jobId);
    return;
  }

  if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
    uInfo("QID:0x%" PRIx64 " mark retired, errCode: 0x%x, allocSize:%" PRId64, jobId, errCode, atomic_load_64(&pJob->memInfo->allocMemSize));
    atomic_add_fetch_64(&mptCtx.runStat.retireNum, 1);
  } else {
    uDebug("QID:0x%" PRIx64 " already retired, retired: %d, errCode: 0x%x, allocSize:%" PRId64, jobId, atomic_load_8(&pJob->retired), atomic_load_32(&pJob->errCode), atomic_load_64(&pJob->memInfo->allocMemSize));
  }
}

void mptInitPool(void) {
  TD_ALWAYS_ASSERT(0 == taosMemoryPoolInit(mptRetireJobsCb, mptRetireJobCb));
}


void mptSimulateAction(SMPTestJobCtx* pJobCtx, SMPTestTaskCtx* pTask) {
  int32_t actId = 0;
  bool actDone = false;
  int32_t size = 0;
  int32_t osize = 0, nsize = 0;
  
  while (!actDone) {
    actId = taosRand() % 10;
    size = (taosRand() % 8) ? (taosRand() % (mptCtrl.maxSingleAllocSize / 100)) : (taosRand() % mptCtrl.maxSingleAllocSize);
    
    switch (actId) {
      case 0: { // malloc
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }
        
        pTask->pMemList[pTask->memIdx].p = mptMemoryMalloc(size);
        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memMalloc.exec++;
          pTask->stat.bytes.memMalloc.exec+=size;        
          pTask->stat.times.memMalloc.fail++;
          pTask->stat.bytes.memMalloc.fail+=size;        
          uError("JOB:0x%x TASK:0x%x mpMalloc %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);
        pTask->stat.times.memMalloc.exec++;
        pTask->stat.bytes.memMalloc.exec+=nsize;        
        pTask->stat.bytes.memMalloc.succ+=nsize;                
        pTask->stat.times.memMalloc.succ++;

        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);
        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 1: { // calloc
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }
        
        pTask->pMemList[pTask->memIdx].p = mptMemoryCalloc(1, size);
        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memCalloc.exec++;
          pTask->stat.bytes.memCalloc.exec+=size;        
          pTask->stat.times.memCalloc.fail++;
          pTask->stat.bytes.memCalloc.fail+=size;        
          uError("JOB:0x%x TASK:0x%x mpCalloc %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);

        pTask->stat.times.memCalloc.exec++;
        pTask->stat.bytes.memCalloc.exec+=nsize;        
        pTask->stat.times.memCalloc.succ++;
        pTask->stat.bytes.memCalloc.succ+=nsize;      

        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);
        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 2:{ // new realloc
        break;
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }
        
        pTask->pMemList[pTask->memIdx].p = mptMemoryRealloc(NULL, size);
        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memRealloc.exec++;
          pTask->stat.bytes.memRealloc.exec+=size;        
          pTask->stat.times.memRealloc.fail++;
          pTask->stat.bytes.memRealloc.fail+=size;        
          uError("JOB:0x%x TASK:0x%x new mpRealloc %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);
        
        pTask->stat.times.memRealloc.exec++;
        pTask->stat.bytes.memRealloc.exec+=nsize;        
        pTask->stat.bytes.memRealloc.succ+=nsize;        
        pTask->stat.times.memRealloc.succ++;

        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);
        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 3:{ // real realloc
        break;
        if (pTask->memIdx <= 0) {
          break;
        }

        TD_ALWAYS_ASSERT(pTask->pMemList[pTask->memIdx - 1].p);
        osize = mptMemorySize(pTask->pMemList[pTask->memIdx - 1].p);
        size++;
        pTask->pMemList[pTask->memIdx - 1].p = mptMemoryRealloc(pTask->pMemList[pTask->memIdx - 1].p, size);
        if (NULL == pTask->pMemList[pTask->memIdx - 1].p) {
          pTask->stat.times.memRealloc.exec++;
          pTask->stat.bytes.memRealloc.exec+=size;        
          pTask->stat.bytes.memRealloc.origExec+=osize;  
          pTask->stat.times.memRealloc.fail++;
          pTask->stat.bytes.memRealloc.fail+=size;  

          pTask->stat.times.memFree.exec++;
          pTask->stat.bytes.memFree.exec+=osize;  
          pTask->stat.times.memFree.succ++;
          pTask->stat.bytes.memFree.succ+=osize;  
          uError("JOB:0x%x TASK:0x%x real mpRealloc %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          pTask->memIdx--;
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx - 1].p);
        pTask->stat.times.memRealloc.exec++;
        pTask->stat.bytes.memRealloc.exec+=nsize;        
        pTask->stat.bytes.memRealloc.origExec+=osize;  
        pTask->stat.bytes.memRealloc.origSucc+=osize;
        pTask->stat.times.memRealloc.succ++;
        pTask->stat.bytes.memRealloc.succ+=nsize;  

        mptWriteMem(pTask->pMemList[pTask->memIdx - 1].p, size);
        
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 4:{ // realloc free
        if (pTask->memIdx <= 0) {
          break;
        }

        TD_ALWAYS_ASSERT(pTask->pMemList[pTask->memIdx - 1].p);
        osize = mptMemorySize(pTask->pMemList[pTask->memIdx - 1].p);

        pTask->pMemList[pTask->memIdx - 1].p = mptMemoryRealloc(pTask->pMemList[pTask->memIdx - 1].p, 0);
        pTask->stat.times.memFree.exec++;
        pTask->stat.bytes.memFree.exec+=osize;  
        TD_ALWAYS_ASSERT(NULL == pTask->pMemList[pTask->memIdx - 1].p);

        pTask->stat.times.memFree.succ++;
        pTask->stat.bytes.memFree.succ+=osize;  

        pTask->memIdx--;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 5:{ // strdup
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }

        size /= 10;
        MPT_LOCK(MPT_WRITE, &mptCtx.stringLock);
        mptCtx.pSrcString[size] = 0;
        pTask->pMemList[pTask->memIdx].p = mptStrdup(mptCtx.pSrcString);
        mptCtx.pSrcString[size] = 'W';
        MPT_UNLOCK(MPT_WRITE, &mptCtx.stringLock);

        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memStrdup.exec++;
          pTask->stat.bytes.memStrdup.exec+=size + 1;        
          pTask->stat.times.memStrdup.fail++;
          pTask->stat.bytes.memStrdup.fail+=size + 1;        
          uError("JOB:0x%x TASK:0x%x mpStrdup %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);  
        pTask->stat.times.memStrdup.exec++;
        pTask->stat.bytes.memStrdup.exec+= nsize;    

        pTask->stat.times.memStrdup.succ++;
        pTask->stat.bytes.memStrdup.succ+=nsize;        

        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);
        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 6:{ // strndup
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }

        size /= 10;

        MPT_LOCK(MPT_WRITE, &mptCtx.stringLock);
        TD_ALWAYS_ASSERT(strlen(mptCtx.pSrcString) > size);
        pTask->pMemList[pTask->memIdx].p = mptStrndup(mptCtx.pSrcString, size);
        MPT_UNLOCK(MPT_WRITE, &mptCtx.stringLock);

        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memStrndup.exec++;
          pTask->stat.bytes.memStrndup.exec+=size + 1;        
          pTask->stat.times.memStrndup.fail++;
          pTask->stat.bytes.memStrndup.fail+=size + 1;        
          uError("JOB:0x%x TASK:0x%x mpStrndup %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        TD_ALWAYS_ASSERT(strlen((char*)pTask->pMemList[pTask->memIdx].p) == size);
        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);

        pTask->stat.times.memStrndup.exec++;
        pTask->stat.bytes.memStrndup.exec+=nsize;        
        pTask->stat.times.memStrndup.succ++;
        pTask->stat.bytes.memStrndup.succ+=nsize;        

        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);
        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 7:{ // free
        if (pTask->memIdx <= 0) {
          break;
        }

        TD_ALWAYS_ASSERT(pTask->pMemList[pTask->memIdx - 1].p);
        osize = mptMemorySize(pTask->pMemList[pTask->memIdx - 1].p);
        mptMemoryFree(pTask->pMemList[pTask->memIdx - 1].p);
        pTask->stat.times.memFree.exec++;
        pTask->stat.times.memFree.succ++;
        pTask->stat.bytes.memFree.exec+=osize;        
        pTask->stat.bytes.memFree.succ+=osize;        
        pTask->pMemList[pTask->memIdx - 1].p = NULL;
        
        pTask->memIdx--;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 8:{ // trim
        bool trimed = false;
        int32_t code = mptMemoryTrim(0, &trimed);
        pTask->stat.times.memTrim.exec++;
        if (code) {
          pTask->stat.times.memTrim.fail++;
        } else {
          pTask->stat.times.memTrim.succ++;
          if (trimed) {
            pTask->stat.bytes.memTrim.succ++;
          }
        }
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      case 9: { // malloc_align
        if (pTask->memIdx >= MPT_MAX_MEM_ACT_TIMES) {
          break;
        }
        
        pTask->pMemList[pTask->memIdx].p = mptMemoryMallocAlign(8, size);
        if (NULL == pTask->pMemList[pTask->memIdx].p) {
          pTask->stat.times.memMalloc.exec++;
          pTask->stat.bytes.memMalloc.exec+=size;        
          pTask->stat.times.memMalloc.fail++;
          pTask->stat.bytes.memMalloc.fail+=size;        
          uError("JOB:0x%x TASK:0x%x mpMallocAlign %d failed, error:%s", pJobCtx->jobId, pTask->taskId, size, tstrerror(terrno));
          return;
        }

        nsize = mptMemorySize(pTask->pMemList[pTask->memIdx].p);
        
        mptWriteMem(pTask->pMemList[pTask->memIdx].p, size);

        pTask->stat.times.memMalloc.exec++;
        pTask->stat.bytes.memMalloc.exec+=nsize;        
        
        pTask->stat.times.memMalloc.succ++;
        pTask->stat.bytes.memMalloc.succ+=nsize;        
        pTask->memIdx++;
        pTask->lastAct = actId;
        actDone = true;
        break;
      }
      default:
        TD_ALWAYS_ASSERT(0);
        break;
    }
  }
}

void mptSimulateTask(SMPTestJobCtx* pJobCtx, SMPTestTaskCtx* pTask, int32_t actTimes) {
  uDebug("JOB:0x%x TASK:0x%x will start total %d actions", pJobCtx->jobId, pTask->taskId, actTimes);
  
  for (int32_t i = 0; i < actTimes; ++i) {
    if (atomic_load_8(&pJobCtx->pJob->retired)) {
      uDebug("JOB:0x%x TASK:0x%x stop running cause of job already retired", pJobCtx->jobId, pTask->taskId);
      return;
    }

    //MPT_PRINTF("\tTASK:0x%x will start %d:%d actions\n", pTask->taskId, i, actTimes);
    
    mptSimulateAction(pJobCtx, pTask);
  }
}

void mptSimulateOutTask(int64_t targetSize) {
  SMPTestMemInfo* pCtx = &mptCtx.npMemList[mptCtx.npIdx];
  pCtx->size = targetSize;
  pCtx->p = taosMemMalloc(pCtx->size);
  if (NULL == pCtx->p) {
    uError("non-pool sim malloc %" PRId64 " failed", pCtx->size);
    pCtx->size = 0;
    return;
  }

  mptWriteMem(pCtx->p, pCtx->size);

  mptCtx.npIdx++;
}


void mptTaskRun(SMPTestJobCtx* pJobCtx, SMPTestTaskCtx* pCtx, int32_t idx, int32_t actTimes) {
  uDebug("JOB:0x%x TASK:0x%x start running", pJobCtx->jobId, pCtx->taskId);

  if (atomic_load_8(&pJobCtx->pJob->retired)) {
    uDebug("JOB:0x%x TASK:0x%x stop running cause of job already retired", pJobCtx->jobId, pCtx->taskId);
    return;
  }
  
  if (taosWTryLockLatch(&pCtx->taskExecLock)) {
    uDebug("JOB:0x%x TASK:0x%x stop running cause of task already running", pJobCtx->jobId, pCtx->taskId);
    return;
  }

  atomic_add_fetch_32(&pJobCtx->taskRunningNum, 1);

  if (mptCtx.param.enableMemPool) {
    mptEnableMemoryPoolUsage(gMemPoolHandle, pJobCtx->pSessions[idx]);
  }
  
  mptSimulateTask(pJobCtx, pCtx, actTimes);
  
  if (mptCtx.param.enableMemPool) {  
    mptDisableMemoryPoolUsage();
  }

  //mptSimulateOutTask(pJobCtx, pCtx);
  
  taosWUnLockLatch(&pCtx->taskExecLock);

  atomic_sub_fetch_32(&pJobCtx->taskRunningNum, 1);

  uDebug("JOB:0x%x TASK:0x%x end running", pJobCtx->jobId, pCtx->taskId);
}


void mptInitJobs() {
  int32_t jobNum = mptCtrl.jobNum ? mptCtrl.jobNum : MPT_MAX_JOB_NUM;

  memset(mptCtx.jobCtxs, 0, sizeof(*mptCtx.jobCtxs) * jobNum);
  mptCtx.totalTaskNum = 0;
  
  for (int32_t i = 0; i < jobNum; ++i) {
    mptInitJob(i);
  }
}

void mptCheckPoolUsedSize(int32_t jobNum) {
  int64_t usedSize = 0;
  bool needEnd = false;
  int64_t poolUsedSize = 0;
  int32_t sleepTimes = 0;

  while (true) {
    if (taosMemPoolTryLockPool(gMemPoolHandle, false)) {
      taosUsleep(1);
      continue;
    }
    
    taosMemPoolGetUsedSizeBegin(gMemPoolHandle, &usedSize, &needEnd);

    poolUsedSize = 0;
    
    for (int32_t i = 0; i < jobNum; ++i) {
      SMPTestJobCtx* pJobCtx = &mptCtx.jobCtxs[i];

      sleepTimes = 0;
      while (MPT_TRY_LOCK(MPT_READ, &pJobCtx->jobExecLock)) {
        taosUsleep(1);
        sleepTimes++;
        if (sleepTimes > 100) {
          break;
        }
      }

      if (sleepTimes > 100) {
        break;
      }

      if (NULL == pJobCtx->pJob) {
        MPT_UNLOCK(MPT_READ, &pJobCtx->jobExecLock);
        continue;
      }

      int64_t jobUsedSize = 0;
      for (int32_t m = 0; m < pJobCtx->taskNum; ++m) {
        if (!pJobCtx->taskCtxs[m].destoryed) {
          SMPStatDetail* pStat = NULL;
          int64_t allocSize = 0;
          taosMemPoolGetSessionStat(pJobCtx->pSessions[m], &pStat, &allocSize, NULL);
          int64_t usedSize = MEMPOOL_GET_USED_SIZE(pStat);
          
          TD_ALWAYS_ASSERT(allocSize == usedSize);
          TD_ALWAYS_ASSERT(0 == memcmp(pStat, &pJobCtx->taskCtxs[m].stat, sizeof(*pStat)));

          jobUsedSize += allocSize;
        }
      }
      
      TD_ALWAYS_ASSERT(pJobCtx->pJob->memInfo->allocMemSize == jobUsedSize);

      MPT_UNLOCK(MPT_READ, &pJobCtx->jobExecLock);

      poolUsedSize += jobUsedSize;
    }

    taosMemPoolGetUsedSizeEnd(gMemPoolHandle);

    if (sleepTimes > 100) {
      continue;
    }

    TD_ALWAYS_ASSERT(poolUsedSize <= usedSize);
    break;
  }  
}

void mptLaunchSingleTask(SMPTestThread* pThread, SMPTestJobCtx* pJobCtx, int32_t taskIdx, int32_t actTimes) {
  if (atomic_load_8(&pJobCtx->pJob->retired) || pJobCtx->taskCtxs[taskIdx].destoryed) {
    return;
  }
  
  MPT_PRINTF("Thread %d start to run %d:%d task\n", pThread->idx, taskIdx, pJobCtx->taskNum);
  mptTaskRun(pJobCtx, &pJobCtx->taskCtxs[taskIdx], taskIdx, actTimes);
  MPT_PRINTF("Thread %d end %d:%d task\n", pThread->idx, taskIdx, pJobCtx->taskNum);
    
}

void mptRunRandTasks(SMPTestThread* pThread) {
  int64_t runTaskTimes = mptCtx.totalTaskNum * MPT_DEFAULT_TASK_RUN_TIMES, taskExecIdx = 0;
  int32_t jobNum = mptCtrl.jobNum ? mptCtrl.jobNum : MPT_MAX_JOB_NUM;
  int32_t jobIdx = 0, taskIdx = 0, code = 0;
  SMPTestJobCtx* pJobCtx = NULL;

  MPT_PRINTF("Thread %d start the %d:%d exection - runTaskTimes:%" PRId64 "\n", pThread->idx, mptExecLoop, mptExecNum, runTaskTimes);

  while (runTaskTimes > 0) {
    int32_t actTimes = mptCtrl.taskActTimes ? mptCtrl.taskActTimes : ((taosRand() % 10) ? (taosRand() % (MPT_MAX_MEM_ACT_TIMES/10)) : (taosRand() % MPT_MAX_MEM_ACT_TIMES));
    jobIdx = taosRand() % jobNum;

    pJobCtx = &mptCtx.jobCtxs[jobIdx];

    if (mptResetJob(pJobCtx)) {
      continue;
    }

    if (MPT_TRY_LOCK(MPT_READ, &pJobCtx->jobExecLock)) {
      continue;
    }
    
    taskIdx = taosRand() % pJobCtx->taskNum;
    
    if (atomic_load_8(&pJobCtx->pJob->retired) || pJobCtx->taskCtxs[taskIdx].destoryed) {
      MPT_UNLOCK(MPT_READ, &pJobCtx->jobExecLock);
      continue;
    }
    
    MPT_PRINTF("Thread %d start to run %d:%d task\n", pThread->idx, taskExecIdx, runTaskTimes);
    mptTaskRun(pJobCtx, &pJobCtx->taskCtxs[taskIdx], taskIdx, actTimes);
    MPT_PRINTF("Thread %d end %d:%d task\n", pThread->idx, taskExecIdx, runTaskTimes);
    
    MPT_UNLOCK(MPT_READ, &pJobCtx->jobExecLock);

    runTaskTimes--;
    taskExecIdx++;
  }
  
}

void mptRunLoopJobs(SMPTestThread* pThread) {
  mptJobNum = (mptCtrl.jobNum) ? mptCtrl.jobNum : (taosRand() % MPT_MAX_JOB_NUM + 1);  

  MPT_PRINTF("Thread %d start the %d:%d exection - jobNum:%d\n", pThread->idx, mptExecLoop, mptExecNum, mptJobNum);

  for (int32_t i = 0; i < mptJobNum; ++i) {
    SMPTestJobCtx* pJobCtx = &mptCtx.jobCtxs[i];

    if (mptResetJob(pJobCtx)) {
      continue;
    }

    if (MPT_TRY_LOCK(MPT_READ, &pJobCtx->jobExecLock)) {
      continue;
    }
    
    MPT_PRINTF("  Thread %d start to run %d:%d job[%d:0x%" PRIx64 "]\n", pThread->idx, i, mptJobNum, pJobCtx->jobIdx, pJobCtx->jobId);
  
    for (int32_t m = 0; m < pJobCtx->taskNum; ++m) {
      if (atomic_load_8(&pJobCtx->pJob->retired)) {
        break;
      }
  
      int32_t actTimes = mptCtrl.taskActTimes ? mptCtrl.taskActTimes : ((taosRand() % 10) ? (taosRand() % (MPT_MAX_MEM_ACT_TIMES/10)) : (taosRand() % MPT_MAX_MEM_ACT_TIMES));
      mptLaunchSingleTask(pThread, pJobCtx, m, actTimes);
    }
  
    MPT_UNLOCK(MPT_READ, &pJobCtx->jobExecLock);
  
    MPT_PRINTF("  Thread %d end %dth JOB 0x%x exec, retired:%d\n", pThread->idx, pJobCtx->jobIdx, pJobCtx->jobId, pJobCtx->pJob->retired);
  }
}

void* mptRunThreadFunc(void* param) {
  SMPTestThread* pThread = (SMPTestThread*)param;
  mptExecNum = (mptCtrl.jobExecTimes) ? mptCtrl.jobExecTimes : taosRand() % MPT_MAX_JOB_LOOP_TIMES + 1;

  for (int32_t n = 0; n < mptExecNum; ++n) {
    mptExecLoop = n;

    if (mptCtx.param.randTask) {
      mptRunRandTasks(pThread);
    } else {
      mptRunLoopJobs(pThread);
    }

    MPT_PRINTF("Thread %d finish the %dth exection\n", pThread->idx, n);

    if (mptCtx.param.threadNum <= 1 && mptCtx.param.enableMemPool && tsMemPoolFullFunc) {
      mptCheckPoolUsedSize(mptJobNum);
    }
  }

  return NULL;
}

void* mptNonPoolThreadFunc(void* param) {
  int64_t targetSize = MPT_NON_POOL_ALLOC_UNIT;
  int64_t allocSize = 0;
  
  while (!atomic_load_8(&mptCtx.testDone)) {
    mptSimulateOutTask(targetSize);
    allocSize += targetSize;

    MPT_EPRINTF("%d:Non-pool malloc and write %" PRId64 " bytes, keep size:%" PRId64 "\n", mptCtx.npIdx - 1, targetSize, allocSize);
    taosUsleep(1);
    
    if ((mptCtx.npIdx * targetSize) >= (tsMinReservedMemorySize * 1048576UL * 10)) {
      for (int32_t i = 0; i < mptCtx.npIdx; ++i) {
        taosMemFreeClear(mptCtx.npMemList[i].p);
      }

      mptCtx.npIdx = 0;
      targetSize += MPT_NON_POOL_ALLOC_UNIT;
      allocSize = 0;
      taosMsleep(100);
    }
  }

  return NULL;
}


void* mptDropThreadFunc(void* param) {
  int32_t jobIdx = 0, taskIdx = 0, code = 0;
  uint64_t taskId = 0;
  int32_t jobNum = mptCtrl.jobNum ? mptCtrl.jobNum : MPT_MAX_JOB_NUM;
  
  while (!atomic_load_8(&mptCtx.testDone)) {
    taosMsleep(400);

    MPT_EPRINTF("%" PRId64 " - initJobs:%" PRId64 " retireJobs:%" PRId64 " destoryJobs:%" PRId64 " remainJobs:%" PRId64 "\n", taosGetTimestampMs(),
        mptCtx.runStat.initNum, mptCtx.runStat.retireNum, mptCtx.runStat.destoryNum, mptCtx.runStat.initNum - mptCtx.runStat.destoryNum);

    if (taosMemPoolTryLockPool(gMemPoolHandle, true)) {
      continue;
    }

    jobIdx = taosRand() % jobNum;
    SMPTestJobCtx* pJobCtx = &mptCtx.jobCtxs[jobIdx];
    MPT_LOCK(MPT_WRITE, &pJobCtx->jobExecLock);
    if (NULL == pJobCtx->pJob || pJobCtx->pJob->destroyed) {
      MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);
      taosMemPoolUnLockPool(gMemPoolHandle, true);
      continue;
    }

    if (taosRand() % 20) {
      taskIdx = taosRand() % pJobCtx->taskNum;
      if (pJobCtx->taskCtxs[taskIdx].destoryed) {
        MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);
        taosMemPoolUnLockPool(gMemPoolHandle, true);
        continue;
      }

      taskId = pJobCtx->taskCtxs[taskIdx].taskId;
      mptDestroyTask(pJobCtx, taskIdx);
      MPT_EPRINTF("Drop Thread destroy task %d:0x%" PRIx64 " in job %d:%" PRIx64 "\n", taskIdx, taskId, jobIdx, pJobCtx->jobId);
      
      MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);
    } else {
      code = mptDestroyJob(pJobCtx, false);
      if (0 == code) {
        MPT_EPRINTF("Drop Thread destroy job %d:%" PRIx64 "\n", jobIdx, pJobCtx->jobId);
      }
      MPT_UNLOCK(MPT_WRITE, &pJobCtx->jobExecLock);
    }

    taosMemPoolUnLockPool(gMemPoolHandle, true);
  }

  return NULL;
}


void mptStartRunThread(int32_t threadIdx) {
  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));
  ASSERT_EQ(0, taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE));
  mptCtx.threadCtxs[threadIdx].idx = threadIdx;
  ASSERT_EQ(0, taosThreadCreate(&mptCtx.threadCtxs[threadIdx].threadFp, &thattr, mptRunThreadFunc, &mptCtx.threadCtxs[threadIdx]));
  ASSERT_EQ(0, taosThreadAttrDestroy(&thattr));
}

void mptStartDropThread() {
  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));
  ASSERT_EQ(0, taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE));
  ASSERT_EQ(0, taosThreadCreate(&mptCtx.dropThreadFp, &thattr, mptDropThreadFunc, NULL));
  ASSERT_EQ(0, taosThreadAttrDestroy(&thattr));
}

void mptStartNonPoolThread() {
  TdThreadAttr thattr;
  ASSERT_EQ(0, taosThreadAttrInit(&thattr));
  ASSERT_EQ(0, taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE));
  ASSERT_EQ(0, taosThreadCreate(&mptCtx.nPoolThreadFp, &thattr, mptNonPoolThreadFunc, NULL));
  ASSERT_EQ(0, taosThreadAttrDestroy(&thattr));
}






void mptDestroyJobs() {
  int32_t jobNum = mptCtrl.jobNum ? mptCtrl.jobNum : MPT_MAX_JOB_NUM;
  
  for (int32_t i = 0; i < jobNum; ++i) {
    mptDestroyJob(&mptCtx.jobCtxs[i], false);
  }

  
}

void mptDestroyNonPoolCtx() {
  for (int32_t i = 0; i < mptCtx.npIdx; ++i) {
    taosMemFreeClear(mptCtx.npMemList[i].p);
  }
  taosMemFreeClear(mptCtx.npMemList);
}

void mptInitNonPoolCtx() {
  mptCtx.npMemList = (SMPTestMemInfo*)taosMemoryCalloc(MPT_MAX_NON_POOL_ALLOC_TIMES, sizeof(*mptCtx.npMemList));
  ASSERT_TRUE(NULL != mptCtx.npMemList);
}

void mptRunCase(SMPTestParam* param, int32_t times) {
  MPT_PRINTF("\t case start the %dth running\n", times);

  mptCaseLoop = times;
  memcpy(&mptCtx.param, param, sizeof(SMPTestParam));
  tsSingleQueryMaxMemorySize = param->jobQuota;
  
  atomic_store_8(&mptCtx.testDone, 0);
  mptCtx.initDone = false;

  mptInitPool();

  mptInitJobs();

  mptInitNonPoolCtx();

  mptCtx.initDone = true;

  for (int32_t i = 0; i < mptCtx.param.threadNum; ++i) {
    mptStartRunThread(i);
  }

  mptStartDropThread();
  mptStartNonPoolThread();

  for (int32_t i = 0; i < mptCtx.param.threadNum; ++i) {
    (void)taosThreadJoin(mptCtx.threadCtxs[i].threadFp, NULL);
  }  

  atomic_store_8(&mptCtx.testDone, 1);

  (void)taosThreadJoin(mptCtx.dropThreadFp, NULL);
  (void)taosThreadJoin(mptCtx.nPoolThreadFp, NULL);

  mptDestroyJobs();
  mptDestroyNonPoolCtx();

  taosMemPoolClose(gMemPoolHandle);

  while (gMemPoolHandle) {
    taosMsleep(10);
  }

  MPT_PRINTF("\t case end the %dth running\n", times);
}

void mptPrintTestBeginInfo(char* caseName, SMPTestParam* param) {
  MPT_PRINTF("Case [%s] begins:\n", caseName);
  MPT_PRINTF("\t case loop times:       %d\n", mptCtrl.caseLoopTimes);
  MPT_PRINTF("\t task max act times:    %d\n", mptCtrl.taskActTimes ? mptCtrl.taskActTimes : MPT_MAX_MEM_ACT_TIMES);
  MPT_PRINTF("\t max single alloc size: %" PRId64 "\n", mptCtrl.maxSingleAllocSize);
  MPT_PRINTF("\t job quota size:        %dMB\n", param->jobQuota);
  MPT_PRINTF("\t reserve mode:          %d\n", param->reserveMode);
  MPT_PRINTF("\t reserve size:          %dMB\n", param->reserveSize);
  MPT_PRINTF("\t test thread num:       %d\n", param->threadNum);
  MPT_PRINTF("\t random exec task:      %d\n", param->randTask);
}

void mptFreeAddrList(void** pList, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    TD_ALWAYS_ASSERT(pList[i]);
    taosMemFree(pList[i]);
  }
}

}  // namespace

#if 1

#if 0
TEST(PerfTest, GetSysAvail) {
  char* caseName = "PerfTest:GetSysAvail";
  int32_t code = 0;

  int64_t msize = 1048576UL*10240;
  char* p = (char*)taosMemMalloc(msize);
  int64_t st = taosGetTimestampUs();
  memset(p, 0, msize);
  int64_t totalUs = taosGetTimestampUs() - st;  
  printf("memset %" PRId64 " used time:%" PRId64 "us, speed:%dMB/ms\n", msize, totalUs, msize/1048576UL/(totalUs/1000UL));

  int64_t freeSize = 0;
  int32_t loopTimes = 1000000;
  st = taosGetTimestampUs();
  int64_t lt = st;
  for (int32_t i = 0; i < loopTimes; ++i) {
    code = taosGetSysAvailMemory(&freeSize);
    TD_ALWAYS_ASSERT(0 == code);
    //taosMsleep(1);
  }
  totalUs = taosGetTimestampUs() - st;
  
  printf("%d times getSysMemory total time:%" PRId64 "us, avg:%dus\n", loopTimes, totalUs, totalUs/loopTimes);
}
#endif

#if 0
TEST(MiscTest, monSysAvailSize) {
  char* caseName = "MiscTest:monSysAvailSize";
  int32_t code = 0;

  int64_t freeSize = 0;
  int32_t loopTimes = 1000000000;
  for (int32_t i = 0; i < loopTimes; ++i) {
    code = taosGetSysAvailMemory(&freeSize);
    TD_ALWAYS_ASSERT(0 == code);
    printf(" %" PRId64, freeSize);
    if (i && 0 == (i % 10)) {
      struct tm      Tm, *ptm;
      struct timeval timeSecs;

      TAOS_UNUSED(taosGetTimeOfDay(&timeSecs));
      time_t curTime = timeSecs.tv_sec;
      ptm = taosLocalTime(&curTime, &Tm, NULL, 0);

      printf("- %02d/%02d %02d:%02d:%02d.%06d \n", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec);
    }
    taosMsleep(1);
  }
}
#endif


#if 0
TEST(MiscTest, simNonPoolAct) {
  char* caseName = "MiscTest:simNonPoolAct";
  int64_t msize = 1048576UL*1024, asize = 0;
  int32_t loopTimes = 1000000;

  for (int32_t i = 0; i < loopTimes; ++i) {
    asize = taosRand() % msize;
    void* p = taosMemMalloc(asize);
    mptWriteMem(p, asize);

    taosMsleep(100);
    taosMemFree(p);
    
    printf("sim %dth alloc/free %" PRId64 " bytes\n", i, asize);
  }  
}
#endif


#if 0
TEST(PerfTest, allocLatency) {
  char* caseName = "PerfTest:allocLatency";
  int32_t code = 0;

  int64_t msize = 10;
  void* pSession = NULL;
  void* pJob = NULL;
  
  mptInitPool();

  memset(mptCtx.jobCtxs, 0, sizeof(*mptCtx.jobCtxs));

  TD_ALWAYS_ASSERT(0 == taosMemPoolCallocJob(0, 0, (void**)&pJob));
  TD_ALWAYS_ASSERT(0 == taosMemPoolInitSession(gMemPoolHandle, &pSession, pJob, "id"));

  int32_t loopTimes = 10000000;
  int64_t st = 0;
  void **addrList = (void**)taosMemCalloc(loopTimes, POINTER_BYTES);
  

  // MALLOC 

  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  int64_t totalUs3 = taosGetTimestampUs() - st;
  mptFreeAddrList(addrList, loopTimes);



  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  int64_t totalUs1 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  int64_t totalUs2 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);




  // CALLOC 

  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  int64_t totalUs11 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  int64_t totalUs12 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  int64_t totalUs13 = taosGetTimestampUs() - st;  
  //mptFreeAddrList(addrList, loopTimes);  NO FREE FOR REALLOC

  // REALLOC 

  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  int64_t totalUs21 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  int64_t totalUs22 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  int64_t totalUs23 = taosGetTimestampUs() - st;  
  mptFreeAddrList(addrList, loopTimes);


  // STRDUP 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  int64_t totalUs31 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  int64_t totalUs32 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  int64_t totalUs33 = taosGetTimestampUs() - st;
  mptFreeAddrList(addrList, loopTimes);

  // STRNDUP 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  int64_t totalUs41 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  int64_t totalUs42 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  int64_t totalUs43 = taosGetTimestampUs() - st;
  mptFreeAddrList(addrList, loopTimes);

  // ALIGNALLOC 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  int64_t totalUs51 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  int64_t totalUs52 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  int64_t totalUs53 = taosGetTimestampUs() - st;
  //mptFreeAddrList(addrList, loopTimes);  NO FREE FOR GETSIZE


  // GETSIZE 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }
  int64_t totalUs61 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }
  int64_t totalUs62 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }
  int64_t totalUs63 = taosGetTimestampUs() - st;

  // FREE 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }
  int64_t totalUs71 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }
  int64_t totalUs72 = taosGetTimestampUs() - st;
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  st = taosGetTimestampUs();
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }
  int64_t totalUs73 = taosGetTimestampUs() - st;

  
  printf("%d times each %" PRId64 " bytes, time consumed:\n"
      "\tnon-fpool malloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool malloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    malloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool calloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool calloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    calloc  total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool realloc total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool realloc total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    realloc total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool strdup  total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool strdup  total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    strdup  total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool strndup total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool strndup total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    strndup total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool alignal total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool alignal total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    alignal total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool getsize total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool getsize total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    getsize total time:%" PRId64 "us, avg:%fus\n"
      "\tnon-fpool free    total time:%" PRId64 "us, avg:%fus\n"
      "\tfull-pool free    total time:%" PRId64 "us, avg:%fus\n"
      "\tdirect    free    total time:%" PRId64 "us, avg:%fus\n",
      loopTimes, msize, 
      totalUs1, ((double)totalUs1)/loopTimes, totalUs2, ((double)totalUs2)/loopTimes, totalUs3, ((double)totalUs3)/loopTimes,
      totalUs11, ((double)totalUs11)/loopTimes, totalUs12, ((double)totalUs12)/loopTimes, totalUs13, ((double)totalUs13)/loopTimes,
      totalUs21, ((double)totalUs21)/loopTimes, totalUs22, ((double)totalUs22)/loopTimes, totalUs23, ((double)totalUs23)/loopTimes,
      totalUs31, ((double)totalUs31)/loopTimes, totalUs32, ((double)totalUs32)/loopTimes, totalUs33, ((double)totalUs33)/loopTimes,
      totalUs41, ((double)totalUs41)/loopTimes, totalUs42, ((double)totalUs42)/loopTimes, totalUs43, ((double)totalUs43)/loopTimes,
      totalUs51, ((double)totalUs51)/loopTimes, totalUs52, ((double)totalUs52)/loopTimes, totalUs53, ((double)totalUs53)/loopTimes,
      totalUs61, ((double)totalUs61)/loopTimes, totalUs62, ((double)totalUs62)/loopTimes, totalUs63, ((double)totalUs63)/loopTimes,
      totalUs71, ((double)totalUs71)/loopTimes, totalUs72, ((double)totalUs72)/loopTimes, totalUs73, ((double)totalUs73)/loopTimes);
}
#endif


#if 0
TEST(poolFuncTest, SingleThreadTest) {
  char* caseName = "poolFuncTest:SingleThreadTest";
  SMPTestParam param = {0};
  param.reserveMode = true; 
  param.threadNum = 1;
  param.jobQuota = 1024;
  param.enableMemPool = true;

  tsMemPoolFullFunc = 0;

  mptPrintTestBeginInfo(caseName, &param);

  for (int32_t i = 0; i < mptCtrl.caseLoopTimes; ++i) {
    mptRunCase(&param, i);
  }

}
#endif
#if 0
TEST(poolFuncTest, MultiThreadTest) {
  char* caseName = "poolFuncTest:MultiThreadTest";
  SMPTestParam param = {0};
  param.reserveMode = true; 
  param.threadNum = 6;
  param.jobQuota = 1024;
  param.randTask = true;
  param.enableMemPool = true;

  tsMemPoolFullFunc = 0;

  mptPrintTestBeginInfo(caseName, &param);

  for (int32_t i = 0; i < mptCtrl.caseLoopTimes; ++i) {
    mptRunCase(&param, i);
  }

}
#endif

#if 0
TEST(poolFullFuncTest, SingleThreadTest) {
  char* caseName = "poolFullFuncTest:SingleThreadTest";
  SMPTestParam param = {0};
  param.reserveMode = true; 
  param.threadNum = 1;
  param.jobQuota = 1024;
  param.enableMemPool = true;

  tsMemPoolFullFunc = 1;

  mptPrintTestBeginInfo(caseName, &param);

  for (int32_t i = 0; i < mptCtrl.caseLoopTimes; ++i) {
    mptRunCase(&param, i);
  }

}
#endif
#if 0
TEST(poolFullFuncTest, MultiThreadTest) {
  char* caseName = "poolFullFuncTest:MultiThreadTest";
  SMPTestParam param = {0};
  param.reserveMode = true; 
  param.threadNum = 6;
  param.jobQuota = 1024;
  param.randTask = true;
  param.enableMemPool = true;

  tsMemPoolFullFunc = 1;

  mptPrintTestBeginInfo(caseName, &param);

  for (int32_t i = 0; i < mptCtrl.caseLoopTimes; ++i) {
    mptRunCase(&param, i);
  }

}
#endif


#if 0
TEST(DisablePoolFuncTest, MultiThreadTest) {
  char* caseName = "FuncTest:MultiThreadTest";
  SMPTestParam param = {0};
  param.reserveMode = true; 
  param.threadNum = 6;
  param.jobQuota = 1024;
  param.randTask = true;
  param.enableMemPool = false;

  mptPrintTestBeginInfo(caseName, &param);

  for (int32_t i = 0; i < mptCtrl.caseLoopTimes; ++i) {
    mptRunCase(&param, i);
  }

}
#endif

#if 1
TEST(functionsTest, internalFunc) {
  char* caseName = "functionsTest:internalFunc";
  int32_t code = 0;

  int64_t msize = 10;
  void* pSession = NULL;
  void* pJob = NULL;
  
  mptInitPool();

  memset(mptCtx.jobCtxs, 0, sizeof(*mptCtx.jobCtxs));

  TD_ALWAYS_ASSERT(0 == taosMemPoolCallocJob(0, 0, (void**)&pJob));
  TD_ALWAYS_ASSERT(0 == taosMemPoolInitSession(gMemPoolHandle, &pSession, pJob, "id"));

  int32_t loopTimes = 1;
  int64_t st = 0;
  void **addrList = (void**)taosMemCalloc(loopTimes, POINTER_BYTES);
  

  // MALLOC 

  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  mptFreeAddrList(addrList, loopTimes);


  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  // CALLOC 

  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryCalloc(1, msize);
  }
  //mptFreeAddrList(addrList, loopTimes);  NO FREE FOR REALLOC

  // REALLOC 

  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  mptDisableMemoryPoolUsage();


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryRealloc(addrList[i], msize);
  }
  mptFreeAddrList(addrList, loopTimes);


  // STRDUP 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrdup("abc");
  }
  mptFreeAddrList(addrList, loopTimes);

  // STRNDUP 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptStrndup("abc", 3);
  }
  mptFreeAddrList(addrList, loopTimes);

  // ALIGNALLOC 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  mptDisableMemoryPoolUsage();
  mptFreeAddrList(addrList, loopTimes);


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMallocAlign(8, msize);
  }
  //mptFreeAddrList(addrList, loopTimes);  NO FREE FOR GETSIZE


  // GETSIZE 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }
  mptDisableMemoryPoolUsage();


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemorySize(addrList[i]);
  }

  // FREE 
  
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }
  mptDisableMemoryPoolUsage();


  for (int32_t i = 0; i < loopTimes; ++i) {
    addrList[i] = (char*)mptMemoryMalloc(msize);
  }
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryFree(addrList[i]);
  }

  // TRIM

  bool trimed = false;
  tsMemPoolFullFunc = 0;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryTrim(0, NULL);
    mptMemoryTrim(0, &trimed);
  }
  mptDisableMemoryPoolUsage();


  tsMemPoolFullFunc = 1;
  mptEnableMemoryPoolUsage(gMemPoolHandle, pSession);
  for (int32_t i = 0; i < loopTimes; ++i) {
    mptMemoryTrim(0, NULL);
    mptMemoryTrim(0, &trimed);
  }
  mptDisableMemoryPoolUsage();

  
}
#endif


#endif


int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  mptInit();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}





#pragma GCC diagnosti
