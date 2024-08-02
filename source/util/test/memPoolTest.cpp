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

#include "executor.h"
#include "executorInt.h"
#include "function.h"
#include "operator.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tvariant.h"
#include "stub.h"
#include "querytask.h"


namespace {

#define MP_PRINTF (void)printf

bool jtErrorRerun = false;
bool jtInRerun = false;
threadlocal void* mptThreadPoolHandle = NULL;
threadlocal void* mptThreadPoolSession = NULL;


#define taosEnableMemoryPoolUsage(_pool, _session) do { mptThreadPoolHandle = _pool; mptThreadPoolSession = _session; } while (0) 
#define taosDisableMemoryPoolUsage() (mptThreadPoolHandle = NULL) 
#define taosSaveDisableMemoryPoolUsage(_handle) do { (_handle) = mptThreadPoolHandle; mptThreadPoolHandle = NULL; } while (0)
#define taosRestoreEnableMemoryPoolUsage(_handle) (mptThreadPoolHandle = (_handle))

#define taosMemoryMalloc(_size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolMalloc(mptThreadPoolHandle, mptThreadPoolSession, _size, __FILE__, __LINE__)) : (taosMemMalloc(_size)))
#define taosMemoryCalloc(_num, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolCalloc(mptThreadPoolHandle, mptThreadPoolSession, _num, _size, __FILE__, __LINE__)) : (taosMemCalloc(_num, _size)))
#define taosMemoryRealloc(_ptr, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolRealloc(mptThreadPoolHandle, mptThreadPoolSession, _ptr, _size, __FILE__, __LINE__)) : (taosMemRealloc(_ptr, _size)))
#define taosStrdup(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolStrdup(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosStrdupi(_ptr)))
#define taosMemoryFree(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolFree(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemFree(_ptr)))
#define taosMemorySize(_ptr) ((NULL != mptThreadPoolHandle) ? (taosMemPoolGetMemorySize(mptThreadPoolHandle, mptThreadPoolSession, _ptr, __FILE__, __LINE__)) : (taosMemSize(_ptr)))
#define taosMemoryTrim(_size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolTrim(mptThreadPoolHandle, mptThreadPoolSession, _size, __FILE__, __LINE__)) : (taosMemTrim(_size)))
#define taosMemoryMallocAlign(_alignment, _size) ((NULL != mptThreadPoolHandle) ? (taosMemPoolMallocAlign(mptThreadPoolHandle, mptThreadPoolSession, _alignment, _size, __FILE__, __LINE__)) : (taosMemMallocAlign(_alignment, _size)))


typedef struct SMPTJobInfo {
  int8_t              retired;
  int32_t             errCode;
  SMemPoolJob*        memInfo;
  SHashObj*           pSessions;
} SMPTJobInfo;


typedef struct {
  bool printTestInfo;
  bool printInputRow;
} SJoinTestCtrl;


typedef struct SMPTestCtx {
  SHashObj* pJobs;
  BoundedQueue* pJobQueue;
  void*        memPoolHandle;
} SMPTestCtx;

SMPTestCtx mptCtx = {0};


void rerunBlockedHere() {
  while (jtInRerun) {
    taosSsleep(1);
  }
}


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

void mptInitLogFile() {
  const char   *defaultLogFileNamePrefix = "mplog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  TAOS_STRCPY(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    JT_PRINTF("failed to open log file in directory:%s\n", tsLogDir);
  }
}

static bool mptJobMemSizeCompFn(void* l, void* r, void* param) {
  SQWJobInfo* left = (SQWJobInfo*)l;
  SQWJobInfo* right = (SQWJobInfo*)r;
  if (atomic_load_8(&right->retired)) {
    return true;
  }
  
  return atomic_load_64(&right->memInfo->allocMemSize) < atomic_load_64(&left->memInfo->allocMemSize);
}


void mptInit() {
  mptInitLogFile();

  mptCtx.pJobs = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  ASSERT_TRUE(NULL != mptCtx.pJobs);

  mptCtx.pJobQueue = createBoundedQueue(10000, qwJobMemSizeCompFn, NULL, NULL);
  ASSERT_TRUE(NULL != mptCtx.pJobQueue);

}

void mptRetireJob(SQWJobInfo* pJob) {
  //TODO
}


void mptCheckUpateCfgCb(void* pHandle, void* cfg) {
  SMemPoolCfg* pCfg = (SMemPoolCfg*)cfg;
  int64_t newJobQuota = tsSingleQueryMaxMemorySize * 1048576UL;
  if (pCfg->jobQuota != newJobQuota) {
    atomic_store_64(&pCfg->jobQuota, newJobQuota);
  }
  
  int64_t maxSize = 0;
  bool autoMaxSize = false;
  int32_t code = qwGetQueryMemPoolMaxSize(&maxSize, &autoMaxSize);
  if (TSDB_CODE_SUCCESS != code) {
    pCfg->maxSize = 0;
    qError("get query memPool maxSize failed, reset maxSize to %" PRId64, pCfg->maxSize);
    return;
  }
  
  if (pCfg->autoMaxSize != autoMaxSize || pCfg->maxSize != maxSize) {
    pCfg->autoMaxSize = autoMaxSize;
    atomic_store_64(&pCfg->maxSize, maxSize);
    taosMemPoolCfgUpdate(pHandle, pCfg);
  }
}

void mptLowLevelRetire(int64_t retireSize, int32_t errCode) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, NULL);
  while (pJob) {
    int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);
    if (aSize >= retireSize && 0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      mptRetireJob(pJob);

      qDebug("QID:0x%" PRIx64 " job retired cause of low level memory retire, usedSize:%" PRId64 ", retireSize:%" PRId64, 
          pJob->memInfo->jobId, aSize, retireSize);
          
      taosHashCancelIterate(mptCtx.pJobs, pJob);
      break;
    }
    
    pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, pJob);
  }
}

void mptMidLevelRetire(int64_t retireSize, int32_t errCode) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobInfo, NULL);
  PriorityQueueNode qNode;
  while (NULL != pJob) {
    if (0 == atomic_load_8(&pJob->retired)) {
      qNode.data = pJob;
      (void)taosBQPush(mptCtx.pJobQueue, &qNode);
    }
    
    pJob = (SMPTJobInfo*)taosHashIterate(mptCtx.pJobs, pJob);
  }

  PriorityQueueNode* pNode = NULL;
  int64_t retiredSize = 0;
  while (retiredSize < retireSize) {
    pNode = taosBQTop(mptCtx.pJobQueue);
    if (NULL == pNode) {
      break;
    }

    pJob = (SMPTJobInfo*)pNode->data;
    if (atomic_load_8(&pJob->retired)) {
      taosBQPop(mptCtx.pJobQueue);
      continue;
    }

    if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
      int64_t aSize = atomic_load_64(&pJob->memInfo->allocMemSize);

      mptRetireJob(pJob);

      qDebug("QID:0x%" PRIx64 " job retired cause of mid level memory retire, usedSize:%" PRId64 ", retireSize:%" PRId64, 
          pJob->memInfo->jobId, aSize, retireSize);

      retiredSize += aSize;    
    }

    taosBQPop(mptCtx.pJobQueue);
  }

  taosBQClear(mptCtx.pJobQueue);
}


void mptRetireJobsCb(int64_t retireSize, bool lowLevelRetire, int32_t errCode) {
  (lowLevelRetire) ? mptLowLevelRetire(retireSize, errCode) : mptMidLevelRetire(retireSize, errCode);
}


void mptRetireJobCb(SMemPoolJob* mpJob, int32_t errCode) {
  SMPTJobInfo* pJob = (SMPTJobInfo*)taosHashGet(mptCtx.pJobs, &mpJob->jobId, sizeof(mpJob->jobId));
  if (NULL == pJob) {
    qError("QID:0x%" PRIx64 " fail to get job from job hash", mpJob->jobId);
    return;
  }

  if (0 == atomic_val_compare_exchange_32(&pJob->errCode, 0, errCode) && 0 == atomic_val_compare_exchange_8(&pJob->retired, 0, 1)) {
    qwRetireJob(pJob);

    qInfo("QID:0x%" PRIx64 " retired directly, errCode: 0x%x", mpJob->jobId, errCode);
  } else {
    qDebug("QID:0x%" PRIx64 " already retired, retired: %d, errCode: 0x%x", mpJob->jobId, atomic_load_8(&pJob->retired), atomic_load_32(&pJob->errCode));
  }
}

int32_t mptInitJobInfo(uint64_t qId, SMPTJobInfo* pJob) {
  pJob->pSessions= taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->pSessions) {
    qError("fail to init session hash, code: 0x%x", terrno);
    return terrno;
  }

  int32_t code = taosMemPoolCallocJob(qId, (void**)&pJob->memInfo);
  if (TSDB_CODE_SUCCESS != code) {
    taosHashCleanup(pJob->pSessions);
    pJob->pSessions = NULL;
    return code;
  }

  return code;
}


int32_t mptInitSession(uint64_t qId, uint64_t tId, int32_t eId, void** ppSession) {
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
        qwDestroyJobInfo(&jobInfo);
        if (TSDB_CODE_DUP_KEY == code) {
          code = TSDB_CODE_SUCCESS;
          continue;
        }
        
        return code;
      }

      pJob = (SMPTJobInfo*)taosHashAcquire(mptCtx.pJobs, &qId, sizeof(qId));
      if (NULL == pJob) {
        qError("QID:0x%" PRIx64 " not in joj hash, may be dropped", qId);
        return TSDB_CODE_QRY_JOB_NOT_EXIST;
      }
    }

    break;
  }

  QW_ERR_JRET(taosMemPoolInitSession(mptCtx.memPoolHandle, ppSession, pJob->memInfo));

  char id[sizeof(tId) + sizeof(eId)] = {0};
  QW_SET_TEID(id, tId, eId);

  code = taosHashPut(pJob->pSessions, id, sizeof(id), ppSession, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS != code) {
    qError("fail to put session into query session hash, code: 0x%x", code);
    QW_ERR_JRET(code);
  }

_return:

  if (NULL != pJob) {
    taosHashRelease(mptCtx.pJobs, pJob);
  }

  return code;
}


void mptInitPool(int64_t jobQuota, bool autoMaxSize, int64_t maxSize) {
  SMemPoolCfg cfg = {0};

  cfg.autoMaxSize = autoMaxSize;
  if (!autoMaxSize) {
    cfg.maxSize = maxSize;
  } else {
    int64_t memSize = 0;
    ASSERT_TRUE(0 == taosGetSysAvailMemory(&memSize));
    cfg.maxSize = memSize * 0.8;
  }
  cfg.threadNum = 10; //TODO
  cfg.evicPolicy = E_EVICT_AUTO; //TODO
  cfg.jobQuota = jobQuota;
  cfg.cb.retireJobsFp = mptRetireJobsCb;
  cfg.cb.retireJobFp  = mptRetireJobCb;
  cfg.cb.cfgUpdateFp = mptCheckUpateCfgCb;

  ASSERT_TRUE(0 == taosMemPoolOpen("SingleThreadTest", &cfg, &mptCtx.memPoolHandle));
}

}  // namespace

#if 1
#if 1
TEST(FuncTest, SingleThreadTest) {
  SJoinTestParam param;
  char* caseName = "FuncTest:SingleThreadTest";
  void* pSession = NULL;
  
  mptInitPool(0, false, 5*1048576UL);

  ASSERT_TRUE(0 == mptInitSession(1, 1, 1, &pSession));

  
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
