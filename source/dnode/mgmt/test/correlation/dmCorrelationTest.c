/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Exercise the private collector and callbacks without exporting test-only runtime APIs.
#define schedulerExecJob p14MockSchedulerExecJob
#define schedulerFetchRows p14MockSchedulerFetchRows
#define schedulerFreeJob p14MockSchedulerFreeJob
#include "../../node_mgmt/src/dmCorrelation.c"
#undef schedulerExecJob
#undef schedulerFetchRows
#undef schedulerFreeJob

#include <stdio.h>

int32_t mpCreateMgmtThread(void);

#define CHECK(condition)                                                    \
  do {                                                                      \
    if (!(condition)) {                                                     \
      fprintf(stderr, "check failed at line %d: %s\n", __LINE__, #condition); \
      return 1;                                                             \
    }                                                                       \
  } while (0)

static void ignoreQuotaReached(uint64_t jobId, uint64_t clientId, int32_t code) {
  (void)jobId;
  (void)clientId;
  (void)code;
}

typedef struct {
  SCorrelationCall* pCall;
  tsem_t            enteringWait;
  int32_t           code;
  bool              blocking;
} STimeoutWaitTest;

static int32_t signalTimeoutWait(void* pContext) {
  STimeoutWaitTest* pTest = pContext;
  return tsem_post(&pTest->enteringWait);
}

static void* waitForTimeout(void* pContext) {
  STimeoutWaitTest*     pTest = pContext;
  SCorrelationWaitHooks hooks = {.pContext = pTest, .beforeBlocking = signalTimeoutWait};
  pTest->code = dmWaitCorrelationCall(pTest->pCall, CORRELATION_WAIT_FETCH, &hooks, &pTest->blocking);
  return NULL;
}

static SRetrieveTableRsp* makeOneRowRsp(int32_t value) {
  SSDataBlock* pBlock = NULL;
  if (TSDB_CODE_SUCCESS != createDataBlock(&pBlock)) {
    return NULL;
  }
  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(value), 0);
  if (TSDB_CODE_SUCCESS != blockDataAppendColInfo(pBlock, &column) ||
      TSDB_CODE_SUCCESS != blockDataEnsureCapacity(pBlock, 1) ||
      TSDB_CODE_SUCCESS != colDataSetVal(taosArrayGet(pBlock->pDataBlock, 0), 0, (const char*)&value, false)) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  pBlock->info.rows = 1;
  int32_t capacity = blockGetEncodeSize(pBlock);
  SRetrieveTableRsp* pRsp = taosMemoryCalloc(1, sizeof(SRetrieveTableRsp) + PAYLOAD_PREFIX_LEN + capacity);
  if (NULL == pRsp) {
    blockDataDestroy(pBlock);
    return NULL;
  }
  int32_t len = blockEncode(pBlock, pRsp->data + PAYLOAD_PREFIX_LEN, capacity, 1);
  blockDataDestroy(pBlock);
  if (len <= 0) {
    taosMemoryFree(pRsp);
    return NULL;
  }
  SET_PAYLOAD_LEN(pRsp->data, len, len);
  pRsp->numOfRows = htobe64(1);
  pRsp->numOfCols = htonl(1);
  pRsp->numOfBlocks = htonl(1);
  pRsp->compLen = htonl(PAYLOAD_PREFIX_LEN + len);
  pRsp->payloadLen = htonl(PAYLOAD_PREFIX_LEN + len);
  return pRsp;
}

typedef enum {
  MOCK_EMPTY_MORE,
  MOCK_EMPTY_DONE,
  MOCK_ONE_MORE,
  MOCK_ONE_DONE,
  MOCK_REMOTE_ERROR,
  MOCK_PENDING_ONE,
} EMockFetchStep;

static struct {
  EMockFetchStep           steps[3];
  int32_t                  stepCount;
  int32_t                  fetchCount;
  int32_t                  releaseCount;
  bool                     submitError;
  bool                     asyncExec;
  bool                     gatedExec;
  bool                     failBeforeBlocking;
  bool                     delayedRelease;
  bool                     closeOnFetchWait;
  int32_t                  closeCode;
  bool                     releaseThreadStarted;
  bool                     callbackBeforePublication;
  bool                     lateFetchHasPayload;
  int32_t                  beforeBlockingCount;
  int32_t                  afterRecoverCount;
  int32_t                  lateFetchCount;
  tsem_t                   execGate;
  TdThread                 execThread;
  TdThread                 releaseThread;
  schedulerExecFp          execCallback;
  schedulerFetchFp         fetchCallback;
  SQueryPlan*              pPlan;
  SCorrelationTaskSession* pSession;
  void*                    pCallback;
  void*                    pFetchCallback;
  void                     (*releaseCallback)(void*);
} mockScheduler;

static void* p14MockAsyncExec(void* param) {
  (void)param;
  if (mockScheduler.gatedExec) {
    (void)tsem_wait(&mockScheduler.execGate);
  }
  mockScheduler.execCallback(NULL, mockScheduler.pCallback, TSDB_CODE_SUCCESS);
  return NULL;
}

static int32_t p14MockBeforeBlocking(void* param) {
  (void)param;
  ++mockScheduler.beforeBlockingCount;
  if (mockScheduler.closeOnFetchWait) {
    dmCloseCorrelationTaskSession(mockScheduler.pSession, mockScheduler.closeCode);
    return TSDB_CODE_SUCCESS;
  }
  (void)tsem_post(&mockScheduler.execGate);
  return mockScheduler.failBeforeBlocking ? TSDB_CODE_QRY_TASK_CANCELLED : TSDB_CODE_SUCCESS;
}

static void p14MockAfterRecover(void* param) {
  (void)param;
  ++mockScheduler.afterRecoverCount;
}

int32_t p14MockSchedulerExecJob(SSchedulerReq* pReq, int64_t* pJobId) {
  *pReq->pPlanTaken = true;
  mockScheduler.pPlan = pReq->pDag;
  mockScheduler.pCallback = pReq->cbParam;
  mockScheduler.releaseCallback = pReq->cbParamFreeFp;
  if (mockScheduler.asyncExec) {
    mockScheduler.execCallback = pReq->execFp;
    *pJobId = 1;
    return taosThreadCreate(&mockScheduler.execThread, NULL, p14MockAsyncExec, NULL);
  }
  mockScheduler.callbackBeforePublication = 0 == *pJobId && 0 == mockScheduler.fetchCount;
  pReq->execFp(NULL, pReq->cbParam,
               mockScheduler.submitError ? TSDB_CODE_QRY_JOB_NOT_EXIST : TSDB_CODE_SUCCESS);
  *pJobId = 1;
  return mockScheduler.submitError ? TSDB_CODE_QRY_JOB_NOT_EXIST : TSDB_CODE_SUCCESS;
}

int32_t p14MockSchedulerFetchRows(int64_t jobId, SSchedulerReq* pReq) {
  if (1 != jobId || mockScheduler.fetchCount >= mockScheduler.stepCount ||
      mockScheduler.fetchCount >= (int32_t)ARRAY_SIZE(mockScheduler.steps)) {
    return TSDB_CODE_QRY_INVALID_MSG;
  }
  EMockFetchStep step = mockScheduler.steps[mockScheduler.fetchCount++];
  if (MOCK_PENDING_ONE == step) {
    mockScheduler.fetchCallback = pReq->fetchFp;
    mockScheduler.pFetchCallback = pReq->cbParam;
    return TSDB_CODE_SUCCESS;
  }
  if (MOCK_REMOTE_ERROR == step) {
    pReq->fetchFp(NULL, pReq->cbParam, TSDB_CODE_QRY_JOB_NOT_EXIST);
    return TSDB_CODE_SUCCESS;
  }
  SRetrieveTableRsp* pRsp = (MOCK_ONE_MORE == step || MOCK_ONE_DONE == step)
                                  ? makeOneRowRsp(42) : taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
  if (NULL == pRsp) {
    return terrno;
  }
  pRsp->completed = MOCK_EMPTY_DONE == step || MOCK_ONE_DONE == step;
  pReq->fetchFp(pRsp, pReq->cbParam, TSDB_CODE_SUCCESS);  // Callback before fetch returns.
  return TSDB_CODE_SUCCESS;
}

static void* p14MockReleaseJob(void* param) {
  (void)param;
  if (mockScheduler.delayedRelease) {
    taosUsleep(5000);
  }
  qDestroyQueryPlan(mockScheduler.pPlan);
  mockScheduler.pPlan = NULL;
  (void)atomic_add_fetch_32(&mockScheduler.releaseCount, 1);
  mockScheduler.releaseCallback(mockScheduler.pCallback);
  return NULL;
}

void p14MockSchedulerFreeJob(int64_t* pJobId, int32_t code) {
  (void)code;
  if (mockScheduler.asyncExec) {
    (void)taosThreadJoin(mockScheduler.execThread, NULL);
  }
  *pJobId = 0;
  if (NULL != mockScheduler.fetchCallback) {
    SRetrieveTableRsp* pRsp = makeOneRowRsp(42);
    mockScheduler.lateFetchHasPayload = NULL != pRsp;
    mockScheduler.fetchCallback(pRsp, mockScheduler.pFetchCallback, TSDB_CODE_SUCCESS);
    ++mockScheduler.lateFetchCount;
  }
  if (mockScheduler.delayedRelease &&
      TSDB_CODE_SUCCESS == taosThreadCreate(&mockScheduler.releaseThread, NULL, p14MockReleaseJob, NULL)) {
    mockScheduler.releaseThreadStarted = true;
    return;
  }
  (void)p14MockReleaseJob(NULL);
}

static int32_t testCorrelationFetchSequences(void) {
  SQueryPlan* pPlan = NULL;
  CHECK(TSDB_CODE_SUCCESS == nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, (SNode**)&pPlan));
  pPlan->queryId = 1;
  SNode* pTemplateNode = NULL;
  CHECK(TSDB_CODE_SUCCESS == nodesMakeNode(QUERY_NODE_CORRELATION_TEMPLATE, &pTemplateNode));
  SCorrelationTemplateNode* pTemplate = (SCorrelationTemplateNode*)pTemplateNode;
  pTemplate->templateId = 0;
  pTemplate->resultType.type = TSDB_DATA_TYPE_INT;
  pTemplate->resultType.bytes = sizeof(int32_t);
  CHECK(TSDB_CODE_SUCCESS == nodesNodeToMsg((SNode*)pPlan, &pTemplate->pPlan, &pTemplate->planLen));
  qDestroyQueryPlan(pPlan);

  SCorrelationRuntime runtime = {.refs = 1};
  SCorrelationRootState root = {0};
  SCorrelationTaskSession session = {.refs = 1, .pRuntime = &runtime, .pRoot = &root};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&runtime.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&runtime.drained, 0, 0));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&session.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == nodesListMakeStrictAppend(&session.pTemplates, pTemplateNode));
  session.context.root.executionId = 1;
  session.context.root.queryId = 2;
  session.context.root.clientId = 3;
  session.context.task.queryId = 2;
  session.context.task.clientId = 3;
  session.context.task.taskId = 4;
  session.context.pTimezoneName = "UTC";
  session.context.pCharsetName = "UTF-8";
  SCorrelationScalarRequest request = {.templateId = 0};
  const struct {
    EMockFetchStep steps[3];
    int32_t       count;
    int32_t       expectedCode;
    bool          expectNull;
    bool          submitError;
    bool          asyncExec;
    bool          gatedExec;
    bool          delayedRelease;
    bool          closeDuringFetch;
    bool          timeoutDuringFetch;
  } cases[] = {
      {{MOCK_EMPTY_MORE, MOCK_EMPTY_DONE}, 2, TSDB_CODE_SUCCESS, true, false, false, false, false},
      {{MOCK_ONE_MORE, MOCK_EMPTY_DONE}, 2, TSDB_CODE_SUCCESS, false, false, false, false, false},
      {{MOCK_ONE_MORE, MOCK_ONE_DONE}, 2, TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS, false, false, false, false,
       false},
      {{MOCK_EMPTY_MORE, MOCK_ONE_MORE, MOCK_REMOTE_ERROR}, 3, TSDB_CODE_QRY_JOB_NOT_EXIST, false, false, false,
       false, false},
      {{MOCK_EMPTY_DONE}, 1, TSDB_CODE_SUCCESS, true, false, false, false, false},
      {{0}, 0, TSDB_CODE_QRY_JOB_NOT_EXIST, false, true, false, false, false},
      {{MOCK_EMPTY_DONE}, 1, TSDB_CODE_SUCCESS, true, false, true, false, false},
      {{MOCK_EMPTY_DONE}, 1, TSDB_CODE_SUCCESS, true, false, false, false, true},
      {{0}, 0, TSDB_CODE_QRY_TASK_CANCELLED, false, false, true, true, false},
      {{MOCK_EMPTY_DONE}, 1, TSDB_CODE_SUCCESS, true, false, true, true, false},
      {{MOCK_PENDING_ONE}, 1, TSDB_CODE_QRY_TASK_CANCELLED, false, false, false, false, false, true},
      {{MOCK_PENDING_ONE}, 1, TSDB_CODE_SCH_TIMEOUT_ERROR, false, false, false, false, false, true, true},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    for (int32_t attempt = 0; attempt < (cases[i].asyncExec && !cases[i].gatedExec ? 32 : 1); ++attempt) {
      memset(&mockScheduler, 0, sizeof(mockScheduler));
      memcpy(mockScheduler.steps, cases[i].steps, sizeof(mockScheduler.steps));
      mockScheduler.stepCount = cases[i].count;
      mockScheduler.submitError = cases[i].submitError;
      mockScheduler.asyncExec = cases[i].asyncExec;
      mockScheduler.gatedExec = cases[i].gatedExec;
      mockScheduler.failBeforeBlocking = cases[i].gatedExec && TSDB_CODE_SUCCESS != cases[i].expectedCode;
      mockScheduler.delayedRelease = cases[i].delayedRelease;
      mockScheduler.closeOnFetchWait = cases[i].closeDuringFetch;
      mockScheduler.closeCode = cases[i].timeoutDuringFetch ? TSDB_CODE_SCH_TIMEOUT_ERROR
                                                           : TSDB_CODE_QRY_TASK_CANCELLED;
      mockScheduler.pSession = &session;
      session.closed = false;
      session.closeCode = TSDB_CODE_SUCCESS;
      request.wait = (SCorrelationWaitHooks){0};
      if (cases[i].gatedExec || cases[i].closeDuringFetch) {
        if (cases[i].gatedExec) {
          CHECK(TSDB_CODE_SUCCESS == tsem_init(&mockScheduler.execGate, 0, 0));
        }
        request.wait.beforeBlocking = p14MockBeforeBlocking;
        request.wait.afterRecoverFromBlocking = p14MockAfterRecover;
      }
      SValueNode* pResult = NULL;
      CHECK(cases[i].expectedCode == dmInvokeCorrelationScalar(&session, &request, &pResult));
      CHECK(cases[i].asyncExec || mockScheduler.callbackBeforePublication);
      CHECK(cases[i].count == mockScheduler.fetchCount && 1 == atomic_load_32(&mockScheduler.releaseCount));
      CHECK(!cases[i].delayedRelease || mockScheduler.releaseThreadStarted);
      CHECK((cases[i].closeDuringFetch ? 1 : 0) == mockScheduler.lateFetchCount);
      CHECK(!cases[i].closeDuringFetch || mockScheduler.lateFetchHasPayload);
      CHECK(0 == root.activeCalls && 0 == runtime.activeCalls && 0 == runtime.blockingCalls);
      CHECK(NULL == session.pCalls && 1 == session.refs);
      CHECK(!(cases[i].gatedExec || cases[i].closeDuringFetch) ||
            (1 == mockScheduler.beforeBlockingCount &&
             (TSDB_CODE_SUCCESS == cases[i].expectedCode || cases[i].closeDuringFetch ? 1 : 0) ==
                 mockScheduler.afterRecoverCount));
      if (TSDB_CODE_SUCCESS == cases[i].expectedCode) {
        CHECK(NULL != pResult && pResult->isNull == cases[i].expectNull);
        CHECK(cases[i].expectNull || 42 == pResult->datum.i);
      } else {
        CHECK(NULL == pResult);
      }
      nodesDestroyNode((SNode*)pResult);
      if (mockScheduler.releaseThreadStarted) {
        CHECK(TSDB_CODE_SUCCESS == taosThreadJoin(mockScheduler.releaseThread, NULL));
      }
      if (cases[i].gatedExec) {
        CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&mockScheduler.execGate));
      }
    }
  }
  nodesDestroyList(session.pTemplates);
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&session.lock));
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&runtime.drained));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&runtime.lock));
  return 0;
}

static int32_t testCorrelationMemoryQuota(void) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  int32_t     quotaMb = 1;
  SMemPoolCfg poolCfg = {.reserveSize = 0, .jobQuota = &quotaMb, .chunkSize = 1048576,
                         .evicPolicy = E_EVICT_AUTO, .cb.reachFp = ignoreQuotaReached};
  void*       pPool = NULL;
  void*       pJob = NULL;
  int64_t     previousAvailableMemory = 0;
  bool        availableMemoryChanged = false;

  QUERY_CHECK_CONDITION(NULL == gMemPoolHandle, code, lino, _return, TSDB_CODE_FAILED);
  code = taosMemPoolOpen((char*)"dmCorrelationTest", &poolCfg, &pPool);
  QUERY_CHECK_CODE(code, lino, _return);
  gMemPoolHandle = pPool;
  code = mpCreateMgmtThread();
  QUERY_CHECK_CODE(code, lino, _return);
  code = taosMemPoolCallocJob(1, 1, &pJob);
  QUERY_CHECK_CODE(code, lino, _return);

  previousAvailableMemory = atomic_load_64(&tsCurrentAvailMemorySize);
  atomic_store_64(&tsCurrentAvailMemorySize, 16 * 1048576L);
  availableMemoryChanged = true;
  code = dmReserveCorrelationMemory(pJob, 512 * 1024);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_CONDITION(512 * 1024 == atomic_load_64(&((SMemPoolJob*)pJob)->allocMemSize), code, lino, _return,
                        TSDB_CODE_FAILED);
  int32_t reserveCode = dmReserveCorrelationMemory(pJob, 600 * 1024);
  QUERY_CHECK_CONDITION(TSDB_CODE_QRY_REACH_QMEM_THRESHOLD == reserveCode, code, lino, _return,
                        TSDB_CODE_FAILED);
  QUERY_CHECK_CONDITION(512 * 1024 == atomic_load_64(&((SMemPoolJob*)pJob)->allocMemSize), code, lino, _return,
                        TSDB_CODE_FAILED);
  taosMemPoolReleaseJobMemory(pPool, pJob, 512 * 1024);
  QUERY_CHECK_CONDITION(0 == atomic_load_64(&((SMemPoolJob*)pJob)->allocMemSize), code, lino, _return,
                        TSDB_CODE_FAILED);

_return:
  if (availableMemoryChanged) {
    atomic_store_64(&tsCurrentAvailMemorySize, previousAvailableMemory);
  }
  if (NULL != pJob) {
    int64_t allocated = atomic_load_64(&((SMemPoolJob*)pJob)->allocMemSize);
    taosMemPoolReleaseJobMemory(pPool, pJob, allocated);
    taosMemPoolReleaseJob(pJob);
  }
  if (NULL != pPool) {
    taosMemPoolClose(pPool);
  }
  if (gMemPoolHandle == pPool) {
    gMemPoolHandle = NULL;
  }
  return code;
}

int main(void) {
  CHECK(0 == testCorrelationFetchSequences());
  SCorrelationTemplateNode templ = {0};
  templ.resultType.type = TSDB_DATA_TYPE_INT;
  templ.resultType.bytes = sizeof(int32_t);
  int64_t rows = 0;
  SValueNode* pCandidate = NULL;
  SRetrieveTableRsp empty = {0};

  CHECK(TSDB_CODE_SUCCESS == dmCollectCorrelationBatch(&templ, &empty, &rows, &pCandidate));
  CHECK(0 == rows && NULL == pCandidate);
  SRetrieveTableRsp emptyDone = {.completed = true};
  CHECK(TSDB_CODE_SUCCESS == dmCollectCorrelationBatch(&templ, &emptyDone, &rows, &pCandidate));
  CHECK(0 == rows && NULL == pCandidate);

  SRetrieveTableRsp* pFirst = makeOneRowRsp(42);
  CHECK(NULL != pFirst);
  CHECK(TSDB_CODE_SUCCESS == dmCollectCorrelationBatch(&templ, pFirst, &rows, &pCandidate));
  CHECK(1 == rows && NULL != pCandidate && 42 == pCandidate->datum.i);
  CHECK(dmCorrelationCandidateSize(pCandidate) >= sizeof(SValueNode));
  taosMemoryFree(pFirst);

  SRetrieveTableRsp completed = {0};
  completed.completed = true;
  CHECK(TSDB_CODE_SUCCESS == dmCollectCorrelationBatch(&templ, &completed, &rows, &pCandidate));
  CHECK(1 == rows && 42 == pCandidate->datum.i);

  SRetrieveTableRsp second = {0};
  second.numOfRows = htobe64(1);
  CHECK(TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS ==
        dmCollectCorrelationBatch(&templ, &second, &rows, &pCandidate));
  CHECK(1 == rows && 42 == pCandidate->datum.i);
  nodesDestroyNode((SNode*)pCandidate);
  pCandidate = NULL;
  rows = 0;

  pFirst = makeOneRowRsp(42);
  SRetrieveTableRsp* pSecond = makeOneRowRsp(42);
  CHECK(NULL != pFirst && NULL != pSecond);
  pSecond->completed = true;
  CHECK(TSDB_CODE_SUCCESS == dmCollectCorrelationBatch(&templ, pFirst, &rows, &pCandidate));
  CHECK(1 == rows && 42 == pCandidate->datum.i);
  CHECK(TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS ==
        dmCollectCorrelationBatch(&templ, pSecond, &rows, &pCandidate));
  CHECK(1 == rows && 42 == pCandidate->datum.i);
  taosMemoryFree(pFirst);
  taosMemoryFree(pSecond);
  nodesDestroyNode((SNode*)pCandidate);
  pCandidate = NULL;

  SRetrieveTableRsp malformed = {0};
  malformed.numOfBlocks = htonl(1);
  malformed.compLen = htonl(PAYLOAD_PREFIX_LEN - 1);
  malformed.payloadLen = malformed.compLen;
  CHECK(TSDB_CODE_QRY_INVALID_MSG == dmCollectCorrelationBatch(&templ, &malformed, &rows, &pCandidate));

  SNode* pParamNode = NULL;
  CHECK(TSDB_CODE_SUCCESS == nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, &pParamNode));
  SCorrelationParamNode* pParam = (SCorrelationParamNode*)pParamNode;
  pParam->paramId = 0;
  pParam->node.resType.type = TSDB_DATA_TYPE_INT;
  pParam->node.resType.bytes = sizeof(int32_t);
  SCorrelationTemplateNode paramTemplate = {0};
  CHECK(TSDB_CODE_SUCCESS == nodesListMakeStrictAppend(&paramTemplate.pParams, pParamNode));
  SCorrelationTaskSession paramSession = {0};
  SArray* paramDefs[2] = {NULL, NULL};
  SArray* paramValues[2] = {NULL, NULL};
  const int32_t inputValues[2] = {101, 202};
  for (int32_t i = 0; i < 2; ++i) {
    SArray* pInput = taosArrayInit(1, sizeof(SCorrelationParamValue));
    CHECK(NULL != pInput);
    int32_t* pInputData = taosMemoryMalloc(sizeof(int32_t));
    CHECK(NULL != pInputData);
    *pInputData = inputValues[i];
    SCorrelationParamValue input = {
        .paramId = 0, .dataType = pParam->node.resType,
        .length = sizeof(int32_t), .pData = pInputData,
    };
    CHECK(NULL != taosArrayPush(pInput, &input));
    CHECK(TSDB_CODE_SUCCESS == dmBuildCorrelationParams(&paramSession, &paramTemplate, pInput,
                                                         &paramDefs[i], &paramValues[i]));
    taosArrayDestroyEx(pInput, dmFreeCorrelationParamValue);
  }
  for (int32_t i = 0; i < 2; ++i) {
    const SCorrelationWireValue* pValue = taosArrayGet(paramValues[i], 0);
    CHECK(NULL != pValue && NULL != pValue->pData);
    CHECK(inputValues[i] == *(const int32_t*)pValue->pData);
    taosArrayDestroy(paramDefs[i]);
    taosArrayDestroyEx(paramValues[i], dmFreeCorrelationWireValue);
  }
  nodesDestroyList(paramTemplate.pParams);

  SCorrelationCall call = {0};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&call.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&call.ready, 0, 0));
  call.state = CORRELATION_CALL_SUBMITTING;
  dmCorrelationExecCallback(NULL, &call, TSDB_CODE_SUCCESS);
  CHECK(call.execDone && CORRELATION_CALL_SUBMITTING == call.state);
  call.execDone = false;
  dmCorrelationExecCallback(NULL, &call, TSDB_CODE_QRY_JOB_NOT_EXIST);
  CHECK(call.execDone && TSDB_CODE_QRY_JOB_NOT_EXIST == call.code && CORRELATION_CALL_TERMINAL == call.state);
  dmCorrelationExecCallback(NULL, &call, TSDB_CODE_SUCCESS);
  CHECK(TSDB_CODE_QRY_JOB_NOT_EXIST == call.code);
  call.code = TSDB_CODE_SUCCESS;
  call.state = CORRELATION_CALL_FETCHING;
  call.fetchInFlight = true;
  dmCorrelationFetchCallback(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)), &call, TSDB_CODE_SUCCESS);
  CHECK(call.fetchDone && !call.fetchInFlight && NULL != call.pFetchRsp);
  call.state = CORRELATION_CALL_TERMINAL;
  dmCorrelationFetchCallback(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)), &call, TSDB_CODE_SUCCESS);
  CHECK(NULL != call.pFetchRsp && CORRELATION_CALL_TERMINAL == call.state);
  taosMemoryFree(call.pFetchRsp);
  call.pFetchRsp = NULL;
  call.fetchDone = false;
  call.fetchInFlight = true;
  call.state = CORRELATION_CALL_FETCHING;
  dmCorrelationFetchCallback(NULL, &call, TSDB_CODE_QRY_JOB_NOT_EXIST);
  CHECK(call.fetchDone && !call.fetchInFlight && CORRELATION_CALL_TERMINAL == call.state);
  CHECK(TSDB_CODE_QRY_JOB_NOT_EXIST == call.code && NULL == call.pFetchRsp);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&call.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&call.lock));

  SCorrelationCall earlyCall = {.state = CORRELATION_CALL_SUBMITTING};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&earlyCall.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&earlyCall.ready, 0, 0));
  dmCorrelationExecCallback(NULL, &earlyCall, TSDB_CODE_SUCCESS);
  CHECK(earlyCall.execDone && CORRELATION_CALL_SUBMITTING == earlyCall.state);
  earlyCall.jobId = 1;
  earlyCall.state = CORRELATION_CALL_JOB_PUBLISHED;
  bool blocked = false;
  SCorrelationWaitHooks wait = {0};
  CHECK(TSDB_CODE_SUCCESS == dmWaitCorrelationCall(&earlyCall, CORRELATION_WAIT_EXEC, &wait, &blocked));
  CHECK(!blocked && CORRELATION_CALL_JOB_PUBLISHED == earlyCall.state);
  earlyCall.state = CORRELATION_CALL_FETCHING;
  earlyCall.fetchInFlight = true;
  SRetrieveTableRsp* pEarlyRsp = taosMemoryCalloc(1, sizeof(SRetrieveTableRsp));
  CHECK(NULL != pEarlyRsp);
  pEarlyRsp->completed = true;
  dmCorrelationFetchCallback(pEarlyRsp, &earlyCall, TSDB_CODE_SUCCESS);
  CHECK(TSDB_CODE_SUCCESS == dmWaitCorrelationCall(&earlyCall, CORRELATION_WAIT_FETCH, &wait, &blocked));
  CHECK(!blocked && earlyCall.fetchDone && !earlyCall.fetchInFlight && earlyCall.pFetchRsp == pEarlyRsp);
  taosMemoryFree(earlyCall.pFetchRsp);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&earlyCall.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&earlyCall.lock));

  SCorrelationCall failedCall = {.state = CORRELATION_CALL_SUBMITTING};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&failedCall.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&failedCall.ready, 0, 0));
  dmCorrelationExecCallback(NULL, &failedCall, TSDB_CODE_QRY_JOB_NOT_EXIST);
  CHECK(TSDB_CODE_QRY_JOB_NOT_EXIST ==
        dmWaitCorrelationCall(&failedCall, CORRELATION_WAIT_EXEC, &wait, &blocked));
  CHECK(!blocked && failedCall.execDone && CORRELATION_CALL_TERMINAL == failedCall.state);
  dmCorrelationExecCallback(NULL, &failedCall, TSDB_CODE_SUCCESS);
  CHECK(TSDB_CODE_QRY_JOB_NOT_EXIST == failedCall.code && CORRELATION_CALL_TERMINAL == failedCall.state);
  failedCall.fetchInFlight = true;
  dmCorrelationFetchCallback(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)), &failedCall,
                             TSDB_CODE_SUCCESS);
  CHECK(NULL == failedCall.pFetchRsp && !failedCall.fetchDone);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&failedCall.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&failedCall.lock));

  SCorrelationRuntime runtime = {0};
  SCorrelationRootState root = {0};
  SCorrelationTaskSession session = {.pRuntime = &runtime, .pRoot = &root};
  SCorrelationCall limitedCall = {.pSession = &session};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&runtime.lock, NULL));
  root.activeCalls = CORRELATION_MAX_ROOT_CALLS;
  CHECK(TSDB_CODE_QRY_CORRELATION_CALL_LIMIT == dmReserveCorrelationCall(&limitedCall));
  SCorrelationRootState otherRoot = {0};
  SCorrelationTaskSession otherSession = {.pRuntime = &runtime, .pRoot = &otherRoot};
  SCorrelationCall otherCall = {.pSession = &otherSession};
  CHECK(TSDB_CODE_SUCCESS == dmReserveCorrelationCall(&otherCall));
  CHECK(CORRELATION_MAX_ROOT_CALLS == root.activeCalls && 1 == otherRoot.activeCalls);
  CHECK(1 == runtime.activeCalls && 1 == runtime.blockingCalls);
  dmReleaseCorrelationBlockingReservation(&otherCall);
  otherRoot.activeCalls = 0;
  runtime.activeCalls = 0;
  root.activeCalls = 0;
  runtime.blockingCalls = CORRELATION_MAX_BLOCKING_CALLS;
  CHECK(TSDB_CODE_QRY_CORRELATION_BLOCK_LIMIT == dmReserveCorrelationCall(&limitedCall));
  runtime.blockingCalls = 0;
  CHECK(TSDB_CODE_SUCCESS == dmReserveCorrelationCall(&limitedCall));
  CHECK(limitedCall.activeReserved && limitedCall.blockingReserved);
  CHECK(1 == root.activeCalls && 1 == runtime.activeCalls && 1 == runtime.blockingCalls);
  dmReleaseCorrelationBlockingReservation(&limitedCall);
  CHECK(!limitedCall.blockingReserved && 0 == runtime.blockingCalls);
  root.activeCalls = 0;
  runtime.activeCalls = 0;
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&runtime.lock));

  SCorrelationRuntime* pIdentityRuntime = taosMemCalloc(1, sizeof(SCorrelationRuntime));
  SCorrelationTaskSession* pIdentityA = taosMemCalloc(1, sizeof(SCorrelationTaskSession));
  SCorrelationTaskSession* pIdentityB = taosMemCalloc(1, sizeof(SCorrelationTaskSession));
  CHECK(NULL != pIdentityRuntime && NULL != pIdentityA && NULL != pIdentityB);
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pIdentityRuntime->lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&pIdentityRuntime->drained, 0, 0));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pIdentityA->lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pIdentityB->lock, NULL));
  pIdentityRuntime->refs = 1;
  pIdentityA->refs = 1;
  pIdentityB->refs = 1;
  pIdentityA->pRuntime = pIdentityRuntime;
  pIdentityB->pRuntime = pIdentityRuntime;
  pIdentityA->context.root = (SCorrelationRootKey){.executionId = 101, .queryId = 7, .clientId = 8, .generation = 1};
  pIdentityB->context.root = (SCorrelationRootKey){.executionId = 202, .queryId = 7, .clientId = 8, .generation = 1};
  pIdentityA->context.task = (SCorrelationTaskKey){.queryId = 7, .clientId = 8, .taskId = 1};
  pIdentityB->context.task = pIdentityA->context.task;
  CHECK(TSDB_CODE_SUCCESS == dmRegisterCorrelationSession(pIdentityRuntime, pIdentityA));
  CHECK(TSDB_CODE_SUCCESS == dmRegisterCorrelationSession(pIdentityRuntime, pIdentityB));
  CHECK(pIdentityA->pRoot != pIdentityB->pRoot);
  SCorrelationCall identityCallA = {0};
  SCorrelationCall identityCallB = {0};
  CHECK(TSDB_CODE_SUCCESS == dmAttachCorrelationCall(pIdentityA, &identityCallA, 1));
  CHECK(TSDB_CODE_SUCCESS == dmAttachCorrelationCall(pIdentityB, &identityCallB, 1));
  CHECK(identityCallA.key.parent.taskId == identityCallB.key.parent.taskId);
  CHECK(identityCallA.key.root.executionId != identityCallB.key.root.executionId);
  CHECK(identityCallA.key.sequence == 1 && identityCallB.key.sequence == 1);
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&identityCallA.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&identityCallB.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&identityCallA.ready, 0, 0));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&identityCallB.ready, 0, 0));
  identityCallA.state = CORRELATION_CALL_TERMINAL;
  identityCallA.code = TSDB_CODE_QRY_TASK_CANCELLED;
  identityCallA.fetchInFlight = true;
  identityCallB.state = CORRELATION_CALL_FETCHING;
  identityCallB.fetchInFlight = true;
  dmCorrelationFetchCallback(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)), &identityCallA, TSDB_CODE_SUCCESS);
  CHECK(NULL == identityCallA.pFetchRsp && TSDB_CODE_QRY_TASK_CANCELLED == identityCallA.code);
  CHECK(!identityCallB.fetchDone && NULL == identityCallB.pFetchRsp);
  dmCorrelationFetchCallback(taosMemoryCalloc(1, sizeof(SRetrieveTableRsp)), &identityCallB, TSDB_CODE_SUCCESS);
  CHECK(identityCallB.fetchDone && NULL != identityCallB.pFetchRsp && TSDB_CODE_SUCCESS == identityCallB.code);
  taosMemoryFree(identityCallB.pFetchRsp);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&identityCallA.ready));
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&identityCallB.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&identityCallA.lock));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&identityCallB.lock));
  pIdentityA->pCalls = NULL;
  pIdentityB->pCalls = NULL;
  dmReleaseCorrelationTaskSession(pIdentityA);
  dmReleaseCorrelationTaskSession(pIdentityB);
  CHECK(NULL == pIdentityRuntime->pRoots && NULL == pIdentityRuntime->pSessions);
  dmReleaseCorrelationRuntime(pIdentityRuntime);

  CHECK(TSDB_CODE_SUCCESS == testCorrelationMemoryQuota());

  SCorrelationRuntime* pRuntime = taosMemCalloc(1, sizeof(SCorrelationRuntime));
  SCorrelationRootState* pRoot = taosMemCalloc(1, sizeof(SCorrelationRootState));
  SCorrelationTaskSession* pSession = taosMemCalloc(1, sizeof(SCorrelationTaskSession));
  SCorrelationCall* pCall = taosMemCalloc(1, sizeof(SCorrelationCall));
  CHECK(NULL != pRuntime && NULL != pRoot && NULL != pSession && NULL != pCall);
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pRuntime->lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&pRuntime->drained, 0, 0));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pSession->lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&pCall->lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&pCall->ready, 0, 0));
  pRuntime->refs = 2;
  pRuntime->activeCalls = 1;
  pRuntime->blockingCalls = 1;
  pRuntime->pRoots = pRoot;
  pRuntime->pSessions = pSession;
  pRoot->refs = 1;
  pRoot->activeCalls = 1;
  pSession->refs = 1;
  pSession->pRuntime = pRuntime;
  pSession->pRoot = pRoot;
  pCall->refs = 1;
  pCall->pSession = pSession;
  pCall->activeReserved = true;
  pCall->blockingReserved = true;
  dmReleaseCorrelationCall(pCall);
  CHECK(0 == pRuntime->activeCalls && 0 == pRuntime->blockingCalls);
  CHECK(NULL == pRuntime->pRoots && NULL == pRuntime->pSessions);
  dmReleaseCorrelationRuntime(pRuntime);

  SCorrelationTaskSession closedSession = {0};
  SCorrelationCall closeCall = {.state = CORRELATION_CALL_JOB_PUBLISHED};
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&closedSession.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&closeCall.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&closeCall.ready, 0, 0));
  closedSession.pCalls = &closeCall;
  dmCloseCorrelationTaskSession(&closedSession, TSDB_CODE_QRY_TASK_CANCELLED);
  dmCloseCorrelationTaskSession(&closedSession, TSDB_CODE_QRY_JOB_NOT_EXIST);
  CHECK(closedSession.closed && closeCall.cancelled);
  CHECK(TSDB_CODE_QRY_TASK_CANCELLED == closedSession.closeCode);
  CHECK(TSDB_CODE_QRY_TASK_CANCELLED == closeCall.code);
  CHECK(TSDB_CODE_QRY_TASK_CANCELLED == dmWaitCorrelationCall(&closeCall, CORRELATION_WAIT_EXEC, &wait, &blocked));
  CHECK(!blocked);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&closeCall.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&closeCall.lock));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&closedSession.lock));

  SCorrelationTaskSession timeoutSession = {0};
  SCorrelationCall timeoutCall = {.state = CORRELATION_CALL_FETCHING};
  STimeoutWaitTest timeoutTest = {.pCall = &timeoutCall};
  TdThread timeoutThread;
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&timeoutSession.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexInit(&timeoutCall.lock, NULL));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&timeoutCall.ready, 0, 0));
  CHECK(TSDB_CODE_SUCCESS == tsem_init(&timeoutTest.enteringWait, 0, 0));
  timeoutSession.pCalls = &timeoutCall;
  CHECK(TSDB_CODE_SUCCESS == taosThreadCreate(&timeoutThread, NULL, waitForTimeout, &timeoutTest));
  CHECK(TSDB_CODE_SUCCESS == tsem_wait(&timeoutTest.enteringWait));
  dmCloseCorrelationTaskSession(&timeoutSession, TSDB_CODE_SCH_TIMEOUT_ERROR);
  CHECK(TSDB_CODE_SUCCESS == taosThreadJoin(timeoutThread, NULL));
  CHECK(TSDB_CODE_SCH_TIMEOUT_ERROR == timeoutTest.code && timeoutTest.blocking);
  CHECK(timeoutCall.cancelled && CORRELATION_CALL_TERMINAL == timeoutCall.state);
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&timeoutTest.enteringWait));
  CHECK(TSDB_CODE_SUCCESS == tsem_destroy(&timeoutCall.ready));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&timeoutCall.lock));
  CHECK(TSDB_CODE_SUCCESS == taosThreadMutexDestroy(&timeoutSession.lock));
  return 0;
}
