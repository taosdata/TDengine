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

#include "streamInt.h"
#include "streamMetric.h"


SStmMsgConfig gStmReaderMsg[] = {
  {ESTMM_READER_FETCH_FROM_READER, ""},
  {ESTMM_READER_PULL_SET_TABLE, ""},
  {ESTMM_READER_PULL_LAST_TS, ""},
  {ESTMM_READER_PULL_FIRST_TS, ""},
  {ESTMM_READER_PULL_TSDB_META, ""},
  {ESTMM_READER_PULL_TSDB_META_NEXT, ""},
  {ESTMM_READER_PULL_TSDB_TS_DATA, ""},
  {ESTMM_READER_PULL_TSDB_TRIGGER_DATA, ""},
  {ESTMM_READER_PULL_TSDB_TRIGGER_DATA_NEXT, ""},
  {ESTMM_READER_PULL_TSDB_CALC_DATA, ""},
  {ESTMM_READER_PULL_TSDB_CALC_DATA_NEXT, ""},
  {ESTMM_READER_PULL_TSDB_DATA, ""},
  {ESTMM_READER_PULL_TSDB_DATA_NEXT, ""},
  {ESTMM_READER_PULL_WAL_META, ""},
  {ESTMM_READER_PULL_WAL_TS_DATA, ""},
  {ESTMM_READER_PULL_WAL_TRIGGER_DATA, ""},
  {ESTMM_READER_PULL_WAL_CALC_DATA, ""},
  {ESTMM_READER_PULL_WAL_DATA, ""},
  {ESTMM_READER_PULL_GROUP_COL_VALUE, ""},
  {ESTMM_READER_PULL_VTABLE_INFO, ""},
  {ESTMM_READER_PULL_VTABLE_PSEUDO_COL, ""},
  {ESTMM_READER_PULL_OTABLE_INFO, ""},
  {ESTMM_READER_PULL_TYPE_MAX, ""},
};

SStmMsgConfig gStmRunnerMsg[] = {
  //{ESTMM_FETCH_FROM_READER, "FetchFromReader"},
};


SStmMsgConfig gStmTriggerMsg[] = {
  //{ESTMM_FETCH_FROM_READER, "FetchFromReader"},
};


SStmMsgConfig gStmMnodeMsg[] = {
  {ESTMM_MNODE_HB, "streamHb"},
  {ESTMM_MNODE_CREATE_STREAM, "createStream"},
  {ESTMM_MNODE_DROP_STREAM, "dropStream"},
  {ESTMM_MNODE_START_STREAM, "startStream"},
  {ESTMM_MNODE_STOP_STREAM, "stopStream"},
  {ESTMM_MNODE_RECALC_STREAM, "recalcStream"},
};


SStmMsgConfig gStmDnodeMsg[] = {
  {ESTMM_DNODE_MGMT_HB_REQ, "streamHbReq"},
  {ESTMM_DNODE_MGMT_HB_RSP, "streamHbRsp"},
};

SStmOpeConfig gStmOps[] = {
  {ESTMM_OP_CREATE_STM, "createStream"},
  {ESTMM_OP_STOP_STM, "stopStream"},
  {ESTMM_OP_START_STM, "startStream"},
  {ESTMM_OP_RECALC_STM, "recalcStream"},
};

SStmMetricConfig gStmTrigMetrics[] = {
  {ESTMM_DATA_PULL_WAIT_TIME, "DataPullWaitTime", 0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_TRIG_CHECK_TIME,     "TrigCheckTime",    0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_CALC_REQ_WAIT_TIME,  "CalcReqWaitTime",  0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_PULL_NUM,       "DataPullNum",      0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_CALC_PARAM_NUM,      "CalcParamNum",     0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_CALC_REQ_NUM,        "CalcReqNum",       0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_TRIG_MSG_PROC_TIME,    "MsgProcessTime", tListLen(gStmTriggerMsg), STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
};


SStmMetricConfig gStmReaderMetrics[] = {
  {ESTMM_WAL_PROC_PROGRESS,    "WalProcessProgress", tListLen(gStmReaderMsg),  STMM_READER, STMM_PERIOD_LAST_TIME, STMM_METRIC_ACTUAL},
  {ESTMM_WAL_BLOCK_NUM,        "WalBlockNum",        0, STMM_READER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_WAL_BLOCK_ROW_NUM,    "WalBlockRowNum",     0, STMM_READER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_WAL_READ_NUM,         "WalReadNum",         tListLen(gStmReaderMsg),  STMM_READER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_READER_MSG_PROC_TIME, "MsgProcessTime",    tListLen(gStmReaderMsg), STMM_READER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
};

SStmMetricConfig gStmRunnerMetrics[] = {
  {ESTMM_DATA_INSERT_ROW_NUM, "DataInsertRowNum", 0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_INSERT_TIME,    "DataInsertTime",   0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_NOTIFY_USED_TIME,    "NotifyUsedTime",   0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_INSERT_NUM,     "DataInsertNum",    0, STMM_RUNNER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_NOTIFY_NUM,          "NotifyNum",        2,  STMM_RUNNER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_RUNNER_MSG_PROC_TIME,     "MsgProcessTime",  tListLen(gStmRunnerMsg), STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
};


SStmMetricConfig gStmMnodeMetrics[] = {
  {ESTMM_STREAM_DEPLOY_NUM,  "StreamDeployNum",      0, STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_ERROR_NUM,   "StreamErrorNum",       0, STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_OPERATIONS,  "StreamOperations",     tListLen(gStmOps),  STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_STATUS_TIME, "StreamStatusTime",     tListLen(gStreamStatusStr),  STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_LAST_OPES,   "StreamLastOperations", 0, STMM_MNODE, STMM_PERIOD_LAST_NTIME,    STMM_METRIC_ACTUAL},
  {ESTMM_MND_MSG_PROC_TIME,     "MsgProcessTime",     tListLen(gStmMnodeMsg), STMM_MNODE, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
};

SStmMetricConfig gStmDnodeMetrics[] = {
  {ESTMM_THREAD_PROC_BEGIN_TS, "ThreadProcBeginTs", -1, STMM_DNODE, STMM_PERIOD_LAST_TIME,     STMM_METRIC_ACTUAL},
  {ESTMM_THREAD_PROC_END_TS,   "ThreadProcEndTs",   -1, STMM_DNODE, STMM_PERIOD_LAST_TIME,     STMM_METRIC_ACTUAL},
  {ESTMM_DNODE_MSG_PROC_TIME,     "MsgProcessTime",    tListLen(gStmDnodeMsg), STMM_DNODE, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_MSG_IN_QUEUE_NUM,     "MsgInQueueNum",     -1, STMM_DNODE, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_MSG_RECV_NUM,         "MsgRecvNum",        tListLen(gStmDnodeMsg), STMM_DNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
};

int32_t stmmGetModuleCfg(SStmMetricConfig** ppCfg, int32_t module, int32_t* metricNum) {
  int32_t code = 0;
  
  switch (module) {
    case STMM_TRIGGER:
      *ppCfg = gStmTrigMetrics;
      *metricNum = tListLen(gStmTrigMetrics);
      break;
    case STMM_READER:
      *ppCfg = gStmReaderMetrics;
      *metricNum = tListLen(gStmReaderMetrics);
      break;
    case STMM_RUNNER:
      *ppCfg = gStmRunnerMetrics;
      *metricNum = tListLen(gStmRunnerMetrics);
      break;
    case STMM_MNODE:
      *ppCfg = gStmMnodeMetrics;
      *metricNum = tListLen(gStmMnodeMetrics);
      break;
    case STMM_DNODE:
      *ppCfg = gStmDnodeMetrics;
      *metricNum = tListLen(gStmDnodeMetrics);
      break;
    default:
      stError("invalid metric module %d", module);
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
  }

_exit:

  return code;
}

void stmmDestroySStmActualValue(SStmActualValue* pActual, int32_t num) {
  if (NULL == pActual) {
    return;
  }
  
  for (int32_t i = 0; i < num; ++i) {
    taosMemoryFree((pActual + i)->pVal);
  }

  taosMemoryFree(pActual);
}

int32_t stmmBuildInitActualValue(SStmActualValue** ppRes, SStmMetricConfig* pCfg, int32_t keepNum) {
  int32_t code = 0, lino = 0;
  SStmActualValue* pActual = taosMemoryCalloc(1, pCfg->subtype_num * sizeof(SStmActualValue));
  TSDB_CHECK_NULL(pActual, code, lino, _exit, terrno);
  
  for (int32_t n = 0; n < pCfg->subtype_num; ++n) {
    (pActual + n)->pVal = taosMemoryCalloc(1, keepNum * sizeof(int64_t));
    TSDB_CHECK_NULL((pActual + n)->pVal, code, lino, _exit, terrno);
    (pActual + n)->size = keepNum;
  }

  *ppRes = pActual;

_exit:  

  if (code) {
    stmmDestroySStmActualValue(pActual, pCfg->subtype_num);
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}


void stmmDestroySStmTimeDuraStat(SStmTimeDuraStat* pStat, int32_t num) {
  if (NULL == pStat) {
    return;
  }
  
  for (int32_t i = 0; i < num; ++i) {
    taosMemoryFree((pStat + i)->pVal);
  }

  taosMemoryFree(pStat);
}


int32_t stmmBuildInitDuraStat(SStmTimeDuraStat** ppRes, SStmMetricConfig* pCfg, int32_t keepNum) {
  int32_t code = 0, lino = 0;
  SStmTimeDuraStat* pStat = taosMemoryCalloc(1, pCfg->subtype_num * sizeof(SStmTimeDuraStat));
  TSDB_CHECK_NULL(pStat, code, lino, _exit, terrno);
  
  for (int32_t n = 0; n < pCfg->subtype_num; ++n) {
    (pStat + n)->pVal = taosMemoryCalloc(1, keepNum * sizeof(SStmStatValue));
    TSDB_CHECK_NULL((pStat + n)->pVal, code, lino, _exit, terrno);
    (pStat + n)->currDay = atomic_load_32(&gStreamMgmt.currDay);
    (pStat + n)->daysNum = keepNum;
    (pStat + n)->daysIdx = 1;
  }

  *ppRes = pStat;

_exit:  

  if (code) {
    stmmDestroySStmTimeDuraStat(pStat, pCfg->subtype_num);
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}



void stmmDestroyCumuValue(int64_t* pValue, int32_t num) {
  if (NULL == pValue) {
    return;
  }
  
  taosMemoryFree(pValue);
}

int32_t stmmBuildInitCumuValue(int64_t** ppRes, SStmMetricConfig* pCfg, int32_t keepNum) {
  int32_t code = 0, lino = 0;
  int64_t* pCumu = taosMemoryCalloc(1, pCfg->subtype_num * keepNum * sizeof(int64_t));
  TSDB_CHECK_NULL(pCumu, code, lino, _exit, terrno);

  *ppRes = pCumu;

_exit:  

  if (code) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

void stmmDestroyMetric(void* param) {
  if (NULL == param) {
    return;
  }
  SStmMetric* pMetric = (SStmMetric*)param;
  if (pMetric->pCfg->metric_type & STMM_METRIC_ACTUAL) {
    stmmDestroySStmActualValue(((SStmActualMetric*)pMetric)->pActual, pMetric->pCfg->subtype_num);
  }
  if (pMetric->pCfg->metric_type & STMM_METRIC_STAT) {
    stmmDestroySStmTimeDuraStat(((SStmActualStatMetric*)pMetric)->pStat, pMetric->pCfg->subtype_num);
  }
  if (pMetric->pCfg->metric_type & STMM_METRIC_CUMU) {
    stmmDestroyCumuValue(((SStmCumuMetric*)pMetric)->pVal, pMetric->pCfg->subtype_num);
  }
}

void stmmDestroyMetricHandle(void* pHandle) {
  if (NULL == pHandle) {
    return;
  }

  SArray* pRes = (SArray*)pHandle;
  taosArrayDestroyEx(pRes, stmmDestroyMetric);
}

int32_t stmmBuildInitMetric(SStmMetricConfig* pCfg, int32_t metricNum, void** ppHandle) {
  int32_t code = 0, lino = 0;
  int32_t actualSize = 0, durationSize = 0, metricSize = 0;
  SStmMetric* pMetric = NULL;
  SStmActualValue* pActual = NULL;
  SStmTimeDuraStat* pStat = NULL;
  int64_t* pCumu = NULL;
  SArray* pRes = taosArrayInit_s(POINTER_BYTES, metricNum);
  TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);

  for (int32_t i = 0; i < metricNum; ++i, ++pCfg) {
    pActual = NULL;
    
    if (pCfg->metric_period & STMM_PERIOD_LAST_NTIME) {
      TAOS_CHECK_EXIT(stmmBuildInitActualValue(&pActual, pCfg, tsStreamMetricKeepNum));
    } else if (pCfg->metric_period & STMM_PERIOD_LAST_TIME) {
      TAOS_CHECK_EXIT(stmmBuildInitActualValue(&pActual, pCfg, 1));
    }
    
    if (pCfg->metric_period & STMM_PERIOD_TIME_DURATION) {
      if (STMM_METRIC_STAT & pCfg->metric_type) {
        TAOS_CHECK_EXIT(stmmBuildInitDuraStat(&pStat, pCfg, STMM_DURATION_KEEP_DAYS + 1));
        pMetric = taosMemoryCalloc(1, sizeof(SStmActualStatMetric));
        TSDB_CHECK_NULL(pMetric, code, lino, _exit, terrno);
        ((SStmActualStatMetric*)pMetric)->pCfg = pCfg;
        ((SStmActualStatMetric*)pMetric)->pActual = pActual;
        ((SStmActualStatMetric*)pMetric)->pStat = pStat;
        pStat = NULL;
      } else if (STMM_METRIC_CUMU & pCfg->metric_type) {
        TAOS_CHECK_EXIT(stmmBuildInitCumuValue(&pCumu, pCfg,  STMM_DURATION_KEEP_DAYS + 1));
        pMetric = taosMemoryCalloc(1, sizeof(SStmCumuMetric));
        TSDB_CHECK_NULL(pMetric, code, lino, _exit, terrno);
        ((SStmCumuMetric*)pMetric)->pCfg = pCfg;
        ((SStmCumuMetric*)pMetric)->pVal = pCumu;
        pCumu = NULL;
      }
    } else {
      pMetric = taosMemoryCalloc(1, sizeof(SStmActualMetric));
      TSDB_CHECK_NULL(pMetric, code, lino, _exit, terrno);
      ((SStmActualMetric*)pMetric)->pCfg = pCfg;
      ((SStmActualMetric*)pMetric)->pActual = pActual;
    }

    void** ppRes = taosArrayGet(pRes, i);
    *ppRes = pMetric;
  }

  *ppHandle = pRes;

_exit:  

  if (code) {
    stmmDestroySStmActualValue(pActual, pMetric->pCfg->subtype_num);
    stmmDestroySStmTimeDuraStat(pStat, pMetric->pCfg->subtype_num);
    stmmDestroyCumuValue(pCumu, pMetric->pCfg->subtype_num);
    stmmDestroyMetricHandle(pRes);
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t stmmInitForModule(void** ppHandle, int32_t module) {
  int32_t code = 0, lino = 0, metricNum = 0;
  SStmMetricConfig* pCfg = NULL;
  SArray* pRes = NULL;
  
  TAOS_CHECK_EXIT(stmmGetModuleCfg(&pCfg, module, &metricNum));
  TAOS_CHECK_EXIT(stmmBuildInitMetric(pCfg, metricNum, ppHandle));

_exit:  

  if (code) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t stmmLogMetric(void* pHandle, int32_t type, int64_t value, int32_t subType) {
  int32_t code = 0, lino = 0;
  SArray* pRes = (SArray*)pHandle;
  SStmTimeDuraStat* pStat = NULL;
  SStmMetric* pCMetric = (SStmMetric*)taosArrayGet(pRes, type);
  TSDB_CHECK_NULL(pCMetric, code, lino, _exit, terrno);

  if (pCMetric->pCfg->metric_type & STMM_METRIC_CUMU) {
    SStmCumuMetric* pMetric = (SStmCumuMetric*)pCMetric;
    *(pMetric->pVal + subType) += value;
    return code;
  }

  if (pCMetric->pCfg->metric_type & STMM_METRIC_ACTUAL) {
    SStmActualMetric* pMetric = (SStmActualMetric*)pCMetric;
    SStmActualValue* pActual = pMetric->pActual + subType;
    *(pActual->pVal + pActual->idx) += value;
    STMM_MOV_ACTUAL_IDX(pActual);
  }

  if (pCMetric->pCfg->metric_type & STMM_METRIC_STAT) {
    SStmTimeDuraStat* pStat = ((SStmActualStatMetric*)pCMetric)->pStat + subType;
    STMM_UPDATE_STAT(pStat->pVal, value);
    STMM_UPDATE_STAT(pStat->pVal + pStat->daysIdx, value);
  }
  
_exit:  

  if (code) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t stmmInit() {
  int32_t queueNum = 0;
  int32_t threadNum = tsNumOfMnodeStreamMgmtThreads + tsNumOfStreamMgmtThreads + tsNumOfVnodeStreamReaderThreads + tsNumOfMnodeStreamReaderThreads + tsNumOfStreamTriggerThreads + tsNumOfStreamRunnerThreads;
  gStmDnodeMetrics[ESTMM_THREAD_PROC_BEGIN_TS].subtype_num = threadNum;
  gStmDnodeMetrics[ESTMM_THREAD_PROC_END_TS].subtype_num = threadNum;
  gStmDnodeMetrics[ESTMM_MSG_IN_QUEUE_NUM].subtype_num = threadNum + queueNum;

  return TSDB_CODE_SUCCESS;
}


