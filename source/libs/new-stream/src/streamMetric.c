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
  {ESTMM_FETCH_FROM_READER, "FetchFromReader"},
};

SStmMsgConfig gStmGlobalMsg[] = {
  {ESTMM_FETCH_FROM_READER, "FetchFromReader"},
};

SStmMetricConfig gStmTrigMetrics[] = {
  {ESTMM_DATA_PULL_WAIT_TIME, "DataPullWaitTime", 0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_TRIG_CHECK_TIME,     "TrigCheckTime",    0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_CALC_REQ_WAIT_TIME,  "CalcReqWaitTime",  0, STMM_TRIGGER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_PULL_NUM,       "DataPullNum",      0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_CALC_PARAM_NUM,      "CalcParamNum",     0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_CALC_REQ_NUM,        "CalcReqNum",       0, STMM_TRIGGER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
};


SStmMetricConfig gStmReaderMetrics[] = {
  {ESTMM_WAL_PROC_PROGRESS,   "WalProcessProgress", true,  STMM_READER, STMM_PERIOD_LAST_TIME, STMM_METRIC_ACTUAL},
  {ESTMM_WAL_BLOCK_NUM,       "WalBlockNum",        0, STMM_READER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_WAL_BLOCK_ROW_NUM,   "WalBlockRowNum",     0, STMM_READER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_WAL_READ_NUM,        "WalReadNum",         true,  STMM_READER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
};

SStmMetricConfig gStmRunnerMetrics[] = {
  {ESTMM_DATA_INSERT_ROW_NUM, "DataInsertRowNum", 0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_INSERT_TIME,    "DataInsertTime",   0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_NOTIFY_USED_TIME,    "NotifyUsedTime",   0, STMM_RUNNER, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_DATA_INSERT_NUM,     "DataInsertNum",    0, STMM_RUNNER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_NOTIFY_NUM,          "NotifyNum",        true,  STMM_RUNNER, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
};

SStmMetricConfig gStmMnodeMetrics[] = {
  {ESTMM_STREAM_DEPLOY_NUM,  "StreamDeployNum",      0, STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_ERROR_NUM,   "StreamErrorNum",       0, STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_OPERATIONS,  "StreamOperations",     true,  STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_STREAM_STATUS_TIME, "StreamStatusTime",     true,  STMM_MNODE, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
  {ESTMM_LAST_OPERATIONS,    "StreamLastOperations", 0, STMM_MNODE, STMM_PERIOD_LAST_NTIME,    STMM_METRIC_ACTUAL},
};

SStmMetricConfig gStmCommonMetrics[] = {
  {ESTMM_THREAD_PROC_BEGIN_TS, "ThreadProcBeginTs", true, STMM_COMMON, STMM_PERIOD_LAST_TIME,     STMM_METRIC_ACTUAL},
  {ESTMM_THREAD_PROC_END_TS,   "ThreadProcEndTs",   true, STMM_COMMON, STMM_PERIOD_LAST_TIME,     STMM_METRIC_ACTUAL},
  {ESTMM_MSG_PROCESS_TIME,     "MsgProcessTime",    true, STMM_COMMON, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_MSG_IN_QUEUE_NUM,     "MsgInQueueNum",     true, STMM_COMMON, STMM_PERIOD_ALL,           STMM_METRIC_STAT|STMM_METRIC_ACTUAL},
  {ESTMM_MSG_RECV_NUM,         "MsgRecvNum",        true, STMM_COMMON, STMM_PERIOD_TIME_DURATION, STMM_METRIC_CUMU},
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
    case STMM_COMMON:
      *ppCfg = gStmCommonMetrics;
      *metricNum = tListLen(gStmCommonMetrics);
      break;
    default:
      stError("invalid metric module %d", module);
      code = TSDB_CODE_STREAM_INTERNAL_ERROR;
      break;
  }

_exit:

  return code;
}

int32_t stmmBuildInitMetric(SStmMetricConfig* pCfg, int32_t metricNum, void** ppHandle) {
  int32_t code = 0, lino = 0;
  int32_t actualSize = 0, durationSize = 0, metricSize = 0;
  SStmMetric* pMetric = NULL;
  SArray* pRes = taosArrayInit_s(POINTER_BYTES, metricNum);
  TSDB_CHECK_NULL(pRes, code, lino, _exit, terrno);

  for (int32_t i = 0; i < metricNum; ++i, ++pCfg) {
    if (pCfg->metric_period & STMM_PERIOD_LAST_TIME) {
      actualSize = pCfg->subtype_num * (sizeof(SStmActualValue) + sizeof(int64_t));
    }

    if ((pCfg->metric_period & STMM_PERIOD_LAST_NTIME) && tsStreamMetricKeepNum != 1) {
      actualSize = pCfg->subtype_num * (sizeof(SStmActualValue) + tsStreamMetricKeepNum * sizeof(int64_t));
    }

    if (pCfg->metric_period & STMM_PERIOD_TIME_DURATION) {
      if (STMM_METRIC_STAT & pCfg->metric_type) {
        durationSize = pCfg->subtype_num * (sizeof(SStmTimeDuraStat) + (STMM_DURATION_KEEP_DAYS + 1) * sizeof(SStmStatValue));
        metricSize = sizeof(SStmActualStatMetric) + durationSize + actualSize;
      } else if (STMM_METRIC_CUMU & pCfg->metric_type) {
        durationSize = pCfg->subtype_num * ((STMM_DURATION_KEEP_DAYS + 1) * sizeof(int64_t));
        metricSize = sizeof(SStmCumuMetric) + durationSize;
      }
    } else {
      metricSize = sizeof(SStmActualMetric) + actualSize;
    }
    
    pMetric = taosMemoryCalloc(1, metricSize);
    TSDB_CHECK_NULL(pMetric, code, lino, _exit, terrno);

    void** ppRes = taosArrayGet(pRes, i);
    *ppRes = pMetric;
  }

  *ppHandle = pRes;

_exit:  

  if (code) {
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

int32_t stmmLogMetric(void* pHandle, int32_t type, int64_t* value, int32_t subType) {
  int32_t code = 0, lino = 0;
  SArray* pRes = (SArray*)pHandle;

  SStmMetric* pMetric = (SStmMetric*)taosArrayGet(pRes, type);
  TSDB_CHECK_NULL(pMetric, code, lino, _exit, terrno);

  if (pMetric->pCfg->metric_type & STMM_METRIC_CUMU) {
    SStmCumuMetric* pCumu = (SStmCumuMetric*)pMetric;
    *(pCumu->pVal + subType) += *value;
    return code;
  }

  if (pMetric->pCfg->metric_type & STMM_METRIC_ACTUAL) {
    SStmActualMetric* pActual = (SStmActualMetric*)pMetric;
    //*pActual->pVal += *value;
    return code;
  }

_exit:  

  if (code) {
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}


