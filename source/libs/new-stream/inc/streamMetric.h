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
#ifndef TDENGINE_STREAM_METRIC_H
#define TDENGINE_STREAM_METRIC_H


#ifdef __cplusplus
extern "C" {
#endif

#define STMM_DURATION_KEEP_DAYS 7 

typedef enum ESTMM_READER_MSG {
  ESTMM_READER_FETCH_FROM_READER = 0,
  ESTMM_READER_PULL_SET_TABLE,
  ESTMM_READER_PULL_LAST_TS,
  ESTMM_READER_PULL_FIRST_TS,
  ESTMM_READER_PULL_TSDB_META,
  ESTMM_READER_PULL_TSDB_META_NEXT,
  ESTMM_READER_PULL_TSDB_TS_DATA,
  ESTMM_READER_PULL_TSDB_TRIGGER_DATA,
  ESTMM_READER_PULL_TSDB_TRIGGER_DATA_NEXT,
  ESTMM_READER_PULL_TSDB_CALC_DATA,
  ESTMM_READER_PULL_TSDB_CALC_DATA_NEXT,
  ESTMM_READER_PULL_TSDB_DATA, 
  ESTMM_READER_PULL_TSDB_DATA_NEXT,
  ESTMM_READER_PULL_WAL_META,
  ESTMM_READER_PULL_WAL_TS_DATA,
  ESTMM_READER_PULL_WAL_TRIGGER_DATA,
  ESTMM_READER_PULL_WAL_CALC_DATA,
  ESTMM_READER_PULL_WAL_DATA,
  ESTMM_READER_PULL_GROUP_COL_VALUE,
  ESTMM_READER_PULL_VTABLE_INFO,
  ESTMM_READER_PULL_VTABLE_PSEUDO_COL,
  ESTMM_READER_PULL_OTABLE_INFO,
  ESTMM_READER_PULL_TYPE_MAX,
} ESTMM_READER_MSG;



typedef enum ESTMM_TRIG_VAL_TYPE {
  ESTMM_DATA_PULL_WAIT_TIME = 0,
  ESTMM_TRIG_CHECK_TIME,
  ESTMM_CALC_REQ_WAIT_TIME,
  ESTMM_DATA_PULL_NUM,
  ESTMM_CALC_PARAM_NUM,
  ESTMM_CALC_REQ_NUM,
  ESTMM_TRIG_MSG_PROC_TIME
} ESTMM_TRIG_VAL_TYPE;

typedef enum ESTMM_READER_VAL_TYPE {
  ESTMM_WAL_PROC_PROGRESS,
  ESTMM_WAL_BLOCK_NUM,
  ESTMM_WAL_BLOCK_ROW_NUM,
  ESTMM_WAL_READ_NUM,
  ESTMM_READER_MSG_PROC_TIME
} ESTMM_READER_VAL_TYPE;

typedef enum ESTMM_RUNNER_VAL_TYPE {
  ESTMM_DATA_INSERT_ROW_NUM,
  ESTMM_DATA_INSERT_TIME,
  ESTMM_NOTIFY_USED_TIME,
  ESTMM_DATA_INSERT_NUM,
  ESTMM_NOTIFY_NUM,
  ESTMM_RUNNER_MSG_PROC_TIME
} ESTMM_RUNNER_VAL_TYPE;

typedef enum ESTMM_MGMT_VAL_TYPE {
  ESTMM_MGMT_MSG_PROC_TIME,
} ESTMM_MGMT_VAL_TYPE;



#define STMM_PERIOD_LAST_TIME        (1<<0)
#define STMM_PERIOD_LAST_NTIME       (1<<1)
#define STMM_PERIOD_TIME_DURATION    (1<<2)
#define STMM_PERIOD_ALL              (STMM_PERIOD_LAST_TIME | STMM_PERIOD_LAST_NTIME | STMM_PERIOD_TIME_DURATION)

#define STMM_METRIC_STAT             (1<<0)
#define STMM_METRIC_ACTUAL           (1<<1)
#define STMM_METRIC_CUMU             (1<<2)

typedef struct SStmMetricConfig {
  int32_t        value_type;
  char*          value_name;
  int32_t        subtype_num;
  int32_t        module;
  int32_t        metric_period;
  int32_t        metric_type;
} SStmMetricConfig;

typedef struct SStmMsgConfig {
  int32_t msgType;
  char*   msgName;
} SStmMsgConfig;


typedef struct SStmOpeConfig {
  int32_t opType;
  char*   opName;
} SStmOpeConfig;

typedef struct SStmActualValue {
  int32_t  size;
  int32_t  idx;
  int64_t* pVal;
  bool     loop;
} SStmActualValue;

typedef struct SStmStatValue {
  int64_t minVal;
  int64_t maxVal;
  int64_t sumVal;
  int64_t num;
} SStmStatValue;

typedef struct SStmTimeDuraStat {
  int16_t daysNum;
  int16_t daysIdx;
  int32_t currDay;
  bool    loop;
  SStmStatValue* pVal;
} SStmTimeDuraStat;

typedef struct SStmActualMetric {
  SStmMetricConfig* pCfg;
  SStmActualValue*  pActual;
} SStmActualMetric;

typedef struct SStmCumuMetric {
  SStmMetricConfig* pCfg;
  int64_t*          pVal;
} SStmCumuMetric;

typedef struct SStmActualStatMetric {
  SStmMetricConfig* pCfg;
  SStmActualValue*  pActual;
  SStmTimeDuraStat* pStat;
} SStmActualStatMetric;

typedef struct SStmMetric {
  SStmMetricConfig* pCfg;
} SStmMetric;

#define STMM_MOV_ACTUAL_IDX(_p) do {      \
    ++(_p)->idx;                          \
    if ((_p)->idx >= (_p)->size) {        \
      (_p)->loop = true;                  \
      (_p)->idx = 0;                      \
    }                                     \
  } while (0)

#define STMM_UPDATE_STAT(_s, _v) do {     \
    if ((_v) > (_s)->maxVal) {            \
      (_s)->maxVal = (_v);                \
    } else if ((_v) < (_s)->minVal) {     \
      (_s)->minVal = (_v);                \
    }                                     \
    (_s)->sumVal += (_v);                 \
    +(_s)->num;                           \
  } while (0)

#define STMM_MOV_DURA_IDX(_p) do {        \
    ++(_p)->daysIdx;                      \
    if ((_p)->daysIdx >= (_p)->daysNum) { \
      (_p)->loop = true;                  \
      (_p)->daysIdx = 1;                  \
    }                                     \
  } while (0)


#ifdef __cplusplus
}
#endif
#endif
