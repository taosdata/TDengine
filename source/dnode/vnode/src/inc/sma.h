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

#ifndef _TD_VNODE_SMA_H_
#define _TD_VNODE_SMA_H_

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// smaDebug ================
// clang-format off
#define smaFatal(...) do { if (smaDebugFlag & DEBUG_FATAL) { taosPrintLog("SMA FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define smaError(...) do { if (smaDebugFlag & DEBUG_ERROR) { taosPrintLog("SMA ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define smaWarn(...)  do { if (smaDebugFlag & DEBUG_WARN)  { taosPrintLog("SMA WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define smaInfo(...)  do { if (smaDebugFlag & DEBUG_INFO)  { taosPrintLog("SMA ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define smaDebug(...) do { if (smaDebugFlag & DEBUG_DEBUG) { taosPrintLog("SMA ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define smaTrace(...) do { if (smaDebugFlag & DEBUG_TRACE) { taosPrintLog("SMA ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#define RSMA_TASK_INFO_HASH_SLOT (8)

typedef struct SSmaEnv       SSmaEnv;
typedef struct SSmaStat      SSmaStat;
typedef struct STSmaStat     STSmaStat;
typedef struct SRSmaStat     SRSmaStat;
typedef struct SRSmaRef      SRSmaRef;
typedef struct SRSmaInfo     SRSmaInfo;
typedef struct SRSmaInfoItem SRSmaInfoItem;
typedef struct SRSmaFS       SRSmaFS;
typedef struct SQTaskFile    SQTaskFile;
typedef struct SQTaskFReader SQTaskFReader;

struct SSmaEnv {
  SRWLatch  lock;
  int8_t    type;
  int8_t    flag;  // 0x01 inClose
  SSmaStat *pStat;
};

#define SMA_ENV_FLG_CLOSE ((int8_t)0x1)

struct SRSmaRef {
  int64_t refId;  // for SRSmaStat
  int64_t suid;
};

typedef struct {
  int8_t  inited;
  int32_t rsetId;
  void   *tmrHandle;  // shared by all fetch tasks
  /**
   * @brief key: void* of SRSmaInfoItem, value: SRSmaRef
   *  N.B. Although there is a very small possibility that "void*" point to different objects while with the same
   * address after release/renew, the functionality is not affected as it just used to fetch the rsma results.
   */
  SHashObj *refHash;  // shared by all vgroups
} SSmaMgmt;

#define SMA_ENV_LOCK(env)  (&(env)->lock)
#define SMA_ENV_TYPE(env)  ((env)->type)
#define SMA_ENV_STAT(env)  ((env)->pStat)
#define SMA_RSMA_STAT(sma) ((SRSmaStat *)SMA_ENV_STAT((SSmaEnv *)(sma)->pRSmaEnv))

struct STSmaStat {
  int8_t    state;  // ETsdbSmaStat
  STSma    *pTSma;  // cache schema
  STSchema *pTSchema;
};

struct SQTaskFile {
  volatile int32_t nRef;
  int8_t           level;
  int64_t          suid;
  int64_t          version;
  int64_t          size;
  int64_t          mtime;
};

struct SQTaskFReader {
  SSma     *pSma;
  int8_t    level;
  int64_t   suid;
  int64_t   version;
  TdFilePtr pReadH;
};

struct SRSmaFS {
  SArray *aQTaskInf;  // array of SQTaskFile
};

struct SRSmaStat {
  SSma            *pSma;
  int64_t          refId;        // shared by fetch tasks
  volatile int64_t nBufItems;    // number of items in queue buffer
  SRWLatch         lock;         // r/w lock for rsma fs(e.g. qtaskinfo)
  volatile int32_t execStat;     // 0 succeed, other failed
  volatile int32_t nFetchAll;    // active number of fetch all
  volatile int8_t  triggerStat;  // shared by fetch tasks
  volatile int8_t  commitStat;   // 0 not in committing, 1 in committing
  volatile int8_t  delFlag;      // 0 no deleted SRSmaInfo, 1 has deleted SRSmaInfo
  SRSmaFS          fs;           // for recovery/snapshot r/w
  SHashObj        *infoHash;     // key: suid, value: SRSmaInfo
  tsem_t           notEmpty;     // has items in queue buffer
  SArray          *blocks;       // SArray<SSDataBlock>
};

struct SSmaStat {
  union {
    STSmaStat tsmaStat;  // time-range-wise sma
    SRSmaStat rsmaStat;  // rollup sma
  };
  T_REF_DECLARE()
  char data[];
};

#define SMA_STAT_TSMA(s)     (&(s)->tsmaStat)
#define SMA_STAT_RSMA(s)     (&(s)->rsmaStat)
#define RSMA_INFO_HASH(r)    ((r)->infoHash)
#define RSMA_TRIGGER_STAT(r) (&(r)->triggerStat)
#define RSMA_COMMIT_STAT(r)  (&(r)->commitStat)
#define RSMA_REF_ID(r)       ((r)->refId)
#define RSMA_FS(r)           (&(r)->fs)
#define RSMA_FS_LOCK(r)      (&(r)->lock)

struct SRSmaInfoItem {
  int8_t  level;
  int8_t  fetchLevel;
  int8_t  triggerStat;
  int32_t nScanned;
  int32_t streamFlushed : 1;
  int32_t maxDelay : 31;  // ms
  int64_t submitReqVer;
  int64_t fetchResultVer;
  tmr_h   tmrId;
  void   *pStreamState;
  void   *pStreamTask;  // SStreamTask
  SArray *pResList;
};

struct SRSmaInfo {
  SSma     *pSma;
  STSchema *pTSchema;
  int64_t   suid;
  int64_t   lastRecv;  // ms
  int8_t    assigned;  // 0 idle, 1 assgined for exec
  int8_t    delFlag;
  int16_t   padding;
  T_REF_DECLARE()
  SRSmaInfoItem items[TSDB_RETENTION_L2];
  void         *taskInfo[TSDB_RETENTION_L2];  // qTaskInfo_t
  STaosQueue   *queue;                        // buffer queue of SubmitReq
  STaosQall    *qall;                         // buffer qall of SubmitReq
};

#define RSMA_INFO_IS_DEL(r)   ((r)->delFlag == 1)
#define RSMA_INFO_SET_DEL(r)  ((r)->delFlag = 1)
#define RSMA_INFO_QTASK(r, i) ((r)->taskInfo[i])
#define RSMA_INFO_ITEM(r, i)  (&(r)->items[i])

enum {
  TASK_TRIGGER_STAT_INIT = 0,
  TASK_TRIGGER_STAT_ACTIVE = 1,
  TASK_TRIGGER_STAT_INACTIVE = 2,
  TASK_TRIGGER_STAT_PAUSED = 3,
  TASK_TRIGGER_STAT_CANCELLED = 4,
  TASK_TRIGGER_STAT_DROPPED = 5,
};

enum {
  RSMA_RESTORE_REBOOT = 1,
  RSMA_RESTORE_SYNC = 2,
};

typedef enum {
  RSMA_EXEC_OVERFLOW = 1,  // triggered by queue buf overflow
  RSMA_EXEC_TIMEOUT = 2,   // triggered by timer
  RSMA_EXEC_COMMIT = 3,    // triggered by commit
} ERsmaExecType;

#define TD_SMA_LOOPS_CHECK(n, limit) \
  if (++(n) > limit) {               \
    sched_yield();                   \
    (n) = 0;                         \
  }

// sma
int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType);
void    tdDestroySmaEnv(SSmaEnv *pSmaEnv);
void   *tdFreeSmaEnv(SSmaEnv *pSmaEnv);
int32_t tdLockSma(SSma *pSma);
int32_t tdUnLockSma(SSma *pSma);
void   *tdAcquireSmaRef(int32_t rsetId, int64_t refId);
int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId);

static FORCE_INLINE void tdRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  int32_t ref = T_REF_INC(pStat);
  smaDebug("vgId:%d, ref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
}
static FORCE_INLINE void tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  int32_t ref = T_REF_DEC(pStat);
  smaDebug("vgId:%d, unref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
}

int32_t smaPreClose(SSma *pSma);

// rsma
void   *tdFreeRSmaInfo(SSma *pSma, SRSmaInfo *pInfo);
int32_t tdRSmaRestore(SSma *pSma, int8_t type, int64_t committedVer, int8_t rollback);
int32_t tdRSmaProcessCreateImpl(SSma *pSma, SRSmaParam *param, int64_t suid, const char *tbName);
int32_t tdRSmaProcessExecImpl(SSma *pSma, ERsmaExecType type);
int32_t tdRSmaPersistExecImpl(SRSmaStat *pRSmaStat, SHashObj *pInfoHash);
int32_t tdRSmaProcessRestoreImpl(SSma *pSma, int8_t type, int64_t qtaskFileVer, int8_t rollback);
void    tdRSmaQTaskInfoGetFullPath(SVnode *pVnode, tb_uid_t suid, int8_t level, STfs *pTfs, char *outputName);

static FORCE_INLINE void tdRefRSmaInfo(SSma *pSma, SRSmaInfo *pRSmaInfo) {
  int32_t ref = T_REF_INC(pRSmaInfo);
  smaTrace("vgId:%d, ref rsma info:%p, val:%d", SMA_VID(pSma), pRSmaInfo, ref);
}
static FORCE_INLINE void tdUnRefRSmaInfo(SSma *pSma, SRSmaInfo *pRSmaInfo) {
  int32_t ref = T_REF_DEC(pRSmaInfo);
  smaTrace("vgId:%d, unref rsma info:%p, val:%d", SMA_VID(pSma), pRSmaInfo, ref);
}

void tdRSmaGetDirName(SVnode *pVnode, STfs *pTfs, bool endWithSep, char *outputName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_SMA_H_*/
