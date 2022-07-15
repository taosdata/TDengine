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

typedef struct SSmaEnv       SSmaEnv;
typedef struct SSmaStat      SSmaStat;
typedef struct STSmaStat     STSmaStat;
typedef struct SRSmaStat     SRSmaStat;
typedef struct SSmaKey       SSmaKey;
typedef struct SRSmaInfo     SRSmaInfo;
typedef struct SRSmaInfoItem SRSmaInfoItem;

struct SSmaEnv {
  TdThreadRwlock lock;
  int8_t         type;
  SSmaStat      *pStat;
};

typedef struct {
  int8_t  inited;
  int32_t rsetId;
  void   *tmrHandle;  // shared by all fetch tasks
} SSmaMgmt;

#define SMA_ENV_LOCK(env) ((env)->lock)
#define SMA_ENV_TYPE(env) ((env)->type)
#define SMA_ENV_STAT(env) ((env)->pStat)

struct STSmaStat {
  int8_t    state;  // ETsdbSmaStat
  STSma    *pTSma;  // cache schema
  STSchema *pTSchema;
};

struct SRSmaStat {
  SSma     *pSma;
  int64_t   submitVer;
  int64_t   refId;         // shared by fetch tasks
  int8_t    triggerStat;   // shared by fetch tasks
  SHashObj *rsmaInfoHash;  // key: stbUid, value: SRSmaInfo;
};

struct SSmaStat {
  union {
    STSmaStat tsmaStat;  // time-range-wise sma
    SRSmaStat rsmaStat;  // rollup sma
  };
  T_REF_DECLARE()
};

#define SMA_TSMA_STAT(s)     (&(s)->tsmaStat)
#define SMA_RSMA_STAT(s)     (&(s)->rsmaStat)
#define RSMA_INFO_HASH(r)    ((r)->rsmaInfoHash)
#define RSMA_TRIGGER_STAT(r) (&(r)->triggerStat)
#define RSMA_REF_ID(r)       ((r)->refId)
#define RSMA_SUBMIT_VER(r)   ((r)->submitVer)

enum {
  TASK_TRIGGER_STAT_INIT = 0,
  TASK_TRIGGER_STAT_ACTIVE = 1,
  TASK_TRIGGER_STAT_INACTIVE = 2,
  TASK_TRIGGER_STAT_PAUSED = 3,
  TASK_TRIGGER_STAT_CANCELLED = 4,
  TASK_TRIGGER_STAT_DROPPED = 5,
};

void  tdDestroySmaEnv(SSmaEnv *pSmaEnv);
void *tdFreeSmaEnv(SSmaEnv *pSmaEnv);

int32_t tdDropTSma(SSma *pSma, char *pMsg);
int32_t tdDropTSmaData(SSma *pSma, int64_t indexUid);
int32_t tdInsertRSmaData(SSma *pSma, char *msg);

int32_t tdRefSmaStat(SSma *pSma, SSmaStat *pStat);
int32_t tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat);

void   *tdAcquireSmaRef(int32_t rsetId, int64_t refId, const char *tags, int32_t ln);
int32_t tdReleaseSmaRef(int32_t rsetId, int64_t refId, const char *tags, int32_t ln);

int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType);

int32_t tdLockSma(SSma *pSma);
int32_t tdUnLockSma(SSma *pSma);

static FORCE_INLINE int32_t tdRLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockRdlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tdWLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockWrlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tdUnLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockUnlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int8_t tdSmaStat(STSmaStat *pTStat) {
  if (pTStat) {
    return atomic_load_8(&pTStat->state);
  }
  return TSDB_SMA_STAT_UNKNOWN;
}

static FORCE_INLINE bool tdSmaStatIsOK(STSmaStat *pTStat, int8_t *state) {
  if (!pTStat) {
    return false;
  }

  if (state) {
    *state = atomic_load_8(&pTStat->state);
    return *state == TSDB_SMA_STAT_OK;
  }
  return atomic_load_8(&pTStat->state) == TSDB_SMA_STAT_OK;
}

static FORCE_INLINE bool tdSmaStatIsExpired(STSmaStat *pTStat) {
  return pTStat ? (atomic_load_8(&pTStat->state) & TSDB_SMA_STAT_EXPIRED) : true;
}

static FORCE_INLINE bool tdSmaStatIsDropped(STSmaStat *pTStat) {
  return pTStat ? (atomic_load_8(&pTStat->state) & TSDB_SMA_STAT_DROPPED) : true;
}

static FORCE_INLINE void tdSmaStatSetOK(STSmaStat *pTStat) {
  if (pTStat) {
    atomic_store_8(&pTStat->state, TSDB_SMA_STAT_OK);
  }
}

static FORCE_INLINE void tdSmaStatSetExpired(STSmaStat *pTStat) {
  if (pTStat) {
    atomic_or_fetch_8(&pTStat->state, TSDB_SMA_STAT_EXPIRED);
  }
}

static FORCE_INLINE void tdSmaStatSetDropped(STSmaStat *pTStat) {
  if (pTStat) {
    atomic_or_fetch_8(&pTStat->state, TSDB_SMA_STAT_DROPPED);
  }
}

static int32_t tdDestroySmaState(SSmaStat *pSmaStat, int8_t smaType);
void          *tdFreeSmaState(SSmaStat *pSmaStat, int8_t smaType);
void          *tdFreeRSmaInfo(SSma *pSma, SRSmaInfo *pInfo);
int32_t        tdRSmaPersistExecImpl(SRSmaStat *pRSmaStat);

int32_t tdProcessRSmaCreateImpl(SSma *pSma, SRSmaParam *param, int64_t suid, const char *tbName);
int32_t tdProcessRSmaRestoreImpl(SSma *pSma);

int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg);
int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg);
int32_t tdProcessTSmaGetDaysImpl(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

// smaFileUtil ================

#define TD_FILE_HEAD_SIZE 512

typedef struct STFInfo STFInfo;
typedef struct STFile  STFile;

struct STFInfo {
  // common fields
  uint32_t magic;
  uint32_t ftype;
  uint32_t fver;
  int64_t  fsize;

  // specific fields
  union {
    struct {
      int64_t submitVer;
    } qTaskInfo;
  };
};

enum {
  TD_FTYPE_RSMA_QTASKINFO = 0,
};

struct STFile {
  uint8_t   state;
  STFInfo   info;
  char     *fname;
  TdFilePtr pFile;
};

#define TD_TFILE_PFILE(tf)        ((tf)->pFile)
#define TD_TFILE_OPENED(tf)       (TD_TFILE_PFILE(tf) != NULL)
#define TD_TFILE_FULL_NAME(tf)    ((tf)->fname)
#define TD_TFILE_OPENED(tf)       (TD_TFILE_PFILE(tf) != NULL)
#define TD_TFILE_CLOSED(tf)       (!TD_TFILE_OPENED(tf))
#define TD_TFILE_SET_CLOSED(f)    (TD_TFILE_PFILE(f) = NULL)
#define TD_TFILE_SET_STATE(tf, s) ((tf)->state = (s))

int32_t tdInitTFile(STFile *pTFile, const char *dname, const char *fname);
int32_t tdCreateTFile(STFile *pTFile, bool updateHeader, int8_t fType);
int32_t tdOpenTFile(STFile *pTFile, int flags);
int64_t tdReadTFile(STFile *pTFile, void *buf, int64_t nbyte);
int64_t tdSeekTFile(STFile *pTFile, int64_t offset, int whence);
int64_t tdWriteTFile(STFile *pTFile, void *buf, int64_t nbyte);
int64_t tdAppendTFile(STFile *pTFile, void *buf, int64_t nbyte, int64_t *offset);
int64_t tdGetTFileSize(STFile *pTFile, int64_t *size);
int32_t tdRemoveTFile(STFile *pTFile);
int32_t tdLoadTFileHeader(STFile *pTFile, STFInfo *pInfo);
int32_t tdUpdateTFileHeader(STFile *pTFile);
void    tdUpdateTFileMagic(STFile *pTFile, void *pCksm);
void    tdCloseTFile(STFile *pTFile);
void    tdDestroyTFile(STFile *pTFile);

void tdGetVndFileName(int32_t vgId, const char *pdname, const char *dname, const char *fname, int64_t version, char *outputName);
void tdGetVndDirName(int32_t vgId,const char *pdname,  const char *dname, bool endWithSep, char *outputName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_SMA_H_*/
