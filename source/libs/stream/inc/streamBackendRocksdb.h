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

#ifndef _STREAM_BACKEDN_ROCKSDB_H_
#define _STREAM_BACKEDN_ROCKSDB_H_

#include "rocksdb/c.h"
//#include "streamInt.h"
#include "streamState.h"
#include "tcommon.h"

typedef struct SCfComparator {
  rocksdb_comparator_t** comp;
  int32_t                numOfComp;
} SCfComparator;

typedef struct {
  rocksdb_t*                         db;
  rocksdb_writeoptions_t*            writeOpts;
  rocksdb_readoptions_t*             readOpts;
  rocksdb_options_t*                 dbOpt;
  void*                              param;
  void*                              env;
  rocksdb_cache_t*                   cache;
  TdThreadMutex                      mutex;
  rocksdb_compactionfilterfactory_t* filterFactory;
  SList*                             list;
  TdThreadMutex                      cfMutex;
  SHashObj*                          cfInst;
  int64_t                            defaultCfInit;

} SBackendWrapper;

typedef struct {
  void* tableOpt;
} RocksdbCfParam;

typedef struct {
  rocksdb_t*              db;
  rocksdb_writeoptions_t* writeOpt;
  rocksdb_readoptions_t*  readOpt;
  rocksdb_options_t*      dbOpt;
  rocksdb_env_t*          env;
  rocksdb_cache_t*        cache;

  rocksdb_column_family_handle_t** pCf;
  rocksdb_comparator_t**           pCompares;
  rocksdb_options_t**              pCfOpts;
  RocksdbCfParam*                  pCfParams;

  rocksdb_compactionfilterfactory_t* filterFactory;
  TdThreadMutex                      mutex;
  char*                              idstr;
  char*                              path;
  int64_t                            refId;

  void*          pTask;
  int64_t        streamId;
  int64_t        taskId;
  int64_t        chkpId;
  SArray*        chkpSaved;
  SArray*        chkpInUse;
  int32_t        chkpCap;
  TdThreadRwlock chkpDirLock;
  int64_t        dataWritten;

  void* pMeta;

} STaskDbWrapper;

typedef struct SDbChkp {
  int8_t  init;
  char*   pCurrent;
  char*   pManifest;
  SArray* pSST;
  int64_t preCkptId;
  int64_t curChkpId;
  char*   path;

  char*   buf;
  int32_t len;

  // ping-pong buf
  SHashObj* pSstTbl[2];
  int8_t    idx;

  SArray* pAdd;
  SArray* pDel;
  int8_t  update;

  TdThreadRwlock rwLock;
} SDbChkp;
typedef struct {
  int8_t  init;
  char*   pCurrent;
  char*   pManifest;
  SArray* pSST;
  int64_t preCkptId;
  int64_t curChkpId;
  char*   path;

  char*   buf;
  int32_t len;

  // ping-pong buf
  SHashObj* pSstTbl[2];
  int8_t    idx;

  SArray* pAdd;
  SArray* pDel;
  int8_t  update;

  SHashObj* pDbChkpTbl;

  TdThreadRwlock rwLock;
} SBkdMgt;

bool       streamBackendDataIsExist(const char* path, int64_t chkpId, int32_t vgId);
void*      streamBackendInit(const char* path, int64_t chkpId, int32_t vgId);
void       streamBackendCleanup(void* arg);
void       streamBackendHandleCleanup(void* arg);
int32_t    streamBackendLoadCheckpointInfo(void* pMeta);
int32_t    streamBackendDoCheckpoint(void* pMeta, int64_t checkpointId);
SListNode* streamBackendAddCompare(void* backend, void* arg);
void       streamBackendDelCompare(void* backend, void* arg);
int32_t    streamStateCvtDataFormat(char* path, char* key, void* cfInst);

STaskDbWrapper* taskDbOpen(char* path, char* key, int64_t chkpId);
void            taskDbDestroy(void* pBackend, bool flush);
void            taskDbDestroy2(void* pBackend);
int32_t         taskDbDoCheckpoint(void* arg, int64_t chkpId);

void taskDbUpdateChkpId(void* pTaskDb, int64_t chkpId);

void* taskDbAddRef(void* pTaskDb);
void  taskDbRemoveRef(void* pTaskDb);

int  streamStateOpenBackend(void* backend, SStreamState* pState);
void streamStateCloseBackend(SStreamState* pState, bool remove);
void streamStateDestroyCompar(void* arg);

// state cf
int32_t streamStatePut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateDel_rocksdb(SStreamState* pState, const SWinKey* key);
int32_t streamStateClear_rocksdb(SStreamState* pState);
int32_t streamStateCurNext_rocksdb(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateGetFirst_rocksdb(SStreamState* pState, SWinKey* key);
int32_t streamStateGetGroupKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
int32_t streamStateAddIfNotExist_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateCurPrev_rocksdb(SStreamStateCur* pCur);
int32_t streamStateGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
SStreamStateCur* streamStateGetAndCheckCur_rocksdb(SStreamState* pState, SWinKey* key);
SStreamStateCur* streamStateSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateSeekToLast_rocksdb(SStreamState* pState);
SStreamStateCur* streamStateGetCur_rocksdb(SStreamState* pState, const SWinKey* key);

// func cf
int32_t streamStateFuncPut_rocksdb(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet_rocksdb(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFuncDel_rocksdb(SStreamState* pState, const STupleKey* key);

//  session cf
int32_t streamStateSessionPut_rocksdb(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen);
int32_t streamStateSessionGet_rocksdb(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateSessionDel_rocksdb(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentPrev_rocksdb(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentNext_rocksdb(SStreamState* pState, SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyNext_rocksdb(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyPrev_rocksdb(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekToLast_rocksdb(SStreamState* pState, int64_t groupId);
int32_t          streamStateSessionCurPrev_rocksdb(SStreamStateCur* pCur);

int32_t streamStateSessionGetKVByCur_rocksdb(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t streamStateSessionGetKeyByRange_rocksdb(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey);
int32_t streamStateSessionAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                                int32_t* pVLen);

int32_t streamStateSessionClear_rocksdb(SStreamState* pState);

int32_t streamStateStateAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, char* pKeyData,
                                              int32_t keyDataLen, state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);

// fill cf
int32_t          streamStateFillPut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t          streamStateFillGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t          streamStateFillDel_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillGetCur_rocksdb(SStreamState* pState, const SWinKey* key);
int32_t streamStateFillGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
SStreamStateCur* streamStateFillSeekKeyPrev_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key);

// partag cf
int32_t streamStatePutParTag_rocksdb(SStreamState* pState, int64_t groupId, const void* tag, int32_t tagLen);
int32_t streamStateGetParTag_rocksdb(SStreamState* pState, int64_t groupId, void** tagVal, int32_t* tagLen);

// parname cf
int32_t streamStatePutParName_rocksdb(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]);
int32_t streamStateGetParName_rocksdb(SStreamState* pState, int64_t groupId, void** pVal);

void streamStateDestroy_rocksdb(SStreamState* pState, bool remove);

// default cf
int32_t streamDefaultPut_rocksdb(SStreamState* pState, const void* key, void* pVal, int32_t pVLen);
int32_t streamDefaultGet_rocksdb(SStreamState* pState, const void* key, void** pVal, int32_t* pVLen);
int32_t streamDefaultDel_rocksdb(SStreamState* pState, const void* key);
int32_t streamDefaultIterGet_rocksdb(SStreamState* pState, const void* start, const void* end, SArray* result);
void*   streamDefaultIterCreate_rocksdb(SStreamState* pState);
bool    streamDefaultIterValid_rocksdb(void* iter);
void    streamDefaultIterSeek_rocksdb(void* iter, const char* key);
void    streamDefaultIterNext_rocksdb(void* iter);
char*   streamDefaultIterKey_rocksdb(void* iter, int32_t* len);
char*   streamDefaultIterVal_rocksdb(void* iter, int32_t* len);

// batch func
int     streamStateGetCfIdx(SStreamState* pState, const char* funcName);
void*   streamStateCreateBatch();
int32_t streamStateGetBatchSize(void* pBatch);
void    streamStateClearBatch(void* pBatch);
void    streamStateDestroyBatch(void* pBatch);
int32_t streamStatePutBatch(SStreamState* pState, const char* cfName, rocksdb_writebatch_t* pBatch, void* key,
                            void* val, int32_t vlen, int64_t ttl);

int32_t streamStatePutBatchOptimize(SStreamState* pState, int32_t cfIdx, rocksdb_writebatch_t* pBatch, void* key,
                                    void* val, int32_t vlen, int64_t ttl, void* tmpBuf);

int32_t streamStatePutBatch_rocksdb(SStreamState* pState, void* pBatch);
int32_t streamBackendTriggerChkp(void* pMeta, char* dst);

int32_t streamBackendAddInUseChkp(void* arg, int64_t chkpId);
int32_t streamBackendDelInUseChkp(void* arg, int64_t chkpId);

int32_t taskDbBuildSnap(void* arg, SArray* pSnap);

// int32_t streamDefaultIter_rocksdb(SStreamState* pState, const void* start, const void* end, SArray* result);

// STaskDbWrapper* taskDbOpen(char* path, char* key, int64_t chkpId);
// void            taskDbDestroy(void* pDb, bool flush);

int32_t taskDbDoCheckpoint(void* arg, int64_t chkpId);

SBkdMgt* bkdMgtCreate(char* path);
int32_t  bkdMgtAddChkp(SBkdMgt* bm, char* task, char* path);
int32_t  bkdMgtGetDelta(SBkdMgt* bm, char* taskId, int64_t chkpId, SArray* list, char* name);
int32_t  bkdMgtDumpTo(SBkdMgt* bm, char* taskId, char* dname);
void     bkdMgtDestroy(SBkdMgt* bm);

int32_t taskDbGenChkpUploadData(void* arg, void* bkdMgt, int64_t chkpId, int8_t type, char** path, SArray* list);
#endif