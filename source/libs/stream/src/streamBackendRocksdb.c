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

#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tcommon.h"
#include "tref.h"

typedef struct SCompactFilteFactory {
  void* status;
} SCompactFilteFactory;

typedef struct {
  rocksdb_t*                       db;
  rocksdb_column_family_handle_t** pHandle;
  rocksdb_writeoptions_t*          wOpt;
  rocksdb_readoptions_t*           rOpt;
  rocksdb_options_t**              cfOpt;
  rocksdb_options_t*               dbOpt;
  RocksdbCfParam*                  param;
  void*                            pBackend;
  SListNode*                       pCompareNode;
  rocksdb_comparator_t**           pCompares;
} RocksdbCfInst;

int32_t streamStateOpenBackendCf(void* backend, char* name, char** cfs, int32_t nCf);

void            destroyRocksdbCfInst(RocksdbCfInst* inst);
int32_t         getCfIdx(const char* cfName);
STaskDbWrapper* taskDbOpenImpl(char* key, char* statePath, char* dbPath);

int32_t backendCopyFiles(char* src, char* dst);

void        destroyCompactFilteFactory(void* arg);
void        destroyCompactFilte(void* arg);
const char* compactFilteFactoryName(void* arg);
const char* compactFilteFactoryNameSess(void* arg);
const char* compactFilteFactoryNameState(void* arg);
const char* compactFilteFactoryNameFunc(void* arg);
const char* compactFilteFactoryNameFill(void* arg);

const char* compactFilteName(void* arg);
const char* compactFilteNameSess(void* arg);
const char* compactFilteNameState(void* arg);
const char* compactFilteNameFill(void* arg);
const char* compactFilteNameFunc(void* arg);

unsigned char compactFilte(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                           char** newval, size_t* newvlen, unsigned char* value_changed);
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilter(void* arg, rocksdb_compactionfiltercontext_t* ctx);
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterSess(void* arg, rocksdb_compactionfiltercontext_t* ctx);
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterState(void* arg, rocksdb_compactionfiltercontext_t* ctx);
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterFunc(void* arg, rocksdb_compactionfiltercontext_t* ctx);
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterFill(void* arg, rocksdb_compactionfiltercontext_t* ctx);

typedef int (*__db_key_encode_fn_t)(void* key, char* buf);
typedef int (*__db_key_decode_fn_t)(void* key, char* buf);
typedef int (*__db_key_tostr_fn_t)(void* key, char* buf);
typedef const char* (*__db_key_cmpname_fn_t)(void* statue);
typedef int (*__db_key_cmp_fn_t)(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
typedef void (*__db_key_cmp_destroy_fn_t)(void* state);
typedef int32_t (*__db_value_encode_fn_t)(void* value, int32_t vlen, int64_t ttl, char** dest);
typedef int32_t (*__db_value_decode_fn_t)(void* value, int32_t vlen, int64_t* ttl, char** dest);

typedef rocksdb_compactionfilter_t* (*__db_factory_create_fn_t)(void* arg, rocksdb_compactionfiltercontext_t* ctx);
typedef const char* (*__db_factory_name_fn_t)(void* arg);
typedef void (*__db_factory_destroy_fn_t)(void* arg);
typedef struct {
  const char*               key;
  int32_t                   len;
  int                       idx;
  __db_key_cmp_fn_t         cmpKey;
  __db_key_encode_fn_t      enFunc;
  __db_key_decode_fn_t      deFunc;
  __db_key_tostr_fn_t       toStrFunc;
  __db_key_cmpname_fn_t     cmpName;
  __db_key_cmp_destroy_fn_t destroyCmp;
  __db_value_encode_fn_t    enValueFunc;
  __db_value_decode_fn_t    deValueFunc;

  __db_factory_create_fn_t  createFilter;
  __db_factory_destroy_fn_t destroyFilter;
  __db_factory_name_fn_t    funcName;

} SCfInit;

const char* compareDefaultName(void* name);
const char* compareStateName(void* name);
const char* compareWinKeyName(void* name);
const char* compareSessionKeyName(void* name);
const char* compareFuncKeyName(void* name);
const char* compareParKeyName(void* name);
const char* comparePartagKeyName(void* name);

int defaultKeyComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int defaultKeyEncode(void* k, char* buf);
int defaultKeyDecode(void* k, char* buf);
int defaultKeyToString(void* k, char* buf);

int stateKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int stateKeyEncode(void* k, char* buf);
int stateKeyDecode(void* k, char* buf);
int stateKeyToString(void* k, char* buf);

int stateSessionKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int stateSessionKeyEncode(void* ses, char* buf);
int stateSessionKeyDecode(void* ses, char* buf);
int stateSessionKeyToString(void* k, char* buf);

int winKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int winKeyEncode(void* k, char* buf);
int winKeyDecode(void* k, char* buf);
int winKeyToString(void* k, char* buf);

int tupleKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int tupleKeyEncode(void* k, char* buf);
int tupleKeyDecode(void* k, char* buf);
int tupleKeyToString(void* k, char* buf);

int parKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
int parKeyEncode(void* k, char* buf);
int parKeyDecode(void* k, char* buf);
int parKeyToString(void* k, char* buf);

int32_t valueEncode(void* value, int32_t vlen, int64_t ttl, char** dest);
int32_t valueDecode(void* value, int32_t vlen, int64_t* ttl, char** dest);
int32_t valueToString(void* k, char* buf);
int32_t valueIsStale(void* k, int64_t ts);

void destroyCompare(void* arg);

static bool                streamStateIterSeekAndValid(rocksdb_iterator_t* iter, char* buf, size_t len);
static rocksdb_iterator_t* streamStateIterCreate(SStreamState* pState, const char* cfName,
                                                 rocksdb_snapshot_t** snapshot, rocksdb_readoptions_t** readOpt);

#define GEN_COLUMN_FAMILY_NAME(name, idstr, SUFFIX) sprintf(name, "%s_%s", idstr, (SUFFIX));
int32_t  copyFiles(const char* src, const char* dst);
uint32_t nextPow2(uint32_t x);

SCfInit ginitDict[] = {
    {"default", 7, 0, defaultKeyComp, defaultKeyEncode, defaultKeyDecode, defaultKeyToString, compareDefaultName,
     destroyCompare, valueEncode, valueDecode, compactFilteFactoryCreateFilter, destroyCompactFilteFactory,
     compactFilteFactoryName},

    {"state", 5, 1, stateKeyDBComp, stateKeyEncode, stateKeyDecode, stateKeyToString, compareStateName, destroyCompare,
     valueEncode, valueDecode, compactFilteFactoryCreateFilterState, destroyCompactFilteFactory,
     compactFilteFactoryNameState},

    {"fill", 4, 2, winKeyDBComp, winKeyEncode, winKeyDecode, winKeyToString, compareWinKeyName, destroyCompare,
     valueEncode, valueDecode, compactFilteFactoryCreateFilterFill, destroyCompactFilteFactory,
     compactFilteFactoryNameFill},

    {"sess", 4, 3, stateSessionKeyDBComp, stateSessionKeyEncode, stateSessionKeyDecode, stateSessionKeyToString,
     compareSessionKeyName, destroyCompare, valueEncode, valueDecode, compactFilteFactoryCreateFilterSess,
     destroyCompactFilteFactory, compactFilteFactoryNameSess},

    {"func", 4, 4, tupleKeyDBComp, tupleKeyEncode, tupleKeyDecode, tupleKeyToString, compareFuncKeyName, destroyCompare,
     valueEncode, valueDecode, compactFilteFactoryCreateFilterFunc, destroyCompactFilteFactory,
     compactFilteFactoryNameFunc},

    {"parname", 7, 5, parKeyDBComp, parKeyEncode, parKeyDecode, parKeyToString, compareParKeyName, destroyCompare,
     valueEncode, valueDecode, compactFilteFactoryCreateFilter, destroyCompactFilteFactory, compactFilteFactoryName},

    {"partag", 6, 6, parKeyDBComp, parKeyEncode, parKeyDecode, parKeyToString, comparePartagKeyName, destroyCompare,
     valueEncode, valueDecode, compactFilteFactoryCreateFilter, destroyCompactFilteFactory, compactFilteFactoryName},
};

int32_t getCfIdx(const char* cfName) {
  int    idx = -1;
  size_t len = strlen(cfName);
  for (int i = 0; i < sizeof(ginitDict) / sizeof(ginitDict[0]); i++) {
    if (len == ginitDict[i].len && strncmp(cfName, ginitDict[i].key, strlen(cfName)) == 0) {
      idx = i;
      break;
    }
  }
  return idx;
}

bool isValidCheckpoint(const char* dir) {
  return true;
  STaskDbWrapper* pDb = taskDbOpenImpl(NULL, NULL, (char*)dir);
  if (pDb == NULL) {
    return false;
  }
  taskDbDestroy(pDb, false);
  return true;
}

int32_t rebuildDirFromCheckpoint(const char* path, int64_t chkpId, char** dst) {
  // impl later
  int32_t code = 0;

  /*param@1: checkpointId dir
    param@2: state
    copy pChkpIdDir's file to state dir
    opt to set hard link to previous file
  */
  char* state = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(state, "%s%s%s", path, TD_DIRSEP, "state");
  if (chkpId != 0) {
    char* chkp = taosMemoryCalloc(1, strlen(path) + 64);
    sprintf(chkp, "%s%s%s%scheckpoint%" PRId64 "", path, TD_DIRSEP, "checkpoints", TD_DIRSEP, chkpId);
    if (taosIsDir(chkp) && isValidCheckpoint(chkp)) {
      if (taosIsDir(state)) {
        // remove dir if exists
        // taosRenameFile(const char *oldName, const char *newName)
        taosRemoveDir(state);
      }
      taosMkDir(state);
      code = backendCopyFiles(chkp, state);
      stInfo("copy snap file from %s to %s", chkp, state);
      if (code != 0) {
        stError("failed to restart stream backend from %s, reason: %s", chkp, tstrerror(TAOS_SYSTEM_ERROR(errno)));
      } else {
        stInfo("start to restart stream backend at checkpoint path: %s", chkp);
      }

    } else {
      stError("failed to start stream backend at %s, reason: %s, restart from default state dir:%s", chkp,
              tstrerror(TAOS_SYSTEM_ERROR(errno)), state);
      taosMkDir(state);
    }
    taosMemoryFree(chkp);
  }
  *dst = state;

  return 0;
}
int32_t remoteChkp_readMetaData(char* path, SArray* list) {
  char* metaPath = taosMemoryCalloc(1, strlen(path));
  sprintf(metaPath, "%s%s%s", path, TD_DIRSEP, "META");

  TdFilePtr pFile = taosOpenFile(path, TD_FILE_READ);

  char buf[128] = {0};
  if (taosReadFile(pFile, buf, sizeof(buf)) <= 0) {
    taosMemoryFree(metaPath);
    taosCloseFile(&pFile);
    return -1;
  }
  int32_t len = strlen(buf);
  for (int i = 0; i < len; i++) {
    if (buf[i] == '\n') {
      char* item = taosMemoryCalloc(1, i + 1);
      memcpy(item, buf, i);
      taosArrayPush(list, &item);

      item = taosMemoryCalloc(1, len - i);
      memcpy(item, buf + i + 1, len - i - 1);
      taosArrayPush(list, &item);
    }
  }

  taosCloseFile(&pFile);
  taosMemoryFree(metaPath);
  return 0;
}
int32_t remoteChkp_validMetaFile(char* name, char* prename, int64_t chkpId) {
  int8_t valid = 0;
  for (int i = 0; i < strlen(name); i++) {
    if (name[i] == '_') {
      memcpy(prename, name, i);
      if (taosStr2int64(name + i + 1) != chkpId) {
        break;
      } else {
        valid = 1;
      }
    }
  }
  return valid;
}
int32_t remoteChkp_validAndCvtMeta(char* path, SArray* list, int64_t chkpId) {
  int32_t complete = 1;
  int32_t len = strlen(path) + 32;
  char*   src = taosMemoryCalloc(1, len);
  char*   dst = taosMemoryCalloc(1, len);

  int8_t count = 0;
  for (int i = 0; i < taosArrayGetSize(list); i++) {
    char* p = taosArrayGetP(list, i);
    sprintf(src, "%s%s%s", path, TD_DIRSEP, p);

    // check file exist
    if (taosStatFile(src, NULL, NULL, NULL) != 0) {
      complete = 0;
      break;
    }

    // check file name
    char temp[64] = {0};
    if (remoteChkp_validMetaFile(p, temp, chkpId)) {
      count++;
    }

    // rename file
    sprintf(dst, "%s%s%s", path, TD_DIRSEP, temp);
    taosRenameFile(src, dst);

    memset(src, 0, len);
    memset(dst, 0, len);
  }
  if (count != taosArrayGetSize(list)) {
    complete = 0;
  }

  taosMemoryFree(src);
  taosMemoryFree(dst);

  return complete == 1 ? 0 : -1;
}

int32_t rebuildFromRemoteChkp_rsync(char* key, char* chkpPath, int64_t chkpId, char* defaultPath) {
  // impl later
  int32_t code = 0;
  if (taosIsDir(chkpPath)) {
    taosRemoveDir(chkpPath);
  }
  if (taosIsDir(defaultPath)) {
    taosRemoveDir(defaultPath);
  }

  code = downloadCheckpoint(key, chkpPath);
  if (code != 0) {
    return code;
  }
  code = backendCopyFiles(chkpPath, defaultPath);

  return code;
}
int32_t rebuildFromRemoteChkp_s3(char* key, char* chkpPath, int64_t chkpId, char* defaultPath) {
  int32_t code = downloadCheckpoint(key, chkpPath);
  if (code != 0) {
    return code;
  }

  int32_t len = strlen(defaultPath) + 32;
  char*   tmp = taosMemoryCalloc(1, len);
  sprintf(tmp, "%s%s", defaultPath, "_tmp");
  if (taosIsDir(tmp)) taosRemoveDir(tmp);
  if (taosIsDir(defaultPath)) taosRenameFile(defaultPath, tmp);

  SArray* list = taosArrayInit(2, sizeof(void*));
  code = remoteChkp_readMetaData(chkpPath, list);
  if (code == 0) {
    code = remoteChkp_validAndCvtMeta(chkpPath, list, chkpId);
  }
  taosArrayDestroyP(list, taosMemoryFree);

  if (code == 0) {
    taosMkDir(defaultPath);
    code = backendCopyFiles(chkpPath, defaultPath);
  }

  if (code != 0) {
    if (taosIsDir(defaultPath)) taosRemoveDir(defaultPath);
    if (taosIsDir(tmp)) taosRenameFile(tmp, defaultPath);
  } else {
    taosRemoveDir(tmp);
  }

  taosMemoryFree(tmp);
  return code;
}
int32_t rebuildFromRemoteChkp(char* key, char* chkpPath, int64_t chkpId, char* defaultPath) {
  UPLOAD_TYPE type = getUploadType();
  if (type == UPLOAD_S3) {
    return rebuildFromRemoteChkp_s3(key, chkpPath, chkpId, defaultPath);
  } else if (type == UPLOAD_RSYNC) {
    return rebuildFromRemoteChkp_rsync(key, chkpPath, chkpId, defaultPath);
  }
  return -1;
}

int32_t copyFiles_create(char* src, char* dst, int8_t type) {
  // create and copy file
  int32_t err = taosCopyFile(src, dst);

  if (errno == EXDEV || errno == ENOTSUP) {
    errno = 0;
    return 0;
  }
  return 0;
}
int32_t copyFiles_hardlink(char* src, char* dst, int8_t type) {
  // same fs and hard link
  return taosLinkFile(src, dst);
}

int32_t backendFileCopyFilesImpl(char* src, char* dst) {
  const char* current = "CURRENT";
  size_t      currLen = strlen(current);

  int32_t code = 0;
  int32_t sLen = strlen(src);
  int32_t dLen = strlen(dst);
  char*   srcName = taosMemoryCalloc(1, sLen + 64);
  char*   dstName = taosMemoryCalloc(1, dLen + 64);
  // copy file to dst

  TdDirPtr pDir = taosOpenDir(src);
  if (pDir == NULL) {
    taosMemoryFree(srcName);
    taosMemoryFree(dstName);
    errno = 0;
    return -1;
  }

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

    sprintf(srcName, "%s%s%s", src, TD_DIRSEP, name);
    sprintf(dstName, "%s%s%s", dst, TD_DIRSEP, name);

    if (strncmp(name, current, strlen(name) <= currLen ? strlen(name) : currLen) == 0) {
      code = copyFiles_create(srcName, dstName, 0);
      if (code != 0) {
        stError("failed to copy file, detail: %s to %s reason: %s", srcName, dstName,
                tstrerror(TAOS_SYSTEM_ERROR(code)));
        goto _ERROR;
      }
    } else {
      code = copyFiles_hardlink(srcName, dstName, 0);
      if (code != 0) {
        stError("failed to hard line file, detail: %s to %s, reason: %s", srcName, dstName,
                tstrerror(TAOS_SYSTEM_ERROR(code)));
        goto _ERROR;
      }
    }
    memset(srcName, 0, sLen + 64);
    memset(dstName, 0, dLen + 64);
  }

  taosMemoryFreeClear(srcName);
  taosMemoryFreeClear(dstName);
  taosCloseDir(&pDir);
  errno = 0;
  return 0;
_ERROR:
  taosMemoryFreeClear(srcName);
  taosMemoryFreeClear(dstName);
  taosCloseDir(&pDir);
  errno = 0;
  return -1;
}
int32_t backendCopyFiles(char* src, char* dst) {
  return backendFileCopyFilesImpl(src, dst);
  // // opt later, just hard link
  // int32_t sLen = strlen(src);
  // int32_t dLen = strlen(dst);
  // char*   srcName = taosMemoryCalloc(1, sLen + 64);
  // char*   dstName = taosMemoryCalloc(1, dLen + 64);

  // TdDirPtr pDir = taosOpenDir(src);
  // if (pDir == NULL) {
  //   taosMemoryFree(srcName);
  //   taosMemoryFree(dstName);
  //   return -1;
  // }

  // TdDirEntryPtr de = NULL;
  // while ((de = taosReadDir(pDir)) != NULL) {
  //   char* name = taosGetDirEntryName(de);
  //   if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

  //   sprintf(srcName, "%s%s%s", src, TD_DIRSEP, name);
  //   sprintf(dstName, "%s%s%s", dst, TD_DIRSEP, name);
  //   // if (!taosDirEntryIsDir(de)) {
  //   //   // code = taosCopyFile(srcName, dstName);
  //   //   if (code == -1) {
  //   //     goto _err;
  //   //   }
  //   // }
  //   return backendFileCopyFilesImpl(src, dst);

  //   memset(srcName, 0, sLen + 64);
  //   memset(dstName, 0, dLen + 64);
  // }

  // _err:
  //   taosMemoryFreeClear(srcName);
  //   taosMemoryFreeClear(dstName);
  //   taosCloseDir(&pDir);
  //   return code >= 0 ? 0 : -1;

  // return 0;
}
int32_t rebuildFromLocalChkp(char* key, char* chkpPath, int64_t chkpId, char* defaultPath) {
  int32_t code = 0;
  if (taosIsDir(defaultPath)) {
    taosRemoveDir(defaultPath);
    taosMkDir(defaultPath);

    stInfo("succ to clear stream backend %s", defaultPath);
  }
  if (taosIsDir(chkpPath) && isValidCheckpoint(chkpPath)) {
    code = backendCopyFiles(chkpPath, defaultPath);
    if (code != 0) {
      taosRemoveDir(defaultPath);
      taosMkDir(defaultPath);

      stError("failed to restart stream backend from %s, reason: %s, start to restart from empty path: %s", chkpPath,
              tstrerror(TAOS_SYSTEM_ERROR(errno)), defaultPath);
      code = 0;
    } else {
      stInfo("start to restart stream backend at checkpoint path: %s", chkpPath);
    }
  }

  return code;
}

int32_t rebuildFromlocalDefault(char* key, char* chkpPath, int64_t chkpId, char* defaultPath) {
  int32_t code = 0;
  return code;
}

int32_t rebuildDirFromChkp2(const char* path, char* key, int64_t chkpId, char** dbPrefixPath, char** dbPath) {
  // impl later
  int32_t code = 0;

  char* prefixPath = taosMemoryCalloc(1, strlen(path) + 128);
  sprintf(prefixPath, "%s%s%s", path, TD_DIRSEP, key);

  if (!taosIsDir(prefixPath)) {
    code = taosMkDir(prefixPath);
    ASSERT(code == 0);
  }

  char* defaultPath = taosMemoryCalloc(1, strlen(path) + 256);
  sprintf(defaultPath, "%s%s%s", prefixPath, TD_DIRSEP, "state");
  if (!taosIsDir(defaultPath)) {
    taosMulMkDir(defaultPath);
  }

  char* chkpPath = taosMemoryCalloc(1, strlen(path) + 256);
  if (chkpId != 0) {
    sprintf(chkpPath, "%s%s%s%s%s%" PRId64 "", prefixPath, TD_DIRSEP, "checkpoints", TD_DIRSEP, "checkpoint", chkpId);
    code = rebuildFromLocalChkp(key, chkpPath, chkpId, defaultPath);
    if (code != 0) {
      code = rebuildFromRemoteChkp(key, chkpPath, chkpId, defaultPath);
    }

    if (code != 0) {
      stInfo("failed to start stream backend at %s, reason: %s, restart from default defaultPath dir:%s", chkpPath,
             tstrerror(TAOS_SYSTEM_ERROR(errno)), defaultPath);
      code = taosMkDir(defaultPath);
    }
  } else {
    sprintf(chkpPath, "%s%s%s%s%s%" PRId64 "", prefixPath, TD_DIRSEP, "checkpoints", TD_DIRSEP, "checkpoint",
            (int64_t)-1);

    code = rebuildFromLocalChkp(key, chkpPath, -1, defaultPath);
    if (code != 0) {
      code = taosMkDir(defaultPath);
    }
  }
  taosMemoryFree(chkpPath);

  *dbPath = defaultPath;
  *dbPrefixPath = prefixPath;

  return code;
}

bool streamBackendDataIsExist(const char* path, int64_t chkpId, int32_t vgId) {
  bool  exist = true;
  char* state = taosMemoryCalloc(1, strlen(path) + 32);
  sprintf(state, "%s%s%s", path, TD_DIRSEP, "state");
  if (!taosDirExist(state)) {
    exist = false;
  }
  taosMemoryFree(state);
  return exist;
}
void* streamBackendInit(const char* streamPath, int64_t chkpId, int32_t vgId) {
  char*   backendPath = NULL;
  int32_t code = rebuildDirFromCheckpoint(streamPath, chkpId, &backendPath);

  stDebug("start to init stream backend at %s, checkpointid: %" PRId64 " vgId:%d", backendPath, chkpId, vgId);

  uint32_t         dbMemLimit = nextPow2(tsMaxStreamBackendCache) << 20;
  SBackendWrapper* pHandle = taosMemoryCalloc(1, sizeof(SBackendWrapper));
  pHandle->list = tdListNew(sizeof(SCfComparator));
  taosThreadMutexInit(&pHandle->mutex, NULL);
  taosThreadMutexInit(&pHandle->cfMutex, NULL);
  pHandle->cfInst = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  rocksdb_env_t* env = rocksdb_create_default_env();  // rocksdb_envoptions_create();

  int32_t nBGThread = tsNumOfSnodeStreamThreads <= 2 ? 1 : tsNumOfSnodeStreamThreads / 2;
  rocksdb_env_set_low_priority_background_threads(env, nBGThread);
  rocksdb_env_set_high_priority_background_threads(env, nBGThread);

  rocksdb_cache_t* cache = rocksdb_cache_create_lru(dbMemLimit / 2);

  rocksdb_options_t* opts = rocksdb_options_create();
  rocksdb_options_set_env(opts, env);
  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_create_missing_column_families(opts, 1);
  rocksdb_options_set_max_total_wal_size(opts, dbMemLimit);
  rocksdb_options_set_recycle_log_file_num(opts, 6);
  rocksdb_options_set_max_write_buffer_number(opts, 3);
  rocksdb_options_set_info_log_level(opts, 1);
  rocksdb_options_set_db_write_buffer_size(opts, dbMemLimit);
  rocksdb_options_set_write_buffer_size(opts, dbMemLimit / 2);
  rocksdb_options_set_atomic_flush(opts, 1);

  pHandle->env = env;
  pHandle->dbOpt = opts;
  pHandle->cache = cache;
  pHandle->filterFactory = rocksdb_compactionfilterfactory_create(
      NULL, destroyCompactFilteFactory, compactFilteFactoryCreateFilter, compactFilteFactoryName);
  rocksdb_options_set_compaction_filter_factory(pHandle->dbOpt, pHandle->filterFactory);

  char*  err = NULL;
  size_t nCf = 0;

  char** cfs = rocksdb_list_column_families(opts, backendPath, &nCf, &err);
  if (nCf == 0 || nCf == 1 || err != NULL) {
    taosMemoryFreeClear(err);
    pHandle->db = rocksdb_open(opts, backendPath, &err);
    if (err != NULL) {
      stError("failed to open rocksdb, path:%s, reason:%s", backendPath, err);
      taosMemoryFreeClear(err);
      rocksdb_list_column_families_destroy(cfs, nCf);
      goto _EXIT;
    }
  } else {
    /*
      list all cf and get prefix
    */
    code = streamStateOpenBackendCf(pHandle, (char*)backendPath, cfs, nCf);
    if (code != 0) {
      rocksdb_list_column_families_destroy(cfs, nCf);
      goto _EXIT;
    }
  }
  if (cfs != NULL) {
    rocksdb_list_column_families_destroy(cfs, nCf);
  }
  stDebug("succ to init stream backend at %s, backend:%p, vgId:%d", backendPath, pHandle, vgId);
  taosMemoryFreeClear(backendPath);

  return (void*)pHandle;
_EXIT:
  rocksdb_options_destroy(opts);
  rocksdb_cache_destroy(cache);
  rocksdb_env_destroy(env);
  taosThreadMutexDestroy(&pHandle->mutex);
  taosThreadMutexDestroy(&pHandle->cfMutex);
  taosHashCleanup(pHandle->cfInst);
  tdListFree(pHandle->list);
  taosMemoryFree(pHandle);
  stDebug("failed to init stream backend at %s", backendPath);
  taosMemoryFree(backendPath);
  return NULL;
}
void streamBackendCleanup(void* arg) {
  SBackendWrapper* pHandle = (SBackendWrapper*)arg;

  void* pIter = taosHashIterate(pHandle->cfInst, NULL);
  while (pIter != NULL) {
    RocksdbCfInst* inst = *(RocksdbCfInst**)pIter;
    destroyRocksdbCfInst(inst);
    pIter = taosHashIterate(pHandle->cfInst, pIter);
  }

  taosHashCleanup(pHandle->cfInst);

  if (pHandle->db) {
    rocksdb_close(pHandle->db);
  }
  rocksdb_options_destroy(pHandle->dbOpt);
  rocksdb_env_destroy(pHandle->env);
  rocksdb_cache_destroy(pHandle->cache);

  SListNode* head = tdListPopHead(pHandle->list);
  while (head != NULL) {
    streamStateDestroyCompar(head->data);
    taosMemoryFree(head);
    head = tdListPopHead(pHandle->list);
  }

  tdListFree(pHandle->list);
  taosThreadMutexDestroy(&pHandle->mutex);

  taosThreadMutexDestroy(&pHandle->cfMutex);
  stDebug("destroy stream backend :%p", pHandle);
  taosMemoryFree(pHandle);
  return;
}
void streamBackendHandleCleanup(void* arg) {
  SBackendCfWrapper* wrapper = arg;
  bool               remove = wrapper->remove;
  taosThreadRwlockWrlock(&wrapper->rwLock);

  stDebug("start to do-close backendwrapper %p, %s", wrapper, wrapper->idstr);
  if (wrapper->rocksdb == NULL) {
    taosThreadRwlockUnlock(&wrapper->rwLock);
    return;
  }

  int cfLen = sizeof(ginitDict) / sizeof(ginitDict[0]);

  char* err = NULL;
  if (remove) {
    for (int i = 0; i < cfLen; i++) {
      if (wrapper->pHandle[i] != NULL) rocksdb_drop_column_family(wrapper->rocksdb, wrapper->pHandle[i], &err);
      if (err != NULL) {
        stError("failed to drop cf:%s_%s, reason:%s", wrapper->idstr, ginitDict[i].key, err);
        taosMemoryFreeClear(err);
      }
    }
  } else {
    rocksdb_flushoptions_t* flushOpt = rocksdb_flushoptions_create();
    rocksdb_flushoptions_set_wait(flushOpt, 1);

    for (int i = 0; i < cfLen; i++) {
      if (wrapper->pHandle[i] != NULL) rocksdb_flush_cf(wrapper->rocksdb, flushOpt, wrapper->pHandle[i], &err);
      if (err != NULL) {
        stError("failed to flush cf:%s_%s, reason:%s", wrapper->idstr, ginitDict[i].key, err);
        taosMemoryFreeClear(err);
      }
    }
    rocksdb_flushoptions_destroy(flushOpt);
  }

  for (int i = 0; i < cfLen; i++) {
    if (wrapper->pHandle[i] != NULL) {
      rocksdb_column_family_handle_destroy(wrapper->pHandle[i]);
    }
  }
  taosMemoryFreeClear(wrapper->pHandle);

  for (int i = 0; i < cfLen; i++) {
    rocksdb_options_destroy(wrapper->cfOpts[i]);
    rocksdb_block_based_options_destroy(((RocksdbCfParam*)wrapper->param)[i].tableOpt);
  }

  if (remove) {
    streamBackendDelCompare(wrapper->pBackend, wrapper->pComparNode);
  }
  rocksdb_writeoptions_destroy(wrapper->writeOpts);
  wrapper->writeOpts = NULL;

  rocksdb_readoptions_destroy(wrapper->readOpts);
  wrapper->readOpts = NULL;
  taosMemoryFreeClear(wrapper->cfOpts);
  taosMemoryFreeClear(wrapper->param);
  taosThreadRwlockUnlock(&wrapper->rwLock);

  taosThreadRwlockDestroy(&wrapper->rwLock);
  wrapper->rocksdb = NULL;
  // taosReleaseRef(streamBackendId, wrapper->backendId);

  stDebug("end to do-close backendwrapper %p, %s", wrapper, wrapper->idstr);
  taosMemoryFree(wrapper);
  return;
}

#ifdef BUILD_NO_CALL
int32_t getLatestCheckpoint(void* arg, int64_t* checkpoint) {
  SStreamMeta* pMeta = arg;
  taosWLockLatch(&pMeta->chkpDirLock);
  int64_t tc = 0;
  int32_t sz = taosArrayGetSize(pMeta->chkpSaved);
  if (sz <= 0) {
    taosWUnLockLatch(&pMeta->chkpDirLock);
    return -1;
  } else {
    tc = *(int64_t*)taosArrayGetLast(pMeta->chkpSaved);
  }

  taosArrayPush(pMeta->chkpInUse, &tc);

  *checkpoint = tc;
  taosWUnLockLatch(&pMeta->chkpDirLock);
  return 0;
}
/*
 *  checkpointSave |--cp1--|--cp2--|--cp3--|--cp4--|--cp5--|
 *  chkpInUse: |--cp2--|--cp4--|
 *  chkpInUse is doing translation, cannot del until
 *  replication is finished
 */
int32_t delObsoleteCheckpoint(void* arg, const char* path) {
  SStreamMeta* pMeta = arg;

  taosWLockLatch(&pMeta->chkpDirLock);

  SArray* chkpDel = taosArrayInit(10, sizeof(int64_t));
  SArray* chkpDup = taosArrayInit(10, sizeof(int64_t));

  int64_t firsId = 0;
  if (taosArrayGetSize(pMeta->chkpInUse) >= 1) {
    firsId = *(int64_t*)taosArrayGet(pMeta->chkpInUse, 0);

    for (int i = 0; i < taosArrayGetSize(pMeta->chkpSaved); i++) {
      int64_t id = *(int64_t*)taosArrayGet(pMeta->chkpSaved, i);
      if (id >= firsId) {
        taosArrayPush(chkpDup, &id);
      } else {
        taosArrayPush(chkpDel, &id);
      }
    }
  } else {
    int32_t sz = taosArrayGetSize(pMeta->chkpSaved);
    int32_t dsz = sz - pMeta->chkpCap;  // del size

    for (int i = 0; i < dsz; i++) {
      int64_t id = *(int64_t*)taosArrayGet(pMeta->chkpSaved, i);
      taosArrayPush(chkpDel, &id);
    }
    for (int i = dsz < 0 ? 0 : dsz; i < sz; i++) {
      int64_t id = *(int64_t*)taosArrayGet(pMeta->chkpSaved, i);
      taosArrayPush(chkpDup, &id);
    }
  }
  taosArrayDestroy(pMeta->chkpSaved);
  pMeta->chkpSaved = chkpDup;

  taosWUnLockLatch(&pMeta->chkpDirLock);

  for (int i = 0; i < taosArrayGetSize(chkpDel); i++) {
    int64_t id = *(int64_t*)taosArrayGet(chkpDel, i);
    char    tbuf[256] = {0};
    sprintf(tbuf, "%s%scheckpoint%" PRId64 "", path, TD_DIRSEP, id);
    if (taosIsDir(tbuf)) {
      taosRemoveDir(tbuf);
    }
  }
  taosArrayDestroy(chkpDel);
  return 0;
}
#endif
/*
 *  checkpointSave |--cp1--|--cp2--|--cp3--|--cp4--|--cp5--|
 *  chkpInUse: |--cp2--|--cp4--|
 *  chkpInUse is doing translation, cannot del until
 *  replication is finished
 */
int32_t chkpMayDelObsolete(void* arg, int64_t chkpId, char* path) {
  STaskDbWrapper* pBackend = arg;

  taosThreadRwlockWrlock(&pBackend->chkpDirLock);

  taosArrayPush(pBackend->chkpSaved, &chkpId);

  SArray* chkpDel = taosArrayInit(8, sizeof(int64_t));
  SArray* chkpDup = taosArrayInit(8, sizeof(int64_t));

  int64_t firsId = 0;
  if (taosArrayGetSize(pBackend->chkpInUse) >= 1) {
    firsId = *(int64_t*)taosArrayGet(pBackend->chkpInUse, 0);

    for (int i = 0; i < taosArrayGetSize(pBackend->chkpSaved); i++) {
      int64_t id = *(int64_t*)taosArrayGet(pBackend->chkpSaved, i);
      if (id >= firsId) {
        taosArrayPush(chkpDup, &id);
      } else {
        taosArrayPush(chkpDel, &id);
      }
    }
  } else {
    int32_t sz = taosArrayGetSize(pBackend->chkpSaved);
    int32_t dsz = sz - pBackend->chkpCap;  // del size

    for (int i = 0; i < dsz; i++) {
      int64_t id = *(int64_t*)taosArrayGet(pBackend->chkpSaved, i);
      taosArrayPush(chkpDel, &id);
    }
    for (int i = dsz < 0 ? 0 : dsz; i < sz; i++) {
      int64_t id = *(int64_t*)taosArrayGet(pBackend->chkpSaved, i);
      taosArrayPush(chkpDup, &id);
    }
  }
  taosArrayDestroy(pBackend->chkpSaved);
  pBackend->chkpSaved = chkpDup;

  taosThreadRwlockUnlock(&pBackend->chkpDirLock);

  for (int i = 0; i < taosArrayGetSize(chkpDel); i++) {
    int64_t id = *(int64_t*)taosArrayGet(chkpDel, i);
    char    tbuf[256] = {0};
    sprintf(tbuf, "%s%scheckpoint%" PRId64 "", path, TD_DIRSEP, id);
    if (taosIsDir(tbuf)) {
      taosRemoveDir(tbuf);
    }
  }
  taosArrayDestroy(chkpDel);
  return 0;
}

static int32_t chkpIdComp(const void* a, const void* b) {
  int64_t x = *(int64_t*)a;
  int64_t y = *(int64_t*)b;
  return x < y ? -1 : 1;
}

int32_t streamBackendLoadCheckpointInfo(void* arg) {
  SStreamMeta* pMeta = arg;
  int32_t      code = 0;
  SArray*      suffix = NULL;

  int32_t len = strlen(pMeta->path) + 30;
  char*   chkpPath = taosMemoryCalloc(1, len);
  sprintf(chkpPath, "%s%s%s", pMeta->path, TD_DIRSEP, "checkpoints");

  if (!taosDirExist(chkpPath)) {
    // no checkpoint, nothing to load
    taosMemoryFree(chkpPath);
    return 0;
  }

  TdDirPtr pDir = taosOpenDir(chkpPath);
  if (pDir == NULL) {
    taosMemoryFree(chkpPath);
    return 0;
  }

  TdDirEntryPtr de = NULL;
  suffix = taosArrayInit(4, sizeof(int64_t));

  while ((de = taosReadDir(pDir)) != NULL) {
    if (strcmp(taosGetDirEntryName(de), ".") == 0 || strcmp(taosGetDirEntryName(de), "..") == 0) continue;

    if (taosDirEntryIsDir(de)) {
      char    checkpointPrefix[32] = {0};
      int64_t checkpointId = 0;

      int ret = sscanf(taosGetDirEntryName(de), "checkpoint%" PRId64 "", &checkpointId);
      if (ret == 1) {
        taosArrayPush(suffix, &checkpointId);
      }
    } else {
      continue;
    }
  }
  taosArraySort(suffix, chkpIdComp);
  // free previous chkpSaved
  taosArrayClear(pMeta->chkpSaved);
  for (int i = 0; i < taosArrayGetSize(suffix); i++) {
    int64_t id = *(int64_t*)taosArrayGet(suffix, i);
    taosArrayPush(pMeta->chkpSaved, &id);
  }

  taosArrayDestroy(suffix);
  taosCloseDir(&pDir);
  taosMemoryFree(chkpPath);
  return 0;
}

#ifdef BUILD_NO_CALL
int32_t chkpGetAllDbCfHandle(SStreamMeta* pMeta, rocksdb_column_family_handle_t*** ppHandle, SArray* refs) {
  return 0;
  // SArray* pHandle = taosArrayInit(16, POINTER_BYTES);
  // void*   pIter = taosHashIterate(pMeta->pTaskDbUnique, NULL);
  // while (pIter) {
  //   int64_t id = *(int64_t*)pIter;

  //   SBackendCfWrapper* wrapper = taosAcquireRef(streamBackendCfWrapperId, id);
  //   if (wrapper == NULL) {
  //     pIter = taosHashIterate(pMeta->pTaskDbUnique, pIter);
  //     continue;
  //   }

  //   taosThreadRwlockRdlock(&wrapper->rwLock);
  //   for (int i = 0; i < sizeof(ginitDict) / sizeof(ginitDict[0]); i++) {
  //     if (wrapper->pHandle[i]) {
  //       rocksdb_column_family_handle_t* p = wrapper->pHandle[i];
  //       taosArrayPush(pHandle, &p);
  //     }
  //   }
  //   taosThreadRwlockUnlock(&wrapper->rwLock);

  //   taosArrayPush(refs, &id);
  // }

  // int32_t nCf = taosArrayGetSize(pHandle);

  // rocksdb_column_family_handle_t** ppCf = taosMemoryCalloc(nCf, sizeof(rocksdb_column_family_handle_t*));
  // for (int i = 0; i < nCf; i++) {
  //   ppCf[i] = taosArrayGetP(pHandle, i);
  // }
  // taosArrayDestroy(pHandle);

  // *ppHandle = ppCf;
  // return nCf;
}
#endif

int32_t chkpGetAllDbCfHandle2(STaskDbWrapper* pBackend, rocksdb_column_family_handle_t*** ppHandle) {
  SArray* pHandle = taosArrayInit(8, POINTER_BYTES);
  for (int i = 0; i < sizeof(ginitDict) / sizeof(ginitDict[0]); i++) {
    if (pBackend->pCf[i]) {
      rocksdb_column_family_handle_t* p = pBackend->pCf[i];
      taosArrayPush(pHandle, &p);
    }
  }
  int32_t nCf = taosArrayGetSize(pHandle);
  if (nCf == 0) {
    taosArrayDestroy(pHandle);
    return nCf;
  }

  rocksdb_column_family_handle_t** ppCf = taosMemoryCalloc(nCf, sizeof(rocksdb_column_family_handle_t*));
  for (int i = 0; i < nCf; i++) {
    ppCf[i] = taosArrayGetP(pHandle, i);
  }
  taosArrayDestroy(pHandle);

  *ppHandle = ppCf;
  return nCf;
}

int32_t chkpDoDbCheckpoint(rocksdb_t* db, char* path) {
  int32_t               code = -1;
  char*                 err = NULL;
  rocksdb_checkpoint_t* cp = rocksdb_checkpoint_object_create(db, &err);
  if (cp == NULL || err != NULL) {
    stError("failed to do checkpoint at:%s, reason:%s", path, err);
    taosMemoryFreeClear(err);
    goto _ERROR;
  }

  rocksdb_checkpoint_create(cp, path, 64 << 20, &err);
  if (err != NULL) {
    stError("failed to do checkpoint at:%s, reason:%s", path, err);
    taosMemoryFreeClear(err);
  } else {
    code = 0;
  }
_ERROR:
  rocksdb_checkpoint_object_destroy(cp);
  return code;
}
int32_t chkpPreFlushDb(rocksdb_t* db, rocksdb_column_family_handle_t** cf, int32_t nCf) {
  if (nCf == 0) return 0;
  int   code = 0;
  char* err = NULL;

  rocksdb_flushoptions_t* flushOpt = rocksdb_flushoptions_create();
  rocksdb_flushoptions_set_wait(flushOpt, 1);

  rocksdb_flush_cfs(db, flushOpt, cf, nCf, &err);
  if (err != NULL) {
    stError("failed to flush db before streamBackend clean up, reason:%s", err);
    taosMemoryFree(err);
    code = -1;
  }
  rocksdb_flushoptions_destroy(flushOpt);
  return code;
}

int32_t chkpPreBuildDir(char* path, int64_t chkpId, char** chkpDir, char** chkpIdDir) {
  int32_t code = 0;
  char*   pChkpDir = taosMemoryCalloc(1, 256);
  char*   pChkpIdDir = taosMemoryCalloc(1, 256);

  sprintf(pChkpDir, "%s%s%s", path, TD_DIRSEP, "checkpoints");
  code = taosMulModeMkDir(pChkpDir, 0755, true);
  if (code != 0) {
    stError("failed to prepare checkpoint dir, path:%s, reason:%s", path, tstrerror(code));
    taosMemoryFree(pChkpDir);
    taosMemoryFree(pChkpIdDir);
    code = -1;
    return code;
  }

  sprintf(pChkpIdDir, "%s%s%s%" PRId64, pChkpDir, TD_DIRSEP, "checkpoint", chkpId);
  if (taosIsDir(pChkpIdDir)) {
    stInfo("stream rm exist checkpoint%s", pChkpIdDir);
    taosRemoveDir(pChkpIdDir);
  }
  *chkpDir = pChkpDir;
  *chkpIdDir = pChkpIdDir;

  return 0;
}
int32_t taskDbBuildSnap(void* arg, SArray* pSnap) {
  SStreamMeta* pMeta = arg;

  taosThreadMutexLock(&pMeta->backendMutex);
  void*   pIter = taosHashIterate(pMeta->pTaskDbUnique, NULL);
  int32_t code = 0;

  while (pIter) {
    STaskDbWrapper* pTaskDb = *(STaskDbWrapper**)pIter;
    taskDbAddRef(pTaskDb);

    code = taskDbDoCheckpoint(pTaskDb, pTaskDb->chkpId);
    taskDbRemoveRef(pTaskDb);

    SStreamTask*    pTask = pTaskDb->pTask;
    SStreamTaskSnap snap = {.streamId = pTask->id.streamId,
                            .taskId = pTask->id.taskId,
                            .chkpId = pTaskDb->chkpId,
                            .dbPrefixPath = taosStrdup(pTaskDb->path)};
    taosArrayPush(pSnap, &snap);
    pIter = taosHashIterate(pMeta->pTaskDbUnique, pIter);
  }
  taosThreadMutexUnlock(&pMeta->backendMutex);

  return code;
}
#ifdef BUILD_NO_CALL
int32_t streamBackendAddInUseChkp(void* arg, int64_t chkpId) {
  // if (arg == NULL) return 0;

  // SStreamMeta* pMeta = arg;
  // taosWLockLatch(&pMeta->chkpDirLock);
  // taosArrayPush(pMeta->chkpInUse, &chkpId);
  // taosWUnLockLatch(&pMeta->chkpDirLock);
  return 0;
}
int32_t streamBackendDelInUseChkp(void* arg, int64_t chkpId) {
  return 0;
  // if (arg == NULL) return 0;

  // SStreamMeta* pMeta = arg;
  // taosWLockLatch(&pMeta->chkpDirLock);
  // if (taosArrayGetSize(pMeta->chkpInUse) > 0) {
  //   int64_t id = *(int64_t*)taosArrayGet(pMeta->chkpInUse, 0);
  //   if (id == chkpId) {
  //     taosArrayPopFrontBatch(pMeta->chkpInUse, 1);
  //   }
  // }
  // taosWUnLockLatch(&pMeta->chkpDirLock);
}
#endif

/*
   0
*/
int32_t taskDbDoCheckpoint(void* arg, int64_t chkpId) {
  STaskDbWrapper* pTaskDb = arg;
  int64_t         st = taosGetTimestampMs();
  int32_t         code = -1;
  int64_t         refId = pTaskDb->refId;

  if (taosAcquireRef(taskDbWrapperId, refId) == NULL) {
    return -1;
  }

  char* pChkpDir = NULL;
  char* pChkpIdDir = NULL;
  if (chkpPreBuildDir(pTaskDb->path, chkpId, &pChkpDir, &pChkpIdDir) != 0) {
    code = -1;
    goto _EXIT;
  }
  // Get all cf and acquire cfWrappter
  rocksdb_column_family_handle_t** ppCf = NULL;

  int32_t nCf = chkpGetAllDbCfHandle2(pTaskDb, &ppCf);
  stDebug("stream backend:%p start to do checkpoint at:%s, cf num: %d ", pTaskDb, pChkpIdDir, nCf);

  if ((code = chkpPreFlushDb(pTaskDb->db, ppCf, nCf)) == 0) {
    if ((code = chkpDoDbCheckpoint(pTaskDb->db, pChkpIdDir)) != 0) {
      stError("stream backend:%p failed to do checkpoint at:%s", pTaskDb, pChkpIdDir);
    } else {
      stDebug("stream backend:%p end to do checkpoint at:%s, time cost:%" PRId64 "ms", pTaskDb, pChkpIdDir,
              taosGetTimestampMs() - st);
    }
  } else {
    stError("stream backend:%p failed to flush db at:%s", pTaskDb, pChkpIdDir);
  }

  code = chkpMayDelObsolete(pTaskDb, chkpId, pChkpDir);
  pTaskDb->dataWritten = 0;

  pTaskDb->chkpId = chkpId;

_EXIT:
  taosMemoryFree(pChkpDir);
  taosMemoryFree(pChkpIdDir);
  taosReleaseRef(taskDbWrapperId, refId);
  taosMemoryFree(ppCf);
  return code;
}
int32_t streamBackendDoCheckpoint(void* arg, int64_t chkpId) { return taskDbDoCheckpoint(arg, chkpId); }

SListNode* streamBackendAddCompare(void* backend, void* arg) {
  SBackendWrapper* pHandle = (SBackendWrapper*)backend;
  SListNode*       node = NULL;
  taosThreadMutexLock(&pHandle->mutex);
  node = tdListAdd(pHandle->list, arg);
  taosThreadMutexUnlock(&pHandle->mutex);
  return node;
}
void streamBackendDelCompare(void* backend, void* arg) {
  SBackendWrapper* pHandle = (SBackendWrapper*)backend;
  SListNode*       node = NULL;
  taosThreadMutexLock(&pHandle->mutex);
  node = tdListPopNode(pHandle->list, arg);
  taosThreadMutexUnlock(&pHandle->mutex);
  if (node) {
    streamStateDestroyCompar(node->data);
    taosMemoryFree(node);
  }
}
#ifdef BUILD_NO_CALL
void streamStateDestroy_rocksdb(SStreamState* pState, bool remove) { streamStateCloseBackend(pState, remove); }
#endif
void destroyRocksdbCfInst(RocksdbCfInst* inst) {
  int cfLen = sizeof(ginitDict) / sizeof(ginitDict[0]);
  if (inst->pHandle) {
    for (int i = 0; i < cfLen; i++) {
      if (inst->pHandle[i]) rocksdb_column_family_handle_destroy((inst->pHandle)[i]);
    }
    taosMemoryFree(inst->pHandle);
  }

  if (inst->cfOpt) {
    for (int i = 0; i < cfLen; i++) {
      rocksdb_options_destroy(inst->cfOpt[i]);
      rocksdb_block_based_options_destroy(((RocksdbCfParam*)inst->param)[i].tableOpt);
    }
    taosMemoryFreeClear(inst->cfOpt);
    taosMemoryFreeClear(inst->param);
  }
  if (inst->wOpt) rocksdb_writeoptions_destroy(inst->wOpt);
  if (inst->rOpt) rocksdb_readoptions_destroy(inst->rOpt);

  taosMemoryFree(inst);
}

// |key|-----value------|
// |key|ttl|len|userData|

int defaultKeyComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  int len = aLen < bLen ? aLen : bLen;
  int ret = memcmp(aBuf, bBuf, len);
  if (ret == 0) {
    if (aLen < bLen)
      return -1;
    else if (aLen > bLen)
      return 1;
    else
      return 0;
  } else {
    return ret;
  }
}
int streamStateValueIsStale(char* v) {
  int64_t ts = 0;
  taosDecodeFixedI64(v, &ts);
  return (ts != 0 && ts < taosGetTimestampMs()) ? 1 : 0;
}
int iterValueIsStale(rocksdb_iterator_t* iter) {
  size_t len;
  char*  v = (char*)rocksdb_iter_value(iter, &len);
  return streamStateValueIsStale(v);
}
int defaultKeyEncode(void* k, char* buf) {
  int len = strlen((char*)k);
  memcpy(buf, (char*)k, len);
  return len;
}
int defaultKeyDecode(void* k, char* buf) {
  int len = strlen(buf);
  memcpy(k, buf, len);
  return len;
}
int defaultKeyToString(void* k, char* buf) {
  // just to debug
  return sprintf(buf, "key: %s", (char*)k);
}
//
//  SStateKey
//  |--groupid--|---ts------|--opNum----|
//  |--uint64_t-|-uint64_t--|--int64_t--|
//
//
//
int stateKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SStateKey key1, key2;
  memset(&key1, 0, sizeof(key1));
  memset(&key2, 0, sizeof(key2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &key1.key.groupId);
  p2 = taosDecodeFixedU64(p2, &key2.key.groupId);

  p1 = taosDecodeFixedI64(p1, &key1.key.ts);
  p2 = taosDecodeFixedI64(p2, &key2.key.ts);

  taosDecodeFixedI64(p1, &key1.opNum);
  taosDecodeFixedI64(p2, &key2.opNum);

  return stateKeyCmpr(&key1, sizeof(key1), &key2, sizeof(key2));
}

int stateKeyEncode(void* k, char* buf) {
  SStateKey* key = k;
  int        len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->key.groupId);
  len += taosEncodeFixedI64((void**)&buf, key->key.ts);
  len += taosEncodeFixedI64((void**)&buf, key->opNum);
  return len;
}
int stateKeyDecode(void* k, char* buf) {
  SStateKey* key = k;
  int        len = 0;
  char*      p = buf;
  p = taosDecodeFixedU64(p, &key->key.groupId);
  p = taosDecodeFixedI64(p, &key->key.ts);
  p = taosDecodeFixedI64(p, &key->opNum);
  return p - buf;
}

int stateKeyToString(void* k, char* buf) {
  SStateKey* key = k;
  int        n = 0;
  n += sprintf(buf + n, "[groupId:%" PRIu64 ",", key->key.groupId);
  n += sprintf(buf + n, "ts:%" PRIi64 ",", key->key.ts);
  n += sprintf(buf + n, "opNum:%" PRIi64 "]", key->opNum);
  return n;
}

//
// SStateSessionKey
//  |-----------SSessionKey----------|
//  |-----STimeWindow-----|
//  |---skey--|---ekey----|--groupId-|--opNum--|
//  |---int64-|--int64_t--|--uint64--|--int64_t|
// |
//
int stateSessionKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SStateSessionKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedI64(p1, &w1.key.win.skey);
  p2 = taosDecodeFixedI64(p2, &w2.key.win.skey);

  p1 = taosDecodeFixedI64(p1, &w1.key.win.ekey);
  p2 = taosDecodeFixedI64(p2, &w2.key.win.ekey);

  p1 = taosDecodeFixedU64(p1, &w1.key.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.key.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.opNum);
  p2 = taosDecodeFixedI64(p2, &w2.opNum);

  return stateSessionKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}
int stateSessionKeyEncode(void* k, char* buf) {
  SStateSessionKey* sess = k;
  int               len = 0;
  len += taosEncodeFixedI64((void**)&buf, sess->key.win.skey);
  len += taosEncodeFixedI64((void**)&buf, sess->key.win.ekey);
  len += taosEncodeFixedU64((void**)&buf, sess->key.groupId);
  len += taosEncodeFixedI64((void**)&buf, sess->opNum);
  return len;
}
int stateSessionKeyDecode(void* k, char* buf) {
  SStateSessionKey* sess = k;
  int               len = 0;

  char* p = buf;
  p = taosDecodeFixedI64(p, &sess->key.win.skey);
  p = taosDecodeFixedI64(p, &sess->key.win.ekey);
  p = taosDecodeFixedU64(p, &sess->key.groupId);
  p = taosDecodeFixedI64(p, &sess->opNum);
  return p - buf;
}
int stateSessionKeyToString(void* k, char* buf) {
  SStateSessionKey* key = k;
  int               n = 0;
  n += sprintf(buf + n, "[skey:%" PRIi64 ",", key->key.win.skey);
  n += sprintf(buf + n, "ekey:%" PRIi64 ",", key->key.win.ekey);
  n += sprintf(buf + n, "groupId:%" PRIu64 ",", key->key.groupId);
  n += sprintf(buf + n, "opNum:%" PRIi64 "]", key->opNum);
  return n;
}

/**
 *  SWinKey
 *  |------groupId------|-----ts------|
 *  |------uint64-------|----int64----|
 */
int winKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SWinKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &w1.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.ts);
  p2 = taosDecodeFixedI64(p2, &w2.ts);

  return winKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}

int winKeyEncode(void* k, char* buf) {
  SWinKey* key = k;
  int      len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->groupId);
  len += taosEncodeFixedI64((void**)&buf, key->ts);
  return len;
}

int winKeyDecode(void* k, char* buf) {
  SWinKey* key = k;
  int      len = 0;
  char*    p = buf;
  p = taosDecodeFixedU64(p, &key->groupId);
  p = taosDecodeFixedI64(p, &key->ts);
  return len;
}

int winKeyToString(void* k, char* buf) {
  SWinKey* key = k;
  int      n = 0;
  n += sprintf(buf + n, "[groupId:%" PRIu64 ",", key->groupId);
  n += sprintf(buf + n, "ts:%" PRIi64 "]", key->ts);
  return n;
}
/*
 * STupleKey
 * |---groupId---|---ts---|---exprIdx---|
 * |---uint64--|---int64--|---int32-----|
 */
int tupleKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  STupleKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &w1.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.ts);
  p2 = taosDecodeFixedI64(p2, &w2.ts);

  p1 = taosDecodeFixedI32(p1, &w1.exprIdx);
  p2 = taosDecodeFixedI32(p2, &w2.exprIdx);

  return STupleKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}

int tupleKeyEncode(void* k, char* buf) {
  STupleKey* key = k;
  int        len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->groupId);
  len += taosEncodeFixedI64((void**)&buf, key->ts);
  len += taosEncodeFixedI32((void**)&buf, key->exprIdx);
  return len;
}
int tupleKeyDecode(void* k, char* buf) {
  STupleKey* key = k;
  int        len = 0;
  char*      p = buf;
  p = taosDecodeFixedU64(p, &key->groupId);
  p = taosDecodeFixedI64(p, &key->ts);
  p = taosDecodeFixedI32(p, &key->exprIdx);
  return len;
}
int tupleKeyToString(void* k, char* buf) {
  int        n = 0;
  STupleKey* key = k;
  n += sprintf(buf + n, "[groupId:%" PRIu64 ",", key->groupId);
  n += sprintf(buf + n, "ts:%" PRIi64 ",", key->ts);
  n += sprintf(buf + n, "exprIdx:%d]", key->exprIdx);
  return n;
}

int parKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  int64_t w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));
  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  taosDecodeFixedI64(p1, &w1);
  taosDecodeFixedI64(p2, &w2);
  if (w1 == w2) {
    return 0;
  } else {
    return w1 < w2 ? -1 : 1;
  }
}
int parKeyEncode(void* k, char* buf) {
  int64_t* groupid = k;
  int      len = taosEncodeFixedI64((void**)&buf, *groupid);
  return len;
}
int parKeyDecode(void* k, char* buf) {
  char*    p = buf;
  int64_t* groupid = k;

  p = taosDecodeFixedI64(p, groupid);
  return p - buf;
}
int parKeyToString(void* k, char* buf) {
  int64_t* key = k;
  int      n = 0;
  n = sprintf(buf + n, "[groupId:%" PRIi64 "]", *key);
  return n;
}
int32_t valueToString(void* k, char* buf) {
  SStreamValue* key = k;
  int           n = 0;
  n += sprintf(buf + n, "[unixTimestamp:%" PRIi64 ",", key->unixTimestamp);
  n += sprintf(buf + n, "len:%d,", key->len);
  n += sprintf(buf + n, "data:%s]", key->data);
  return n;
}

/*1: stale, 0: no stale*/
int32_t valueIsStale(void* k, int64_t ts) {
  SStreamValue* key = k;
  if (key->unixTimestamp < ts) {
    return 1;
  }
  return 0;
}

void destroyCompare(void* arg) {
  (void)arg;
  return;
}

int32_t valueEncode(void* value, int32_t vlen, int64_t ttl, char** dest) {
  SStreamValue key = {.unixTimestamp = ttl, .len = vlen, .data = (char*)(value)};
  int32_t      len = 0;
  if (*dest == NULL) {
    char* p = taosMemoryCalloc(1, sizeof(int64_t) + sizeof(int32_t) + key.len);
    char* buf = p;
    len += taosEncodeFixedI64((void**)&buf, key.unixTimestamp);
    len += taosEncodeFixedI32((void**)&buf, key.len);
    len += taosEncodeBinary((void**)&buf, (char*)value, vlen);
    *dest = p;
  } else {
    char* buf = *dest;
    len += taosEncodeFixedI64((void**)&buf, key.unixTimestamp);
    len += taosEncodeFixedI32((void**)&buf, key.len);
    len += taosEncodeBinary((void**)&buf, (char*)value, vlen);
  }
  return len;
}
/*
 *  ret >= 0 : found valid value
 *  ret < 0 : error or timeout
 */
int32_t valueDecode(void* value, int32_t vlen, int64_t* ttl, char** dest) {
  SStreamValue key = {0};
  char*        p = value;
  if (streamStateValueIsStale(p)) {
    goto _EXCEPT;
  }
  p = taosDecodeFixedI64(p, &key.unixTimestamp);
  p = taosDecodeFixedI32(p, &key.len);
  if (vlen != (sizeof(int64_t) + sizeof(int32_t) + key.len)) {
    stError("vlen: %d, read len: %d", vlen, key.len);
    goto _EXCEPT;
  }
  if (key.len != 0 && dest != NULL) p = taosDecodeBinary(p, (void**)dest, key.len);

  if (ttl != NULL) *ttl = key.unixTimestamp == 0 ? 0 : key.unixTimestamp - taosGetTimestampMs();
  return key.len;

_EXCEPT:
  if (dest != NULL) *dest = NULL;
  if (ttl != NULL) *ttl = 0;
  return -1;
}

const char* compareDefaultName(void* arg) {
  (void)arg;
  return ginitDict[0].key;
}
const char* compareStateName(void* arg) {
  (void)arg;
  return ginitDict[1].key;
}
const char* compareWinKeyName(void* arg) {
  (void)arg;
  return ginitDict[2].key;
}
const char* compareSessionKeyName(void* arg) {
  (void)arg;
  return ginitDict[3].key;
}
const char* compareFuncKeyName(void* arg) {
  (void)arg;
  return ginitDict[4].key;
}
const char* compareParKeyName(void* arg) {
  (void)arg;
  return ginitDict[5].key;
}
const char* comparePartagKeyName(void* arg) {
  (void)arg;
  return ginitDict[6].key;
}

void destroyCompactFilteFactory(void* arg) {
  if (arg == NULL) return;
}
const char* compactFilteFactoryName(void* arg) {
  SCompactFilteFactory* state = arg;
  return "stream_compact_factory_filter_default";
}
const char* compactFilteFactoryNameSess(void* arg) {
  SCompactFilteFactory* state = arg;
  return "stream_compact_factory_filter_sess";
}
const char* compactFilteFactoryNameState(void* arg) {
  SCompactFilteFactory* state = arg;
  return "stream_compact_factory_filter_state";
}
const char* compactFilteFactoryNameFill(void* arg) {
  SCompactFilteFactory* state = arg;
  return "stream_compact_factory_filter_fill";
}
const char* compactFilteFactoryNameFunc(void* arg) {
  SCompactFilteFactory* state = arg;
  return "stream_compact_factory_filter_func";
}

void          destroyCompactFilte(void* arg) { (void)arg; }
unsigned char compactFilte(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                           char** newval, size_t* newvlen, unsigned char* value_changed) {
  return streamStateValueIsStale((char*)val) ? 1 : 0;
}
const char* compactFilteName(void* arg) { return "stream_filte_default"; }
const char* compactFilteNameSess(void* arg) { return "stream_filte_sess"; }
const char* compactFilteNameState(void* arg) { return "stream_filte_state"; }
const char* compactFilteNameFill(void* arg) { return "stream_filte_fill"; }
const char* compactFilteNameFunc(void* arg) { return "stream_filte_func"; }

unsigned char compactFilteSess(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                               char** newval, size_t* newvlen, unsigned char* value_changed) {
  // not impl yet
  return 0;
}

unsigned char compactFilteState(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                                char** newval, size_t* newvlen, unsigned char* value_changed) {
  // not impl yet
  return 0;
}

unsigned char compactFilteFill(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                               char** newval, size_t* newvlen, unsigned char* value_changed) {
  // not impl yet
  return 0;
}

unsigned char compactFilteFunc(void* arg, int level, const char* key, size_t klen, const char* val, size_t vlen,
                               char** newval, size_t* newvlen, unsigned char* value_changed) {
  // not impl yet
  return 0;
  // return streamStateValueIsStale((char*)val) ? 1 : 0;
}

rocksdb_compactionfilter_t* compactFilteFactoryCreateFilter(void* arg, rocksdb_compactionfiltercontext_t* ctx) {
  SCompactFilteFactory*       state = arg;
  rocksdb_compactionfilter_t* filter =
      rocksdb_compactionfilter_create(state, destroyCompactFilte, compactFilte, compactFilteName);
  return filter;
}
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterSess(void* arg, rocksdb_compactionfiltercontext_t* ctx) {
  SCompactFilteFactory*       state = arg;
  rocksdb_compactionfilter_t* filter =
      rocksdb_compactionfilter_create(state, destroyCompactFilte, compactFilteSess, compactFilteNameSess);
  return filter;
}
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterState(void* arg, rocksdb_compactionfiltercontext_t* ctx) {
  SCompactFilteFactory*       state = arg;
  rocksdb_compactionfilter_t* filter =
      rocksdb_compactionfilter_create(state, destroyCompactFilte, compactFilteState, compactFilteNameState);
  return filter;
}
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterFill(void* arg, rocksdb_compactionfiltercontext_t* ctx) {
  SCompactFilteFactory*       state = arg;
  rocksdb_compactionfilter_t* filter =
      rocksdb_compactionfilter_create(state, destroyCompactFilte, compactFilteFill, compactFilteNameFill);
  return filter;
}
rocksdb_compactionfilter_t* compactFilteFactoryCreateFilterFunc(void* arg, rocksdb_compactionfiltercontext_t* ctx) {
  SCompactFilteFactory*       state = arg;
  rocksdb_compactionfilter_t* filter =
      rocksdb_compactionfilter_create(state, destroyCompactFilte, compactFilteFunc, compactFilteNameFunc);
  return filter;
}

int32_t taskDbOpenCfs(STaskDbWrapper* pTask, char* path, char** pCfNames, int32_t nCf) {
  int32_t code = -1;
  char*   err = NULL;

  rocksdb_options_t**              cfOpts = taosMemoryCalloc(nCf, sizeof(rocksdb_options_t*));
  rocksdb_column_family_handle_t** cfHandle = taosMemoryCalloc(nCf, sizeof(rocksdb_column_family_handle_t*));

  for (int i = 0; i < nCf; i++) {
    int32_t idx = getCfIdx(pCfNames[i]);
    cfOpts[i] = pTask->pCfOpts[idx];
  }

  rocksdb_t* db = rocksdb_open_column_families(pTask->dbOpt, path, nCf, (const char* const*)pCfNames,
                                               (const rocksdb_options_t* const*)cfOpts, cfHandle, &err);

  if (err != NULL) {
    stError("failed to open cf path: %s", err);
    taosMemoryFree(err);
    goto _EXIT;
  }

  for (int i = 0; i < nCf; i++) {
    int32_t idx = getCfIdx(pCfNames[i]);
    pTask->pCf[idx] = cfHandle[i];
  }

  pTask->db = db;
  code = 0;

_EXIT:
  taosMemoryFree(cfOpts);
  taosMemoryFree(cfHandle);
  return code;
}
void* taskDbAddRef(void* pTaskDb) {
  STaskDbWrapper* pBackend = pTaskDb;
  return taosAcquireRef(taskDbWrapperId, pBackend->refId);
}
void taskDbRemoveRef(void* pTaskDb) {
  if (pTaskDb == NULL) return;
  STaskDbWrapper* pBackend = pTaskDb;
  taosReleaseRef(taskDbWrapperId, pBackend->refId);
}

void taskDbInitOpt(STaskDbWrapper* pTaskDb) {
  rocksdb_env_t* env = rocksdb_create_default_env();

  rocksdb_cache_t*   cache = rocksdb_cache_create_lru(256);
  rocksdb_options_t* opts = rocksdb_options_create();
  rocksdb_options_set_env(opts, env);
  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_create_missing_column_families(opts, 1);
  // rocksdb_options_set_max_total_wal_size(opts, dbMemLimit);
  rocksdb_options_set_recycle_log_file_num(opts, 6);
  rocksdb_options_set_max_write_buffer_number(opts, 3);
  rocksdb_options_set_info_log_level(opts, 1);
  rocksdb_options_set_db_write_buffer_size(opts, 256 << 20);
  rocksdb_options_set_write_buffer_size(opts, 128 << 20);
  rocksdb_options_set_atomic_flush(opts, 1);

  pTaskDb->dbOpt = opts;
  pTaskDb->env = env;
  pTaskDb->cache = cache;
  pTaskDb->filterFactory = rocksdb_compactionfilterfactory_create(
      NULL, destroyCompactFilteFactory, compactFilteFactoryCreateFilter, compactFilteFactoryName);
  rocksdb_options_set_compaction_filter_factory(pTaskDb->dbOpt, pTaskDb->filterFactory);
  pTaskDb->readOpt = rocksdb_readoptions_create();
  pTaskDb->writeOpt = rocksdb_writeoptions_create();
  rocksdb_writeoptions_disable_WAL(pTaskDb->writeOpt, 1);

  size_t nCf = sizeof(ginitDict) / sizeof(ginitDict[0]);
  pTaskDb->pCf = taosMemoryCalloc(nCf, sizeof(rocksdb_column_family_handle_t*));
  pTaskDb->pCfParams = taosMemoryCalloc(nCf, sizeof(RocksdbCfParam));
  pTaskDb->pCfOpts = taosMemoryCalloc(nCf, sizeof(rocksdb_options_t*));
  pTaskDb->pCompares = taosMemoryCalloc(nCf, sizeof(rocksdb_comparator_t*));

  for (int i = 0; i < nCf; i++) {
    rocksdb_options_t*                   opt = rocksdb_options_create_copy(pTaskDb->dbOpt);
    rocksdb_block_based_table_options_t* tableOpt = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(tableOpt, pTaskDb->cache);
    rocksdb_block_based_options_set_partition_filters(tableOpt, 1);

    rocksdb_filterpolicy_t* filter = rocksdb_filterpolicy_create_bloom(15);
    rocksdb_block_based_options_set_filter_policy(tableOpt, filter);

    rocksdb_options_set_block_based_table_factory((rocksdb_options_t*)opt, tableOpt);

    SCfInit* cfPara = &ginitDict[i];

    rocksdb_comparator_t* compare =
        rocksdb_comparator_create(NULL, cfPara->destroyCmp, cfPara->cmpKey, cfPara->cmpName);
    rocksdb_options_set_comparator((rocksdb_options_t*)opt, compare);

    rocksdb_compactionfilterfactory_t* filterFactory =
        rocksdb_compactionfilterfactory_create(NULL, cfPara->destroyFilter, cfPara->createFilter, cfPara->funcName);
    rocksdb_options_set_compaction_filter_factory(opt, filterFactory);

    pTaskDb->pCompares[i] = compare;
    pTaskDb->pCfOpts[i] = opt;
    pTaskDb->pCfParams[i].tableOpt = tableOpt;
  }
  return;
}
void taskDbInitChkpOpt(STaskDbWrapper* pTaskDb) {
  pTaskDb->chkpId = -1;
  pTaskDb->chkpCap = 4;
  pTaskDb->chkpSaved = taosArrayInit(4, sizeof(int64_t));
  pTaskDb->chkpInUse = taosArrayInit(4, sizeof(int64_t));

  taosThreadRwlockInit(&pTaskDb->chkpDirLock, NULL);
}

void taskDbDestroyChkpOpt(STaskDbWrapper* pTaskDb) {
  taosArrayDestroy(pTaskDb->chkpSaved);
  taosArrayDestroy(pTaskDb->chkpInUse);
  taosThreadRwlockDestroy(&pTaskDb->chkpDirLock);
}

int32_t taskDbBuildFullPath(char* path, char* key, char** dbFullPath, char** stateFullPath) {
  int32_t code = 0;

  char* statePath = taosMemoryCalloc(1, strlen(path) + 128);
  sprintf(statePath, "%s%s%s", path, TD_DIRSEP, key);
  if (!taosDirExist(statePath)) {
    code = taosMulMkDir(statePath);
    if (code != 0) {
      stError("failed to create dir: %s, reason:%s", statePath, tstrerror(code));
      taosMemoryFree(statePath);
      return code;
    }
  }

  char* dbPath = taosMemoryCalloc(1, strlen(statePath) + 128);
  sprintf(dbPath, "%s%s%s", statePath, TD_DIRSEP, "state");
  if (!taosDirExist(dbPath)) {
    code = taosMulMkDir(dbPath);
    if (code != 0) {
      stError("failed to create dir: %s, reason:%s", dbPath, tstrerror(code));
      taosMemoryFree(statePath);
      taosMemoryFree(dbPath);
      return code;
    }
  }

  *dbFullPath = dbPath;
  *stateFullPath = statePath;
  return 0;
}
void taskDbUpdateChkpId(void* pTaskDb, int64_t chkpId) {
  STaskDbWrapper* p = pTaskDb;
  taosThreadMutexLock(&p->mutex);
  p->chkpId = chkpId;
  taosThreadMutexUnlock(&p->mutex);
}

STaskDbWrapper* taskDbOpenImpl(char* key, char* statePath, char* dbPath) {
  char*  err = NULL;
  char** cfNames = NULL;
  size_t nCf = 0;

  STaskDbWrapper* pTaskDb = taosMemoryCalloc(1, sizeof(STaskDbWrapper));
  pTaskDb->idstr = key ? taosStrdup(key) : NULL;
  pTaskDb->path = statePath ? taosStrdup(statePath) : NULL;

  taosThreadMutexInit(&pTaskDb->mutex, NULL);
  taskDbInitChkpOpt(pTaskDb);
  taskDbInitOpt(pTaskDb);

  cfNames = rocksdb_list_column_families(pTaskDb->dbOpt, dbPath, &nCf, &err);
  if (nCf == 0) {
    stInfo("newly create db, need to restart");
    // pre create db
    pTaskDb->db = rocksdb_open(pTaskDb->pCfOpts[0], dbPath, &err);
    rocksdb_close(pTaskDb->db);

    if (cfNames != NULL) {
      rocksdb_list_column_families_destroy(cfNames, nCf);
    }
    taosMemoryFree(err);
    err = NULL;

    cfNames = rocksdb_list_column_families(pTaskDb->dbOpt, dbPath, &nCf, &err);
    ASSERT(err == NULL);
  }

  if (taskDbOpenCfs(pTaskDb, dbPath, cfNames, nCf) != 0) {
    goto _EXIT;
  }

  if (cfNames != NULL) {
    rocksdb_list_column_families_destroy(cfNames, nCf);
    cfNames = NULL;
  }

  stDebug("succ to init stream backend at %s, backend:%p", dbPath, pTaskDb);
  return pTaskDb;
_EXIT:

  taskDbDestroy(pTaskDb, false);
  if (err) taosMemoryFree(err);
  if (cfNames) rocksdb_list_column_families_destroy(cfNames, nCf);
  return NULL;
}
STaskDbWrapper* taskDbOpen(char* path, char* key, int64_t chkpId) {
  char* statePath = NULL;
  char* dbPath = NULL;

  if (rebuildDirFromChkp2(path, key, chkpId, &statePath, &dbPath) != 0) {
    return NULL;
  }

  STaskDbWrapper* pTaskDb = taskDbOpenImpl(key, statePath, dbPath);
  taosMemoryFree(dbPath);
  taosMemoryFree(statePath);
  return pTaskDb;
}

void taskDbDestroy(void* pDb, bool flush) {
  STaskDbWrapper* wrapper = pDb;
  if (wrapper == NULL) return;

  streamMetaRemoveDB(wrapper->pMeta, wrapper->idstr);

  stDebug("succ to destroy stream backend:%p", wrapper);

  int8_t nCf = sizeof(ginitDict) / sizeof(ginitDict[0]);

  if (wrapper == NULL) return;

  if (flush) {
    if (wrapper->db && wrapper->pCf) {
      rocksdb_flushoptions_t* flushOpt = rocksdb_flushoptions_create();
      rocksdb_flushoptions_set_wait(flushOpt, 1);

      char*                            err = NULL;
      rocksdb_column_family_handle_t** cfs = taosMemoryCalloc(1, sizeof(rocksdb_column_family_handle_t*) * nCf);
      int                              numOfFlushCf = 0;
      for (int i = 0; i < nCf; i++) {
        if (wrapper->pCf[i] != NULL) {
          cfs[numOfFlushCf++] = wrapper->pCf[i];
        }
      }
      if (numOfFlushCf != 0) {
        rocksdb_flush_cfs(wrapper->db, flushOpt, cfs, numOfFlushCf, &err);
        if (err != NULL) {
          stError("failed to flush all cfs, reason:%s", err);
          taosMemoryFreeClear(err);
        }
      }
      taosMemoryFree(cfs);
      rocksdb_flushoptions_destroy(flushOpt);
    }
  }
  for (int i = 0; i < nCf; i++) {
    if (wrapper->pCf[i] != NULL) {
      rocksdb_column_family_handle_destroy(wrapper->pCf[i]);
    }
  }

  if (wrapper->db) rocksdb_close(wrapper->db);

  rocksdb_options_destroy(wrapper->dbOpt);
  rocksdb_readoptions_destroy(wrapper->readOpt);
  rocksdb_writeoptions_destroy(wrapper->writeOpt);
  rocksdb_env_destroy(wrapper->env);
  rocksdb_cache_destroy(wrapper->cache);

  taosMemoryFree(wrapper->pCf);
  for (int i = 0; i < nCf; i++) {
    rocksdb_options_t*                   opt = wrapper->pCfOpts[i];
    rocksdb_comparator_t*                compare = wrapper->pCompares[i];
    rocksdb_block_based_table_options_t* tblOpt = wrapper->pCfParams[i].tableOpt;

    rocksdb_options_destroy(opt);
    rocksdb_comparator_destroy(compare);
    rocksdb_block_based_options_destroy(tblOpt);
  }
  taosMemoryFree(wrapper->pCompares);
  taosMemoryFree(wrapper->pCfOpts);
  taosMemoryFree(wrapper->pCfParams);

  taosThreadMutexDestroy(&wrapper->mutex);

  taskDbDestroyChkpOpt(wrapper);

  taosMemoryFree(wrapper->idstr);
  taosMemoryFree(wrapper->path);
  taosMemoryFree(wrapper);

  return;
}

void taskDbDestroy2(void* pDb) { taskDbDestroy(pDb, true); }

int32_t taskDbGenChkpUploadData__rsync(STaskDbWrapper* pDb, int64_t chkpId, char** path) {
  int64_t st = taosGetTimestampMs();
  int32_t code = -1;
  int64_t refId = pDb->refId;

  if (taosAcquireRef(taskDbWrapperId, refId) == NULL) {
    return -1;
  }

  char* buf = taosMemoryCalloc(1, strlen(pDb->path) + 128);
  sprintf(buf, "%s%s%s%s%s%" PRId64 "", pDb->path, TD_DIRSEP, "checkpoints", TD_DIRSEP, "checkpoint", chkpId);
  if (taosIsDir(buf)) {
    code = 0;
    *path = buf;
  } else {
    taosMemoryFree(buf);
  }

  taosReleaseRef(taskDbWrapperId, refId);
  return code;
}

int32_t taskDbGenChkpUploadData__s3(STaskDbWrapper* pDb, void* bkdChkpMgt, int64_t chkpId, char** path, SArray* list) {
  int32_t  code = 0;
  SBkdMgt* p = (SBkdMgt*)bkdChkpMgt;

  char* temp = taosMemoryCalloc(1, strlen(pDb->path) + 32);
  sprintf(temp, "%s%s%s%" PRId64 "", pDb->path, TD_DIRSEP, "tmp", chkpId);

  if (taosDirExist(temp)) {
    taosRemoveDir(temp);
    taosMkDir(temp);
  } else {
    taosMkDir(temp);
  }
  code = bkdMgtGetDelta(p, pDb->idstr, chkpId, list, temp);

  *path = temp;

  return code;
}
int32_t taskDbGenChkpUploadData(void* arg, void* mgt, int64_t chkpId, int8_t type, char** path, SArray* list) {
  STaskDbWrapper* pDb = arg;
  UPLOAD_TYPE     utype = type;

  if (utype == UPLOAD_RSYNC) {
    return taskDbGenChkpUploadData__rsync(pDb, chkpId, path);
  } else if (utype == UPLOAD_S3) {
    return taskDbGenChkpUploadData__s3(pDb, mgt, chkpId, path, list);
  }
  return -1;
}

int32_t taskDbOpenCfByKey(STaskDbWrapper* pDb, const char* key) {
  int32_t code = 0;
  char*   err = NULL;
  int8_t  idx = getCfIdx(key);

  if (idx == -1) return -1;

  if (pDb->pCf[idx] != NULL) return code;

  rocksdb_column_family_handle_t* cf =
      rocksdb_create_column_family(pDb->db, pDb->pCfOpts[idx], ginitDict[idx].key, &err);
  if (err != NULL) {
    stError("failed to open cf, key:%s, reason: %s", key, err);
    taosMemoryFree(err);
    code = -1;
    return code;
  }

  pDb->pCf[idx] = cf;
  return code;
}
int32_t copyDataAt(RocksdbCfInst* pSrc, STaskDbWrapper* pDst, int8_t i) {
  int32_t WRITE_BATCH = 1024;
  char*   err = NULL;
  int     code = 0;

  rocksdb_readoptions_t* pRdOpt = rocksdb_readoptions_create();

  rocksdb_writebatch_t* wb = rocksdb_writebatch_create();
  rocksdb_iterator_t*   pIter = rocksdb_create_iterator_cf(pSrc->db, pRdOpt, pSrc->pHandle[i]);
  rocksdb_iter_seek_to_first(pIter);
  while (rocksdb_iter_valid(pIter)) {
    if (rocksdb_writebatch_count(wb) >= WRITE_BATCH) {
      rocksdb_write(pDst->db, pDst->writeOpt, wb, &err);
      if (err != NULL) {
        code = -1;
        goto _EXIT;
      }
      rocksdb_writebatch_clear(wb);
    }

    size_t klen = 0, vlen = 0;
    char*  key = (char*)rocksdb_iter_key(pIter, &klen);
    char*  val = (char*)rocksdb_iter_value(pIter, &vlen);

    rocksdb_writebatch_put_cf(wb, pDst->pCf[i], key, klen, val, vlen);
    rocksdb_iter_next(pIter);
  }

  if (rocksdb_writebatch_count(wb) > 0) {
    rocksdb_write(pDst->db, pDst->writeOpt, wb, &err);
    if (err != NULL) {
      code = -1;
      goto _EXIT;
    }
  }

_EXIT:
  rocksdb_iter_destroy(pIter);
  rocksdb_readoptions_destroy(pRdOpt);
  taosMemoryFree(err);

  return code;
}

int32_t streamStateCvtDataFormat(char* path, char* key, void* pCfInst) {
  int nCf = sizeof(ginitDict) / sizeof(ginitDict[0]);

  int32_t code = 0;

  STaskDbWrapper* pTaskDb = taskDbOpen(path, key, 0);
  RocksdbCfInst*  pSrcBackend = pCfInst;

  for (int i = 0; i < nCf; i++) {
    rocksdb_column_family_handle_t* pSrcCf = pSrcBackend->pHandle[i];
    if (pSrcCf == NULL) continue;

    code = taskDbOpenCfByKey(pTaskDb, ginitDict[i].key);
    if (code != 0) goto _EXIT;

    code = copyDataAt(pSrcBackend, pTaskDb, i);
    if (code != 0) goto _EXIT;
  }

_EXIT:
  taskDbDestroy(pTaskDb, true);

  return code;
}
int32_t streamStateOpenBackendCf(void* backend, char* name, char** cfs, int32_t nCf) {
  SBackendWrapper* handle = backend;
  char*            err = NULL;
  int64_t          streamId;
  int32_t          taskId, dummy = 0;
  char             suffix[64] = {0};

  rocksdb_options_t**              cfOpts = taosMemoryCalloc(nCf, sizeof(rocksdb_options_t*));
  RocksdbCfParam*                  params = taosMemoryCalloc(nCf, sizeof(RocksdbCfParam));
  rocksdb_comparator_t**           pCompare = taosMemoryCalloc(nCf, sizeof(rocksdb_comparator_t*));
  rocksdb_column_family_handle_t** cfHandle = taosMemoryCalloc(nCf, sizeof(rocksdb_column_family_handle_t*));

  for (int i = 0; i < nCf; i++) {
    char* cf = cfs[i];
    char  funcname[64] = {0};
    cfOpts[i] = rocksdb_options_create_copy(handle->dbOpt);
    if (i == 0) continue;
    if (3 == sscanf(cf, "0x%" PRIx64 "-%d_%s", &streamId, &taskId, funcname)) {
      rocksdb_block_based_table_options_t* tableOpt = rocksdb_block_based_options_create();
      rocksdb_block_based_options_set_block_cache(tableOpt, handle->cache);
      rocksdb_block_based_options_set_partition_filters(tableOpt, 1);

      rocksdb_filterpolicy_t* filter = rocksdb_filterpolicy_create_bloom(15);
      rocksdb_block_based_options_set_filter_policy(tableOpt, filter);

      rocksdb_options_set_block_based_table_factory((rocksdb_options_t*)cfOpts[i], tableOpt);
      params[i].tableOpt = tableOpt;

      int idx = streamStateGetCfIdx(NULL, funcname);
      if (idx < 0 || idx >= sizeof(ginitDict) / sizeof(ginitDict[0])) {
        stError("failed to open cf");
        return -1;
      }
      SCfInit* cfPara = &ginitDict[idx];

      rocksdb_comparator_t* compare =
          rocksdb_comparator_create(NULL, cfPara->destroyCmp, cfPara->cmpKey, cfPara->cmpName);
      rocksdb_options_set_comparator((rocksdb_options_t*)cfOpts[i], compare);
      pCompare[i] = compare;
    }
  }
  rocksdb_t* db = rocksdb_open_column_families(handle->dbOpt, name, nCf, (const char* const*)cfs,
                                               (const rocksdb_options_t* const*)cfOpts, cfHandle, &err);
  if (err != NULL) {
    stError("failed to open rocksdb cf, reason:%s", err);
    taosMemoryFree(err);
    taosMemoryFree(cfHandle);
    taosMemoryFree(pCompare);
    taosMemoryFree(params);
    taosMemoryFree(cfOpts);
    // fix other leak
    return -1;
  } else {
    stDebug("succ to open rocksdb cf");
  }
  // close default cf
  if (((rocksdb_column_family_handle_t**)cfHandle)[0] != 0) {
    rocksdb_column_family_handle_destroy(cfHandle[0]);
    cfHandle[0] = NULL;
  }
  rocksdb_options_destroy(cfOpts[0]);

  handle->db = db;

  static int32_t cfLen = sizeof(ginitDict) / sizeof(ginitDict[0]);
  for (int i = 0; i < nCf; i++) {
    char* cf = cfs[i];
    if (i == 0) continue;  // skip default column family, not set opt

    char funcname[64] = {0};
    if (3 == sscanf(cf, "0x%" PRIx64 "-%d_%s", &streamId, &taskId, funcname)) {
      char idstr[128] = {0};
      sprintf(idstr, "0x%" PRIx64 "-%d", streamId, taskId);

      int idx = streamStateGetCfIdx(NULL, funcname);

      RocksdbCfInst*  inst = NULL;
      RocksdbCfInst** pInst = taosHashGet(handle->cfInst, idstr, strlen(idstr) + 1);
      if (pInst == NULL || *pInst == NULL) {
        inst = taosMemoryCalloc(1, sizeof(RocksdbCfInst));
        inst->pHandle = taosMemoryCalloc(cfLen, sizeof(rocksdb_column_family_handle_t*));
        inst->cfOpt = taosMemoryCalloc(cfLen, sizeof(rocksdb_options_t*));
        inst->wOpt = rocksdb_writeoptions_create();
        inst->rOpt = rocksdb_readoptions_create();
        inst->param = taosMemoryCalloc(cfLen, sizeof(RocksdbCfParam));
        inst->pBackend = handle;
        inst->db = db;
        inst->pCompares = taosMemoryCalloc(cfLen, sizeof(rocksdb_comparator_t*));

        inst->dbOpt = handle->dbOpt;
        rocksdb_writeoptions_disable_WAL(inst->wOpt, 1);
        taosHashPut(handle->cfInst, idstr, strlen(idstr) + 1, &inst, sizeof(void*));
      } else {
        inst = *pInst;
      }
      inst->cfOpt[idx] = cfOpts[i];
      inst->pCompares[idx] = pCompare[i];
      memcpy(&(inst->param[idx]), &(params[i]), sizeof(RocksdbCfParam));
      inst->pHandle[idx] = cfHandle[i];
    }
  }
  void* pIter = taosHashIterate(handle->cfInst, NULL);
  while (pIter) {
    RocksdbCfInst* inst = *(RocksdbCfInst**)pIter;

    for (int i = 0; i < cfLen; i++) {
      if (inst->cfOpt[i] == NULL) {
        rocksdb_options_t*                   opt = rocksdb_options_create_copy(handle->dbOpt);
        rocksdb_block_based_table_options_t* tableOpt = rocksdb_block_based_options_create();
        rocksdb_block_based_options_set_block_cache(tableOpt, handle->cache);
        rocksdb_block_based_options_set_partition_filters(tableOpt, 1);

        rocksdb_filterpolicy_t* filter = rocksdb_filterpolicy_create_bloom(15);
        rocksdb_block_based_options_set_filter_policy(tableOpt, filter);

        rocksdb_options_set_block_based_table_factory((rocksdb_options_t*)opt, tableOpt);

        SCfInit* cfPara = &ginitDict[i];

        rocksdb_comparator_t* compare =
            rocksdb_comparator_create(NULL, cfPara->destroyCmp, cfPara->cmpKey, cfPara->cmpName);
        rocksdb_options_set_comparator((rocksdb_options_t*)opt, compare);

        inst->pCompares[i] = compare;
        inst->cfOpt[i] = opt;
        inst->param[i].tableOpt = tableOpt;
      }
    }
    SCfComparator compare = {.comp = inst->pCompares, .numOfComp = cfLen};
    inst->pCompareNode = streamBackendAddCompare(handle, &compare);
    pIter = taosHashIterate(handle->cfInst, pIter);
  }

  taosMemoryFree(cfHandle);
  taosMemoryFree(pCompare);
  taosMemoryFree(params);
  taosMemoryFree(cfOpts);
  return 0;
}
#ifdef BUILD_NO_CALL
int streamStateOpenBackend(void* backend, SStreamState* pState) {
  taosAcquireRef(streamBackendId, pState->streamBackendRid);
  SBackendWrapper*   handle = backend;
  SBackendCfWrapper* pBackendCfWrapper = taosMemoryCalloc(1, sizeof(SBackendCfWrapper));

  taosThreadMutexLock(&handle->cfMutex);
  RocksdbCfInst** ppInst = taosHashGet(handle->cfInst, pState->pTdbState->idstr, strlen(pState->pTdbState->idstr) + 1);
  if (ppInst != NULL && *ppInst != NULL) {
    RocksdbCfInst* inst = *ppInst;
    pBackendCfWrapper->rocksdb = inst->db;
    pBackendCfWrapper->pHandle = (void**)inst->pHandle;
    pBackendCfWrapper->writeOpts = inst->wOpt;
    pBackendCfWrapper->readOpts = inst->rOpt;
    pBackendCfWrapper->cfOpts = (void**)(inst->cfOpt);
    pBackendCfWrapper->dbOpt = handle->dbOpt;
    pBackendCfWrapper->param = inst->param;
    pBackendCfWrapper->pBackend = handle;
    pBackendCfWrapper->pComparNode = inst->pCompareNode;
    taosThreadMutexUnlock(&handle->cfMutex);
    pBackendCfWrapper->backendId = pState->streamBackendRid;
    memcpy(pBackendCfWrapper->idstr, pState->pTdbState->idstr, sizeof(pState->pTdbState->idstr));

    int64_t id = taosAddRef(streamBackendCfWrapperId, pBackendCfWrapper);
    pState->pTdbState->backendCfWrapperId = id;
    pState->pTdbState->pBackendCfWrapper = pBackendCfWrapper;
    stInfo("succ to open state %p on backendWrapper, %p, %s", pState, pBackendCfWrapper, pBackendCfWrapper->idstr);

    inst->pHandle = NULL;
    inst->cfOpt = NULL;
    inst->param = NULL;

    inst->wOpt = NULL;
    inst->rOpt = NULL;
    return 0;
  }
  taosThreadMutexUnlock(&handle->cfMutex);

  char* err = NULL;
  int   cfLen = sizeof(ginitDict) / sizeof(ginitDict[0]);

  RocksdbCfParam*           param = taosMemoryCalloc(cfLen, sizeof(RocksdbCfParam));
  const rocksdb_options_t** cfOpt = taosMemoryCalloc(cfLen, sizeof(rocksdb_options_t*));
  for (int i = 0; i < cfLen; i++) {
    cfOpt[i] = rocksdb_options_create_copy(handle->dbOpt);
    // refactor later
    rocksdb_block_based_table_options_t* tableOpt = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(tableOpt, handle->cache);
    rocksdb_block_based_options_set_partition_filters(tableOpt, 1);

    rocksdb_filterpolicy_t* filter = rocksdb_filterpolicy_create_bloom(15);
    rocksdb_block_based_options_set_filter_policy(tableOpt, filter);

    rocksdb_options_set_block_based_table_factory((rocksdb_options_t*)cfOpt[i], tableOpt);

    param[i].tableOpt = tableOpt;
  };

  rocksdb_comparator_t** pCompare = taosMemoryCalloc(cfLen, sizeof(rocksdb_comparator_t*));
  for (int i = 0; i < cfLen; i++) {
    SCfInit* cf = &ginitDict[i];

    rocksdb_comparator_t* compare = rocksdb_comparator_create(NULL, cf->destroyCmp, cf->cmpKey, cf->cmpName);
    rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[i], compare);
    pCompare[i] = compare;
  }
  rocksdb_column_family_handle_t** cfHandle = taosMemoryCalloc(cfLen, sizeof(rocksdb_column_family_handle_t*));
  pBackendCfWrapper->rocksdb = handle->db;
  pBackendCfWrapper->pHandle = (void**)cfHandle;
  pBackendCfWrapper->writeOpts = rocksdb_writeoptions_create();
  pBackendCfWrapper->readOpts = rocksdb_readoptions_create();
  pBackendCfWrapper->cfOpts = (void**)cfOpt;
  pBackendCfWrapper->dbOpt = handle->dbOpt;
  pBackendCfWrapper->param = param;
  pBackendCfWrapper->pBackend = handle;
  pBackendCfWrapper->backendId = pState->streamBackendRid;
  taosThreadRwlockInit(&pBackendCfWrapper->rwLock, NULL);
  SCfComparator compare = {.comp = pCompare, .numOfComp = cfLen};
  pBackendCfWrapper->pComparNode = streamBackendAddCompare(handle, &compare);
  rocksdb_writeoptions_disable_WAL(pBackendCfWrapper->writeOpts, 1);
  memcpy(pBackendCfWrapper->idstr, pState->pTdbState->idstr, sizeof(pState->pTdbState->idstr));

  int64_t id = taosAddRef(streamBackendCfWrapperId, pBackendCfWrapper);
  pState->pTdbState->backendCfWrapperId = id;
  pState->pTdbState->pBackendCfWrapper = pBackendCfWrapper;
  stInfo("succ to open state %p on backendWrapper %p %s", pState, pBackendCfWrapper, pBackendCfWrapper->idstr);
  return 0;
}

void streamStateCloseBackend(SStreamState* pState, bool remove) {
  SBackendCfWrapper* wrapper = pState->pTdbState->pBackendCfWrapper;
  SBackendWrapper*   pHandle = wrapper->pBackend;

  stInfo("start to close state on backend: %p", pHandle);

  taosThreadMutexLock(&pHandle->cfMutex);
  RocksdbCfInst** ppInst = taosHashGet(pHandle->cfInst, wrapper->idstr, strlen(pState->pTdbState->idstr) + 1);
  if (ppInst != NULL && *ppInst != NULL) {
    RocksdbCfInst* inst = *ppInst;
    taosMemoryFree(inst);
    taosHashRemove(pHandle->cfInst, pState->pTdbState->idstr, strlen(pState->pTdbState->idstr) + 1);
  }
  taosThreadMutexUnlock(&pHandle->cfMutex);

  char* status[] = {"close", "drop"};
  stInfo("start to %s state %p on backendWrapper %p %s", status[remove == false ? 0 : 1], pState, wrapper,
         wrapper->idstr);
  wrapper->remove |= remove;  // update by other pState
  taosReleaseRef(streamBackendCfWrapperId, pState->pTdbState->backendCfWrapperId);
}
#endif
void streamStateDestroyCompar(void* arg) {
  SCfComparator* comp = (SCfComparator*)arg;
  for (int i = 0; i < comp->numOfComp; i++) {
    if (comp->comp[i]) rocksdb_comparator_destroy(comp->comp[i]);
  }
  taosMemoryFree(comp->comp);
}

int streamStateGetCfIdx(SStreamState* pState, const char* funcName) {
  int    idx = -1;
  size_t len = strlen(funcName);
  for (int i = 0; i < sizeof(ginitDict) / sizeof(ginitDict[0]); i++) {
    if (len == ginitDict[i].len && strncmp(funcName, ginitDict[i].key, strlen(funcName)) == 0) {
      idx = i;
      break;
    }
  }
  if (pState != NULL && idx != -1) {
    STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
    if (wrapper == NULL) {
      return -1;
    }

    taosThreadMutexLock(&wrapper->mutex);

    rocksdb_column_family_handle_t* cf = wrapper->pCf[idx];
    if (cf == NULL) {
      char* err = NULL;
      cf = rocksdb_create_column_family(wrapper->db, wrapper->pCfOpts[idx], ginitDict[idx].key, &err);
      if (err != NULL) {
        idx = -1;
        stError("failed to open cf, %p %s_%s, reason:%s", pState, wrapper->idstr, funcName, err);
        taosMemoryFree(err);
      } else {
        stDebug("succ to open cf, %p %s_%s", pState, wrapper->idstr, funcName);
        wrapper->pCf[idx] = cf;
      }
    }
    taosThreadMutexUnlock(&wrapper->mutex);
  }

  return idx;
}
bool streamStateIterSeekAndValid(rocksdb_iterator_t* iter, char* buf, size_t len) {
  rocksdb_iter_seek(iter, buf, len);
  if (!rocksdb_iter_valid(iter)) {
    rocksdb_iter_seek_for_prev(iter, buf, len);
    if (!rocksdb_iter_valid(iter)) {
      return false;
    }
  }
  return true;
}
rocksdb_iterator_t* streamStateIterCreate(SStreamState* pState, const char* cfKeyName, rocksdb_snapshot_t** snapshot,
                                          rocksdb_readoptions_t** readOpt) {
  int idx = streamStateGetCfIdx(pState, cfKeyName);

  *readOpt = rocksdb_readoptions_create();

  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  if (snapshot != NULL) {
    *snapshot = (rocksdb_snapshot_t*)rocksdb_create_snapshot(wrapper->db);
    rocksdb_readoptions_set_snapshot(*readOpt, *snapshot);
    rocksdb_readoptions_set_fill_cache(*readOpt, 0);
  }

  return rocksdb_create_iterator_cf(wrapper->db, *readOpt, ((rocksdb_column_family_handle_t**)wrapper->pCf)[idx]);
}

#define STREAM_STATE_PUT_ROCKSDB(pState, funcname, key, value, vLen)                                              \
  do {                                                                                                            \
    code = 0;                                                                                                     \
    char  buf[128] = {0};                                                                                         \
    char* err = NULL;                                                                                             \
    int   i = streamStateGetCfIdx(pState, funcname);                                                              \
    if (i < 0) {                                                                                                  \
      stWarn("streamState failed to get cf name: %s", funcname);                                                  \
      code = -1;                                                                                                  \
      break;                                                                                                      \
    }                                                                                                             \
    STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;                                                \
    wrapper->dataWritten += 1;                                                                                    \
    char toString[128] = {0};                                                                                     \
    if (stDebugFlag & DEBUG_TRACE) ginitDict[i].toStrFunc((void*)key, toString);                                  \
    int32_t                         klen = ginitDict[i].enFunc((void*)key, buf);                                  \
    rocksdb_column_family_handle_t* pHandle = ((rocksdb_column_family_handle_t**)wrapper->pCf)[ginitDict[i].idx]; \
    rocksdb_writeoptions_t*         opts = wrapper->writeOpt;                                                     \
    rocksdb_t*                      db = wrapper->db;                                                             \
    char*                           ttlV = NULL;                                                                  \
    int32_t                         ttlVLen = ginitDict[i].enValueFunc((char*)value, vLen, 0, &ttlV);             \
    rocksdb_put_cf(db, opts, pHandle, (const char*)buf, klen, (const char*)ttlV, (size_t)ttlVLen, &err);          \
    if (err != NULL) {                                                                                            \
      stError("streamState str: %s failed to write to %s, err: %s", toString, funcname, err);                     \
      taosMemoryFree(err);                                                                                        \
      code = -1;                                                                                                  \
    } else {                                                                                                      \
      stTrace("streamState str:%s succ to write to %s, rowValLen:%d, ttlValLen:%d, %p", toString, funcname, vLen, \
              ttlVLen, wrapper);                                                                                  \
    }                                                                                                             \
    taosMemoryFree(ttlV);                                                                                         \
  } while (0);

#define STREAM_STATE_GET_ROCKSDB(pState, funcname, key, pVal, vLen)                                                   \
  do {                                                                                                                \
    code = 0;                                                                                                         \
    char  buf[128] = {0};                                                                                             \
    char* err = NULL;                                                                                                 \
    int   i = streamStateGetCfIdx(pState, funcname);                                                                  \
    if (i < 0) {                                                                                                      \
      stWarn("streamState failed to get cf name: %s", funcname);                                                      \
      code = -1;                                                                                                      \
      break;                                                                                                          \
    }                                                                                                                 \
    STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;                                                    \
    char            toString[128] = {0};                                                                              \
    if (stDebugFlag & DEBUG_TRACE) ginitDict[i].toStrFunc((void*)key, toString);                                      \
    int32_t                         klen = ginitDict[i].enFunc((void*)key, buf);                                      \
    rocksdb_column_family_handle_t* pHandle = ((rocksdb_column_family_handle_t**)wrapper->pCf)[ginitDict[i].idx];     \
    rocksdb_t*                      db = wrapper->db;                                                                 \
    rocksdb_readoptions_t*          opts = wrapper->readOpt;                                                          \
    size_t                          len = 0;                                                                          \
    char* val = rocksdb_get_cf(db, opts, pHandle, (const char*)buf, klen, (size_t*)&len, &err);                       \
    if (val == NULL || len == 0) {                                                                                    \
      if (err == NULL) {                                                                                              \
        stTrace("streamState str: %s failed to read from %s_%s, err: not exist", toString, wrapper->idstr, funcname); \
      } else {                                                                                                        \
        stError("streamState str: %s failed to read from %s_%s, err: %s", toString, wrapper->idstr, funcname, err);   \
        taosMemoryFreeClear(err);                                                                                     \
      }                                                                                                               \
      code = -1;                                                                                                      \
    } else {                                                                                                          \
      char*   p = NULL;                                                                                               \
      int32_t tlen = ginitDict[i].deValueFunc(val, len, NULL, (char**)pVal);                                          \
      if (tlen <= 0) {                                                                                                \
        stError("streamState str: %s failed to read from %s_%s, err: already ttl ", toString, wrapper->idstr,         \
                funcname);                                                                                            \
        code = -1;                                                                                                    \
      } else {                                                                                                        \
        stTrace("streamState str: %s succ to read from %s_%s, valLen:%d, %p", toString, wrapper->idstr, funcname,     \
                tlen, wrapper);                                                                                       \
      }                                                                                                               \
      taosMemoryFree(val);                                                                                            \
      if (vLen != NULL) *vLen = tlen;                                                                                 \
    }                                                                                                                 \
  } while (0);

#define STREAM_STATE_DEL_ROCKSDB(pState, funcname, key)                                                           \
  do {                                                                                                            \
    code = 0;                                                                                                     \
    char  buf[128] = {0};                                                                                         \
    char* err = NULL;                                                                                             \
    int   i = streamStateGetCfIdx(pState, funcname);                                                              \
    if (i < 0) {                                                                                                  \
      stWarn("streamState failed to get cf name: %s_%s", pState->pTdbState->idstr, funcname);                     \
      code = -1;                                                                                                  \
      break;                                                                                                      \
    }                                                                                                             \
    STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;                                                \
    wrapper->dataWritten += 1;                                                                                    \
    char toString[128] = {0};                                                                                     \
    if (stDebugFlag & DEBUG_TRACE) ginitDict[i].toStrFunc((void*)key, toString);                                  \
    int32_t                         klen = ginitDict[i].enFunc((void*)key, buf);                                  \
    rocksdb_column_family_handle_t* pHandle = ((rocksdb_column_family_handle_t**)wrapper->pCf)[ginitDict[i].idx]; \
    rocksdb_t*                      db = wrapper->db;                                                             \
    rocksdb_writeoptions_t*         opts = wrapper->writeOpt;                                                     \
    rocksdb_delete_cf(db, opts, pHandle, (const char*)buf, klen, &err);                                           \
    if (err != NULL) {                                                                                            \
      stError("streamState str: %s failed to del from %s_%s, err: %s", toString, wrapper->idstr, funcname, err);  \
      taosMemoryFree(err);                                                                                        \
      code = -1;                                                                                                  \
    } else {                                                                                                      \
      stTrace("streamState str: %s succ to del from %s_%s", toString, wrapper->idstr, funcname);                  \
    }                                                                                                             \
  } while (0);

// state cf
int32_t streamStatePut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  int code = 0;

  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_PUT_ROCKSDB(pState, "state", &sKey, (void*)value, vLen);
  return code;
}
int32_t streamStateGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  int       code = 0;
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_GET_ROCKSDB(pState, "state", &sKey, pVal, pVLen);
  return code;
}
int32_t streamStateDel_rocksdb(SStreamState* pState, const SWinKey* key) {
  int       code = 0;
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_DEL_ROCKSDB(pState, "state", &sKey);
  return code;
}
int32_t streamStateClear_rocksdb(SStreamState* pState) {
  stDebug("streamStateClear_rocksdb");

  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  wrapper->dataWritten += 1;

  char      sKeyStr[128] = {0};
  char      eKeyStr[128] = {0};
  SStateKey sKey = {.key = {.ts = 0, .groupId = 0}, .opNum = pState->number};
  SStateKey eKey = {.key = {.ts = INT64_MAX, .groupId = UINT64_MAX}, .opNum = pState->number};

  int sLen = stateKeyEncode(&sKey, sKeyStr);
  int eLen = stateKeyEncode(&eKey, eKeyStr);

  if (wrapper->pCf[1] != NULL) {
    char* err = NULL;
    rocksdb_delete_range_cf(wrapper->db, wrapper->writeOpt, wrapper->pCf[1], sKeyStr, sLen, eKeyStr, eLen, &err);
    if (err != NULL) {
      char toStringStart[128] = {0};
      char toStringEnd[128] = {0};
      stateKeyToString(&sKey, toStringStart);
      stateKeyToString(&eKey, toStringEnd);

      stWarn("failed to delete range cf(state) start: %s, end:%s, reason:%s", toStringStart, toStringEnd, err);
      taosMemoryFree(err);
    } else {
      rocksdb_compact_range_cf(wrapper->db, wrapper->pCf[1], sKeyStr, sLen, eKeyStr, eLen);
    }
  }

  return 0;
}
int32_t streamStateCurNext_rocksdb(SStreamState* pState, SStreamStateCur* pCur) {
  if (!pCur) {
    return -1;
  }
  rocksdb_iter_next(pCur->iter);
  return 0;
}
int32_t streamStateGetFirst_rocksdb(SStreamState* pState, SWinKey* key) {
  stDebug("streamStateGetFirst_rocksdb");
  SWinKey tmp = {.ts = 0, .groupId = 0};
  streamStatePut_rocksdb(pState, &tmp, NULL, 0);

  SStreamStateCur* pCur = streamStateSeekKeyNext_rocksdb(pState, &tmp);
  int32_t          code = streamStateGetKVByCur_rocksdb(pCur, key, NULL, 0);
  streamStateFreeCur(pCur);
  streamStateDel_rocksdb(pState, &tmp);
  return code;
}

int32_t streamStateGetGroupKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  stDebug("streamStateGetGroupKVByCur_rocksdb");
  if (!pCur) {
    return -1;
  }
  uint64_t groupId = pKey->groupId;

  int32_t code = streamStateFillGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
  if (code == 0) {
    if (pKey->groupId == groupId) {
      return 0;
    }
    taosMemoryFree((void*)*pVal);
    *pVal = NULL;
  }
  return -1;
}
int32_t streamStateAddIfNotExist_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  stDebug("streamStateAddIfNotExist_rocksdb");
  int32_t size = *pVLen;
  if (streamStateGet_rocksdb(pState, key, pVal, pVLen) == 0) {
    return 0;
  }
  *pVal = taosMemoryMalloc(size);
  memset(*pVal, 0, size);
  return 0;
}
int32_t streamStateCurPrev_rocksdb(SStreamStateCur* pCur) {
  if (!pCur) return -1;

  rocksdb_iter_prev(pCur->iter);
  return 0;
}
int32_t streamStateGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  if (!pCur) return -1;
  SStateKey  tkey;
  SStateKey* pKtmp = &tkey;

  if (rocksdb_iter_valid(pCur->iter) && !iterValueIsStale(pCur->iter)) {
    size_t tlen;
    char*  keyStr = (char*)rocksdb_iter_key(pCur->iter, &tlen);
    stateKeyDecode((void*)pKtmp, keyStr);
    if (pKtmp->opNum != pCur->number) {
      return -1;
    }

    if (pVLen != NULL) {
      size_t      vlen = 0;
      const char* valStr = rocksdb_iter_value(pCur->iter, &vlen);
      *pVLen = valueDecode((void*)valStr, vlen, NULL, (char**)pVal);
    }

    *pKey = pKtmp->key;
    return 0;
  }
  return -1;
}
SStreamStateCur* streamStateGetAndCheckCur_rocksdb(SStreamState* pState, SWinKey* key) {
  SStreamStateCur* pCur = streamStateFillGetCur_rocksdb(pState, key);
  if (pCur) {
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pCur, key, NULL, 0);
    if (code == 0) return pCur;
    streamStateFreeCur(pCur);
  }
  return NULL;
}

SStreamStateCur* streamStateSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }
  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  pCur->number = pState->number;
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "state", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);

  SStateKey sKey = {.key = *key, .opNum = pState->number};
  char      buf[128] = {0};
  int       len = stateKeyEncode((void*)&sKey, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  // skip ttl expired data
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) {
    rocksdb_iter_next(pCur->iter);
  }

  if (rocksdb_iter_valid(pCur->iter)) {
    SStateKey curKey;
    size_t    kLen;
    char*     keyStr = (char*)rocksdb_iter_key(pCur->iter, &kLen);
    stateKeyDecode((void*)&curKey, keyStr);
    if (stateKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) > 0) {
      return pCur;
    }
    rocksdb_iter_next(pCur->iter);
    return pCur;
  }
  streamStateFreeCur(pCur);
  return NULL;
}

SStreamStateCur* streamStateSeekToLast_rocksdb(SStreamState* pState) {
  int32_t code = 0;

  const SStateKey maxStateKey = {.key = {.groupId = UINT64_MAX, .ts = INT64_MAX}, .opNum = INT64_MAX};
  STREAM_STATE_PUT_ROCKSDB(pState, "state", &maxStateKey, "", 0);
  if (code != 0) {
    return NULL;
  }

  {
    char tbuf[256] = {0};
    stateKeyToString((void*)&maxStateKey, tbuf);
    stDebug("seek to last:%s", tbuf);
  }

  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) return NULL;

  pCur->number = pState->number;
  pCur->db = ((STaskDbWrapper*)pState->pTdbState->pOwner->pBackend)->db;
  pCur->iter = streamStateIterCreate(pState, "state", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);

  char    buf[128] = {0};
  int32_t klen = stateKeyEncode((void*)&maxStateKey, buf);
  rocksdb_iter_seek(pCur->iter, buf, (size_t)klen);
  rocksdb_iter_prev(pCur->iter);
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) {
    rocksdb_iter_prev(pCur->iter);
  }

  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    pCur = NULL;
  }

  STREAM_STATE_DEL_ROCKSDB(pState, "state", &maxStateKey);
  return pCur;
}
#ifdef BUILD_NO_CALL
SStreamStateCur* streamStateGetCur_rocksdb(SStreamState* pState, const SWinKey* key) {
  stDebug("streamStateGetCur_rocksdb");
  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;

  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) return NULL;

  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "state", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  SStateKey sKey = {.key = *key, .opNum = pState->number};
  char      buf[128] = {0};
  int       len = stateKeyEncode((void*)&sKey, buf);

  rocksdb_iter_seek(pCur->iter, buf, len);

  if (rocksdb_iter_valid(pCur->iter) && !iterValueIsStale(pCur->iter)) {
    SStateKey curKey;
    size_t    kLen = 0;
    char*     keyStr = (char*)rocksdb_iter_key(pCur->iter, &kLen);
    stateKeyDecode((void*)&curKey, keyStr);

    if (stateKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) == 0) {
      pCur->number = pState->number;
      return pCur;
    }
  }
  streamStateFreeCur(pCur);
  return NULL;
}

// func cf
int32_t streamStateFuncPut_rocksdb(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "func", key, (void*)value, vLen);
  return code;
}
int32_t streamStateFuncGet_rocksdb(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "func", key, pVal, pVLen);
  return 0;
}
int32_t streamStateFuncDel_rocksdb(SStreamState* pState, const STupleKey* key) {
  int code = 0;
  STREAM_STATE_DEL_ROCKSDB(pState, "func", key);
  return 0;
}
#endif

// session cf
int32_t streamStateSessionPut_rocksdb(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen) {
  int              code = 0;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  if (value == NULL || vLen == 0) {
    stError("streamStateSessionPut_rocksdb val: %p, len: %d", value, vLen);
  }
  STREAM_STATE_PUT_ROCKSDB(pState, "sess", &sKey, value, vLen);
  return code;
}
int32_t streamStateSessionGet_rocksdb(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen) {
  stDebug("streamStateSessionGet_rocksdb");
  int              code = 0;
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, key);
  SSessionKey      resKey = *key;
  void*            tmp = NULL;
  int32_t          vLen = 0;

  code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, &tmp, &vLen);
  if (code == 0 && key->win.skey == resKey.win.skey) {
    *key = resKey;

    if (pVal) {
      *pVal = tmp;
      tmp = NULL;
    };
    if (pVLen) *pVLen = vLen;
  } else {
    code = -1;
  }

  taosMemoryFree(tmp);
  streamStateFreeCur(pCur);
  return code;
}

int32_t streamStateSessionDel_rocksdb(SStreamState* pState, const SSessionKey* key) {
  int              code = 0;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_DEL_ROCKSDB(pState, "sess", &sKey);
  return code;
}

SStreamStateCur* streamStateSessionSeekToLast_rocksdb(SStreamState* pState, int64_t groupId) {
  stDebug("streamStateSessionSeekToLast_rocksdb");

  int32_t code = 0;

  SSessionKey      maxSessionKey = {.groupId = groupId, .win = {.skey = INT64_MAX, .ekey = INT64_MAX}};
  SStateSessionKey maxKey = {.key = maxSessionKey, .opNum = pState->number};

  STREAM_STATE_PUT_ROCKSDB(pState, "sess", &maxKey, "", 0);
  if (code != 0) {
    return NULL;
  }
  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;

  SStreamStateCur* pCur = createStreamStateCursor();
  pCur->number = pState->number;
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);

  char    buf[128] = {0};
  int32_t klen = stateSessionKeyEncode((void*)&maxKey, buf);
  rocksdb_iter_seek(pCur->iter, buf, (size_t)klen);
  rocksdb_iter_prev(pCur->iter);
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) {
    rocksdb_iter_prev(pCur->iter);
  }

  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    pCur = NULL;
  }

  STREAM_STATE_DEL_ROCKSDB(pState, "sess", &maxKey);
  return pCur;
}

int32_t streamStateSessionCurPrev_rocksdb(SStreamStateCur* pCur) {
  stDebug("streamStateCurPrev_rocksdb");
  if (!pCur) return -1;

  rocksdb_iter_prev(pCur->iter);
  return 0;
}

SStreamStateCur* streamStateSessionSeekKeyCurrentPrev_rocksdb(SStreamState* pState, const SSessionKey* key) {
  stDebug("streamStateSessionSeekKeyCurrentPrev_rocksdb");

  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }

  pCur->number = pState->number;
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);

  char             buf[128] = {0};
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int              len = stateSessionKeyEncode(&sKey, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) rocksdb_iter_prev(pCur->iter);

  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  int32_t          c = 0;
  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) >= 0) return pCur;

  rocksdb_iter_prev(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}
SStreamStateCur* streamStateSessionSeekKeyCurrentNext_rocksdb(SStreamState* pState, SSessionKey* key) {
  stDebug("streamStateSessionSeekKeyCurrentNext_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  char             buf[128] = {0};
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int              len = stateSessionKeyEncode(&sKey, buf);

  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (iterValueIsStale(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) <= 0) return pCur;

  rocksdb_iter_next(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

SStreamStateCur* streamStateSessionSeekKeyNext_rocksdb(SStreamState* pState, const SSessionKey* key) {
  stDebug("streamStateSessionSeekKeyNext_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};

  char buf[128] = {0};
  int  len = stateSessionKeyEncode(&sKey, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) rocksdb_iter_next(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) < 0) return pCur;

  rocksdb_iter_next(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

SStreamStateCur* streamStateSessionSeekKeyPrev_rocksdb(SStreamState* pState, const SSessionKey* key) {
  stDebug("streamStateSessionSeekKeyPrev_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};

  char buf[128] = {0};
  int  len = stateSessionKeyEncode(&sKey, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) rocksdb_iter_prev(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) > 0) return pCur;

  rocksdb_iter_prev(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

int32_t streamStateSessionGetKVByCur_rocksdb(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  stDebug("streamStateSessionGetKVByCur_rocksdb");
  if (!pCur) {
    return -1;
  }
  SStateSessionKey ktmp = {0};
  size_t           kLen = 0, vLen = 0;

  if (!rocksdb_iter_valid(pCur->iter) || iterValueIsStale(pCur->iter)) {
    return -1;
  }
  const char* curKey = rocksdb_iter_key(pCur->iter, (size_t*)&kLen);
  stateSessionKeyDecode((void*)&ktmp, (char*)curKey);

  if (pVal != NULL) *pVal = NULL;
  if (pVLen != NULL) *pVLen = 0;

  SStateSessionKey* pKTmp = &ktmp;
  const char*       vval = rocksdb_iter_value(pCur->iter, (size_t*)&vLen);
  char*             val = NULL;
  int32_t           len = valueDecode((void*)vval, vLen, NULL, &val);
  if (len < 0) {
    taosMemoryFree(val);
    return -1;
  }

  if (pKTmp->opNum != pCur->number) {
    taosMemoryFree(val);
    return -1;
  }
  if (pKey->groupId != 0 && pKey->groupId != pKTmp->key.groupId) {
    taosMemoryFree(val);
    return -1;
  }

  if (pVal != NULL) {
    *pVal = (char*)val;
  } else {
    taosMemoryFree(val);
  }

  if (pVLen != NULL) *pVLen = len;
  *pKey = pKTmp->key;
  return 0;
}
// fill cf
int32_t streamStateFillPut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  int code = 0;

  STREAM_STATE_PUT_ROCKSDB(pState, "fill", key, value, vLen);
  return code;
}

int32_t streamStateFillGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "fill", key, pVal, pVLen);
  return code;
}
int32_t streamStateFillDel_rocksdb(SStreamState* pState, const SWinKey* key) {
  int code = 0;
  STREAM_STATE_DEL_ROCKSDB(pState, "fill", key);
  return code;
}

SStreamStateCur* streamStateFillGetCur_rocksdb(SStreamState* pState, const SWinKey* key) {
  stDebug("streamStateFillGetCur_rocksdb");
  SStreamStateCur* pCur = createStreamStateCursor();
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;

  if (pCur == NULL) return NULL;

  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "fill", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  char buf[128] = {0};
  int  len = winKeyEncode((void*)key, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (iterValueIsStale(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  if (rocksdb_iter_valid(pCur->iter)) {
    size_t  kLen;
    SWinKey curKey;
    char*   keyStr = (char*)rocksdb_iter_key(pCur->iter, &kLen);
    winKeyDecode((void*)&curKey, keyStr);
    if (winKeyCmpr(key, sizeof(*key), &curKey, sizeof(curKey)) == 0) {
      return pCur;
    }
  }

  streamStateFreeCur(pCur);
  return NULL;
}
int32_t streamStateFillGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  stDebug("streamStateFillGetKVByCur_rocksdb");
  if (!pCur) {
    return -1;
  }
  SWinKey winKey;
  if (!rocksdb_iter_valid(pCur->iter) || iterValueIsStale(pCur->iter)) {
    return -1;
  }
  size_t klen, vlen;
  char*  keyStr = (char*)rocksdb_iter_key(pCur->iter, &klen);
  winKeyDecode(&winKey, keyStr);

  const char* valStr = rocksdb_iter_value(pCur->iter, &vlen);
  int32_t     len = valueDecode((void*)valStr, vlen, NULL, (char**)pVal);
  if (len < 0) {
    return -1;
  }
  if (pVLen != NULL) *pVLen = len;

  *pKey = winKey;
  return 0;
}

SStreamStateCur* streamStateFillSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key) {
  stDebug("streamStateFillSeekKeyNext_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (!pCur) {
    return NULL;
  }

  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "fill", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  char buf[128] = {0};
  int  len = winKeyEncode((void*)key, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  // skip stale data
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) {
    rocksdb_iter_next(pCur->iter);
  }

  if (rocksdb_iter_valid(pCur->iter)) {
    SWinKey curKey;
    size_t  kLen = 0;
    char*   keyStr = (char*)rocksdb_iter_key(pCur->iter, &kLen);
    winKeyDecode((void*)&curKey, keyStr);
    if (winKeyCmpr(key, sizeof(*key), &curKey, sizeof(curKey)) > 0) {
      return pCur;
    }
    rocksdb_iter_next(pCur->iter);
    return pCur;
  }
  streamStateFreeCur(pCur);
  return NULL;
}
SStreamStateCur* streamStateFillSeekKeyPrev_rocksdb(SStreamState* pState, const SWinKey* key) {
  stDebug("streamStateFillSeekKeyPrev_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return NULL;
  }

  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "fill", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  char buf[128] = {0};
  int  len = winKeyEncode((void*)key, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  while (rocksdb_iter_valid(pCur->iter) && iterValueIsStale(pCur->iter)) {
    rocksdb_iter_prev(pCur->iter);
  }

  if (rocksdb_iter_valid(pCur->iter)) {
    SWinKey curKey;
    size_t  kLen = 0;
    char*   keyStr = (char*)rocksdb_iter_key(pCur->iter, &kLen);
    winKeyDecode((void*)&curKey, keyStr);
    if (winKeyCmpr(key, sizeof(*key), &curKey, sizeof(curKey)) < 0) {
      return pCur;
    }
    rocksdb_iter_prev(pCur->iter);
    return pCur;
  }

  streamStateFreeCur(pCur);
  return NULL;
}
#ifdef BUILD_NO_CALL
int32_t streamStateSessionGetKeyByRange_rocksdb(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
  stDebug("streamStateSessionGetKeyByRange_rocksdb");
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;
  SStreamStateCur* pCur = createStreamStateCursor();
  if (pCur == NULL) {
    return -1;
  }
  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "sess", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  char             buf[128] = {0};
  int              len = stateSessionKeyEncode(&sKey, buf);
  if (!streamStateIterSeekAndValid(pCur->iter, buf, len)) {
    streamStateFreeCur(pCur);
    return -1;
  }

  size_t           kLen;
  const char*      iKeyStr = rocksdb_iter_key(pCur->iter, (size_t*)&kLen);
  SStateSessionKey iKey = {0};
  stateSessionKeyDecode(&iKey, (char*)iKeyStr);

  c = stateSessionKeyCmpr(&sKey, sizeof(sKey), &iKey, sizeof(iKey));

  SSessionKey resKey = *key;
  int32_t     code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
  if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
    *curKey = resKey;
    streamStateFreeCur(pCur);
    return code;
  }

  if (c > 0) {
    streamStateCurNext_rocksdb(pState, pCur);
    code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  } else if (c < 0) {
    streamStateCurPrev(pState, pCur);
    code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  }

  streamStateFreeCur(pCur);
  return -1;
}
#endif

int32_t streamStateSessionAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                                int32_t* pVLen) {
  stDebug("streamStateSessionAddIfNotExist_rocksdb");
  // todo refactor
  int32_t     res = 0;
  SSessionKey originKey = *key;
  SSessionKey searchKey = *key;
  searchKey.win.skey = key->win.skey - gap;
  searchKey.win.ekey = key->win.ekey + gap;
  int32_t valSize = *pVLen;

  void* tmp = taosMemoryMalloc(valSize);

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev_rocksdb(pState, key);
  if (pCur == NULL) {
  }
  int32_t code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);

  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      taosMemoryFreeClear(*pVal);
      goto _end;
    }
    taosMemoryFreeClear(*pVal);
    streamStateCurNext_rocksdb(pState, pCur);
  } else {
    *key = originKey;
    streamStateFreeCur(pCur);
    taosMemoryFreeClear(*pVal);
    pCur = streamStateSessionSeekKeyNext_rocksdb(pState, key);
  }

  code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      goto _end;
    }
  }

  *key = originKey;
  res = 1;
  memset(tmp, 0, valSize);

_end:
  taosMemoryFree(*pVal);
  *pVal = tmp;
  streamStateFreeCur(pCur);
  return res;
}
int32_t streamStateSessionClear_rocksdb(SStreamState* pState) {
  stDebug("streamStateSessionClear_rocksdb");
  SSessionKey      key = {.win.skey = 0, .win.ekey = 0, .groupId = 0};
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, &key);

  while (1) {
    SSessionKey delKey = {0};
    void*       buf = NULL;
    int32_t     size = 0;
    int32_t     code = streamStateSessionGetKVByCur_rocksdb(pCur, &delKey, &buf, &size);
    if (code == 0 && size > 0) {
      memset(buf, 0, size);
      // refactor later
      streamStateSessionPut_rocksdb(pState, &delKey, buf, size);
    } else {
      taosMemoryFreeClear(buf);
      break;
    }
    taosMemoryFreeClear(buf);

    streamStateCurNext_rocksdb(pState, pCur);
  }
  streamStateFreeCur(pCur);
  return -1;
}
int32_t streamStateStateAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, char* pKeyData,
                                              int32_t keyDataLen, state_key_cmpr_fn fn, void** pVal, int32_t* pVLen) {
  stDebug("streamStateStateAddIfNotExist_rocksdb");
  // todo refactor
  int32_t     res = 0;
  SSessionKey tmpKey = *key;
  int32_t     valSize = *pVLen;
  void*       tmp = taosMemoryMalloc(valSize);
  // tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev_rocksdb(pState, key);
  int32_t          code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (key->win.skey <= tmpKey.win.skey && tmpKey.win.ekey <= key->win.ekey) {
      memcpy(tmp, *pVal, valSize);
      goto _end;
    }

    void* stateKey = (char*)(*pVal) + (valSize - keyDataLen);
    if (fn(pKeyData, stateKey) == true) {
      memcpy(tmp, *pVal, valSize);
      goto _end;
    }

    streamStateCurNext_rocksdb(pState, pCur);
  } else {
    *key = tmpKey;
    streamStateFreeCur(pCur);
    pCur = streamStateSessionSeekKeyNext_rocksdb(pState, key);
  }
  taosMemoryFreeClear(*pVal);
  code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    void* stateKey = (char*)(*pVal) + (valSize - keyDataLen);
    if (fn(pKeyData, stateKey) == true) {
      memcpy(tmp, *pVal, valSize);
      goto _end;
    }
  }
  taosMemoryFreeClear(*pVal);

  *key = tmpKey;
  res = 1;
  memset(tmp, 0, valSize);

_end:
  taosMemoryFreeClear(*pVal);
  *pVal = tmp;
  streamStateFreeCur(pCur);
  return res;
}

#ifdef BUILD_NO_CALL
//  partag cf
int32_t streamStatePutParTag_rocksdb(SStreamState* pState, int64_t groupId, const void* tag, int32_t tagLen) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "partag", &groupId, tag, tagLen);
  return code;
}

int32_t streamStateGetParTag_rocksdb(SStreamState* pState, int64_t groupId, void** tagVal, int32_t* tagLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "partag", &groupId, tagVal, tagLen);
  return code;
}
#endif
// parname cfg
int32_t streamStatePutParName_rocksdb(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "parname", &groupId, (char*)tbname, TSDB_TABLE_NAME_LEN);
  return code;
}
int32_t streamStateGetParName_rocksdb(SStreamState* pState, int64_t groupId, void** pVal) {
  int    code = 0;
  size_t tagLen;
  STREAM_STATE_GET_ROCKSDB(pState, "parname", &groupId, pVal, &tagLen);
  return code;
}

#ifdef BUILD_NO_CALL
int32_t streamDefaultPut_rocksdb(SStreamState* pState, const void* key, void* pVal, int32_t pVLen) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "default", key, pVal, pVLen);
  return code;
}
#endif
int32_t streamDefaultGet_rocksdb(SStreamState* pState, const void* key, void** pVal, int32_t* pVLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "default", key, pVal, pVLen);
  return code;
}
int32_t streamDefaultDel_rocksdb(SStreamState* pState, const void* key) {
  int code = 0;
  STREAM_STATE_DEL_ROCKSDB(pState, "default", key);
  return code;
}

int32_t streamDefaultIterGet_rocksdb(SStreamState* pState, const void* start, const void* end, SArray* result) {
  int   code = 0;
  char* err = NULL;

  STaskDbWrapper*        wrapper = pState->pTdbState->pOwner->pBackend;
  rocksdb_snapshot_t*    snapshot = NULL;
  rocksdb_readoptions_t* readopts = NULL;
  rocksdb_iterator_t*    pIter = streamStateIterCreate(pState, "default", &snapshot, &readopts);
  if (pIter == NULL) {
    return -1;
  }

  rocksdb_iter_seek(pIter, start, strlen(start));
  while (rocksdb_iter_valid(pIter)) {
    const char* key = rocksdb_iter_key(pIter, NULL);
    int32_t     vlen = 0;
    const char* vval = rocksdb_iter_value(pIter, (size_t*)&vlen);
    char*       val = NULL;
    int32_t     len = valueDecode((void*)vval, vlen, NULL, NULL);
    if (len < 0) {
      rocksdb_iter_next(pIter);
      continue;
    }

    if (end != NULL && strcmp(key, end) > 0) {
      break;
    }
    if (strncmp(key, start, strlen(start)) == 0 && strlen(key) >= strlen(start) + 1) {
      int64_t checkPoint = 0;
      if (sscanf(key + strlen(key), ":%" PRId64 "", &checkPoint) == 1) {
        taosArrayPush(result, &checkPoint);
      }
    } else {
      break;
    }
    rocksdb_iter_next(pIter);
  }
  rocksdb_release_snapshot(wrapper->db, snapshot);
  rocksdb_readoptions_destroy(readopts);
  rocksdb_iter_destroy(pIter);
  return code;
}
#ifdef BUILD_NO_CALL
void* streamDefaultIterCreate_rocksdb(SStreamState* pState) {
  SStreamStateCur* pCur = createStreamStateCursor();
  STaskDbWrapper*  wrapper = pState->pTdbState->pOwner->pBackend;

  pCur->db = wrapper->db;
  pCur->iter = streamStateIterCreate(pState, "default", (rocksdb_snapshot_t**)&pCur->snapshot,
                                     (rocksdb_readoptions_t**)&pCur->readOpt);
  pCur->number = pState->number;
  return pCur;
}
bool streamDefaultIterValid_rocksdb(void* iter) {
  if (iter) {
    return false;
  }
  SStreamStateCur* pCur = iter;
  return (rocksdb_iter_valid(pCur->iter) && !iterValueIsStale(pCur->iter)) ? true : false;
}
void streamDefaultIterSeek_rocksdb(void* iter, const char* key) {
  SStreamStateCur* pCur = iter;
  rocksdb_iter_seek(pCur->iter, key, strlen(key));
}
void streamDefaultIterNext_rocksdb(void* iter) {
  SStreamStateCur* pCur = iter;
  rocksdb_iter_next(pCur->iter);
}
char* streamDefaultIterKey_rocksdb(void* iter, int32_t* len) {
  SStreamStateCur* pCur = iter;
  return (char*)rocksdb_iter_key(pCur->iter, (size_t*)len);
}
char* streamDefaultIterVal_rocksdb(void* iter, int32_t* len) {
  SStreamStateCur* pCur = iter;
  char*            ret = NULL;

  int32_t     vlen = 0;
  const char* val = rocksdb_iter_value(pCur->iter, (size_t*)&vlen);
  *len = valueDecode((void*)val, vlen, NULL, &ret);
  if (*len < 0) {
    taosMemoryFree(ret);
    return NULL;
  }

  return ret;
}
#endif
// batch func
void* streamStateCreateBatch() {
  rocksdb_writebatch_t* pBatch = rocksdb_writebatch_create();
  return pBatch;
}
int32_t streamStateGetBatchSize(void* pBatch) {
  if (pBatch == NULL) return 0;
  return rocksdb_writebatch_count(pBatch);
}

void    streamStateClearBatch(void* pBatch) { rocksdb_writebatch_clear((rocksdb_writebatch_t*)pBatch); }
void    streamStateDestroyBatch(void* pBatch) { rocksdb_writebatch_destroy((rocksdb_writebatch_t*)pBatch); }
int32_t streamStatePutBatch(SStreamState* pState, const char* cfKeyName, rocksdb_writebatch_t* pBatch, void* key,
                            void* val, int32_t vlen, int64_t ttl) {
  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  wrapper->dataWritten += 1;

  int i = streamStateGetCfIdx(pState, cfKeyName);
  if (i < 0) {
    stError("streamState failed to put to cf name:%s", cfKeyName);
    return -1;
  }

  char    buf[128] = {0};
  int32_t klen = ginitDict[i].enFunc((void*)key, buf);

  char*   ttlV = NULL;
  int32_t ttlVLen = ginitDict[i].enValueFunc(val, vlen, ttl, &ttlV);

  rocksdb_column_family_handle_t* pCf = wrapper->pCf[ginitDict[i].idx];
  rocksdb_writebatch_put_cf((rocksdb_writebatch_t*)pBatch, pCf, buf, (size_t)klen, ttlV, (size_t)ttlVLen);
  taosMemoryFree(ttlV);

  {
    char tbuf[256] = {0};
    ginitDict[i].toStrFunc((void*)key, tbuf);
    stDebug("streamState str: %s succ to write to %s_%s, len: %d", tbuf, wrapper->idstr, ginitDict[i].key, vlen);
  }
  return 0;
}

int32_t streamStatePutBatchOptimize(SStreamState* pState, int32_t cfIdx, rocksdb_writebatch_t* pBatch, void* key,
                                    void* val, int32_t vlen, int64_t ttl, void* tmpBuf) {
  char    buf[128] = {0};
  int32_t klen = ginitDict[cfIdx].enFunc((void*)key, buf);
  char*   ttlV = tmpBuf;
  int32_t ttlVLen = ginitDict[cfIdx].enValueFunc(val, vlen, ttl, &ttlV);

  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  wrapper->dataWritten += 1;

  rocksdb_column_family_handle_t* pCf = wrapper->pCf[ginitDict[cfIdx].idx];
  rocksdb_writebatch_put_cf((rocksdb_writebatch_t*)pBatch, pCf, buf, (size_t)klen, ttlV, (size_t)ttlVLen);

  if (tmpBuf == NULL) {
    taosMemoryFree(ttlV);
  }

  {
    char tbuf[256] = {0};
    ginitDict[cfIdx].toStrFunc((void*)key, tbuf);
    stDebug("streamState str: %s succ to write to %s_%s", tbuf, wrapper->idstr, ginitDict[cfIdx].key);
  }
  return 0;
}
int32_t streamStatePutBatch_rocksdb(SStreamState* pState, void* pBatch) {
  char*           err = NULL;
  STaskDbWrapper* wrapper = pState->pTdbState->pOwner->pBackend;
  wrapper->dataWritten += 1;
  rocksdb_write(wrapper->db, wrapper->writeOpt, (rocksdb_writebatch_t*)pBatch, &err);
  if (err != NULL) {
    stError("streamState failed to write batch, err:%s", err);
    taosMemoryFree(err);
    return -1;
  } else {
    stDebug("write batch to backend:%p", wrapper->db);
  }
  return 0;
}
uint32_t nextPow2(uint32_t x) {
  if (x <= 1) return 2;
  x = x - 1;
  x = x | (x >> 1);
  x = x | (x >> 2);
  x = x | (x >> 4);
  x = x | (x >> 8);
  x = x | (x >> 16);
  return x + 1;
}
int32_t copyFiles(const char* src, const char* dst) {
  int32_t code = 0;
  // opt later, just hard link
  int32_t sLen = strlen(src);
  int32_t dLen = strlen(dst);
  char*   srcName = taosMemoryCalloc(1, sLen + 64);
  char*   dstName = taosMemoryCalloc(1, dLen + 64);

  TdDirPtr pDir = taosOpenDir(src);
  if (pDir == NULL) {
    taosMemoryFree(srcName);
    taosMemoryFree(dstName);
    return -1;
  }

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

    sprintf(srcName, "%s%s%s", src, TD_DIRSEP, name);
    sprintf(dstName, "%s%s%s", dst, TD_DIRSEP, name);
    if (!taosDirEntryIsDir(de)) {
      code = taosCopyFile(srcName, dstName);
      if (code == -1) {
        goto _err;
      }
    }

    memset(srcName, 0, sLen + 64);
    memset(dstName, 0, dLen + 64);
  }

_err:
  taosMemoryFreeClear(srcName);
  taosMemoryFreeClear(dstName);
  taosCloseDir(&pDir);
  return code >= 0 ? 0 : -1;
}

int32_t isBkdDataMeta(char* name, int32_t len) {
  const char* pCurrent = "CURRENT";
  int32_t     currLen = strlen(pCurrent);

  const char* pManifest = "MANIFEST-";
  int32_t     maniLen = strlen(pManifest);

  if (len >= maniLen && strncmp(name, pManifest, maniLen) == 0) {
    return 1;
  } else if (len == currLen && strcmp(name, pCurrent) == 0) {
    return 1;
  }
  return 0;
}
int32_t compareHashTableImpl(SHashObj* p1, SHashObj* p2, SArray* diff) {
  int32_t code = 0;
  size_t  len = 0;
  void*   pIter = taosHashIterate(p2, NULL);
  while (pIter) {
    char* name = taosHashGetKey(pIter, &len);
    if (!isBkdDataMeta(name, len) && !taosHashGet(p1, name, len)) {
      char* fname = taosMemoryCalloc(1, len + 1);
      strncpy(fname, name, len);
      taosArrayPush(diff, &fname);
    }
    pIter = taosHashIterate(p2, pIter);
  }
  return code;
}
int32_t compareHashTable(SHashObj* p1, SHashObj* p2, SArray* add, SArray* del) {
  int32_t code = 0;

  code = compareHashTableImpl(p1, p2, add);
  code = compareHashTableImpl(p2, p1, del);

  return code;
}

void hashTableToDebug(SHashObj* pTbl, char** buf) {
  size_t  sz = taosHashGetSize(pTbl);
  int32_t total = 0;
  char*   p = taosMemoryCalloc(1, sz * 16 + 4);
  void*   pIter = taosHashIterate(pTbl, NULL);
  while (pIter) {
    size_t len = 0;
    char*  name = taosHashGetKey(pIter, &len);
    char*  tname = taosMemoryCalloc(1, len + 1);
    memcpy(tname, name, len);
    total += sprintf(p + total, "%s,", tname);

    pIter = taosHashIterate(pTbl, pIter);
    taosMemoryFree(tname);
  }
  if (total > 0) {
    p[total - 1] = 0;
  }
  *buf = p;
}
void strArrayDebugInfo(SArray* pArr, char** buf) {
  int32_t sz = taosArrayGetSize(pArr);
  if (sz <= 0) return;

  char*   p = (char*)taosMemoryCalloc(1, 64 + sz * 64);
  int32_t total = 0;

  for (int i = 0; i < sz; i++) {
    char* name = taosArrayGetP(pArr, i);
    total += sprintf(p + total, "%s,", name);
  }
  p[total - 1] = 0;

  *buf = p;
}
void dbChkpDebugInfo(SDbChkp* pDb) {
  // stTrace("chkp get file list: curr");
  char* p[4] = {NULL};

  hashTableToDebug(pDb->pSstTbl[pDb->idx], &p[0]);
  stTrace("chkp previous file: [%s]", p[0]);

  hashTableToDebug(pDb->pSstTbl[1 - pDb->idx], &p[1]);
  stTrace("chkp curr file: [%s]", p[1]);

  strArrayDebugInfo(pDb->pAdd, &p[2]);
  stTrace("chkp newly addded file: [%s]", p[2]);

  strArrayDebugInfo(pDb->pDel, &p[3]);
  stTrace("chkp newly deleted file: [%s]", p[3]);

  for (int i = 0; i < 4; i++) {
    taosMemoryFree(p[i]);
  }
}
int32_t dbChkpGetDelta(SDbChkp* p, int64_t chkpId, SArray* list) {
  taosThreadRwlockWrlock(&p->rwLock);

  p->preCkptId = p->curChkpId;
  p->curChkpId = chkpId;
  const char* pCurrent = "CURRENT";
  int32_t     currLen = strlen(pCurrent);

  const char* pManifest = "MANIFEST-";
  int32_t     maniLen = strlen(pManifest);

  const char* pSST = ".sst";
  int32_t     sstLen = strlen(pSST);

  memset(p->buf, 0, p->len);
  sprintf(p->buf, "%s%s%s%scheckpoint%" PRId64 "", p->path, TD_DIRSEP, "checkpoints", TD_DIRSEP, chkpId);

  taosArrayClearP(p->pAdd, taosMemoryFree);
  taosArrayClearP(p->pDel, taosMemoryFree);
  taosHashClear(p->pSstTbl[1 - p->idx]);

  TdDirPtr      pDir = taosOpenDir(p->buf);
  TdDirEntryPtr de = NULL;
  int8_t        dummy = 0;
  while ((de = taosReadDir(pDir)) != NULL) {
    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
    if (strlen(name) == currLen && strcmp(name, pCurrent) == 0) {
      taosMemoryFreeClear(p->pCurrent);
      p->pCurrent = taosStrdup(name);
      // taosHashPut(p->pSstTbl[1 - p->idx], name, strlen(name), &dummy, sizeof(dummy));
      continue;
    }

    if (strlen(name) >= maniLen && strncmp(name, pManifest, maniLen) == 0) {
      taosMemoryFreeClear(p->pManifest);
      p->pManifest = taosStrdup(name);
      // taosHashPut(p->pSstTbl[1 - p->idx], name, strlen(name), &dummy, sizeof(dummy));
      continue;
    }
    if (strlen(name) >= sstLen && strncmp(name + strlen(name) - 4, pSST, sstLen) == 0) {
      taosHashPut(p->pSstTbl[1 - p->idx], name, strlen(name), &dummy, sizeof(dummy));
      continue;
    }
  }
  taosCloseDir(&pDir);

  if (p->init == 0) {
    void* pIter = taosHashIterate(p->pSstTbl[1 - p->idx], NULL);
    while (pIter) {
      size_t len = 0;
      char*  name = taosHashGetKey(pIter, &len);
      if (name != NULL && !isBkdDataMeta(name, len)) {
        char* fname = taosMemoryCalloc(1, len + 1);
        strncpy(fname, name, len);
        taosArrayPush(p->pAdd, &fname);
      }
      pIter = taosHashIterate(p->pSstTbl[1 - p->idx], pIter);
    }
    if (taosArrayGetSize(p->pAdd) > 0) p->update = 1;

    p->init = 1;
    p->preCkptId = -1;
    p->curChkpId = chkpId;
  } else {
    int32_t code = compareHashTable(p->pSstTbl[p->idx], p->pSstTbl[1 - p->idx], p->pAdd, p->pDel);
    if (code != 0) {
      // dead code
      taosArrayClearP(p->pAdd, taosMemoryFree);
      taosArrayClearP(p->pDel, taosMemoryFree);
      taosHashClear(p->pSstTbl[1 - p->idx]);
      p->update = 0;
      return code;
    }

    if (taosArrayGetSize(p->pAdd) == 0 && taosArrayGetSize(p->pDel) == 0) {
      p->update = 0;
    }

    p->preCkptId = p->curChkpId;
    p->curChkpId = chkpId;
  }

  dbChkpDebugInfo(p);

  p->idx = 1 - p->idx;

  taosThreadRwlockUnlock(&p->rwLock);

  return 0;
}

SDbChkp* dbChkpCreate(char* path, int64_t initChkpId) {
  SDbChkp* p = taosMemoryCalloc(1, sizeof(SDbChkp));
  p->curChkpId = initChkpId;
  p->preCkptId = -1;
  p->pSST = taosArrayInit(64, sizeof(void*));
  p->path = path;
  p->len = strlen(path) + 128;
  p->buf = taosMemoryCalloc(1, p->len);

  p->idx = 0;
  p->pSstTbl[0] = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  p->pSstTbl[1] = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);

  p->pAdd = taosArrayInit(64, sizeof(void*));
  p->pDel = taosArrayInit(64, sizeof(void*));
  p->update = 0;
  taosThreadRwlockInit(&p->rwLock, NULL);

  SArray* list = NULL;
  int32_t code = dbChkpGetDelta(p, initChkpId, list);

  return p;
}

void dbChkpDestroy(SDbChkp* pChkp) {
  taosMemoryFree(pChkp->buf);
  taosMemoryFree(pChkp->path);

  taosArrayDestroyP(pChkp->pSST, taosMemoryFree);
  taosArrayDestroyP(pChkp->pAdd, taosMemoryFree);
  taosArrayDestroyP(pChkp->pDel, taosMemoryFree);

  taosHashCleanup(pChkp->pSstTbl[0]);
  taosHashCleanup(pChkp->pSstTbl[1]);

  taosMemoryFree(pChkp->pCurrent);
  taosMemoryFree(pChkp->pManifest);
  taosMemoryFree(pChkp);
}
#ifdef BUILD_NO_CALL
int32_t dbChkpInit(SDbChkp* p) {
  if (p == NULL) return 0;
  return 0;
}
#endif
int32_t dbChkpDumpTo(SDbChkp* p, char* dname, SArray* list) {
  taosThreadRwlockRdlock(&p->rwLock);
  int32_t code = -1;
  int32_t len = p->len + 128;

  char* srcBuf = taosMemoryCalloc(1, len);
  char* dstBuf = taosMemoryCalloc(1, len);

  char* srcDir = taosMemoryCalloc(1, len);
  char* dstDir = taosMemoryCalloc(1, len);

  sprintf(srcDir, "%s%s%s%s%s%" PRId64 "", p->path, TD_DIRSEP, "checkpoints", TD_DIRSEP, "checkpoint", p->curChkpId);
  sprintf(dstDir, "%s", dname);

  if (!taosDirExist(srcDir)) {
    stError("failed to dump srcDir %s, reason: not exist such dir", srcDir);
    goto _ERROR;
  }

  // add file to $name dir
  for (int i = 0; i < taosArrayGetSize(p->pAdd); i++) {
    memset(srcBuf, 0, len);
    memset(dstBuf, 0, len);

    char* filename = taosArrayGetP(p->pAdd, i);
    sprintf(srcBuf, "%s%s%s", srcDir, TD_DIRSEP, filename);
    sprintf(dstBuf, "%s%s%s", dstDir, TD_DIRSEP, filename);

    if (taosCopyFile(srcBuf, dstBuf) < 0) {
      stError("failed to copy file from %s to %s", srcBuf, dstBuf);
      goto _ERROR;
    }
  }
  // del file in $name
  for (int i = 0; i < taosArrayGetSize(p->pDel); i++) {
    char* filename = taosArrayGetP(p->pDel, i);
    char* p = taosStrdup(filename);
    taosArrayPush(list, &p);
  }

  // copy current file to dst dir
  memset(srcBuf, 0, len);
  memset(dstBuf, 0, len);
  sprintf(srcBuf, "%s%s%s", srcDir, TD_DIRSEP, p->pCurrent);
  sprintf(dstBuf, "%s%s%s_%" PRId64 "", dstDir, TD_DIRSEP, p->pCurrent, p->curChkpId);
  if (taosCopyFile(srcBuf, dstBuf) < 0) {
    stError("failed to copy file from %s to %s", srcBuf, dstBuf);
    goto _ERROR;
  }

  // copy manifest file to dst dir
  memset(srcBuf, 0, len);
  memset(dstBuf, 0, len);
  sprintf(srcBuf, "%s%s%s", srcDir, TD_DIRSEP, p->pManifest);
  sprintf(dstBuf, "%s%s%s_%" PRId64 "", dstDir, TD_DIRSEP, p->pManifest, p->curChkpId);
  if (taosCopyFile(srcBuf, dstBuf) < 0) {
    stError("failed to copy file from %s to %s", srcBuf, dstBuf);
    goto _ERROR;
  }

  static char* chkpMeta = "META";
  memset(dstBuf, 0, len);
  sprintf(dstDir, "%s%s%s", dstDir, TD_DIRSEP, chkpMeta);

  TdFilePtr pFile = taosOpenFile(dstDir, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    stError("chkp failed to create meta file: %s", dstDir);
    goto _ERROR;
  }
  char content[128] = {0};
  snprintf(content, sizeof(content), "%s_%" PRId64 "\n%s_%" PRId64 "", p->pCurrent, p->curChkpId, p->pManifest,
           p->curChkpId);
  if (taosWriteFile(pFile, content, strlen(content)) <= 0) {
    stError("chkp failed to write meta file: %s", dstDir);
    taosCloseFile(&pFile);
    goto _ERROR;
  }
  taosCloseFile(&pFile);

  // clear delta data buf
  taosArrayClearP(p->pAdd, taosMemoryFree);
  taosArrayClearP(p->pDel, taosMemoryFree);
  code = 0;

_ERROR:
  taosThreadRwlockUnlock(&p->rwLock);
  taosMemoryFree(srcBuf);
  taosMemoryFree(dstBuf);
  taosMemoryFree(srcDir);
  taosMemoryFree(dstDir);
  return code;
}
SBkdMgt* bkdMgtCreate(char* path) {
  SBkdMgt* p = taosMemoryCalloc(1, sizeof(SBkdMgt));
  p->pDbChkpTbl = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  p->path = taosStrdup(path);
  taosThreadRwlockInit(&p->rwLock, NULL);
  return p;
}

void bkdMgtDestroy(SBkdMgt* bm) {
  if (bm == NULL) return;
  void* pIter = taosHashIterate(bm->pDbChkpTbl, NULL);
  while (pIter) {
    SDbChkp* pChkp = *(SDbChkp**)(pIter);
    dbChkpDestroy(pChkp);

    pIter = taosHashIterate(bm->pDbChkpTbl, pIter);
  }

  taosThreadRwlockDestroy(&bm->rwLock);
  taosMemoryFree(bm->path);
  taosHashCleanup(bm->pDbChkpTbl);

  taosMemoryFree(bm);
}
int32_t bkdMgtGetDelta(SBkdMgt* bm, char* taskId, int64_t chkpId, SArray* list, char* dname) {
  int32_t code = 0;

  taosThreadRwlockWrlock(&bm->rwLock);
  SDbChkp** ppChkp = taosHashGet(bm->pDbChkpTbl, taskId, strlen(taskId));
  SDbChkp*  pChkp = ppChkp != NULL ? *ppChkp : NULL;

  if (pChkp == NULL) {
    char* path = taosMemoryCalloc(1, strlen(bm->path) + 64);
    sprintf(path, "%s%s%s", bm->path, TD_DIRSEP, taskId);

    SDbChkp* p = dbChkpCreate(path, chkpId);
    taosHashPut(bm->pDbChkpTbl, taskId, strlen(taskId), &p, sizeof(void*));

    pChkp = p;

    code = dbChkpDumpTo(pChkp, dname, list);
    taosThreadRwlockUnlock(&bm->rwLock);
    return code;
  }

  code = dbChkpGetDelta(pChkp, chkpId, NULL);
  code = dbChkpDumpTo(pChkp, dname, list);

  taosThreadRwlockUnlock(&bm->rwLock);
  return code;
}

#ifdef BUILD_NO_CALL
int32_t bkdMgtAddChkp(SBkdMgt* bm, char* task, char* path) {
  int32_t code = -1;

  taosThreadRwlockWrlock(&bm->rwLock);
  SDbChkp** pp = taosHashGet(bm->pDbChkpTbl, task, strlen(task));
  if (pp == NULL) {
    SDbChkp* p = dbChkpCreate(path, 0);
    if (p != NULL) {
      taosHashPut(bm->pDbChkpTbl, task, strlen(task), &p, sizeof(void*));
      code = 0;
    }
  } else {
    stError("task chkp already exists");
  }

  taosThreadRwlockUnlock(&bm->rwLock);

  return code;
}

int32_t bkdMgtDumpTo(SBkdMgt* bm, char* taskId, char* dname) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&bm->rwLock);

  SDbChkp* p = taosHashGet(bm->pDbChkpTbl, taskId, strlen(taskId));
  code = dbChkpDumpTo(p, dname, NULL);

  taosThreadRwlockUnlock(&bm->rwLock);
  return code;
}
#endif