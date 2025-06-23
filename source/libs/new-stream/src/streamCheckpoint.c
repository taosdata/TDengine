#include "stream.h"
#include "trpc.h"

TdThreadMutex mtx;
SHashObj* checkpointReadyMap = NULL;

static int32_t getFilePath(char* filepath) {
  if (snprintf(filepath, PATH_MAX, "%ssnode%scheckpoint", tsDataDir, TD_DIRSEP) < 0) {
    stError("failed to generate checkpoint path for");
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getFileName(char* filepath, int64_t streamId) {
  if (snprintf(filepath, PATH_MAX, "%ssnode%scheckpoint%s%" PRId64 ".ck", tsDataDir, TD_DIRSEP,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRId64, streamId);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getFileNameTmp(char* filepath, int64_t streamId) {
  if (snprintf(filepath, PATH_MAX, "%ssnode%scheckpoint%s%" PRId64 ".tmp", tsDataDir, TD_DIRSEP,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRId64, streamId);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t writeFile(char* filepath, void* data, int64_t dataLen) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_CLOEXEC);
  STREAM_CHECK_NULL_GOTO(pFile, terrno);

  STREAM_CHECK_CONDITION_GOTO(taosWriteFile(pFile, data, dataLen) <= 0, terrno);
  STREAM_CHECK_CONDITION_GOTO(taosFsyncFile(pFile) < 0, terrno);
  STREAM_CHECK_CONDITION_GOTO(taosCloseFile(&pFile) != 0, terrno);
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

// checkpoint format: ver(int32)+streamId(int64)+data
int32_t streamWriteCheckPoint(int64_t streamId, void* data, int64_t dataLen) {
  int32_t code = 0;
  int32_t lino = 0;
  char    filepath[PATH_MAX] = {0};
  STREAM_CHECK_NULL_GOTO(data, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_CONDITION_GOTO(dataLen <= 0, TSDB_CODE_INVALID_PARA);
  
  STREAM_CHECK_RET_GOTO(getFilePath(filepath));
  STREAM_CHECK_RET_GOTO(taosMkDir(filepath));

  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId));
  bool exist = taosCheckExistFile(filepath);
  if (exist) {
    stDebug("checkpoint file already exists for streamId:%" PRId64 ", file:%s", streamId, filepath);
    char filepathTmp[PATH_MAX] = {0};
    STREAM_CHECK_RET_GOTO(getFileNameTmp(filepathTmp, streamId));
    STREAM_CHECK_RET_GOTO(writeFile(filepathTmp, data, dataLen));
    if (taosRenameFile(filepathTmp, filepath) != 0) {
      stError("failed to rename checkpoint file from %s to %s for streamId:%" PRId64, filepathTmp,
              filepath, streamId);
      STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepathTmp) != 0, terrno);
      code = terrno;
      goto end;
    }
  } else {
    stDebug("writing checkpoint file for streamId:%" PRId64 ", file:%s", streamId, filepath);
    STREAM_CHECK_RET_GOTO(writeFile(filepath, data, dataLen));
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamReadCheckPoint(int64_t streamId, void** data, int64_t* dataLen) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char      filepath[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;
  STREAM_CHECK_NULL_GOTO(data, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(dataLen, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId));

  pFile = taosOpenFile(filepath, TD_FILE_READ);
  STREAM_CHECK_NULL_GOTO(pFile, terrno);

  STREAM_CHECK_RET_GOTO(taosFStatFile(pFile, dataLen, NULL));
  *data = taosMemoryMalloc(*dataLen);
  STREAM_CHECK_NULL_GOTO(*data, terrno);

  STREAM_CHECK_CONDITION_GOTO(taosReadFile(pFile, *data, *dataLen) != *dataLen, terrno);

end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(*data);
    *dataLen = 0;
  }
  (void)taosCloseFile(&pFile);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

// int32_t streamReadCheckPointVer(int64_t streamId, int32_t* ver) {
//   int32_t   code = 0;
//   int32_t   lino = 0;
//   char      filepath[PATH_MAX] = {0};
//   TdFilePtr pFile = NULL;
//   STREAM_CHECK_NULL_GOTO(ver, TSDB_CODE_INVALID_PARA);
//   STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId));

//   pFile = taosOpenFile(filepath, TD_FILE_READ);
//   STREAM_CHECK_NULL_GOTO(pFile, terrno);

//   STREAM_CHECK_CONDITION_GOTO(taosReadFile(pFile, ver, INT_BYTES) != INT_BYTES, terrno);
//   stDebug("read checkpoint version for streamId:%" PRId64 ", ver:%d", streamId, *ver);

// end:
//   (void)taosCloseFile(&pFile);
//   STREAM_PRINT_LOG_END(code, lino);
//   return code;
// }

void streamDeleteCheckPoint(int64_t streamId) {
  int32_t code = 0;
  int32_t lino = 0;
  char    filepath[PATH_MAX] = {0};
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId));

  if (taosCheckExistFile(filepath)) {
    stDebug("deleting checkpoint file for streamId:%" PRId64 ", file:%s", streamId, filepath);
    STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepath) != 0, terrno);
  } else {
    stDebug("checkpoint file does not exist for streamId:%" PRId64 ", file:%s", streamId, filepath);
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  return;
}

static int32_t sendSyncMsg(void* data, int64_t dataLen, SEpSet* epSet){
  int32_t code = 0;
  SRpcMsg msg = {.msgType = TDMT_STREAM_SYNC_CHECKPOINT};
  msg.contLen = dataLen + sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  if (msg.pCont == NULL) {
    return terrno;
  }
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  memcpy(msg.pCont + sizeof(SMsgHead), data, dataLen);
  return tmsgSendReq(epSet, &msg);
}

static int32_t sendDeleteMsg(int64_t streamId, SEpSet* epSet){
  int32_t code = 0;
  SRpcMsg msg = {.msgType = TDMT_STREAM_DELETE_CHECKPOINT};
  msg.contLen = LONG_BYTES + sizeof(SMsgHead);
  msg.pCont = rpcMallocCont(msg.contLen);
  if (msg.pCont == NULL) {
    return terrno;
  }
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  memcpy(msg.pCont + sizeof(SMsgHead), &streamId, LONG_BYTES);
  return tmsgSendReq(epSet, &msg);
}

int32_t streamCheckpointSetNotReady(int64_t streamId) {
  int32_t code = 0;
  int32_t lino = 0;

  if (checkpointReadyMap == NULL) {
    
    taosThreadMutexLock(&mtx);
    if (checkpointReadyMap == NULL) {
      checkpointReadyMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
      if (checkpointReadyMap == NULL) {
        code = terrno;
        taosThreadMutexUnlock(&mtx);
        stError("failed to initialize checkpoint ready map, error:%s", tstrerror(code));
        goto end;
      }
    }
    taosThreadMutexUnlock(&mtx);
  }
  bool checkpointReady = false;
  STREAM_CHECK_RET_GOTO(taosHashPut(checkpointReadyMap, &streamId, sizeof(streamId), &checkpointReady, sizeof(checkpointReady)));
  stDebug("checkpoint set not ready for streamId:%" PRId64, streamId);
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamCheckpointSetReady(int64_t streamId) {
  bool checkpointReady = true;
  int32_t code = taosHashPut(checkpointReadyMap, &streamId, sizeof(streamId), &checkpointReady, sizeof(checkpointReady));
  if (code != 0) {
    stError("failed to set checkpoint ready for streamId:%" PRId64 ", error:%s", streamId, tstrerror(code));
    return code;
  }
  stDebug("checkpoint set ready for streamId:%" PRId64, streamId);
  return code;
}

bool streamCheckpointIsReady(int64_t streamId) {
  if (checkpointReadyMap == NULL) { 
    return true;
  }
  void* data = taosHashGet(checkpointReadyMap, &streamId, sizeof(streamId));
  if (data == NULL) {
    stDebug("checkpoint not generated for streamId:%" PRId64, streamId);
    return true;
  }
  bool ready = *(bool*)data;
  if (ready) {
    stDebug("checkpoint is ready for streamId:%" PRId64, streamId);
  } else {
    stDebug("checkpoint is not ready for streamId:%" PRId64, streamId);
  }
  return ready;
}

int32_t streamSyncWriteCheckpoint(int64_t streamId, SEpSet* epSet, void* data, int64_t dataLen) {
  int32_t code = 0;
  int32_t lino = 0;

  if (data == NULL) {
    STREAM_CHECK_RET_GOTO(streamReadCheckPoint(streamId, &data, &dataLen));
  }
  STREAM_CHECK_RET_GOTO(sendSyncMsg(data, dataLen, epSet));
  stDebug("sync checkpoint for streamId:%" PRId64 ", dataLen:%" PRId64, streamId, dataLen);
end:
  taosMemoryFreeClear(data);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamSyncDeleteCheckpoint(int64_t streamId, SEpSet* epSet) {
  int32_t code = 0;
  int32_t lino = 0;

  STREAM_CHECK_RET_GOTO(sendDeleteMsg(streamId, epSet));
  stDebug("delete checkpoint for streamId:%" PRId64, streamId);
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamSyncAllCheckpoints(SEpSet* epSet) {
  int32_t code = 0;
  int32_t lino = 0;
  TdDirPtr pDir = NULL;
  
  char snodePath[PATH_MAX] = {0};
  STREAM_CHECK_RET_GOTO(getFilePath(snodePath));
  pDir = taosOpenDir(snodePath);
  STREAM_CHECK_NULL_GOTO(pDir, terrno);

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    if (taosDirEntryIsDir(de)) {
      continue;
    }

    char* name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      stDebug("skip file:%s, for checkpoint", name);
      continue;
    }

    int64_t streamId = 0;
    if (taosStr2int64(name, &streamId) != 0) {
      stError("failed to convert file name:%s to streamId", name);
      continue;
    }
    code = streamSyncWriteCheckpoint(streamId, epSet, NULL, 0);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to sync checkpoint for streamId:%" PRId64 ", code:%d", streamId, code);
      continue;
    }
  }

  STREAM_CHECK_RET_GOTO(taosCloseDir(&pDir));
  stDebug("sync all checkpoints to snodes successfully");
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

void streamDeleteAllCheckpoints(void) {
  char snodePath[PATH_MAX] = {0};
  if (getFilePath(snodePath) != TSDB_CODE_SUCCESS) {
    stError("failed to get snode checkpoint path");
    return;
  }
  
  taosRemoveDir(snodePath);
}
