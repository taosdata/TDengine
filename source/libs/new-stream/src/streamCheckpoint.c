#include "stream.h"
#include "trpc.h"
#include "streamInt.h"

TdThreadMutex mtx;
SHashObj* checkpointReadyMap = NULL;

static int32_t getFilePath(char* filepath) {
  if (snprintf(filepath, PATH_MAX, "%s%ssnode%scheckpoint", tsDataDir, TD_DIRSEP, TD_DIRSEP) < 0) {
    stError("failed to generate checkpoint path for");
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getFileName(char* filepath, int64_t streamId) {
  if (snprintf(filepath, PATH_MAX, "%s%ssnode%scheckpoint%s%" PRIx64 ".ck", tsDataDir, TD_DIRSEP, TD_DIRSEP,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRIx64, streamId);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getFileNameTmp(char* filepath, int64_t streamId) {
  if (snprintf(filepath, PATH_MAX, "%s%ssnode%scheckpoint%s%" PRIx64 ".tmp", tsDataDir, TD_DIRSEP, TD_DIRSEP,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRIx64, streamId);
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
    stDebug("[checkpoint] checkpoint file already exists for streamId:%" PRIx64 ", file:%s, overwrite", streamId, filepath);
    char filepathTmp[PATH_MAX] = {0};
    STREAM_CHECK_RET_GOTO(getFileNameTmp(filepathTmp, streamId));
    STREAM_CHECK_RET_GOTO(writeFile(filepathTmp, data, dataLen));
    if (taosRenameFile(filepathTmp, filepath) != 0) {
      stError("failed to rename checkpoint file from %s to %s for streamId:%" PRIx64, filepathTmp,
              filepath, streamId);
      STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepathTmp) != 0, terrno);
      code = terrno;
      goto end;
    }
  } else {
    stDebug("[checkpoint] write checkpoint file for streamId:%" PRIx64 ", file:%s, content(%d, %"PRIx64") len:%"PRId64, 
      streamId, filepath, *(int32_t*)(POINTER_SHIFT(data, INT_BYTES)), *(int64_t*)POINTER_SHIFT(data, 2 * INT_BYTES), dataLen);
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

  terrno = 0;
  pFile = taosOpenFile(filepath, TD_FILE_READ);
  STREAM_CHECK_NULL_GOTO(pFile, 0);

  STREAM_CHECK_RET_GOTO(taosFStatFile(pFile, dataLen, NULL));
  *data = taosMemoryMalloc(*dataLen);
  STREAM_CHECK_NULL_GOTO(*data, terrno);

  STREAM_CHECK_CONDITION_GOTO(taosReadFile(pFile, *data, *dataLen) != *dataLen, terrno);
  stDebug("[checkpoint] read checkpoint file for streamId:%" PRIx64 ", file:%s, content:(%d %" PRIx64") len:%"PRId64, 
    streamId, filepath, *(int32_t*)(POINTER_SHIFT(*data, INT_BYTES)), *(int64_t*)POINTER_SHIFT(*data, 2 * INT_BYTES), *dataLen);

end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(*data);
    *dataLen = 0;
  }
  (void)taosCloseFile(&pFile);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

void streamDeleteCheckPoint(int64_t streamId) {
  int32_t code = 0;
  int32_t lino = 0;
  char    filepath[PATH_MAX] = {0};
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId));

  if (taosCheckExistFile(filepath)) {
    stDebug("[checkpoint] delete file for streamId:%" PRIx64 ", file:%s", streamId, filepath);
    STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepath) != 0, terrno);
  } else {
    stDebug("[checkpoint] file does not exist for streamId:%" PRIx64 ", file:%s", streamId, filepath);
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
  memcpy((char*)msg.pCont + sizeof(SMsgHead), data, dataLen);
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
  msg.info.noResp = 1;
  SMsgHead *pMsgHead = (SMsgHead *)msg.pCont;
  pMsgHead->contLen = htonl(msg.contLen);
  pMsgHead->vgId = htonl(SNODE_HANDLE);
  memcpy((char*)msg.pCont + sizeof(SMsgHead), &streamId, LONG_BYTES);
  return tmsgSendReq(epSet, &msg);
}

int32_t streamCheckpointSetReady(int64_t streamId) {
  SStreamTriggerTask* pTriggerTask = NULL;
  void* taskAddr = NULL;
  int32_t code = streamAcquireTriggerTask(streamId, (SStreamTask**)&pTriggerTask, &taskAddr);
  if (code == 0){
    atomic_store_8(&pTriggerTask->isCheckpointReady, 1);
    streamReleaseTask(taskAddr);
  }
  return code;
}

int32_t streamSyncWriteCheckpoint(int64_t streamId, SEpSet* epSet, void* data, int64_t dataLen) {
  int32_t code = 0;
  int32_t lino = 0;

  if (data == NULL) {
    int32_t ret = streamReadCheckPoint(streamId, &data, &dataLen);
    if (ret != TSDB_CODE_SUCCESS || terrno == TAOS_SYSTEM_ERROR(ENOENT)) {
      dataLen = 2 * INT_BYTES + LONG_BYTES;
      taosMemoryFreeClear(data);
      data = taosMemoryCalloc(1, 2 * INT_BYTES + LONG_BYTES);
      STREAM_CHECK_NULL_GOTO(data, terrno);
      *(int32_t*)(POINTER_SHIFT(data, INT_BYTES)) = -1;
      *(int64_t*)(POINTER_SHIFT(data, 2 * INT_BYTES)) = streamId;
    }
  }
  STREAM_CHECK_RET_GOTO(sendSyncMsg(data, dataLen, epSet));
  stDebug("[checkpoint] sync checkpoint for streamId:%" PRIx64 ", dataLen:%" PRId64, streamId, dataLen);
end:
  if (code) {
    stsWarn("[checkpoint] %s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  taosMemoryFreeClear(data);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamSyncDeleteCheckpoint(int64_t streamId, SEpSet* epSet) {
  int32_t code = 0;
  int32_t lino = 0;

  STREAM_CHECK_RET_GOTO(sendDeleteMsg(streamId, epSet));
  stDebug("[checkpoint] delete checkpoint for streamId:%" PRIx64, streamId);
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
  STREAM_CHECK_NULL_GOTO(pDir, 0);

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

    int64_t streamId = taosStr2Int64(name, NULL, 16);
    code = streamSyncWriteCheckpoint(streamId, epSet, NULL, 0);
    if (code != TSDB_CODE_SUCCESS) {
      stError("failed to sync checkpoint for streamId:%" PRIx64 ", code:%d", streamId, code);
      continue;
    }
  }

  STREAM_CHECK_RET_GOTO(taosCloseDir(&pDir));
  stDebug("[checkpoint] sync all checkpoints to snodes successfully");
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
