#include "stream.h"

static int32_t getFileName(char* filepath, int64_t streamId, int32_t snodeId) {
  if (snprintf(filepath, PATH_MAX, "%s%ssnode%ssnode%d%s%" PRId64 ".ck", tsDataDir, TD_DIRSEP, TD_DIRSEP, snodeId,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRId64 ", snodeId:%d", streamId, snodeId);
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getFileNameTmp(char* filepath, int64_t streamId, int32_t snodeId) {
  if (snprintf(filepath, PATH_MAX, "%s%ssnode%ssnode%d%s%" PRId64 ".tmp", tsDataDir, TD_DIRSEP, TD_DIRSEP, snodeId,
               TD_DIRSEP, streamId) < 0) {
    stError("failed to generate checkpoint file name for streamId:%" PRId64 ", snodeId:%d", streamId, snodeId);
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

int32_t streamWriteCheckPoint(int64_t streamId, int32_t snodeId, void* data, int64_t dataLen) {
  int32_t code = 0;
  int32_t lino = 0;
  char    filepath[PATH_MAX] = {0};
  STREAM_CHECK_NULL_GOTO(data, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_CONDITION_GOTO(dataLen <= 0, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId, snodeId));
  bool exist = taosCheckExistFile(filepath);
  if (exist) {
    stDebug("checkpoint file already exists for streamId:%" PRId64 ", snodeId:%d, file:%s", streamId, snodeId,
            filepath);
    char filepathTmp[PATH_MAX] = {0};
    STREAM_CHECK_RET_GOTO(getFileNameTmp(filepathTmp, streamId, snodeId));
    STREAM_CHECK_RET_GOTO(writeFile(filepathTmp, data, dataLen));
    if (taosRenameFile(filepathTmp, filepath) != 0) {
      stError("failed to rename checkpoint file from %s to %s for streamId:%" PRId64 ", snodeId:%d", filepathTmp,
              filepath, streamId, snodeId);
      STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepathTmp) != 0, terrno);
      code = terrno;
      goto end;
    }
  } else {
    stDebug("writing checkpoint file for streamId:%" PRId64 ", snodeId:%d, file:%s", streamId, snodeId, filepath);
    STREAM_CHECK_RET_GOTO(writeFile(filepath, data, dataLen));
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamReadCheckPoint(int64_t streamId, int32_t snodeId, void** data, int64_t* dataLen) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char      filepath[PATH_MAX] = {0};
  TdFilePtr pFile = NULL;
  STREAM_CHECK_NULL_GOTO(data, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(dataLen, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId, snodeId));

  pFile = taosOpenFile(filepath, TD_FILE_READ);
  STREAM_CHECK_NULL_GOTO(pFile, terrno);

  STREAM_CHECK_RET_GOTO(taosFStatFile(pFile, dataLen, NULL));
  *data = taosMemoryMalloc(*dataLen);
  STREAM_CHECK_NULL_GOTO(*data, terrno);

  STREAM_CHECK_CONDITION_GOTO(taosReadFile(pFile, *data, *dataLen) != *dataLen, terrno);

end:
  (void)taosCloseFile(&pFile);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

int32_t streamDeleteCheckPoint(int64_t streamId, int32_t snodeId) {
  int32_t code = 0;
  int32_t lino = 0;
  char    filepath[PATH_MAX] = {0};
  STREAM_CHECK_RET_GOTO(getFileName(filepath, streamId, snodeId));

  if (taosCheckExistFile(filepath)) {
    stDebug("deleting checkpoint file for streamId:%" PRId64 ", snodeId:%d, file:%s", streamId, snodeId, filepath);
    STREAM_CHECK_CONDITION_GOTO(taosRemoveFile(filepath) != 0, terrno);
  } else {
    stDebug("checkpoint file does not exist for streamId:%" PRId64 ", snodeId:%d, file:%s", streamId, snodeId,
            filepath);
  }

end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}