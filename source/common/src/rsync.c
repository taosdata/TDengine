//
// Created by mingming wanng on 2023/11/2.
//
#include "rsync.h"
#include <stdlib.h>
#include "tglobal.h"

#define ERRNO_ERR_FORMAT "errno:%d,msg:%s"
#define ERRNO_ERR_DATA   errno, strerror(errno)

// deleteRsync function produce empty directories, traverse base directory to remove them
static void removeEmptyDir() {
  TdDirPtr pDir = taosOpenDir(tsCheckpointBackupDir);
  if (pDir == NULL) return;

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    if (!taosDirEntryIsDir(de)) {
      continue;
    }

    if (strcmp(taosGetDirEntryName(de), ".") == 0 || strcmp(taosGetDirEntryName(de), "..") == 0) continue;

    char filename[PATH_MAX] = {0};
    snprintf(filename, sizeof(filename), "%s%s", tsCheckpointBackupDir, taosGetDirEntryName(de));

    TdDirPtr      pDirTmp = taosOpenDir(filename);
    TdDirEntryPtr deTmp = NULL;
    bool          empty = true;
    while ((deTmp = taosReadDir(pDirTmp)) != NULL) {
      if (strcmp(taosGetDirEntryName(deTmp), ".") == 0 || strcmp(taosGetDirEntryName(deTmp), "..") == 0) continue;
      empty = false;
    }
    if (empty) taosRemoveDir(filename);
    if (taosCloseDir(&pDirTmp) != 0) {
      uError("[rsync] close dir error," ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
    }
  }

  if (taosCloseDir(&pDir) != 0) {
    uError("[rsync] close dir error," ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
  }
}

#ifdef WINDOWS
// C:\TDengine\data\backup\checkpoint\ -> /c/TDengine/data/backup/checkpoint/
static void changeDirFromWindowsToLinux(char* from, char* to) {
  to[0] = '/';
  to[1] = from[0];
  for (int32_t i = 2; i < strlen(from); i++) {
    if (from[i] == '\\') {
      to[i] = '/';
    } else {
      to[i] = from[i];
    }
  }
}
#endif

static int32_t generateConfigFile(char* confDir) {
  int32_t   code = 0;
  TdFilePtr pFile = taosOpenFile(confDir, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    uError("[rsync] open conf file error, dir:%s," ERRNO_ERR_FORMAT, confDir, ERRNO_ERR_DATA);
    return terrno;
  }

#ifdef WINDOWS
  char path[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(tsCheckpointBackupDir, path);
#endif

  char confContent[PATH_MAX * 4] = {0};
  snprintf(confContent, PATH_MAX * 4,
#ifndef WINDOWS
           "uid = root\n"
           "gid = root\n"
#endif
           "use chroot = false\n"
           "max connections = 200\n"
           "timeout = 100\n"
           "lock file = %srsync.lock\n"
           "log file = %srsync.log\n"
           "ignore errors = true\n"
           "read only = false\n"
           "list = false\n"
           "[checkpoint]\n"
           "path = %s",
           tsCheckpointBackupDir, tsCheckpointBackupDir,
#ifdef WINDOWS
           path
#else
           tsCheckpointBackupDir
#endif
  );
  uDebug("[rsync] conf:%s", confContent);
  uInfo("[rsync] conf:%s", confContent);
  // if (taosWriteFile(pFile, confContent, strlen(confContent)) != TSDB_CODE_SUCCESS) {
  if (taosWriteFile(pFile, confContent, strlen(confContent)) <= 0) {
    uError("[rsync] write conf file error," ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
    (void)taosCloseFile(&pFile);
    code = terrno;
    return code;
  }

  (void)taosCloseFile(&pFile);
  return 0;
}

static int32_t execCommand(char* command) {
  int32_t try = 3;
  int32_t code = 0;
  while (try-- > 0) {
    code = system(command);
    if (code == 0) {
      break;
    }
    taosMsleep(10);
  }
  return code;
}

void stopRsync() {
  int32_t code =
#ifdef WINDOWS
      system("taskkill /f /im rsync.exe");
#else
      system("pkill rsync");
#endif

  if (code != 0) {
    uError("[rsync] stop rsync server failed," ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
  } else {
    uDebug("[rsync] stop rsync server successful");
  }

  taosMsleep(500);  // sleep 500 ms to wait for the completion of kill operation.
}

#include "tutil.h"

static int32_t dumpRsyncLog() {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, PATH_MAX, "%srsync.log", tsCheckpointBackupDir);

  int32_t code = 0, lino = 0;
  char*   cp_body = NULL;

  TdFilePtr fd = taosOpenFile(filepath, TD_FILE_READ);
  if (!fd) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  int64_t size = -1;
  TAOS_CHECK_GOTO(taosStatFile(filepath, &size, NULL, NULL), &lino, _exit);

  cp_body = taosMemoryMalloc(size + 1);
  if (!cp_body) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  int64_t n = taosReadFile(fd, cp_body, size);
  if (n < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  } else if (n != size) {
    TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
  }
  (void)taosCloseFile(&fd);
  cp_body[size] = '\0';

  // return cos_cp_parse_body(cp_body, checkpoint);
  uInfo("rsynclog: %s", cp_body);

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (fd) {
    (void)taosCloseFile(&fd);
  }
  if (cp_body) {
    taosMemoryFree(cp_body);
  }
  TAOS_RETURN(code);
}

int32_t startRsync() {
  int32_t code = 0;
  if (taosMulMkDir(tsCheckpointBackupDir) != 0) {
    uError("[rsync] build checkpoint backup dir failed, path:%s," ERRNO_ERR_FORMAT, tsCheckpointBackupDir,
           ERRNO_ERR_DATA);
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }

  removeEmptyDir();

  char confDir[PATH_MAX] = {0};
  snprintf(confDir, PATH_MAX, "%srsync.conf", tsCheckpointBackupDir);

  code = generateConfigFile(confDir);
  if (code != 0) {
    return code;
  }

  char cmd[PATH_MAX] = {0};
  snprintf(cmd, PATH_MAX, "rsync --daemon --port=%d --config=%s", tsRsyncPort, confDir);
  // start rsync service to backup checkpoint
  code = system(cmd);
  if (code != 0) {
    uError("[rsync] cmd:%s start server failed, code:%d," ERRNO_ERR_FORMAT, cmd, code, ERRNO_ERR_DATA);
    if (errno == 0) {
      return 0;
    } else {
      code = TAOS_SYSTEM_ERROR(errno);
    }
    (void)dumpRsyncLog();
  } else {
    uInfo("[rsync] cmd:%s start server successful", cmd);
  }
  return code;
}

int32_t uploadByRsync(const char* id, const char* path, int64_t checkpointId) {
  int64_t st = taosGetTimestampMs();
  char    command[PATH_MAX] = {0};

#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);

  if (pathTransform[strlen(pathTransform) - 1] != '/') {
#else
  if (path[strlen(path) - 1] != '/') {
#endif
    snprintf(command, PATH_MAX,
             "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 --bwlimit=100000 --exclude=\"*\" %s/ "
             "rsync://%s/checkpoint/%s/",
             tsLogDir,
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             ,
             tsSnodeAddress, id);
  } else {
    snprintf(command, PATH_MAX,
             "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 --bwlimit=100000 --exclude=\"*\" %s "
             "rsync://%s/checkpoint/%s/",
             tsLogDir,
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             ,
             tsSnodeAddress, id);
  }

  // prepare the data directory
  int32_t code = execCommand(command);
  if (code != 0) {
    uError("[rsync] s-task:%s prepare checkpoint dir in %s to %s failed, code:%d," ERRNO_ERR_FORMAT, id, path,
           tsSnodeAddress, code, ERRNO_ERR_DATA);
    code = TAOS_SYSTEM_ERROR(errno);
  } else {
    int64_t el = (taosGetTimestampMs() - st);
    uDebug("[rsync] s-task:%s prepare checkpoint dir in:%s to %s successfully, elapsed time:%" PRId64 "ms", id, path,
           tsSnodeAddress, el);
  }

#ifdef WINDOWS
  memset(pathTransform, 0, PATH_MAX);
  changeDirFromWindowsToLinux(path, pathTransform);

  if (pathTransform[strlen(pathTransform) - 1] != '/') {
#else
  if (path[strlen(path) - 1] != '/') {
#endif
    snprintf(command, PATH_MAX,
             "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 --bwlimit=100000 %s/ "
             "rsync://%s/checkpoint/%s/%" PRId64 "/",
             tsLogDir,
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             ,
             tsSnodeAddress, id, checkpointId);
  } else {
    snprintf(command, PATH_MAX,
             "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 --bwlimit=100000 %s "
             "rsync://%s/checkpoint/%s/%" PRId64 "/",
             tsLogDir,
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             ,
             tsSnodeAddress, id, checkpointId);
  }

  code = execCommand(command);
  if (code != 0) {
    uError("[rsync] s-task:%s upload checkpoint data in %s to %s failed, code:%d," ERRNO_ERR_FORMAT, id, path,
           tsSnodeAddress, code, ERRNO_ERR_DATA);
    code = TAOS_SYSTEM_ERROR(errno);
  } else {
    int64_t el = (taosGetTimestampMs() - st);
    uDebug("[rsync] s-task:%s upload checkpoint data in:%s to %s successfully, elapsed time:%" PRId64 "ms", id, path,
           tsSnodeAddress, el);
  }

  return code;
}

// abort from retry if quit
int32_t downloadByRsync(const char* id, const char* path, int64_t checkpointId) {
  int64_t st = taosGetTimestampMs();
  int32_t MAX_RETRY = 10;
  int32_t times = 0;
  int32_t code = 0;

#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);
#endif

  char command[PATH_MAX] = {0};
  snprintf(
      command, PATH_MAX,
      "rsync -av --debug=all --log-file=%s/rsynclog --timeout=10 --bwlimit=100000 rsync://%s/checkpoint/%s/%" PRId64
      "/ %s",
      tsLogDir, tsSnodeAddress, id, checkpointId,
#ifdef WINDOWS
      pathTransform
#else
      path
#endif
  );

  uDebug("[rsync] %s start to sync data from remote to:%s, cmd:%s", id, path, command);

  code = execCommand(command);
  if (code != TSDB_CODE_SUCCESS) {
    uError("[rsync] %s download checkpointId:%" PRId64
           " data:%s failed, retry after 1sec, times:%d, code:%d," ERRNO_ERR_FORMAT,
           id, checkpointId, path, times, code, ERRNO_ERR_DATA);
  } else {
    int32_t el = taosGetTimestampMs() - st;
    uDebug("[rsync] %s download checkpointId:%" PRId64 " data:%s successfully, elapsed time:%dms", id, checkpointId,
           path, el);
  }

  if (code != TSDB_CODE_SUCCESS) {  // if failed, try to load it from data directory
#ifdef WINDOWS
    memset(pathTransform, 0, PATH_MAX);
    changeDirFromWindowsToLinux(path, pathTransform);
#endif

    memset(command, 0, PATH_MAX);
    snprintf(
        command, PATH_MAX,
        "rsync -av --debug=all --log-file=%s/rsynclog --timeout=10 --bwlimit=100000 rsync://%s/checkpoint/%s/data/ %s",
        tsLogDir, tsSnodeAddress, id,
#ifdef WINDOWS
        pathTransform
#else
        path
#endif
    );

    uDebug("[rsync] %s start to sync data from remote data dir to:%s, cmd:%s", id, path, command);

    code = execCommand(command);
    if (code != TSDB_CODE_SUCCESS) {
      uError("[rsync] %s download checkpointId:%" PRId64
             " data:%s failed, retry after 1sec, times:%d, code:%d," ERRNO_ERR_FORMAT,
             id, checkpointId, path, times, code, ERRNO_ERR_DATA);
      code = TAOS_SYSTEM_ERROR(code);
    } else {
      int32_t el = taosGetTimestampMs() - st;
      uDebug("[rsync] %s download checkpointId:%" PRId64 " data:%s successfully, elapsed time:%dms", id, checkpointId,
             path, el);
    }
  }
  return code;
}

int32_t deleteRsync(const char* id) {
  char*   tmp = "./tmp_empty/";
  int32_t code = taosMkDir(tmp);
  if (code != 0) {
    uError("[rsync] make tmp dir failed. code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return TAOS_SYSTEM_ERROR(errno);
  }

  char command[PATH_MAX] = {0};
  snprintf(command, PATH_MAX,
           "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 %s rsync://%s/checkpoint/%s/data/",
           tsLogDir, tmp, tsSnodeAddress, id);

  code = execCommand(command);
  taosRemoveDir(tmp);
  if (code != 0) {
    uError("[rsync] get failed code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return TAOS_SYSTEM_ERROR(errno);
  }

  uDebug("[rsync] delete data:%s successful", id);
  return 0;
}
