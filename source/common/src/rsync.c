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
    taosCloseDir(&pDirTmp);
  }

  taosCloseDir(&pDir);
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
  TdFilePtr pFile = taosOpenFile(confDir, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    uError("[rsync] open conf file error, dir:%s," ERRNO_ERR_FORMAT, confDir, ERRNO_ERR_DATA);
    return -1;
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
  if (taosWriteFile(pFile, confContent, strlen(confContent)) <= 0) {
    uError("[rsync] write conf file error," ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
    taosCloseFile(&pFile);
    return -1;
  }

  taosCloseFile(&pFile);
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

void startRsync() {
  if (taosMulMkDir(tsCheckpointBackupDir) != 0) {
    uError("[rsync] build checkpoint backup dir failed, path:%s," ERRNO_ERR_FORMAT, tsCheckpointBackupDir,
           ERRNO_ERR_DATA);
    return;
  }

  removeEmptyDir();

  char confDir[PATH_MAX] = {0};
  snprintf(confDir, PATH_MAX, "%srsync.conf", tsCheckpointBackupDir);

  int32_t code = generateConfigFile(confDir);
  if (code != 0) {
    return;
  }

  char cmd[PATH_MAX] = {0};
  snprintf(cmd, PATH_MAX, "rsync --daemon --port=%d --config=%s", tsRsyncPort, confDir);
  // start rsync service to backup checkpoint
  code = system(cmd);
  if (code != 0) {
    uError("[rsync] start server failed, code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
  } else {
    uDebug("[rsync] start server successful");
  }

}

int32_t uploadByRsync(const char* id, const char* path) {
  int64_t st = taosGetTimestampMs();
  char    command[PATH_MAX] = {0};

#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);

  if(pathTransform[strlen(pathTransform) - 1] != '/') {
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
      uError("[rsync] s-task:%s prepare checkpoint data in %s to %s failed, code:%d," ERRNO_ERR_FORMAT, id, path,
             tsSnodeAddress, code, ERRNO_ERR_DATA);
  } else {
      int64_t el = (taosGetTimestampMs() - st);
      uDebug("[rsync] s-task:%s prepare checkpoint data in:%s to %s successfully, elapsed time:%" PRId64 "ms", id, path,
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
             "rsync://%s/checkpoint/%s/data/",
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
             "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 --bwlimit=100000 %s "
             "rsync://%s/checkpoint/%s/data/",
             tsLogDir,
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             ,
             tsSnodeAddress, id);
  }

  code = execCommand(command);
  if (code != 0) {
    uError("[rsync] s-task:%s upload checkpoint data in %s to %s failed, code:%d," ERRNO_ERR_FORMAT, id, path,
           tsSnodeAddress, code, ERRNO_ERR_DATA);
  } else {
    int64_t el = (taosGetTimestampMs() - st);
    uDebug("[rsync] s-task:%s upload checkpoint data in:%s to %s successfully, elapsed time:%" PRId64 "ms", id, path,
           tsSnodeAddress, el);
  }

  return code;
}

// abort from retry if quit
int32_t downloadRsync(const char* id, const char* path) {
  int64_t st = taosGetTimestampMs();
  int32_t MAX_RETRY = 10;
  int32_t times = 0;
  int32_t code = 0;

#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);
#endif

  char command[PATH_MAX] = {0};
  snprintf(command, PATH_MAX,
           "rsync -av --debug=all --log-file=%s/rsynclog --timeout=10 --bwlimit=100000 rsync://%s/checkpoint/%s/data/ %s",
           tsLogDir, tsSnodeAddress, id,
#ifdef WINDOWS
           pathTransform
#else
           path
#endif
  );

  uDebug("[rsync] %s start to sync data from remote to:%s, %s", id, path, command);

  while(times++ < MAX_RETRY) {
    code = execCommand(command);
    if (code != TSDB_CODE_SUCCESS) {
      uError("[rsync] %s download checkpoint data:%s failed, retry after 1sec, times:%d, code:%d," ERRNO_ERR_FORMAT, id,
             path, times, code, ERRNO_ERR_DATA);
      taosSsleep(1);
    } else {
      int32_t el = taosGetTimestampMs() - st;
      uDebug("[rsync] %s download checkpoint data:%s successfully, elapsed time:%dms", id, path, el);
      break;
    }
  }

  return code;
}

int32_t deleteRsync(const char* id) {
  char*   tmp = "./tmp_empty/";
  int32_t code = taosMkDir(tmp);
  if (code != 0) {
    uError("[rsync] make tmp dir failed. code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }

  char command[PATH_MAX] = {0};
  snprintf(command, PATH_MAX,
           "rsync -av --debug=all --log-file=%s/rsynclog --delete --timeout=10 %s rsync://%s/checkpoint/%s/data/", tsLogDir,
           tmp, tsSnodeAddress, id);

  code = execCommand(command);
  taosRemoveDir(tmp);
  if (code != 0) {
    uError("[rsync] get failed code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }

  uDebug("[rsync] delete data:%s successful", id);
  return 0;
}