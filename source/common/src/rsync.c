//
// Created by mingming wanng on 2023/11/2.
//
#include "rsync.h"
#include <stdlib.h>
#include "tglobal.h"

#define ERRNO_ERR_FORMAT  "errno:%d,msg:%s"
#define ERRNO_ERR_DATA    errno,strerror(errno)

// deleteRsync function produce empty directories, traverse base directory to remove them
static void removeEmptyDir(){
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

    TdDirPtr pDirTmp = taosOpenDir(filename);
    TdDirEntryPtr deTmp = NULL;
    bool empty = true;
    while ((deTmp = taosReadDir(pDirTmp)) != NULL){
      if (strcmp(taosGetDirEntryName(deTmp), ".") == 0 || strcmp(taosGetDirEntryName(deTmp), "..") == 0) continue;
      empty = false;
    }
    if(empty) taosRemoveDir(filename);
    taosCloseDir(&pDirTmp);
  }

  taosCloseDir(&pDir);
}

#ifdef WINDOWS
// C:\TDengine\data\backup\checkpoint\ -> /c/TDengine/data/backup/checkpoint/
static void changeDirFromWindowsToLinux(char* from, char* to){
  to[0] = '/';
  to[1] = from[0];
  for(int i = 2; i < strlen(from); i++) {
    if (from[i] == '\\') {
      to[i] = '/';
    } else {
      to[i] = from[i];
    }
  }
}
#endif

static int generateConfigFile(char* confDir){
  TdFilePtr pFile = taosOpenFile(confDir, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    uError("[rsync] open conf file error, dir:%s,"ERRNO_ERR_FORMAT, confDir, ERRNO_ERR_DATA);
    return -1;
  }

#ifdef WINDOWS
  char path[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(tsCheckpointBackupDir, path);
#endif

  char confContent[PATH_MAX*4] = {0};
  snprintf(confContent, PATH_MAX*4,
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
           "path = %s", tsCheckpointBackupDir, tsCheckpointBackupDir,
#ifdef WINDOWS
           path
#else
           tsCheckpointBackupDir
#endif
           );
  uDebug("[rsync] conf:%s", confContent);
  if (taosWriteFile(pFile, confContent, strlen(confContent)) <= 0){
    uError("[rsync] write conf file error,"ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
    taosCloseFile(&pFile);
    return -1;
  }

  taosCloseFile(&pFile);
  return 0;
}

static int execCommand(char* command){
  int try = 3;
  int32_t code = 0;
  while(try-- > 0) {
    code = system(command);
    if (code == 0) {
      break;
    }
    taosMsleep(10);
  }
  return code;
}

void stopRsync(){
  int code =
#ifdef WINDOWS
  system("taskkill /f /im rsync.exe");
#else
  system("pkill rsync");
#endif
  if(code != 0){
    uError("[rsync] stop rsync server failed,"ERRNO_ERR_FORMAT, ERRNO_ERR_DATA);
    return;
  }
  uDebug("[rsync] stop rsync server successful");
}

void startRsync(){
  if(taosMulMkDir(tsCheckpointBackupDir) != 0){
    uError("[rsync] build checkpoint backup dir failed, dir:%s,"ERRNO_ERR_FORMAT, tsCheckpointBackupDir, ERRNO_ERR_DATA);
    return;
  }
  removeEmptyDir();

  char confDir[PATH_MAX] = {0};
  snprintf(confDir, PATH_MAX, "%srsync.conf", tsCheckpointBackupDir);

  int code = generateConfigFile(confDir);
  if(code != 0){
    return;
  }

  char cmd[PATH_MAX] = {0};
  snprintf(cmd, PATH_MAX, "rsync --daemon --port=%d --config=%s", tsRsyncPort, confDir);
  // start rsync service to backup checkpoint
  code = system(cmd);
  if(code != 0){
    uError("[rsync] start server failed, code:%d,"ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return;
  }
  uDebug("[rsync] start server successful");
}

int uploadRsync(char* id, char* path){
#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);
#endif
  char command[PATH_MAX] = {0};
#ifdef WINDOWS
  if(pathTransform[strlen(pathTransform) - 1] != '/'){
#else
  if(path[strlen(path) - 1] != '/'){
#endif
    snprintf(command, PATH_MAX, "rsync -av --delete --timeout=10 --bwlimit=100000 %s/ rsync://%s/checkpoint/%s/",
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             , tsSnodeAddress, id);
  }else{
    snprintf(command, PATH_MAX, "rsync -av --delete --timeout=10 --bwlimit=100000 %s rsync://%s/checkpoint/%s/",
#ifdef WINDOWS
             pathTransform
#else
             path
#endif
             , tsSnodeAddress, id);
  }

  int code = execCommand(command);
  if(code != 0){
    uError("[rsync] send failed code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }
  uDebug("[rsync] upload data:%s successful", id);
  return 0;
}

int downloadRsync(char* id, char* path){
#ifdef WINDOWS
  char pathTransform[PATH_MAX] = {0};
  changeDirFromWindowsToLinux(path, pathTransform);
#endif
  char command[PATH_MAX] = {0};
  snprintf(command, PATH_MAX, "rsync -av --timeout=10 --bwlimit=100000 rsync://%s/checkpoint/%s/ %s",
           tsSnodeAddress, id,
#ifdef WINDOWS
           pathTransform
#else
           path
#endif
           );

  int code = execCommand(command);
  if(code != 0){
    uError("[rsync] get failed code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }
  uDebug("[rsync] down data:%s successful", id);
  return 0;
}

int deleteRsync(char* id){
  char* tmp = "./tmp_empty/";
  int code = taosMkDir(tmp);
  if(code != 0){
    uError("[rsync] make tmp dir failed. code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }
  char command[PATH_MAX] = {0};
  snprintf(command, PATH_MAX, "rsync -av --delete --timeout=10 %s rsync://%s/checkpoint/%s/",
           tmp, tsSnodeAddress, id);

  code = execCommand(command);
  taosRemoveDir(tmp);
  if(code != 0){
    uError("[rsync] get failed code:%d," ERRNO_ERR_FORMAT, code, ERRNO_ERR_DATA);
    return -1;
  }
  uDebug("[rsync] delete data:%s successful", id);

  return 0;
}