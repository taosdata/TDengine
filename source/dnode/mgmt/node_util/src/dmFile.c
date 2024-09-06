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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmUtil.h"
#include "tjson.h"
#include "tgrant.h"
#include "crypt.h"
#include "tchecksum.h"

#define MAXLEN 1024
#define DM_KEY_INDICATOR      "this indicator!"
#define DM_ENCRYPT_CODE_FILE  "encryptCode.cfg"
#define DM_CHECK_CODE_FILE    "checkCode.bin"

static int32_t dmDecodeFile(SJson *pJson, bool *deployed) {
  int32_t code = 0;
  int32_t value = 0;

  tjsonGetInt32ValueFromDouble(pJson, "deployed", value, code);
  if (code < 0) return -1;

  *deployed = (value != 0);
  return code;
}

int32_t dmReadFile(const char *path, const char *name, bool *pDeployed) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("file:%s not exist", file);
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (dmDecodeFile(pJson, pDeployed) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read mnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read dnode file:%s since %s", file, terrstr());
  }
  return code;
}

static int32_t dmEncodeFile(SJson *pJson, bool deployed) {
  if (tjsonAddDoubleToObject(pJson, "deployed", deployed) < 0) return -1;
  return 0;
}

int32_t dmWriteFile(const char *path, const char *name, bool deployed) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  snprintf(realfile, sizeof(realfile), "%s%s%s.json", path, TD_DIRSEP, name);

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (dmEncodeFile(pJson, deployed) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  dInfo("succeed to write file:%s, deloyed:%d", realfile, deployed);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write file:%s since %s, deloyed:%d", realfile, terrstr(), deployed);
  }
  return code;
}

TdFilePtr dmCheckRunning(const char *dataDir) {
  char filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.running", dataDir, TD_DIRSEP);

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open file:%s since %s", filepath, terrstr());
    return NULL;
  }

  int32_t retryTimes = 0;
  int32_t ret = 0;
  do {
    ret = taosLockFile(pFile);
    if (ret == 0) break;
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosMsleep(1000);
    retryTimes++;
    dError("failed to lock file:%s since %s, retryTimes:%d", filepath, terrstr(), retryTimes);
  } while (retryTimes < 12);

  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFile);
    return NULL;
  }

  terrno = 0;
  dDebug("lock file:%s to prevent repeated starts", filepath);
  return pFile;
}

extern int32_t generateEncryptCode(const char *key, const char *machineId, char **encryptCode);

static int32_t dmWriteCheckCodeFile(char* file, char* realfile, char* key, bool toLogFile){
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  int32_t len = ENCRYPTED_LEN(sizeof(DM_KEY_INDICATOR));
  result = taosMemoryMalloc(len);

  SCryptOpts opts;
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = DM_KEY_INDICATOR;
  opts.result = result;
  opts.unitLen = 16;
  CBC_Encrypt(&opts);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  if (taosWriteFile(pFile, opts.result, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  encryptDebug("succeed to write checkCode file:%s", realfile);

  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);
  if(result != NULL) taosMemoryFree(result);

  return code;
}

static int32_t dmWriteEncryptCodeFile(char* file, char* realfile, char* encryptCode, bool toLogFile){
  TdFilePtr pFile = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(encryptCode);
  if (taosWriteFile(pFile, encryptCode, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  encryptDebug("succeed to write encryptCode file:%s", realfile);

  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t dmCompareEncryptKey(char* file, char* key, bool toLogFile){
  char     *content = NULL;
  int64_t   size = 0;
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    encryptError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    encryptError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    encryptError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  encryptDebug("succeed to read checkCode file:%s", file);
  
  int len = ENCRYPTED_LEN(size);
  result = taosMemoryMalloc(len);

  SCryptOpts opts = {0};
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = content;
  opts.result = result;
  opts.unitLen = 16;
  CBC_Decrypt(&opts);

  if(strcmp(opts.result, DM_KEY_INDICATOR) != 0) {
    terrno = TSDB_CODE_DNODE_ENCRYPTKEY_CHANGED;
    encryptError("failed to compare decrypted result");
    goto _OVER;
  }

  encryptDebug("succeed to compare checkCode file:%s", file);
  code = 0;
_OVER:
  if(result != NULL) taosMemoryFree(result);
  if(content != NULL) taosMemoryFree(content);
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

int32_t dmUpdateEncryptKey(char *key, bool toLogFile) {
#ifdef TD_ENTERPRISE
  int32_t code = -1;
  char   *machineId = NULL;
  char   *encryptCode = NULL;

  char folder[PATH_MAX] = {0};

  char encryptFile[PATH_MAX] = {0};
  char realEncryptFile[PATH_MAX] = {0};

  char checkFile[PATH_MAX] = {0};
  char realCheckFile[PATH_MAX] = {0};

  snprintf(folder, sizeof(folder), "%s%sdnode", tsDataDir, TD_DIRSEP);
  snprintf(encryptFile, sizeof(realEncryptFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(realEncryptFile, sizeof(realEncryptFile), "%s%s%s", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(checkFile, sizeof(checkFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);
  snprintf(realCheckFile, sizeof(realCheckFile), "%s%s%s", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);

  terrno = 0;

  if (taosMkDir(folder) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    encryptError("failed to create dir:%s since %s", folder, terrstr());
    goto _OVER;
  }

  if(taosCheckExistFile(realCheckFile)){
    if(dmCompareEncryptKey(realCheckFile, key, toLogFile) != 0){
      goto _OVER;
    }
  }
  
  if (!(machineId = tGetMachineId())) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (generateEncryptCode(key, machineId, &encryptCode) != 0) {
    goto _OVER;
  }

  if(dmWriteEncryptCodeFile(encryptFile, realEncryptFile, encryptCode, toLogFile) != 0){
    goto _OVER;
  }

  if(dmWriteCheckCodeFile(checkFile, realCheckFile, key, toLogFile) != 0){
    goto _OVER;
  }

  encryptInfo("Succeed to update encrypt key\n");

  code = 0;
_OVER:
  taosMemoryFree(encryptCode);
  taosMemoryFree(machineId);
  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    encryptError("failed to update encrypt key since %s", terrstr());
  }
  return code;
#else
  return 0;
#endif
}

extern int32_t checkAndGetCryptKey(const char *encryptCode, const char *machineId, char **key);

static int32_t dmReadEncryptCodeFile(char* file, char** output){
  TdFilePtr pFile = NULL;
  int32_t   code = -1;
  char     *content = NULL;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  *output = content;

  dInfo("succeed to read encryptCode file:%s", file);
  code = 0;
_OVER:
  if(pFile != NULL) taosCloseFile(&pFile);

  return code;
}

int32_t dmGetEncryptKey(){
#ifdef TD_ENTERPRISE
  int32_t   code = -1;
  char      encryptFile[PATH_MAX] = {0};
  char      checkFile[PATH_MAX] = {0};
  char     *machineId = NULL;
  char     *encryptKey = NULL;
  char     *content = NULL;

  snprintf(encryptFile, sizeof(encryptFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  snprintf(checkFile, sizeof(checkFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, DM_CHECK_CODE_FILE);

  if(!taosCheckExistFile(encryptFile)){
    dInfo("no exist, checkCode file:%s", encryptFile);
    return 0;
  }

  if(dmReadEncryptCodeFile(encryptFile, &content) != 0){
    goto _OVER;
  }

  if (!(machineId = tGetMachineId())) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if(checkAndGetCryptKey(content, machineId, &encryptKey) != 0){
    goto _OVER;
  }

  taosMemoryFreeClear(machineId);
  taosMemoryFreeClear(content);

  if(encryptKey[0] == '\0'){
    terrno = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    dError("failed to read key since %s", terrstr());
    goto _OVER;
  }

  if(dmCompareEncryptKey(checkFile, encryptKey, true) != 0){
    goto _OVER;
  }

  strncpy(tsEncryptKey, encryptKey, ENCRYPT_KEY_LEN);
  taosMemoryFreeClear(encryptKey);
  tsEncryptionKeyChksum = taosCalcChecksum(0, tsEncryptKey, strlen(tsEncryptKey));
  tsEncryptionKeyStat = ENCRYPT_KEY_STAT_LOADED;

  code = 0;
_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (encryptKey != NULL) taosMemoryFree(encryptKey);
  if (machineId != NULL) taosMemoryFree(machineId);
  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to get encrypt key since %s", terrstr());
  }
  return code;
#else
  return 0;
#endif
}