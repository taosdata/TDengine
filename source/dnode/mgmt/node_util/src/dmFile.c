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
#include "crypt.h"
#include "dmUtil.h"
#include "tchecksum.h"
#include "tgrant.h"
#include "tjson.h"

#define MAXLEN               1024
#define DM_KEY_INDICATOR     "this indicator!"
#define DM_ENCRYPT_CODE_FILE "encryptCode.cfg"
#define DM_CHECK_CODE_FILE   "checkCode.bin"

static int32_t dmDecodeFile(SJson *pJson, bool *deployed) {
  int32_t code = 0;
  int32_t value = 0;

  tjsonGetInt32ValueFromDouble(pJson, "deployed", value, code);
  if (code < 0) return code;

  *deployed = (value != 0);
  return code;
}

int32_t dmReadFile(const char *path, const char *name, bool *pDeployed) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  int32_t   nBytes = tsnprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("file:%s not exist", file);
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    code = terrno;
    dError("failed to read file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (dmDecodeFile(pJson, pDeployed) < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read mnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read dnode file:%s since %s", file, tstrerror(code));
  }
  return code;
}

static int32_t dmEncodeFile(SJson *pJson, bool deployed) {
  if (tjsonAddDoubleToObject(pJson, "deployed", deployed) < 0) return TSDB_CODE_INVALID_JSON_FORMAT;
  return 0;
}

int32_t dmWriteFile(const char *path, const char *name, bool deployed) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};

  int32_t nBytes = tsnprintf(file, sizeof(file), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  nBytes = tsnprintf(realfile, sizeof(realfile), "%s%s%s.json", path, TD_DIRSEP, name);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  if ((code = dmEncodeFile(pJson, deployed)) != 0) goto _OVER;

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno;
    goto _OVER;
  }

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) {
    code = terrno;
    goto _OVER;
  }
  if (taosFsyncFile(pFile) < 0) {
    code = terrno;
    goto _OVER;
  }

  if (taosCloseFile(&pFile) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _OVER;
  }
  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), NULL, _OVER);

  dInfo("succeed to write file:%s, deloyed:%d", realfile, deployed);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to write file:%s since %s, deloyed:%d", realfile, tstrerror(code), deployed);
  }
  return code;
}

int32_t dmCheckRunning(const char *dataDir, TdFilePtr *pFile) {
  int32_t code = 0;
  char    filepath[PATH_MAX] = {0};
  snprintf(filepath, sizeof(filepath), "%s%s.running", dataDir, TD_DIRSEP);

  *pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CLOEXEC);
  if (*pFile == NULL) {
    code = terrno;
    dError("failed to open file:%s since %s", filepath, tstrerror(code));
    return code;
  }

  int32_t retryTimes = 0;
  int32_t ret = 0;
  do {
    ret = taosLockFile(*pFile);
    if (ret == 0) break;

    code = terrno;
    taosMsleep(1000);
    retryTimes++;
    dError("failed to lock file:%s since %s, retryTimes:%d", filepath, tstrerror(code), retryTimes);
  } while (retryTimes < 12);

  if (ret < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    (void)taosCloseFile(pFile);
    *pFile = NULL;
    return code;
  }

  dDebug("lock file:%s to prevent repeated starts", filepath);
  return code;
}

extern int32_t generateEncryptCode(const char *key, const char *machineId, char **encryptCode);

static int32_t dmWriteCheckCodeFile(char *file, char *realfile, char *key, bool toLogFile) {
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  int32_t len = ENCRYPTED_LEN(sizeof(DM_KEY_INDICATOR));
  result = taosMemoryMalloc(len);
  if (result == NULL) {
    return terrno;
  }

  SCryptOpts opts;
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = DM_KEY_INDICATOR;
  opts.result = result;
  opts.unitLen = 16;
  (void)CBC_Encrypt(&opts);

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosWriteFile(pFile, opts.result, len) <= 0) {
    code = terrno;
    goto _OVER;
  }

  if (taosFsyncFile(pFile) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _OVER;
  }

  if (taosCloseFile(&pFile) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _OVER;
  }

  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), NULL, _OVER);

  encryptDebug("succeed to write checkCode file:%s", realfile);

  code = 0;
_OVER:
  if (pFile != NULL) taosCloseFile(&pFile);
  if (result != NULL) taosMemoryFree(result);

  return code;
}

static int32_t dmWriteEncryptCodeFile(char *file, char *realfile, char *encryptCode, bool toLogFile) {
  TdFilePtr pFile = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    code = terrno;
    goto _OVER;
  }

  int32_t len = strlen(encryptCode);
  if (taosWriteFile(pFile, encryptCode, len) <= 0) {
    code = terrno;
    goto _OVER;
  }
  if (taosFsyncFile(pFile) < 0) {
    code = terrno;
    goto _OVER;
  }

  if (taosCloseFile(&pFile) != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _OVER;
  }

  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), NULL, _OVER);

  encryptDebug("succeed to write encryptCode file:%s", realfile);

  code = 0;
_OVER:
  if (pFile != NULL) taosCloseFile(&pFile);

  return code;
}

static int32_t dmCompareEncryptKey(char *file, char *key, bool toLogFile) {
  char     *content = NULL;
  int64_t   size = 0;
  TdFilePtr pFile = NULL;
  char     *result = NULL;
  int32_t   code = -1;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    encryptError("failed to open dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    encryptError("failed to fstat dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content = taosMemoryMalloc(size);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    code = terrno;
    encryptError("failed to read dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  encryptDebug("succeed to read checkCode file:%s", file);

  int len = ENCRYPTED_LEN(size);
  result = taosMemoryMalloc(len);
  if (result == NULL) {
    code = terrno;
    encryptError("failed to alloc memory file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  SCryptOpts opts = {0};
  strncpy(opts.key, key, ENCRYPT_KEY_LEN);
  opts.len = len;
  opts.source = content;
  opts.result = result;
  opts.unitLen = 16;
  (void)CBC_Decrypt(&opts);

  if (strcmp(opts.result, DM_KEY_INDICATOR) != 0) {
    code = TSDB_CODE_DNODE_ENCRYPTKEY_CHANGED;
    encryptError("failed to compare decrypted result");
    goto _OVER;
  }

  encryptDebug("succeed to compare checkCode file:%s", file);
  code = 0;
_OVER:
  if (result != NULL) taosMemoryFree(result);
  if (content != NULL) taosMemoryFree(content);
  if (pFile != NULL) taosCloseFile(&pFile);

  return code;
}

int32_t dmUpdateEncryptKey(char *key, bool toLogFile) {
#ifdef TD_ENTERPRISE
  int32_t code = -1;
  int32_t lino = 0;
  char   *machineId = NULL;
  char   *encryptCode = NULL;

  char folder[PATH_MAX] = {0};

  char encryptFile[PATH_MAX] = {0};
  char realEncryptFile[PATH_MAX] = {0};

  char checkFile[PATH_MAX] = {0};
  char realCheckFile[PATH_MAX] = {0};

  int32_t nBytes = tsnprintf(folder, sizeof(folder), "%s%sdnode", tsDataDir, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = tsnprintf(encryptFile, sizeof(realEncryptFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = tsnprintf(realEncryptFile, sizeof(realEncryptFile), "%s%s%s", folder, TD_DIRSEP, DM_ENCRYPT_CODE_FILE);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  nBytes = tsnprintf(checkFile, sizeof(checkFile), "%s%s%s.bak", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  snprintf(realCheckFile, sizeof(realCheckFile), "%s%s%s", folder, TD_DIRSEP, DM_CHECK_CODE_FILE);
  if (nBytes <= 0 || nBytes >= PATH_MAX) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  if (taosMkDir(folder) != 0) {
    code = terrno;
    encryptError("failed to create dir:%s since %s", folder, tstrerror(code));
    goto _OVER;
  }

  if (taosCheckExistFile(realCheckFile)) {
    if ((code = dmCompareEncryptKey(realCheckFile, key, toLogFile)) != 0) {
      goto _OVER;
    }
  }

  TAOS_CHECK_GOTO(tGetMachineId(&machineId), &lino, _OVER);

  TAOS_CHECK_GOTO(generateEncryptCode(key, machineId, &encryptCode), &lino, _OVER);

  if ((code = dmWriteEncryptCodeFile(encryptFile, realEncryptFile, encryptCode, toLogFile)) != 0) {
    goto _OVER;
  }

  if ((code = dmWriteCheckCodeFile(checkFile, realCheckFile, key, toLogFile)) != 0) {
    goto _OVER;
  }

  encryptInfo("Succeed to update encrypt key\n");

  code = 0;
_OVER:
  taosMemoryFree(encryptCode);
  taosMemoryFree(machineId);
  if (code != 0) {
    encryptError("failed to update encrypt key at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
#else
  return 0;
#endif
}

extern int32_t checkAndGetCryptKey(const char *encryptCode, const char *machineId, char **key);

static int32_t dmReadEncryptCodeFile(char *file, char **output) {
  TdFilePtr pFile = NULL;
  int32_t   code = -1;
  char     *content = NULL;

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    code = terrno;
    dError("failed to read dnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  content[size] = '\0';

  *output = content;
  content = NULL;

  dInfo("succeed to read encryptCode file:%s", file);
  code = 0;
_OVER:
  if (pFile != NULL) taosCloseFile(&pFile);
  taosMemoryFree(content);

  return code;
}

int32_t dmGetEncryptKey() {
#ifdef TD_ENTERPRISE
  int32_t code = -1;
  char    encryptFile[PATH_MAX] = {0};
  char    checkFile[PATH_MAX] = {0};
  char   *machineId = NULL;
  char   *encryptKey = NULL;
  char   *content = NULL;

  int32_t nBytes = tsnprintf(encryptFile, sizeof(encryptFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP,
                            DM_ENCRYPT_CODE_FILE);
  if (nBytes <= 0 || nBytes >= sizeof(encryptFile)) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    return code;
  }

  nBytes = tsnprintf(checkFile, sizeof(checkFile), "%s%sdnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, DM_CHECK_CODE_FILE);
  if (nBytes <= 0 || nBytes >= sizeof(checkFile)) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    return code;
  }

  if (!taosCheckExistFile(encryptFile)) {
    code = TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
    dInfo("no exist, checkCode file:%s", encryptFile);
    return 0;
  }

  if ((code = dmReadEncryptCodeFile(encryptFile, &content)) != 0) {
    goto _OVER;
  }

  if ((code = tGetMachineId(&machineId)) != 0) {
    goto _OVER;
  }

  if ((code = checkAndGetCryptKey(content, machineId, &encryptKey)) != 0) {
    goto _OVER;
  }

  taosMemoryFreeClear(machineId);
  taosMemoryFreeClear(content);

  if (encryptKey[0] == '\0') {
    code = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    dError("failed to read key since %s", tstrerror(code));
    goto _OVER;
  }

  if ((code = dmCompareEncryptKey(checkFile, encryptKey, true)) != 0) {
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
    dError("failed to get encrypt key since %s", tstrerror(code));
  }
  TAOS_RETURN(code);
#else
  return 0;
#endif
}
