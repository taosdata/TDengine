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

#define _DEFAULT_SOURCE
#include "tanal.h"
#include <curl/curl.h>
#include "tmsg.h"
#include "ttypes.h"
#include "tutil.h"

#define ANAL_FUNC_FUNC_PREFIX "func="
#define ANAL_FUNC_SPLIT       ","

typedef struct {
  int64_t       ver;
  SHashObj     *hash;  // funcname -> SAnalFuncUrl
  TdThreadMutex lock;
} SAFuncMgmt;

typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

static SAFuncMgmt tsFuncs = {0};
static int32_t    taosCurlTestStr(const char *url, SCurlResp *pRsp);

const char *taosAnalFuncStr(EAnalFuncType type) {
  switch (type) {
    case ANAL_FUNC_TYPE_ANOMALY_WINDOW:
      return "anomaly_window";
    case ANAL_FUNC_TYPE_ANOMALY_DETECT:
      return "anomaly_detect";
    case ANAL_FUNC_TYPE_FORECAST:
      return "forecast";
    case ANAL_FUNC_TYPE_HISTORIC:
      return "historic";
    default:
      return "unknown";
  }
}

EAnalFuncType taosAnalFuncInt(const char *name) {
  for (EAnalFuncType i = ANAL_FUNC_TYPE_START; i < ANAL_FUNC_TYPE_END; ++i) {
    if (strcasecmp(name, taosAnalFuncStr(i)) == 0) {
      return i;
    }
  }

  return ANAL_FUNC_TYPE_START;
}

int32_t taosFuncInit() {
  if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
    uError("failed to init curl env");
    return -1;
  }

  tsFuncs.ver = 0;
  taosThreadMutexInit(&tsFuncs.lock, NULL);
  tsFuncs.hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (tsFuncs.hash == NULL) {
    uError("failed to init tfunc hash");
    return -1;
  }

  uInfo("analysis func env is initialized");
  return 0;
}

void taosFuncFreeHash(SHashObj *hash) {
  void *pIter = taosHashIterate(hash, NULL);
  while (pIter != NULL) {
    SAnalFuncUrl *pUrl = (SAnalFuncUrl *)pIter;
    taosMemoryFree(pUrl->url);
    pIter = taosHashIterate(hash, pIter);
  }
  taosHashCleanup(hash);
}

void taosFuncCleanup() {
  curl_global_cleanup();
  taosThreadMutexDestroy(&tsFuncs.lock);
  taosFuncFreeHash(tsFuncs.hash);
  tsFuncs.hash = NULL;
  uInfo("analysis func env is cleaned up");
}

void taosFuncUpdate(int64_t newVer, SHashObj *pHash) {
  if (newVer > tsFuncs.ver) {
    taosThreadMutexLock(&tsFuncs.lock);
    SHashObj *hash = tsFuncs.hash;
    tsFuncs.ver = newVer;
    tsFuncs.hash = pHash;
    taosThreadMutexUnlock(&tsFuncs.lock);
    taosFuncFreeHash(hash);
  } else {
    taosFuncFreeHash(pHash);
  }
}

bool taosFuncGetParaStr(const char *option, const char *paraName, char *paraValue, int32_t paraValueMaxLen) {
  char *pos1 = strstr(option, paraName);
  char *pos2 = strstr(option, ANAL_FUNC_SPLIT);
  if (pos1 != NULL) {
    if (paraValueMaxLen > 0) {
      int32_t copyLen = paraValueMaxLen;
      if (pos2 != NULL) {
        copyLen = (int32_t)(pos2 - pos1 - strlen(paraName) + 1);
        copyLen = MIN(copyLen, paraValueMaxLen);
      }
      tstrncpy(paraValue, pos1 + strlen(paraName), copyLen);
    }
    return true;
  } else {
    return false;
  }
}

bool taosFuncGetParaInt(const char *option, const char *paraName, int32_t *paraValue) {
  char *pos1 = strstr(option, paraName);
  char *pos2 = strstr(option, ANAL_FUNC_SPLIT);
  if (pos1 != NULL) {
    *paraValue = taosStr2Int32(pos1 + strlen(paraName) + 1, NULL, 10);
    return true;
  } else {
    return false;
  }
}

int32_t taosFuncGetUrl(const char *funcName, EAnalFuncType type, char *url, int32_t urlLen) {
  int32_t code = 0;
  char    name[TSDB_ANAL_FUNC_KEY_LEN] = {0};
  int32_t nameLen = snprintf(name, sizeof(name) - 1, "%s:%d", funcName, type);

  taosThreadMutexLock(&tsFuncs.lock);
  SAnalFuncUrl *pUrl = taosHashAcquire(tsFuncs.hash, name, nameLen);
  if (pUrl != NULL) {
    tstrncpy(url, pUrl->url, urlLen);
    uInfo("func:%s, type:%s, url:%s", funcName, taosAnalFuncStr(type), url);
  } else {
    url[0] = 0;
    terrno = TSDB_CODE_ANAL_FUNC_NOT_FOUND;
    code = terrno;
    uInfo("func:%s,type:%s, url not found", funcName, taosAnalFuncStr(type));
  }
  taosThreadMutexUnlock(&tsFuncs.lock);

  return code;
}

int64_t taosFuncGetVersion() { return tsFuncs.ver; }

static size_t taosCurlWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SCurlResp *pRsp = userdata;
  pRsp->dataLen = (int64_t)contLen * (int64_t)nmemb;
  pRsp->data = taosMemoryMalloc(pRsp->dataLen + 1);

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data, pCont, pRsp->dataLen);
    pRsp->data[pRsp->dataLen] = 0;
    uInfo("curl resp is received, len:%" PRId64 ", cont:%s", pRsp->dataLen, pRsp->data);
    return pRsp->dataLen;
  } else {
    pRsp->dataLen = 0;
    uInfo("failed to malloc curl resp");
    return 0;
  }
}

static int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp) {
#if 0
  return taosCurlTestStr(url, pRsp);
#else
  CURL    *curl = NULL;
  CURLcode code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 100);

  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) curl_easy_cleanup(curl);
  return code;
#endif
}

static int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp, const char *buf, int32_t bufLen) {
#if 0
  return taosCurlTestStr(url, pRsp);
#else
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  CURLcode           code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosCurlWriteData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 60000);
  curl_easy_setopt(curl, CURLOPT_POST, 1);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf);

  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    uError("failed to perform curl action, code:%d", code);
  }

_OVER:
  if (curl != NULL) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  return code;
#endif
}

SJson *taosFuncGetJson(const char *url, bool isGet, const char *file) {
  int32_t   code = -1;
  char     *content = NULL;
  int64_t   contentLen;
  SJson    *pJson = NULL;
  SCurlResp curlRsp = {0};
  TdFilePtr pFile = NULL;

  if (isGet) {
    if (taosCurlGetRequest(url, &curlRsp) != 0) {
      code = TSDB_CODE_ANAL_URL_CANT_ACCESS;
      goto _OVER;
    }
  } else {
    pFile = taosOpenFile(file, TD_FILE_READ);
    if (pFile == NULL) {
      code = terrno;
      goto _OVER;
    }

    code = taosFStatFile(pFile, &contentLen, NULL);
    if (code != 0) goto _OVER;

    content = taosMemoryMalloc(contentLen + 1);
    if (content == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    if (taosReadFile(pFile, content, contentLen) != contentLen) {
      code = terrno;
      goto _OVER;
    }

    content[contentLen] = '\0';

    if (taosCurlPostRequest(url, &curlRsp, content, contentLen) != 0) {
      code = TSDB_CODE_ANAL_URL_CANT_ACCESS;
      goto _OVER;
    }
  }

  if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
    code = TSDB_CODE_ANAL_URL_RSP_IS_NULL;
    goto _OVER;
  }

  pJson = tjsonParse(curlRsp.data);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

_OVER:
  if (curlRsp.data != NULL) taosMemoryFreeClear(curlRsp.data);
  if (content != NULL) taosMemoryFree(content);
  if (pFile != NULL) taosCloseFile(&pFile);
  return pJson;
}

static int32_t taosCurlTestStr(const char *url, SCurlResp *pRsp) {
  const char *listStr =
      "{"
      "\"protocol\": 1,"
      "\"version\": 1,"
      "\"functions\": [{"
      "          \"name\": \"arima\","
      "          \"types\": [\"forecast\", \"anomaly_window\"]"
      "      },"
      "      {"
      "          \"name\": \"ai\","
      "          \"types\": [\"forecast\", \"historic\"]"
      "      }"
      "  ]"
      "}";

  const char *statusStr =
      "{"
      "  \"protocol\": 1,"
      "  \"status\": \"ready\""
      "}";

  const char *anomalyWindowStr =
      "{\n"
      "    \"rows\": 3,\n"
      "    \"data\": [\n"
      "        [1577808000000, 1578153600000],\n"
      "        [1578153600000, 1578240000000],\n"
      "        [1578240000000, 1578499200000]\n"
      "    ]\n"
      "}";

  if (strstr(url, "list") != NULL) {
    pRsp->dataLen = strlen(listStr);
    pRsp->data = taosMemoryCalloc(1, pRsp->dataLen + 1);
    strcpy(pRsp->data, listStr);
  } else if (strstr(url, "status") != NULL) {
    pRsp->dataLen = strlen(statusStr);
    pRsp->data = taosMemoryCalloc(1, pRsp->dataLen + 1);
    strcpy(pRsp->data, statusStr);
  } else if (strstr(url, "anomaly_window") != NULL) {
    pRsp->dataLen = strlen(anomalyWindowStr);
    pRsp->data = taosMemoryCalloc(1, pRsp->dataLen + 1);
    strcpy(pRsp->data, anomalyWindowStr);
  } else {
  }

  return 0;
}

int32_t taosFuncOpenJson(SAnalFuncJson *pFile) {
  int32_t code = 0;
  pFile->filePtr =
      taosOpenFile(pFile->fileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile->filePtr == NULL) TAOS_CHECK_GOTO(terrno, NULL, _OVER);

_OVER:
  return code;
}

int32_t taosFuncWritePara(SAnalFuncJson *pFile, const char *paras, const char *fmt, const char *prec) {
  const char *js =
      "{\n"
      "\"input\": {\n"
      "  \"parameters\": \"%s\",\n"
      "    \"timestamp\": {\n"
      "    \"format\": \"%s\",\n"
      "    \"precision\": \"%s\"\n"
      "  },\n"
      "  \"type\": \"forecast\",\n"
      "  \"weather\": {},\n"
      "  \"holiday\": {},\n"
      "  \"season\": {}\n"
      "},\n"
      "\"output\": {\n"
      "  \"columns\": [\"start\", \"end\"]\n"
      "},\n";
  char    buf[256] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), js, paras, fmt, prec);

  if (taosWriteFile(pFile->filePtr, buf, bufLen) <= 0) {
    return terrno;
  }
  return 0;
}

int32_t taosFuncWriteMeta(SAnalFuncJson *pFile, int32_t c1, int32_t c2) {
  const char *js =
      "\"column_meta\": [\n"
      "  [\"ts\", \"%s\", %d],\n"
      "  [\"val\", \"%s\", %d]\n"
      "],\n"
      "\"data\": [\n";
  char    buf[256] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), js, tDataTypes[c1].name, tDataTypes[c1].bytes, tDataTypes[c2].name,
                            tDataTypes[c2].bytes);

  if (taosWriteFile(pFile->filePtr, buf, bufLen) <= 0) {
    return terrno;
  }
  return 0;
}

int32_t taosFuncWriteData(SAnalFuncJson *pFile, const char *data, bool isLast) {
  const char *js = "[%s]%s\n";
  char        buf[86] = {0};
  int32_t     bufLen = snprintf(buf, sizeof(buf), js, data, isLast ? "" : ",");

  if (taosWriteFile(pFile->filePtr, buf, bufLen) <= 0) {
    return terrno;
  }
  return 0;
}

int32_t taosFuncWriteRows(SAnalFuncJson *pFile, int32_t numOfRows) {
  const char *js =
      "],\n"
      "\"rows\": %d\n"
      "}";
  char    buf[256] = {0};
  int32_t bufLen = snprintf(buf, sizeof(buf), js, numOfRows);

  if (taosWriteFile(pFile->filePtr, buf, bufLen) <= 0) {
    return terrno;
  }
  if (taosFsyncFile(pFile->filePtr) < 0) {
    return terrno;
  }
  return 0;
}

void taosFuncCloseJson(SAnalFuncJson *pFile) { (void)taosCloseFile(&pFile->filePtr); }
