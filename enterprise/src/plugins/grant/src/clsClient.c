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
#include "auth.h"
#include "cls.h"
#include "cJSON.h"
#include "grant.h"
#include "machine.h"
#include "mndCluster.h"
#include "mndDef.h"
#include "mndDnode.h"
#include "mndGrant.h"
#include "sdb.h"
#include "tbase64.h"
#include "tchecksum.h"
#include "tdes.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlog.h"
#include "trpc.h"
#include "ttime.h"
#include "tutil.h"
#include "mndTrans.h"
#ifndef WINDOWS
#include <openssl/evp.h>
#endif
#if defined(WINDOWS)
#define CURL_STATICLIB
#endif
#include "curl/curl.h"

extern SGrantStatus  gStatus;
extern SGrantUniqObj grantObj;
extern const char *gGrantState[GRANT_STATE_MAX];

static int32_t mndClsCollectClusterInfo(SMnode *pMnode, SClsReqData *pClsReqData);
static int32_t clsAddDynamicGrantItem(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int64_t number);
static int32_t clsAddDynamicGrantItem2(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int32_t number, int32_t speed);

#define GRACE_PERIOD_DAYS 15
#define GRANT_TS_SEC_LEN 20
#define GRANT_EXPIRE (gStatus.basicExpireSec)
#define TSDB_CLS_RESP_RESERVE_SIZE 64
#define TSDB_CLS_RESP_VER_NUMBER   1
#define TSDB_CLS_RESP_MAX_LEN      48*1024
#define INNER_TSDB_CODE_CLS_SIGNATURE_SAME TSDB_CODE_MND_XNODE_NAME_DUPLICATE

typedef struct SClsHBTask {
  SMnode *pMnode;
} SClsHBTask;

static TdThread      gClsHBThread;
static bool          gClsHBThreadInit = false;
static bool          gClsHBThreadStop = false;
static TdThreadCond  gClsHBCond;
static TdThreadMutex gClsHBMutex;
static bool          gClsHBPending = false;
static bool          isClsEnabledClosing = false;

static const char* CLUSTER_CATEGORY = "tsdb";
static const char* DEFAULT_SLOT_ID = "tsdb-1";
static const char* CLS_GRANTS_VERIFY_PUBLIC_KEY = "vRTTWNBW6Y1V528apbqDiFibPJSggeIOSaks3aabGJM=";
static char pre_signature[128] = {0};

static bool clsFormatLocalRfc3339(char *buf, int32_t bufLen) {
  time_t    nowSec = taosGetTimestampSec();
  struct tm tmInfo = {0};
  char      timePart[32] = {0};
  char      offset[8] = {0};

  if (taosLocalTime(&nowSec, &tmInfo, NULL, 0, NULL) == NULL) {
    uWarn("failed to get local time for cls heartbeat info, errno:%d", ERRNO);
    return false;
  }

  if (taosStrfTime(timePart, sizeof(timePart), "%Y-%m-%dT%H:%M:%S", &tmInfo) == 0) {
    uWarn("failed to format local time for cls heartbeat info");
    return false;
  }

  if (taosStrfTime(offset, sizeof(offset), "%z", &tmInfo) == 0) {
    uWarn("failed to format local timezone offset for cls heartbeat info");
    return false;
  }

  if (strlen(offset) == 5) {
    (void)snprintf(buf, bufLen, "%s%c%c%c:%c%c", timePart, offset[0], offset[1], offset[2], offset[3], offset[4]);
  } else {
    (void)snprintf(buf, bufLen, "%s%s", timePart, offset);
  }

  return true;
}

static void clsSyncRuntimeVar(const char *cfgName, char *runtimeVar, const char *value, const int32_t len) {
  const char *newValue = value == NULL ? "" : value;
  tstrncpy(runtimeVar, newValue, len);

  int32_t code = cfgSetItem(taosGetCfg(), cfgName, runtimeVar, CFG_STYPE_DEFAULT, true);
  if (code != 0) {
    uWarn("failed to sync cls runtime variable:%s, code:0x%x", cfgName, code);
  }
}

static void clsUpdateRuntimeTime(const char *cfgName, char *runtimeVar) {
  char value[TSDB_GRANT_CLS_TIME_LEN] = {0};
  if (!clsFormatLocalRfc3339(value, sizeof(value))) {
    return;
  }

  clsSyncRuntimeVar(cfgName, runtimeVar, value, TSDB_GRANT_CLS_TIME_LEN);
}

static void clsPersistRuntimeVars(void) {
  SConfig *pCfg = taosGetCfg();
  if (pCfg == NULL || tsDataDir[0] == '\0') {
    return;
  }

  char cfgPath[PATH_MAX] = {0};
  (void)snprintf(cfgPath, sizeof(cfgPath), "%s%sdnode", tsDataDir, TD_DIRSEP);

  int32_t code = taosPersistGlobalConfig(taosGetGlobalCfg(pCfg), cfgPath, tsdmConfigVersion);
  if (code != TSDB_CODE_SUCCESS) {
    uWarn("failed to persist cls runtime variables, code:0x%x", code);
  }
}


SSdbRaw *mndClsRespGrantActionEncode(SGrantClsObj *pObj) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == pObj) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int32_t rawDataLen =
      sizeof(SGrantClsObj) + TSDB_CLS_RESP_RESERVE_SIZE + pObj->clsRespLen + pObj->extendLen;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_GRANT_CLS, TSDB_CLS_RESP_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->clsRespLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->clsResp, pObj->clsRespLen, _OVER)
  SDB_SET_BOOL(pRaw, dataPos, pObj->isValid, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pObj->extendLen, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pObj->extend, pObj->extendLen, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_CLS_RESP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("cls clsResp:%d, failed to encode to raw:%p since %s", pObj->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("cls clsResp:%d, encode to raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRaw;
}
SSdbRow *mndClsRespGrantActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow      *pRow = NULL;
  SGrantClsObj *pObj = NULL;

  if (NULL == pRaw) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TSDB_CLS_RESP_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SGrantClsObj));
  if (pRow == NULL) goto _OVER;

  pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &pObj->id, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->clsRespLen, _OVER)
  if (pObj->clsRespLen > 0) {
    pObj->clsResp = taosMemoryCalloc(1, pObj->clsRespLen + 1);
    if (pObj->clsResp == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->clsResp, pObj->clsRespLen, _OVER)
  }
  SDB_GET_BOOL(pRaw, dataPos, &pObj->isValid, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pObj->extendLen, _OVER)
  if (pObj->extendLen > 0) {
    pObj->extend = taosMemoryCalloc(1, pObj->extendLen + 1);
    if (pObj->extend == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, pObj->extend, pObj->extendLen, _OVER)
  }
  SDB_GET_INT64(pRaw, dataPos, &pObj->updateTime, _OVER)

  SDB_GET_RESERVE(pRaw, dataPos, TSDB_CLS_RESP_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("cls clsResp:%d, failed to decode from raw:%p since %s", pObj == NULL ? 0 : pObj->id, pRaw, terrstr());
    if (pObj != NULL) {
      taosMemoryFreeClear(pObj->clsResp);
      taosMemoryFreeClear(pObj->extend);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("cls clsResp:%d, decode from raw:%p, row:%p", pObj->id, pRaw, pObj);
  return pRow;
}

int32_t mndClsRespGrantActionInsert(SSdb *pSdb, SGrantClsObj *pObj) {
  mDebug("cls clsResp:%d, perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

int32_t mndClsRespGrantActionUpdate(SSdb *pSdb, SGrantClsObj *pOld, SGrantClsObj *pNew) {
  mDebug("cls clsResp:%d, perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  if (pNew->updateTime > pOld->updateTime) {
    pOld->updateTime = pNew->updateTime;
  }
  pOld->clsRespLen = pNew->clsRespLen;
  TSWAP(pNew->clsResp, pOld->clsResp);

  pOld->extendLen = pNew->extendLen;
  TSWAP(pNew->extend, pOld->extend);

  pOld->isValid = pNew->isValid;

  taosWUnLockLatch(&pOld->lock);
  return 0;
}

int32_t mndClsRespGrantActionDelete(SSdb *pSdb, SGrantClsObj *pObj) {
  mDebug("cls clsResp:%d, perform delete action, row:%p", pObj->id, pObj);
  taosMemoryFreeClear(pObj->clsResp);
  taosMemoryFreeClear(pObj->extend);
  return 0;
}

static int32_t mndSetCreateClsRespGrantCommitLogs(STrans *pTrans, SGrantClsObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndClsRespGrantActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  TAOS_RETURN(code);
}

void mndReleaseClsRespGrant(SMnode *pMnode, SGrantClsObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

static int32_t mndStoreClsRespGrant(SMnode *pMnode, char* clsResp, int32_t clsRespLen) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SGrantClsObj clsObj = {0};
  clsObj.id = sdbGetMaxId(pMnode->pSdb, SDB_GRANT_CLS);
  clsObj.isValid = true;

  if (clsResp) {
    clsObj.clsRespLen = clsRespLen;
    if (clsObj.clsRespLen > TSDB_CLS_RESP_MAX_LEN) {
      code = TSDB_CODE_INVALID_PARA;
      lino = __LINE__;
      goto _OVER;
    }
    clsObj.clsResp = taosMemoryCalloc(1, clsRespLen);
    if (clsObj.clsResp == NULL) goto _OVER;
    (void)memcpy(clsObj.clsResp, clsResp, clsRespLen);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "create-clsResp");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    uInfo("failed to create transaction for clsResp:%d, code:0x%x:%s", clsObj.id, code, tstrerror(code));
    lino = __LINE__;
    goto _OVER;
  }
  mndTransSetSerial(pTrans);

  uInfo("cls trans:%d, used to create clsResp:%d", pTrans->id, clsObj.id);

  TAOS_CHECK_GOTO(mndSetCreateClsRespGrantCommitLogs(pTrans, &clsObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("cls failed to store clsResp, code:0x%x:%s, line:%d", code, tstrerror(code), lino);
  }
  taosMemoryFreeClear(clsObj.clsResp);
  taosMemoryFreeClear(clsObj.extend);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

SGrantClsObj *mndAcquireFirstClsRespGrant(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SGrantClsObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_GRANT_CLS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    if (pObj != NULL) {
      sdbCancelFetch(pSdb, pIter);
      return pObj;
    }

    sdbRelease(pSdb, pObj);
  }
  terrno = TSDB_CODE_FAILED;
  return NULL;
}

static int32_t mndUpdateClsRespGrant(SMnode *pMnode, int32_t id, char* clsResp, int32_t clsRespLen, bool isValid) {
  int32_t           code = 0, lino = 0;
  STrans           *pTrans = NULL;
  SGrantClsObj      upObj = {0};
  upObj.id = id;
  upObj.updateTime = taosGetTimestampMs();
  upObj.isValid = isValid;

  if (clsResp != NULL) {
    upObj.clsRespLen = clsRespLen;
    upObj.clsResp = taosMemoryCalloc(1, upObj.clsRespLen);
    if (upObj.clsResp == NULL) goto _OVER;
    (void)memcpy(upObj.clsResp, clsResp, clsRespLen);
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "update-clsRespGrant");
  if (pTrans == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _OVER;
  }
  mDebug("trans:%d, used to update clsResp:%d, isValid:%d", pTrans->id, upObj.id, upObj.isValid);

  TAOS_CHECK_GOTO(mndSetCreateClsRespGrantCommitLogs(pTrans, &upObj), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("cls failed to update clsResp:%d since %s, line:%d", upObj.id, tstrerror(code), lino);
  }
  taosMemoryFreeClear(upObj.clsResp);
  taosMemoryFreeClear(upObj.extend);
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static void mndClsReqDataCleanup(SClsReqData *pClsReqData) {
  if (pClsReqData == NULL) {
    return;
  }

  taosMemoryFreeClear(pClsReqData->pGrantUsage);
  taosMemoryFreeClear(pClsReqData->pInstance);
}

static int32_t clsReqInstanceToJson(const void *pObj, SJson *pJson) {
  const SClsReqInstance *pInstance = (const SClsReqInstance *)pObj;

  if (pInstance == NULL || pJson == NULL) {
    return TSDB_CODE_INVALID_PTR;
  }

  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "id", pInstance->id));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "machineCode", pInstance->machine_code));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "endpoint", pInstance->ep));

  return TSDB_CODE_SUCCESS;
}

static int32_t clsAddInt64ToObject(SJson *pJson, const char *pName, int64_t number) {
  if (NULL == cJSON_AddNumberToObject((cJSON *)pJson, pName, (double)number)) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t clsAddBoolToObject(SJson *pJson, const char *pName, bool value) {
  if (NULL == cJSON_AddBoolToObject((cJSON *)pJson, pName, value)) {
    return terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t clsGrantUsageToJson(const void *pObj, SJson *pJson) {
  const SClsGrant *pGrant = (const SClsGrant *)pObj;

  if (pGrant == NULL || pJson == NULL) {
    return TSDB_CODE_INVALID_PTR;
  }

  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "key", pGrant->key));
  TAOS_CHECK_RETURN(clsAddInt64ToObject(pJson, "value", pGrant->value));

  return TSDB_CODE_SUCCESS;
}

int32_t mndClsReqDataToJson(SClsReqData *pData, SJson *pJson) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t grantUsageNum = 0;
  int32_t instanceNum = 0;

  if (!pData || !pJson) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  if (pData->pGrantUsage != NULL) {
    while (pData->pGrantUsage[grantUsageNum].key[0] != '\0') {
      ++grantUsageNum;
    }
  }

  if (pData->pInstance != NULL) {
    while (pData->pInstance[instanceNum].id[0] != '\0') {
      ++instanceNum;
    }
  }

  TAOS_CHECK_EXIT(clsAddInt64ToObject(pJson, "ts", pData->ts));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "auth_time", pData->auth_time));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "auth_status", pData->auth_status));
  TAOS_CHECK_EXIT(tjsonAddArray(pJson, "usages", clsGrantUsageToJson, pData->pGrantUsage, sizeof(SClsGrant),
                                grantUsageNum));
  TAOS_CHECK_EXIT(clsAddBoolToObject(pJson, "auth_updated", pData->auth_updated));
  TAOS_CHECK_EXIT(tjsonAddArray(pJson, "instances", clsReqInstanceToJson, pData->pInstance, sizeof(SClsReqInstance),
                                instanceNum));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "first_ep", pData->first_ep));
  TAOS_CHECK_EXIT(clsAddInt64ToObject(pJson, "create_time", pData->create_time));
  TAOS_CHECK_EXIT(clsAddInt64ToObject(pJson, "boot_time", pData->boot_time));
  TAOS_CHECK_EXIT(clsAddInt64ToObject(pJson, "authReqInterval", pData->authReqInterval));
  TAOS_CHECK_EXIT(clsAddInt64ToObject(pJson, "expireDays", pData->expireDays));

_exit:
  TAOS_RETURN(code);
}

typedef struct {
  char   *data;
  int64_t dataLen;
} SClsHttpResp;

static size_t clsWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
  size_t        totalSize = size * nmemb;
  SClsHttpResp *pBuf = (SClsHttpResp *)userdata;
  char          *newData = taosMemoryRealloc(pBuf->data, pBuf->dataLen + (int64_t)totalSize + 1);
  if (!newData) return 0;
  pBuf->data = newData;
  memcpy(pBuf->data + pBuf->dataLen, ptr, totalSize);
  pBuf->dataLen += (int64_t)totalSize;
  pBuf->data[pBuf->dataLen] = '\0';
  return totalSize;
}

static int32_t clsBuildGracePeriodValidUntil(char *buf, int32_t bufLen) {
  if (buf == NULL || bufLen <= 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  int64_t baseMs = taosGetTimestampMs();
  if (tsClsLastSucTime[0] != '\0') {
    int64_t parsedMs = 0;
    int32_t code = taosParseTime(tsClsLastSucTime, &parsedMs, (int32_t)strlen(tsClsLastSucTime),
                                 TSDB_TIME_PRECISION_MILLI, NULL);
    if (code == TSDB_CODE_SUCCESS) {
      baseMs = parsedMs;
    } else {
      uWarn("failed to parse clsLastSucTime:%s, fallback to current time, code:0x%x", tsClsLastSucTime, code);
    }
  }

  int64_t futureMs = baseMs + GRACE_PERIOD_DAYS * 86400000LL;
  time_t  futureSec = (time_t)(futureMs / 1000LL);
  struct tm tmInfo = {0};

  if (taosGmTimeR(&futureSec, &tmInfo) == NULL) {
    uError("failed to convert grace period timestamp to utc time");
    TAOS_RETURN(TSDB_CODE_TIME_ERROR);
  }

  int32_t len = (int32_t)taosStrfTime(buf, bufLen, "%Y-%m-%dT%H:%M:%S", &tmInfo);
  if (len <= 0 || len >= bufLen) {
    uError("failed to format grace period valid_until");
    TAOS_RETURN(TSDB_CODE_TIME_ERROR);
  }

  if (snprintf(buf + len, bufLen - len, ".000Z") >= bufLen - len) {
    uError("failed to append grace period timezone suffix");
    TAOS_RETURN(TSDB_CODE_TIME_ERROR);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t mndTestClsServerConnectivity() {
  CURL    *curl = NULL;
  CURLcode curlCode = CURLE_OK;
  long     httpCode = 0;
  int32_t  code = TSDB_CODE_SUCCESS;

  if (strlen(tsClsUrl) == 0) {
    uWarn("cls address not configured, skip cls request");
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle for cls connectivity test");
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  const char *clsUrl = tsClsUrl;
  curl_easy_setopt(curl, CURLOPT_URL, clsUrl);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 3000L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

  curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    uError("cls connectivity test failed, url:%s code:%d msg:%s", tsClsUrl, curlCode, curl_easy_strerror(curlCode));
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (httpCode <= 0) {
    uError("cls connectivity test did not return a valid http response code, url:%s code:%ld", tsClsUrl, httpCode);
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  uDebug("cls connectivity test succeeded, url:%s httpCode:%ld", tsClsUrl, httpCode);

_exit:
  curl_easy_cleanup(curl);
  return code;
}

static int32_t mndSendClsReq(const char *pCont, int32_t contLen, char **ppResp) {
  if (!pCont || contLen <= 0 || !ppResp) {
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  char reason[TSDB_GRANT_CLS_REASON_LEN] = {0};
  *ppResp = NULL;

  if (strlen(tsClsUrl) == 0) {
    uWarn("cls address not configured, skip cls request");
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  // tsClsUrl format: http://host:port
  char url[TSDB_GRANT_CLS_URL_LEN];
  (void)snprintf(url, sizeof(url), "%s/api/v1/cluster/heartbeat", tsClsUrl);

  SClsHttpResp       curlResp = {0};
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  CURLcode           curlCode = CURLE_OK;
  int32_t            code = TSDB_CODE_SUCCESS;

  curl = curl_easy_init();
  if (!curl) {
    uError("failed to create curl handle for cls request");
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  headers = curl_slist_append(NULL, "Content-Type: application/json;charset=UTF-8");
  if (!headers) {
    uError("failed to allocate curl headers for cls request");
    curl_easy_cleanup(curl);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, clsWriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &curlResp);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)contLen);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, pCont);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  uDebugL("cls POST request url:%s len:%d body:%s", url, contLen, pCont);

  curlCode = curl_easy_perform(curl);
  if (curlCode != CURLE_OK) {
    uError("cls curl POST failed, code:%d msg:%s", curlCode, curl_easy_strerror(curlCode));
    (void)snprintf(reason, sizeof(reason), "cls POST failed, code:%d err: %s", curlCode, curl_easy_strerror(curlCode));
    code = TSDB_CODE_FAILED;
  } else {
    long httpCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    uDebug("cls HTTP response code:%ld body_len:%" PRId64, httpCode, curlResp.dataLen);
    if (httpCode >= 200 && httpCode < 300) {
      *ppResp = curlResp.data;
      curlResp.data = NULL;
    } else {
      uError("cls HTTP error response code:%ld body:%s", httpCode,
             curlResp.data ? curlResp.data : "(empty)");
      (void)snprintf(reason, sizeof(reason), "cls resp: httpCode:%ld err: %s", httpCode, curlResp.data ? curlResp.data : "(empty)");
      code = TSDB_CODE_FAILED;
    }
  }

  if (reason[0] != '\0') {
    clsSyncRuntimeVar("clsLastFailReason", tsClsLastFailReason, reason, TSDB_GRANT_CLS_REASON_LEN);
    clsPersistRuntimeVars();
  }

  if (curlResp.data) taosMemoryFree(curlResp.data);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  TAOS_RETURN(code);
}

static void *clsHBProcessThread(void *param) {
  setThreadName("cls-hb");
  SMnode *pMnode = (SMnode *)param;

  uInfo("cls client heartbeat process thread started");

  while (!gClsHBThreadStop) {
    taosThreadMutexLock(&gClsHBMutex);

    while (!gClsHBPending && !gClsHBThreadStop) {
      taosThreadCondWait(&gClsHBCond, &gClsHBMutex);
    }

    if (gClsHBThreadStop) {
      taosThreadMutexUnlock(&gClsHBMutex);
      break;
    }

    gClsHBPending = false;
    taosThreadMutexUnlock(&gClsHBMutex);

    if (!isClsEnabledClosing && (!pMnode || !tsClsEnabled)) {
      continue;
    }

    int32_t     code = 0;
    SClsReqData clsReqData = {0};
    SJson       *pJson = NULL;
    char        *pCont = NULL;

    //test server connectivity
    if (isClsEnabledClosing || ((code = mndTestClsServerConnectivity()) != 0)) {
      uDebug("failed to test cls server connectivity, code:0x%x", code);

      if (code != TSDB_CODE_SUCCESS) {
        char reason[TSDB_GRANT_CLS_REASON_LEN] = {0};
        (void)snprintf(reason, sizeof(reason), "cls connectivity test failed: 0x%x:%s", code, tstrerror(code));
        clsSyncRuntimeVar("clsLastFailReason", tsClsLastFailReason, reason, TSDB_GRANT_CLS_REASON_LEN);
        clsUpdateRuntimeTime("clsLastReqTime", tsClsLastReqTime);
        clsPersistRuntimeVars();
      }

      // get stored cls resp
      SGrantClsObj *pClsRespGrant = mndAcquireFirstClsRespGrant(pMnode);
      if (!pClsRespGrant || !pClsRespGrant->isValid) {
        uWarn("failed to acquire stored cls resp grant or grant record is invalid");
        if(pClsRespGrant) mndReleaseClsRespGrant(pMnode, pClsRespGrant);
        continue;
      }

      uDebug("cls HB process: pClsRespGrant->clsResp:%s pClsRespGrant->clsRespLen:%d", pClsRespGrant->clsResp, pClsRespGrant->clsRespLen);

      // use old cls req data
      code = mndProcessClsRspGrant(pMnode, pClsRespGrant->clsResp, pClsRespGrant->clsRespLen, true);
      if (code != 0) {
        uError("failed to process cls rsp grant in cls hb thread, code:0x%x", code);
        mndReleaseClsRespGrant(pMnode, pClsRespGrant);
        continue;
      }
      pre_signature[0] = '\0';
      //invalid old cls resp grant
      mndUpdateClsRespGrant(pMnode, pClsRespGrant->id, pClsRespGrant->clsResp, pClsRespGrant->clsRespLen, false);
      mndReleaseClsRespGrant(pMnode, pClsRespGrant);
      continue;
    }

    if (!tsClsUrl[0] || !tsClsLicenseId[0] || !tsClsQuotaSlotId[0]) {
      uWarn("cls url or license or quota slot id not configured, must be set when cls is enabled");
      continue;
    }
    
    grantRetrieveGrantInfo(pMnode);
    code = mndClsCollectClusterInfo(pMnode, &clsReqData);
    if (code != 0) {
      uError("failed to collect cluster info in cls hb thread, code:%d", code);
      continue;
    }

    pJson = tjsonCreateObject();
    if (!pJson) {
      mndClsReqDataCleanup(&clsReqData);
      uError("failed to create json object in cls hb thread");
      continue;
    }

    code = mndClsReqDataToJson(&clsReqData, pJson);
    if (code != 0) {
      mndClsReqDataCleanup(&clsReqData);
      tjsonDelete(pJson);
      uError("failed to convert cls req data to json, code:%d", code);
      continue;
    }
    mndClsReqDataCleanup(&clsReqData);

    if (grantObj.clusterId[0] == 0) {
      grantSetClusterId(pMnode, grantObj.clusterId);
    }

    if (tjsonAddStringToObject(pJson, "clusterId", grantObj.clusterId) != 0) {
      tjsonDelete(pJson);
      uError("failed to add clusterId to json");
      continue;
    }

    if (tjsonAddStringToObject(pJson, "clusterCategory", CLUSTER_CATEGORY) != 0) {
      tjsonDelete(pJson);
      uError("failed to add clusterCategory to json");
      continue;
    }

    if (tjsonAddStringToObject(pJson, "licenseId", tsClsLicenseId) != 0) {
      tjsonDelete(pJson);
      uError("failed to add licenseId to json");
      continue;
    }

    if (tsClsQuotaSlotId[0] == '\0') {
      strncpy(tsClsQuotaSlotId, DEFAULT_SLOT_ID, TSDB_GRANT_CLS_ID_LEN);
    }
    if (tjsonAddStringToObject(pJson, "quotaSlotId", tsClsQuotaSlotId) != 0) {
      tjsonDelete(pJson);
      uError("failed to add quotaSlotId to json");
      continue;
    }

    if (pre_signature[0] != '\0') {
      if (tjsonAddStringToObject(pJson, "old_signature", pre_signature) != 0) {
        tjsonDelete(pJson);
        uError("failed to add old_signature to json");
        continue;
      }
    }

    pCont = tjsonToString(pJson);
    tjsonDelete(pJson);

    if (!pCont) {
      uError("failed to convert json to string in cls hb thread");
      continue;
    }
    uDebug("cls request body:%s", pCont);

    int32_t contLen = strlen(pCont);

    // send to cls server
    char *pResp = NULL;
    code = mndSendClsReq(pCont, contLen, &pResp);
    taosMemoryFreeClear(pCont);
    clsUpdateRuntimeTime("clsLastReqTime", tsClsLastReqTime);

    if (code != 0) {
      uError("failed to send cls request in cls hb thread, code:%d", code);
      continue;
    }

    if (!pResp) {
      uWarn("cls request skipped (no address configured or empty response)");
      clsSyncRuntimeVar("clsLastFailReason", tsClsLastFailReason, "cls request skipped: empty response", TSDB_GRANT_CLS_REASON_LEN);
      clsPersistRuntimeVars();
      continue;
    }

    uDebug("cls response body:%s", pResp);

    // process response
    int32_t respLen = strlen(pResp);
    code = mndProcessClsRspGrant(pMnode, pResp, respLen, false);
    if (code != 0 && code != INNER_TSDB_CODE_CLS_SIGNATURE_SAME && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      uError("failed to process cls check rsp in cls hb thread, code:0x%x", code);
      taosMemoryFreeClear(pResp);
      continue;
    }
    clsUpdateRuntimeTime("clsLastSucTime", tsClsLastSucTime);
    clsSyncRuntimeVar("clsLastFailReason", tsClsLastFailReason, "", TSDB_GRANT_CLS_REASON_LEN);
    clsPersistRuntimeVars();

    //save to clsRespGrantObj
    if (code != INNER_TSDB_CODE_CLS_SIGNATURE_SAME) {
      int32_t numOfRows = sdbGetSize(pMnode->pSdb, SDB_GRANT_CLS);
      if(numOfRows == 0) {
        if((code = mndStoreClsRespGrant(pMnode, pResp, respLen)) != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          uError("failed to store cls resp grant in cls hb thread, code:0x%x", code);
          taosMemoryFreeClear(pResp);
          continue;
        }
      } else if(numOfRows >= 1) {
        SGrantClsObj *pClsRespGrant = mndAcquireFirstClsRespGrant(pMnode);
        if (!pClsRespGrant) {
          uWarn("store cls resp: failed to acquire stored cls resp grant");
          taosMemoryFreeClear(pResp);
          continue;
        }
        if((code = mndUpdateClsRespGrant(pMnode, pClsRespGrant->id, pResp, respLen, true)) != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          uError("failed to update cls resp grant in cls hb thread, code:0x%x", code);
          taosMemoryFreeClear(pResp);
          mndReleaseClsRespGrant(pMnode, pClsRespGrant);
          continue;
        }
        mndReleaseClsRespGrant(pMnode, pClsRespGrant);
      }
    }
    taosMemoryFreeClear(pResp);
  }

  uInfo("cls heartbeat process thread stopped");
  return NULL;
}

// transfer cls heartbeat from mnode timer to background thread
static int32_t mndProcessClsHB(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  if (!pMnode) {
    TAOS_RETURN(TSDB_CODE_INVALID_PTR);
  }

  if (tsClsRefreshInterval == GRANT_CLS_CLOSING || tsClsRefreshInterval == GRANT_CLS_OPENING) {
    if (tsClsRefreshInterval == GRANT_CLS_CLOSING) {
      isClsEnabledClosing = true;
    } else {
      isClsEnabledClosing = false;
    }
    tsClsRefreshInterval = gGrantClsPreRefreshInterval;
  } else {
    gGrantClsPreRefreshInterval = tsClsRefreshInterval;
    isClsEnabledClosing = false;
  }
  uDebug("process cls hb refresh, interval:%d, isClsEnabledClosing:%d", tsClsRefreshInterval, isClsEnabledClosing);

  taosThreadMutexLock(&gClsHBMutex);
  if (!gClsHBPending) {
    gClsHBPending = true;
    taosThreadCondSignal(&gClsHBCond);
    uDebug("cls heartbeat task submitted to background thread");
  } else {
    uDebug("cls heartbeat task already pending, skip");
  }
  taosThreadMutexUnlock(&gClsHBMutex);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

// CLS JSON uses native number types (not string-encoded), so tjsonGetBigIntValue /
// tjsonGetIntValue (which use cJSON_GetStringValue internally) silently return the
// default when given a numeric node.  Read numbers directly via valuedouble instead.
static int64_t clsJsonGetInt64(SJson *pJson, const char *key) {
  cJSON *item = cJSON_GetObjectItem((cJSON *)pJson, key);
  if (!item) return GRANT_UNIQ_UNLIMITED;
  if (cJSON_IsString(item) && item->valuestring)
    return taosStr2Int64(item->valuestring, NULL, 10);
  if (cJSON_IsNumber(item))
    return (int64_t)item->valuedouble;
  return GRANT_UNIQ_UNLIMITED;
}
static int32_t clsJsonGetInt32(SJson *pJson, const char *key) {
  return (int32_t)clsJsonGetInt64(pJson, key);
}

static int64_t clsGrantJsonGetInt64(cJSON *pJson, const char *key, int64_t dft) {
  cJSON *item = cJSON_GetObjectItem(pJson, key);
  if (!item) return dft;
  if (cJSON_IsString(item) && item->valuestring) return taosStr2Int64(item->valuestring, NULL, 10);
  if (cJSON_IsNumber(item)) return (int64_t)item->valuedouble;
  return dft;
}

static const char *clsGrantJsonGetString(cJSON *pJson, const char *key) {
  cJSON *item = cJSON_GetObjectItem(pJson, key);
  if (item && cJSON_IsString(item) && item->valuestring) {
    return item->valuestring;
  }
  return NULL;
}

static int32_t clsParseExpireToDays(const char *expire, int32_t capDays, int32_t *pExpireDays) {
  int64_t expireMs = 0;

  if (expire == NULL || expire[0] == '\0' || pExpireDays == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (taosParseTime(expire, &expireMs, (int32_t)strlen(expire), TSDB_TIME_PRECISION_MILLI, NULL) != 0 || expireMs <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t expireDays = (int32_t)(expireMs / 86400000LL);
  *pExpireDays = capDays > 0 ? TMIN(expireDays, capDays) : expireDays;
  return TSDB_CODE_SUCCESS;
}

static int32_t clsVerifyPayloadSignature(const uint8_t *pPayload, int32_t payloadLen, const char *signatureBase64) {
  int32_t     code = TSDB_CODE_SUCCESS;
#ifndef WINDOWS
  uint8_t    *publicKey = NULL;
  uint8_t    *signature = NULL;
  int32_t     publicKeyLen = 0;
  int32_t     signatureLen = 0;
  EVP_PKEY   *pkey = NULL;
  EVP_MD_CTX *mdctx = NULL;

  if (pPayload == NULL || payloadLen <= 0 || signatureBase64 == NULL || signatureBase64[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  code = base64_decode(CLS_GRANTS_VERIFY_PUBLIC_KEY, (int32_t)strlen(CLS_GRANTS_VERIFY_PUBLIC_KEY), &publicKeyLen, &publicKey);
  if (code != TSDB_CODE_SUCCESS || publicKey == NULL || publicKeyLen != 32) {
    code = code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = base64_decode(signatureBase64, (int32_t)strlen(signatureBase64), &signatureLen, &signature);
  if (code != TSDB_CODE_SUCCESS || signature == NULL || signatureLen != 64) {
    code = code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  pkey = EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, NULL, publicKey, publicKeyLen);
  if (pkey == NULL) {
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  mdctx = EVP_MD_CTX_new();
  if (mdctx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (EVP_DigestVerifyInit(mdctx, NULL, NULL, NULL, pkey) != 1) {
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  code = EVP_DigestVerify(mdctx, signature, signatureLen, pPayload, payloadLen) == 1 ? TSDB_CODE_SUCCESS
                                                                                       : TSDB_CODE_FAILED;

_exit:
  EVP_MD_CTX_free(mdctx);
  EVP_PKEY_free(pkey);
  taosMemoryFree(publicKey);
  taosMemoryFree(signature);
#endif
  return code;
}

static int32_t convertClsGrantsToGrantUniqObj(const char *validUntil, SJson *pGrantsJson,
                                               SGrantUniqObj *pGrantObj) {
  typedef struct {
    const char *jsonName;
    const char *grantName;
    int64_t     number;
    int64_t     speed;
    int32_t     expireDays;
    bool        seen;
    bool        enabled;
  } SClsDataInGrant;

  static const struct {
    const char *key;
    int32_t     index;
  } simpleGrantMap[] = {
      {"service", GRANT_OPT_SERVICE},
      {"audit", GRANT_OPT_AUDIT},
      {"storage", GRANT_OPT_STORAGE},
      {"backup_restore", GRANT_OPT_DATA_BAK_RST},
  };

  SClsDataInGrant dataInGrants[] = {
      {"avevahistorian", "avevahistorian", 0, 0, 0, false, false},
      {"csv", "csv", 0, 0, 0, false, false},
      {"influxdb", "influxdb", 0, 0, 0, false, false},
      {"kafka", "kafka", 0, 0, 0, false, false},
      {"kinghist", "kinghist", 0, 0, 0, false, false},
      {"mongodb", "mongodb", 0, 0, 0, false, false},
      {"mqtt", "mqtt", 0, 0, 0, false, false},
      {"mssql", "mssql", 0, 0, 0, false, false},
      {"mysql", "mysql", 0, 0, 0, false, false},
      {"opc_da", "opc_da", 0, 0, 0, false, false},
      {"opc_ua", "opc_ua", 0, 0, 0, false, false},
      {"opentsdb", "opentsdb", 0, 0, 0, false, false},
      {"oracle", "oracle", 0, 0, 0, false, false},
      {"orc", "orc", 0, 0, 0, false, false},
      {"pi", "pi", 0, 0, 0, false, false},
      {"postgres", "postgres", 0, 0, 0, false, false},
      {"pspace", "pspace", 0, 0, 0, false, false},
      {"pulsar", "pulsar", 0, 0, 0, false, false},
      {"sparkplugb", "sparkplugb", 0, 0, 0, false, false},
      {"td2_6", "td2.6", 0, 0, 0, false, false},
      {"td3_0", "td3.0", 0, 0, 0, false, false},
  };

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t grantDays = (int32_t)(taosGetTimestampMs() / 86400000) + GRACE_PERIOD_DAYS;
  cJSON  *pGrantItem = NULL;

  if (!pGrantsJson || !pGrantObj) return TSDB_CODE_INVALID_PARA;

  if (validUntil && validUntil[0]) {
    code = clsParseExpireToDays(validUntil, -1, &pGrantObj->expireDays[GRANT_OPT_BASIC]);
    if (code != TSDB_CODE_SUCCESS) {
      uWarn("failed to parse cls valid_until: %s, using default", validUntil);
      pGrantObj->expireDays[GRANT_OPT_BASIC] = grantDays;
    }
  } else {
    pGrantObj->expireDays[GRANT_OPT_BASIC] = grantDays;
  }

  cJSON_ArrayForEach(pGrantItem, (cJSON *)pGrantsJson) {
    int64_t     value = 0;
    int32_t     expireDays = pGrantObj->expireDays[GRANT_OPT_BASIC];
    const char *grantKey = NULL;
    const char *expire = NULL;

    if (!cJSON_IsObject(pGrantItem)) {
      continue;
    }

    grantKey = pGrantItem->string;
    if (grantKey == NULL || grantKey[0] == '\0') {
      grantKey = clsGrantJsonGetString(pGrantItem, "key");
    }

    if (grantKey == NULL || grantKey[0] == '\0') {
      continue;
    }

    value = clsGrantJsonGetInt64(pGrantItem, "value", GRANT_UNIQ_UNDEFINED);
    expire = clsGrantJsonGetString(pGrantItem, "expire");
    if (expire != NULL && expire[0] != '\0') {
      code = clsParseExpireToDays(expire, pGrantObj->expireDays[GRANT_OPT_BASIC], &expireDays);
      if (code != TSDB_CODE_SUCCESS) {
        uError("failed to parse cls grant expire, key:%s expire:%s", grantKey, expire);
        return code;
      }
    }

    if (strncmp(grantKey, "tsdb.", 5) == 0) {
      const char *name = grantKey + 5;

      if (strcmp(name, "timeseries") == 0) {
        pGrantObj->limitTimeSeries = value;
      } else if (strcmp(name, "cpu_cores") == 0) {
        pGrantObj->limitCpuCores =
            value > INT32_MAX ? INT32_MAX : (value < INT32_MIN ? INT32_MIN : (int32_t)value);
      } else if (strcmp(name, "dnodes") == 0) {
        pGrantObj->limitDnodes =
            value > INT16_MAX ? INT16_MAX : (value < INT16_MIN ? INT16_MIN : (int16_t)value);
      } else if (strcmp(name, "vnodes") == 0) {
        pGrantObj->limitVnodes =
            value > INT32_MAX ? INT32_MAX : (value < INT32_MIN ? INT32_MIN : (int32_t)value);
      } else if (strcmp(name, "storage_size") == 0) {
        pGrantObj->limitStorageSize = value;
      } else if (strcmp(name, "stream") == 0) {
        pGrantObj->expireDays[GRANT_OPT_STREAM] = value != 0 ? expireDays : 0;
        pGrantObj->limitStreams =
            value > INT16_MAX ? INT16_MAX : (value < INT16_MIN ? INT16_MIN : (int16_t)value);
      } else if (strcmp(name, "subscription") == 0) {
        pGrantObj->expireDays[GRANT_OPT_SUBSCRIPTION] = value != 0 ? expireDays : 0;
        pGrantObj->limitSubscriptions =
            value > INT16_MAX ? INT16_MAX : (value < INT16_MIN ? INT16_MIN : (int16_t)value);
      } else if (strcmp(name, "view") == 0) {
        pGrantObj->expireDays[GRANT_OPT_VIEW] = value != 0 ? expireDays : 0;
        pGrantObj->limitViews =
            value > INT32_MAX ? INT32_MAX : (value < INT32_MIN ? INT32_MIN : (int32_t)value);
      } else if (strcmp(name, "data_sync") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "data_sync", expireDays, value);
      } else if (strcmp(name, "object_storage") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "object_storage", expireDays, value);
      } else if (strcmp(name, "active_active") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "active_active", expireDays, value);
      } else if (strcmp(name, "dual_replica") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "dual_replica", expireDays, value);
      } else if (strcmp(name, "db_encryption") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "db_encryption", expireDays, value);
      } else if (strcmp(name, "tdgpt") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "tdgpt", expireDays, value);
      } else if (strcmp(name, "mount") == 0) {
        code = clsAddDynamicGrantItem(pGrantObj, "mount", expireDays, value);
      } else {
        for (int32_t i = 0; i < (int32_t)(sizeof(simpleGrantMap) / sizeof(simpleGrantMap[0])); ++i) {
          if (strcmp(name, simpleGrantMap[i].key) == 0) {
            pGrantObj->expireDays[simpleGrantMap[i].index] = value != 0 ? expireDays : 0;
            break;
          }
        }
      }

      if (code != TSDB_CODE_SUCCESS) {
        uError("failed to add cls tsdb grant item %s, code:%d", grantKey, code);
        return code;
      }
      continue;
    }

    if (strncmp(grantKey, "datain.", 7) == 0) {
      const char *metric = strrchr(grantKey, '.');
      if (metric != NULL && metric > grantKey + 7) {
        char connector[64] = {0};
        int32_t connectorLen = (int32_t)(metric - (grantKey + 7));
        if (connectorLen > 0 && connectorLen < (int32_t)sizeof(connector)) {
          memcpy(connector, grantKey + 7, connectorLen);
          connector[connectorLen] = '\0';
          for (int32_t i = 0; i < (int32_t)(sizeof(dataInGrants) / sizeof(dataInGrants[0])); ++i) {
            if (strcmp(dataInGrants[i].jsonName, connector) == 0) {
              dataInGrants[i].seen = true;
              if (value != 0) {
                dataInGrants[i].enabled = true;
                if (expireDays > dataInGrants[i].expireDays) {
                  dataInGrants[i].expireDays = expireDays;
                }
              }
              if (strcmp(metric + 1, "number") == 0) {
                dataInGrants[i].number = value;
              } else if (strcmp(metric + 1, "speed") == 0) {
                dataInGrants[i].speed = value;
              }
              break;
            }
          }
        }
      }
    }
  }

  for (int32_t i = 0; i < (int32_t)(sizeof(dataInGrants) / sizeof(dataInGrants[0])); ++i) {
    if (!dataInGrants[i].seen) {
      continue;
    }

    if (strcmp(dataInGrants[i].grantName, "csv") == 0) {
      pGrantObj->expireDays[GRANT_OPT_CSV] = dataInGrants[i].enabled ? dataInGrants[i].expireDays : 0;
    }

    code = clsAddDynamicGrantItem2(pGrantObj, dataInGrants[i].grantName, dataInGrants[i].expireDays,
                                dataInGrants[i].number > INT32_MAX ? INT32_MAX
                                                                   : (dataInGrants[i].number < INT32_MIN
                                                                          ? INT32_MIN
                                                                          : (int32_t)dataInGrants[i].number),
                                dataInGrants[i].speed > INT32_MAX ? INT32_MAX
                                                                  : (dataInGrants[i].speed < INT32_MIN
                                                                         ? INT32_MIN
                                                                         : (int32_t)dataInGrants[i].speed));
    if (code != TSDB_CODE_SUCCESS) {
      uError("failed to add cls datain connector %s, code:%d", dataInGrants[i].grantName, code);
      return code;
    }
  }

  for (int32_t i = 0; i < GRANT_OPT_IDMP_MAX; ++i) {
    pGrantObj->idmpExpireDays[i] = 0;
  }
  pGrantObj->idmpLimitTsAttributes = 0;
  pGrantObj->idmpLimitNonTsAttributes = 0;
  pGrantObj->idmpLimitElements = 0;
  pGrantObj->idmpLimitServers = 0;
  pGrantObj->idmpLimitCpuCores = 0;
  pGrantObj->idmpLimitUsers = 0;
  pGrantObj->flags |= GRANT_ACTIVE_FLG_TDENGINE_ASSIGNED | GRANT_ACTIVE_FLG_IDMP_ASSIGNED;

  return code;
}

static void clsCleanupGrantObj(SGrantUniqObj *pGrantObj) {
  if (pGrantObj == NULL) {
    return;
  }

  taosMemoryFreeClear(pGrantObj->active);
  taosMemoryFreeClear(pGrantObj->historicalActive);
  taosArrayDestroy(pGrantObj->pMachines);
  taosArrayDestroy(pGrantObj->pDataIns);
  taosArrayDestroy(pGrantObj->pItem64);
  taosArrayDestroy(pGrantObj->pItemI64);
  taosArrayDestroy(pGrantObj->pItemN64);
  taosArrayDestroy(pGrantObj->pItemT64);

  pGrantObj->pMachines = NULL;
  pGrantObj->pDataIns = NULL;
  pGrantObj->pItem64 = NULL;
  pGrantObj->pItemI64 = NULL;
  pGrantObj->pItemN64 = NULL;
  pGrantObj->pItemT64 = NULL;
}

#ifdef GRANT_TEST_HELPER
int32_t clsTestParseExpireToDays(const char *expire, int32_t capDays, int32_t *pExpireDays) {
  return clsParseExpireToDays(expire, capDays, pExpireDays);
}

int32_t clsTestVerifyPayloadSignature(const uint8_t *pPayload, int32_t payloadLen, const char *signatureBase64) {
  return clsVerifyPayloadSignature(pPayload, payloadLen, signatureBase64);
}

int32_t clsTestConvertClsGrantsToGrantUniqObj(const char *validUntil, SJson *pGrantsJson, SGrantUniqObj *pGrantObj) {
  return convertClsGrantsToGrantUniqObj(validUntil, pGrantsJson, pGrantObj);
}

int32_t clsTestBuildGracePeriodValidUntil(char *buf, int32_t bufLen) {
  return clsBuildGracePeriodValidUntil(buf, bufLen);
}

void clsTestCleanupGrantObj(SGrantUniqObj *pGrantObj) { clsCleanupGrantObj(pGrantObj); }
#endif

// process CLS heartbeat response: parse grants, generate and apply activeCode
int32_t mndProcessClsRspGrant(SMnode *pMnode, char *pCont, int32_t contLen, bool useGracePeriod) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SJson         *pRspJson = NULL;
  SJson         *pGrantJson = NULL;
  char          *pActiveCode = NULL;
  char          *grantPayloadText = NULL;
  uint8_t       *grantPayload = NULL;
  int32_t        grantPayloadLen = 0;
  const char    *signature = NULL;
  const char    *grantsBase64 = NULL;
  SGrantUniqObj  localGrantObj = {0};
  char           reason[TSDB_GRANT_CLS_REASON_LEN] = {0};
  char           validUntil[64] = {0};

  if (!pCont || contLen <= 0) {
    uError("cls response is empty");
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  pRspJson = tjsonParse(pCont);
  if (!pRspJson) {
    uError("failed to parse cls server response JSON: %.*s", TMIN(contLen, 200), pCont);
    TAOS_RETURN(TSDB_CODE_INVALID_JSON_FORMAT);
  }

  int32_t rspCode = clsJsonGetInt32(pRspJson, "code");

  if (rspCode != 0) {
    char message[256] = {0};
    if (rspCode == 1) {
      // No active license on CLS server — log warning but don't crash
      TAOS_UNUSED(tjsonGetStringValue(pRspJson, "message", message));
      uError("cls reports no available license: %s", message[0] ? message : "no available license available");
      (void)snprintf(reason, sizeof(reason), "0x%x:%s", rspCode, message);
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_EXPIRED);
    }

    if (rspCode == 3 && !useGracePeriod) {
      signature = clsGrantJsonGetString((cJSON *)pRspJson, "signature");
      if (signature != NULL && signature[0] != '\0' && strcmp(pre_signature, signature) == 0) {
        uDebug("cls signature unchanged, skip grant update");
        TAOS_CHECK_EXIT_SET_CODE(TSDB_CODE_FAILED, code, INNER_TSDB_CODE_CLS_SIGNATURE_SAME);
      }
      uWarn("cls heartbeat returned code 3 with missing or unexpected signature");
    } else if (rspCode == 5) {
      TAOS_UNUSED(tjsonGetStringValue(pRspJson, "message", message));
      uWarn("cls license revoked: %s", message[0] ? message : "license has been revoked");
      (void)snprintf(reason, sizeof(reason), "0x%x:%s", rspCode, message);
      // license has been revoked
      if(tsClsLastSucTime[0] == '\0') {
        TAOS_CHECK_EXIT(TSDB_CODE_GRANT_EXPIRED);
      } else {
        //utilize GRANT_CLS_CLOSING to refresh cls grants
        tsClsRefreshInterval = GRANT_CLS_CLOSING;
        TAOS_CHECK_EXIT(TSDB_CODE_GRANT_EXPIRED);
      }
    } else {
      TAOS_UNUSED(tjsonGetStringValue(pRspJson, "message", message));
      uError("cls heartbeat error, code:%d message:%s", rspCode, message[0] ? message : "unknown error");
      (void)snprintf(reason, sizeof(reason), "0x%x:%s", rspCode, message);
      TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
    }
  }

  // Extract valid_until
  if (validUntil[0] == '\0') {
    if (useGracePeriod) {
      TAOS_CHECK_EXIT(clsBuildGracePeriodValidUntil(validUntil, sizeof(validUntil)));
    } else {
      TAOS_UNUSED(tjsonGetStringValue(pRspJson, "valid_until", validUntil));
    }
  }

  signature = clsGrantJsonGetString((cJSON *)pRspJson, "signature");
  grantsBase64 = clsGrantJsonGetString((cJSON *)pRspJson, "grants");
  if (signature == NULL || signature[0] == '\0' || grantsBase64 == NULL || grantsBase64[0] == '\0') {
    uError("cls response missing grants or signature");
    TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
  }

  code = base64_decode(grantsBase64, (int32_t)strlen(grantsBase64), &grantPayloadLen, &grantPayload);
  if (code != TSDB_CODE_SUCCESS || grantPayload == NULL || grantPayloadLen <= 0) {
    uError("failed to base64 decode cls grants, code:%d", code);
    TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
  }

  code = clsVerifyPayloadSignature(grantPayload, grantPayloadLen, signature);
  if (code != TSDB_CODE_SUCCESS) {
    uError("cls grants signature verification failed");
    TAOS_CHECK_EXIT(code);
  }

  grantPayloadText = taosMemoryMalloc(grantPayloadLen + 1);
  if (grantPayloadText == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  memcpy(grantPayloadText, grantPayload, grantPayloadLen);
  grantPayloadText[grantPayloadLen] = '\0';

  uDebug("cls grants payload: %s", grantPayloadText);

  pGrantJson = tjsonParse(grantPayloadText);
  if (pGrantJson == NULL) {
    uError("failed to parse decoded cls grants payload");
    TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
  }

  grantObjInit(&localGrantObj, 1);
  localGrantObj.distribute = (uint64_t)(taosGetTimestampMs() / 1000);
  if (grantObj.clusterId[0] != 0) {
    tstrncpy(localGrantObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN + 1);
  }

  code = convertClsGrantsToGrantUniqObj(validUntil, pGrantJson, &localGrantObj);
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to convert cls grants to grant obj, code:%d", code);
    TAOS_CHECK_EXIT(code);
  }

  // Guarantee the 4 fields required by grantLackOfBasic() are never GRANT_UNIQ_UNDEFINED (-2)
  // This prevents "Lack of basic functions in active code" even if tsdb section was missing/empty
  // {
  //   int32_t nowDays = (int32_t)(taosGetTimestampMs() / 86400000);
  //   int32_t safeExpiry = nowDays + MAX_AUTH_DAY;
  //   if (localGrantObj.expireDays[GRANT_OPT_BASIC] == GRANT_UNIQ_UNDEFINED)
  //     localGrantObj.expireDays[GRANT_OPT_BASIC] = safeExpiry;
  //   if (localGrantObj.limitTimeSeries == (int64_t)GRANT_UNIQ_UNDEFINED)
  //     localGrantObj.limitTimeSeries = GRANT_UNIQ_UNLIMITED;
  //   if (localGrantObj.limitDnodes == (int16_t)GRANT_UNIQ_UNDEFINED)
  //     localGrantObj.limitDnodes = (int16_t)GRANT_UNIQ_UNLIMITED;
  //   if (localGrantObj.limitCpuCores == (int32_t)GRANT_UNIQ_UNDEFINED)
  //     localGrantObj.limitCpuCores = (int32_t)GRANT_UNIQ_UNLIMITED;
  // }

  uDebug("cls grant obj: cluster=%s basic=%d timeseries=%" PRId64 " dnodes=%d cpuCores=%d",
         localGrantObj.clusterId, localGrantObj.expireDays[GRANT_OPT_BASIC],
         localGrantObj.limitTimeSeries, (int)localGrantObj.limitDnodes, localGrantObj.limitCpuCores);

  // Generate activeCode
  code = grantUniqGenActiveCode(&localGrantObj);
  if (code != TSDB_CODE_SUCCESS) {
    uError("cls failed to generate active code for cluster %s", localGrantObj.clusterId);
    TAOS_CHECK_EXIT(code);
  }
  pActiveCode = localGrantObj.active;

  if (!pActiveCode || pActiveCode[0] == 0) {
    uError("cls generated empty active code");
    TAOS_CHECK_EXIT(TSDB_CODE_FAILED);
  }

  // Apply activeCode
  SMCfgClusterReq cfgReq = {0};
  tstrncpy(cfgReq.config, "clsGrant", sizeof(cfgReq.config));
  tstrncpy(cfgReq.value, pActiveCode, sizeof(cfgReq.value));

  code = mndProcessConfigGrantReq(pMnode, NULL, &cfgReq);
  if (code != 0) {
    uError("cls failed to apply active code: %s", tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  tstrncpy(pre_signature, signature, sizeof(pre_signature));
  uInfo("cls activated cluster %s successfully", localGrantObj.clusterId);

_exit:
  if (pGrantJson) tjsonDelete(pGrantJson);
  if (pRspJson) tjsonDelete(pRspJson);
  taosMemoryFree(grantPayloadText);
  taosMemoryFree(grantPayload);
  clsCleanupGrantObj(&localGrantObj);

  if (code != 0 && code != INNER_TSDB_CODE_CLS_SIGNATURE_SAME) {
    uError("cls failed to process heartbeat response at line %d: %s", lino, tstrerror(code));
    if (reason[0] == '\0') {
      (void)snprintf(reason, sizeof(reason), "cls rsp grant err: %s", tstrerror(code));
    }
  }
  if (reason[0] != '\0') {
    clsSyncRuntimeVar("clsLastFailReason", tsClsLastFailReason, reason, TSDB_GRANT_CLS_REASON_LEN);
    clsPersistRuntimeVars();
  }

  TAOS_RETURN(code);
}

int32_t initClsClient(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_CLS_HB_TIMER, mndProcessClsHB);

  gClsHBThreadStop = false;
  gClsHBPending = false;
  gClsHBThreadInit = false;

  taosThreadMutexInit(&gClsHBMutex, NULL);
  taosThreadCondInit(&gClsHBCond, NULL);

  TdThreadAttr attr;
  taosThreadAttrInit(&attr);
  int32_t code = taosThreadCreate(&gClsHBThread, &attr, clsHBProcessThread, pMnode);
  taosThreadAttrDestroy(&attr);

  if (code == 0) {
    gClsHBThreadInit = true;
    uInfo("cls heartbeat process thread created successfully");
  } else {
    uError("failed to create cls heartbeat process thread, code:%d", code);
    return code;
  }

  //clsResp table
  SSdbTable clsRespTable = {
      .sdbType = SDB_GRANT_CLS,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndClsRespGrantActionEncode,
      .decodeFp = (SdbDecodeFp)mndClsRespGrantActionDecode,
      .insertFp = (SdbInsertFp)mndClsRespGrantActionInsert,
      .updateFp = (SdbUpdateFp)mndClsRespGrantActionUpdate,
      .deleteFp = (SdbDeleteFp)mndClsRespGrantActionDelete,
  };

  code = sdbSetTable(pMnode->pSdb, clsRespTable);
  if (code != 0) {
    uError("failed to set clsResp table, code:0x%x", code);
    return code;
  }

  //execute immediately
  if (!tsClsEnabled || !tsClsUrl[0] || !tsClsLicenseId[0] || !tsClsQuotaSlotId[0]) {
    uWarn("can't get license when cls client init: clsEnabled, cls url, license id and quota slot id must be configured");
    return TSDB_CODE_SUCCESS;
  }

  taosThreadMutexLock(&gClsHBMutex);
  if (!gClsHBPending) {
    gClsHBPending = true;
    taosThreadCondSignal(&gClsHBCond);
    uDebug("cls heartbeat task submitted to background thread");
  } else {
    uDebug("cls heartbeat task already pending, skip");
  }
  taosThreadMutexUnlock(&gClsHBMutex);

  return code;
}


void cleanupClsClient() {
  if (gClsHBThreadInit) {
    taosThreadMutexLock(&gClsHBMutex);
    gClsHBThreadStop = true;
    taosThreadCondSignal(&gClsHBCond);
    taosThreadMutexUnlock(&gClsHBMutex);

    taosThreadJoin(gClsHBThread, NULL);
    gClsHBThreadInit = false;
    uInfo("cls heartbeat process thread stopped");
  }

  taosThreadMutexDestroy(&gClsHBMutex);
  taosThreadCondDestroy(&gClsHBCond);
  uInfo("cls client cleaned up");
}

static int32_t mndClsCollectClusterInfo(SMnode *pMnode, SClsReqData *pClsReqData) {
  int32_t code = 0;
  int32_t lino = 0;
  SSdb   *pSdb = pMnode->pSdb;
  char    tempBuf[8192] = {0};
  int32_t offset = 0;

  if (!pClsReqData) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  memset(pClsReqData, 0, sizeof(SClsReqData));

  // 1. ts
  pClsReqData->ts = taosGetTimestampMs();

  // 12. expireDays
  pClsReqData->expireDays = (int32_t)(grantGetExpireSec(GRANT_EXPIRE) / 86400);
  if (gStatus.grantState != GRANT_STATE_GRANTED ||
      pClsReqData->expireDays - (int32_t)(pClsReqData->ts / 1000 / 86400) < MAX_AUTH_DAY / 2) {
    pClsReqData->auth_updated = 1;
  }
  // 2. auth_time
  char    ts[GRANT_TS_SEC_LEN] = {0};
  int64_t expireSec = gStatus.grantState == GRANT_STATE_REVOKED ? gStatus.revokedExpireSec : gStatus.basicExpireSec;
  if (expireSec != GRANT_UNIQ_UNLIMITED) {
    TAOS_UNUSED(grantSecondsToString(expireSec, ts));
    tstrncpy(pClsReqData->auth_time, ts, sizeof(pClsReqData->auth_time));
  } else {
    tstrncpy(pClsReqData->auth_time, GRANT_UNIQ_UNLIMITED_S, sizeof(pClsReqData->auth_time));
  }

  // 3. grantState
  if (gStatus.grantState >= 0 && gStatus.grantState < GRANT_STATE_MAX) {
    tstrncpy(pClsReqData->auth_status, gGrantState[gStatus.grantState], sizeof(pClsReqData->auth_status));
  } else {
    tstrncpy(pClsReqData->auth_status, "unknown", sizeof(pClsReqData->auth_status));
  }

  // 4. grant usages
  struct {
    const char *key;
    int64_t     current;
  } grantUsageItems[] = {
      {"tsdb.timeseries", gStatus.curTimeSeries},
      {"tsdb.dnodes", gStatus.curDnodes},
      {"tsdb.cpu_cores", gStatus.curCpuCores},
      {"tsdb.vnodes", gStatus.curVnodes},
      {"tsdb.storage_size", gStatus.curStorageSize},
  };
  int32_t grantUsageItemNum = sizeof(grantUsageItems) / sizeof(grantUsageItems[0]);

  pClsReqData->pGrantUsage = taosMemoryCalloc(grantUsageItemNum + 1, sizeof(SClsGrant));
  if (pClsReqData->pGrantUsage == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < grantUsageItemNum; ++i) {
    tstrncpy(pClsReqData->pGrantUsage[i].key, grantUsageItems[i].key, sizeof(pClsReqData->pGrantUsage[i].key));
    pClsReqData->pGrantUsage[i].value = grantUsageItems[i].current;
  }

  // 5. auth_updated
  pClsReqData->auth_code = 0;
  // pClsReqData->auth_updated = 0;

  // 6. machine_code
  int32_t instanceCap = sdbGetSize(pSdb, SDB_DNODE);
  if (instanceCap > 0) {
    pClsReqData->pInstance = taosMemoryCalloc(instanceCap + 1, sizeof(SClsReqInstance));
    if (pClsReqData->pInstance == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  int32_t  instanceNum = 0;
  void    *pIter = NULL;
  SDnodeObj *pDnode = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    SClsReqInstance *pInstance = NULL;
    bool hasInstanceData = pDnode->machineId[0] != 0 || pDnode->fqdn[0] != 0;
    if (hasInstanceData && pClsReqData->pInstance != NULL && instanceNum < instanceCap) {
      pInstance = &pClsReqData->pInstance[instanceNum++];
      snprintf(pInstance->id, sizeof(pInstance->id), "%d", pDnode->id);
    }

    if (pDnode->machineId[0] != 0) {
      if (pInstance != NULL) {
        tstrncpy(pInstance->machine_code, pDnode->machineId, sizeof(pInstance->machine_code));
      }
    }

    if (pDnode->fqdn[0] != 0) {
      if (pInstance != NULL) {
        snprintf(pInstance->ep, sizeof(pInstance->ep), "%s:%u", pDnode->fqdn, pDnode->port);
      }
    }
    sdbRelease(pSdb, pDnode);
  }

  // 8. FirstEP
  offset = 0;
  pIter = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    if (pDnode->fqdn[0] != 0) {
      if (offset > 0 && offset < sizeof(pClsReqData->first_ep) - 1) {
        pClsReqData->first_ep[offset++] = ',';
      }
      int32_t len = snprintf(tempBuf, sizeof(tempBuf), "%s:%u", pDnode->fqdn, pDnode->port);
      if (offset + len < sizeof(pClsReqData->first_ep)) {
        memcpy(pClsReqData->first_ep + offset, tempBuf, len);
        offset += len;
      }
    }
    sdbRelease(pSdb, pDnode);
  }
  pClsReqData->first_ep[offset] = '\0';

  // 9. cluster create_time
  pClsReqData->create_time = mndGetClusterCreateTime(pMnode);
  // 10. cluster boot_time
  pClsReqData->boot_time = mndGetClusterUpTime(pMnode);

  // 11.authReqInterval
  pClsReqData->authReqInterval = tsAuthReqHBInterval;

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mndClsReqDataCleanup(pClsReqData);
  }
  if (code < 0) {
    uError("failed to collect cluster info at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t clsAddDynamicGrantItem(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int64_t number) {
  int32_t index = tClsGetGrantIndex(itemName);
  if (index < GRANT_OPT_MAX || index >= GRANT_OPT_DYN_MAX) {
    uError("failed to add dynamic grant item '%s', invalid index: %d", itemName, index);
    return TSDB_CODE_FAILED;
  }

  SGrantItemI64 item = {.index = index, .number = number, .expire = expire};

  if (!pGrantObj->pItemI64) {
    pGrantObj->pItemI64 = taosArrayInit(8, sizeof(SGrantItemI64));
    if (!pGrantObj->pItemI64) {
      uError("failed to init pItemI64 array for '%s', out of memory", itemName);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (taosArrayPush(pGrantObj->pItemI64, &item) == NULL) {
    uError("failed to push grant item '%s' to array", itemName);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t clsAddDynamicGrantItem2(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int32_t number,
                             int32_t speed) {
  int32_t type = tClsGetDataInType(itemName);
  if (type < 0 || type > CONN_TYPE_DYN_MAX) {
    uError("failed to add dynamic grant item '%s', invalid index: %d", itemName, type);
    return TSDB_CODE_FAILED;
  }

  if (type < CONN_TYPE_MAX) {
    int32_t idx = type * 3;
    pGrantObj->dataIns[idx] = expire;       // expire
    pGrantObj->dataIns[++idx] = speed;      // speed
    pGrantObj->dataIns[++idx] = number;     // number
  } else {
    if (!pGrantObj->pDataIns) {
      if (!(pGrantObj->pDataIns = taosArrayInit(1, sizeof(SGrantDataIns)))) {
        uError("failed to init pDataIns array for '%s', out of memory", itemName);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    if (!taosArrayPush(pGrantObj->pDataIns, &(SGrantDataIns){0})) {
      uError("failed to push grant item '%s' to array", itemName);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SGrantDataIns *pLast = taosArrayGetLast(pGrantObj->pDataIns);
    tstrncpy(pLast->name, itemName, GRANT_ITEM_NAME_LEN);
    int32_t nameLen = strlen(pLast->name);
    strntolower(pLast->name, pLast->name, TMIN(GRANT_ITEM_NAME_LEN, nameLen));
    pLast->expire = expire;
    if (strncasecmp(pLast->name, "CSV", nameLen) == 0) {
      if (number == GRANT_UNIQ_UNLIMITED) {
        pLast->number = number;  // data-in csv range:[0,un]
      } else {
        pLast->number = 0;
      }
    } else {
      pLast->number = number;
    }
    pLast->speed = speed;
  }
  return TSDB_CODE_SUCCESS;
}
