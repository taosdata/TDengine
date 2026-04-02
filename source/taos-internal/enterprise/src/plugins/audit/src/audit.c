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

#ifndef WINDOWS
#include <curl/curl.h>
#endif
#include "auditInt.h"
#include "osMemory.h"
#include "osThread.h"
#include "taoserror.h"
#include "tarray.h"
#include "tglobal.h"
#include "thttp.h"
#include "tjson.h"

#define AUDIT_CURL_TIMEOUT 5000

extern char  *tsAuditUri;
extern char  *tsAuditBatchUri;
extern SAudit tsAudit;

void getAuditDbNameToken(char *pDb, char *pToken) {
  (void)taosThreadRwlockRdlock(&tsAudit.infoLock);
  tstrncpy(pDb, tsAudit.auditDB, TSDB_DB_FNAME_LEN);
  tstrncpy(pToken, tsAudit.auditToken, TSDB_TOKEN_LEN);
  (void)taosThreadRwlockUnlock(&tsAudit.infoLock);
}

void getAuditEpSet(SEpSet *ep, int32_t *pVgId) {
  (void)taosThreadRwlockRdlock(&tsAudit.infoLock);
  *ep = tsAudit.auditEpSet;
  *pVgId = tsAudit.auditVgId;
  (void)taosThreadRwlockUnlock(&tsAudit.infoLock);
}

void setAuditDbNameToken(char *pDb, char *pToken, SEpSet *ep, int32_t auditVgId) {
  if (pDb == NULL || pToken == NULL || ep == NULL) {
    return;
  }
  (void)taosThreadRwlockRdlock(&tsAudit.infoLock);
  if (strncmp(pDb, tsAudit.auditDB, TSDB_DB_FNAME_LEN) == 0 &&
      strncmp(pToken, tsAudit.auditToken, TSDB_TOKEN_LEN) == 0 && auditVgId == tsAudit.auditVgId) {
    if (tsAudit.auditEpSet.numOfEps == ep->numOfEps) {
      bool epMatch = true;
      for (int32_t i = 0; i < tsAudit.auditEpSet.numOfEps; i++) {
        if (strncmp(tsAudit.auditEpSet.eps[i].fqdn, ep->eps[i].fqdn, TSDB_FQDN_LEN) != 0 ||
            tsAudit.auditEpSet.eps[i].port != ep->eps[i].port) {
          epMatch = false;
          break;
        }
      }
      if (epMatch) {
        (void)taosThreadRwlockUnlock(&tsAudit.infoLock);
        return;
      }
    }
  }
  (void)taosThreadRwlockUnlock(&tsAudit.infoLock);

  uInfo("set audit db:%s, token:xxx, auditVgId:%d", pDb, auditVgId);
  (void)taosThreadRwlockWrlock(&tsAudit.infoLock);
  tstrncpy(tsAudit.auditDB, pDb, TSDB_DB_FNAME_LEN);
  tstrncpy(tsAudit.auditToken, pToken, TSDB_TOKEN_LEN);
  tsAudit.auditEpSet = *ep;
  tsAudit.auditVgId = auditVgId;
  (void)taosThreadRwlockUnlock(&tsAudit.infoLock);
}

typedef struct {
  char   *data;
  int64_t dataLen;
} SAuditResp;

#ifndef WINDOWS
typedef enum {
  ANALYTICS_HTTP_TYPE_GET = 0,
  ANALYTICS_HTTP_TYPE_POST,
} EAuditHttpType;

static size_t taosAuditWriteData(char *pCont, size_t contLen, size_t nmemb, void *userdata) {
  SAuditResp *pRsp = userdata;
  if (contLen == 0 || nmemb == 0 || pCont == NULL) {
    pRsp->dataLen = 0;
    pRsp->data = NULL;
    uError("curl response is received, len:%" PRId64, pRsp->dataLen);
    return 0;
  }

  int64_t newDataSize = (int64_t)contLen * nmemb;
  int64_t size = pRsp->dataLen + newDataSize;

  if (pRsp->data == NULL) {
    pRsp->data = taosMemoryMalloc(size + 1);
    if (pRsp->data == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }
  } else {
    char *p = taosMemoryRealloc(pRsp->data, size + 1);
    if (p == NULL) {
      uError("failed to prepare recv buffer for post rsp, len:%d, code:%s", (int32_t)size + 1, tstrerror(terrno));
      return 0;  // return the recv length, if failed, return 0
    }

    pRsp->data = p;
  }

  if (pRsp->data != NULL) {
    (void)memcpy(pRsp->data + pRsp->dataLen, pCont, newDataSize);

    pRsp->dataLen = size;
    pRsp->data[size] = 0;

    uDebugL("curl response is received, len:%" PRId64 ", content:%s", size, pRsp->data);
    return newDataSize;
  } else {
    pRsp->dataLen = 0;
    uError("failed to malloc curl response");
    return 0;
  }
}

static int32_t taosAuditPostRequest(const char *url, SAuditResp *pRsp, const char *buf, int32_t bufLen, int32_t timeout,
                                    char *qid) {
  struct curl_slist *headers = NULL;
  CURL              *curl = NULL;
  CURLcode           code = 0;

  curl = curl_easy_init();
  if (curl == NULL) {
    uError("failed to create curl handle");
    return -1;
  }

  char headersBuf[200] = {0};
  (void)snprintf(headersBuf, 200, "X-QID:%s", qid);

  headers = curl_slist_append(headers, "Content-Type:application/json;charset=UTF-8");
  headers = curl_slist_append(headers, headersBuf);
  if ((code = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_URL, url)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, taosAuditWriteData)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_WRITEDATA, pRsp)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_POST, 1)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, bufLen)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L)) != CURLE_OK) goto _OVER;
  if ((code = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L)) != CURLE_OK) goto _OVER;

  uDebug("curl post request will sent, url:%s len:%d", url, bufLen);
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
}

static int32_t taosAuditSendReqByCurl(const char *url, char *pCont, int64_t contentLen, int64_t timeout, char *qid) {
  int32_t    code = -1;
  SAuditResp curlRsp = {0};

  int32_t post_code;
  if ((post_code = taosAuditPostRequest(url, &curlRsp, pCont, contentLen, timeout, qid)) != 0) {
    uError("failed with code %d", post_code);
    code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
    goto _OVER;
  }

  if (curlRsp.data == NULL || curlRsp.dataLen == 0) {
    code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
    goto _OVER;
  }

  code = 0;

_OVER:
  if (curlRsp.data != NULL) taosMemoryFreeClear(curlRsp.data);
  return code;
}
#endif

static int32_t auditSendToVnode(char *pCont) {
  int32_t          code = 0;
  int32_t          lino = 0;
  SVAuditRecordReq req = {0};

  SEpSet  epSet = {0};
  int32_t vgId = 0;
  getAuditEpSet(&epSet, &vgId);
  if (epSet.numOfEps == 0) {
    uError("vgId:%d, failed to send audit record request since no epSet found", vgId);
    code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
    return code;
  }

  req.data = pCont;

  int32_t reqLen = tSerializeSVAuditRecordReq(NULL, 0, &req);
  if (reqLen <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  int32_t   contLen = reqLen + sizeof(SMsgHead);
  SMsgHead *pHead = rpcMallocCont(contLen);
  if (pHead == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  if (tSerializeSVAuditRecordReq((char *)pHead + sizeof(SMsgHead), reqLen, &req) <= 0) {
    rpcFreeCont(pHead);
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

  SRpcMsg rpcMsg = {.msgType = TDMT_VND_AUDIT_RECORD, .pCont = pHead, .contLen = contLen};

  code = tmsgSendReq(&epSet, &rpcMsg);
  if (code != 0) {
    rpcFreeCont(pHead);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  } else {
    uTrace("vgId:%d, send audit record request", vgId);
  }

_exit:
  if (code != 0) {
    uError("vgId:%d, failed to self save audit record request at:%d since %s", vgId, lino, tstrerror(code));
  }

  return code;
}

static int32_t auditSendToKeeper(char *uri, char *pCont, int32_t setSize) {
  int32_t code = 0;
  char    db[TSDB_DB_FNAME_LEN] = {0};
  char    token[TSDB_TOKEN_LEN] = {0};

  if (tsAuditUseToken) {
    getAuditDbNameToken(db, token);

    if (db[0] == 0 || token[0] == 0) {
      uTrace("auditDB or auditToken is empty, can't send audit record, db:%s, token:xxxx", db);
      return 0;
    }
  }

  char httpPath[1000] = {0};
  if (tsAuditUseToken) {
    tsnprintf(httpPath, 1000, "%s?db=%s&token=%s", uri, db, token);
  } else {
    tsnprintf(httpPath, 1000, "%s", uri);
  }

  char qid[100] = {0};
  (void)snprintf(qid, 100, "0x%" PRIxLEAST64, tGenQid64(tsAudit.dnodeId));
  // Do not print httpPath, since it may include token
  if (setSize == 1) {
    uDebug("audit record with path:xxxx QID:%s cont:%s", qid, pCont);
  } else {
    uDebug("audit batch record with QID:%s cont: %d", qid, setSize);
    // uTrace("audit batch record with QID:%s cont: %d\n%s", qid, setSize, pCont);
  }

  if (tsAuditHttps) {
#ifndef WINDOWS
    char path[1000] = {0};
    (void)tsnprintf(path, 1000, "https://%s:%d%s", tsAudit.cfg.server, tsAudit.cfg.port, httpPath);
    if ((code = taosAuditSendReqByCurl(path, pCont, strlen(pCont), AUDIT_CURL_TIMEOUT, qid)) != 0) {
      uError("failed to send audit msg since %s, cont:%s", tstrerror(code), pCont);
      code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
      return code;
    }
#endif
  } else {
    EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if ((code = taosSendHttpReportWithQID(tsAudit.cfg.server, httpPath, tsAudit.cfg.port, pCont, strlen(pCont), flag,
                                          qid)) != 0) {
      uError("failed to send audit msg since %s, cont:%s", tstrerror(code), pCont);
      code = TSDB_CODE_AUDIT_FAIL_SEND_AUDIT_RECORD;
      return code;
    }
  }

  return code;
}

static int32_t auditSend(SJson *pJson) {
  int32_t code = 0;

  char *pCont = tjsonToString(pJson);
  if (pCont == NULL) {
    code = TSDB_CODE_AUDIT_NOT_FORMAT_TO_JSON;
    return code;
  }

  if (tsAuditSaveInSelf)
    code = auditSendToVnode(pCont);
  else
    code = auditSendToKeeper(tsAuditUri, pCont, 1);

  taosMemoryFree(pCont);
  return code;
}

void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, char *detail,
                    int32_t len, double duration, int64_t affectedRows) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uWarn("can't record total audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uWarn("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  char* buf = taosMemoryMalloc(min);
  if(buf == NULL){
    uError("failed to audit since can't alloc a tmp buf");
    return;
  }

  memset(buf, 0, min);
  if(detail != NULL && len > 0){
    if(len >= AUDIT_DETAIL_MAX){
      memcpy(buf, detail, min - 1);
    } else {
      memcpy(buf, detail, len);
    }
  }

  char user[TSDB_USER_LEN] = {0};
  if(pReq != NULL && RPC_MSG_USER(pReq)[0] != 0){
    tstrncpy(user, RPC_MSG_USER(pReq), sizeof(user));
  }
  uTrace("audit record user:%s, len:%" PRId32, user, (int32_t)strlen(user));

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    taosMemoryFreeClear(buf);
    uError("failed to audit since failed to create json object");
    return;
  }

  //char   ts[40] = {0};
  int64_t curTime = taosGetTimestampNs();
  //taosFormatUtcTime(ts, sizeof(ts), curTime, TSDB_TIME_PRECISION_MILLI);

  char strClusterId[TSDB_CLUSTER_ID_LEN] = {0};
  sprintf(strClusterId, "%" PRId64, clusterId);

  char clientAddress[AUDIT_CLIENT_ADD_LEN] = {0};
  if (pReq != NULL) {
    SIpAddr *ipAddr = &pReq->info.conn.cliAddr;
    sprintf(clientAddress, "%s:%d", IP_ADDR_STR(ipAddr), ipAddr->port);
  }

  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "timestamp", curTime), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "cluster_id", strClusterId), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "user", user), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "operation", operation), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "client_add", clientAddress), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "db", target1), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "resource", target2), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "details", buf), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "affected_rows", affectedRows), &lino, _error);
  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "duration", duration), &lino, _error);

  TAOS_CHECK_GOTO(auditSend(pJson), &lino, _error);

  goto _exit;

_error:
  uError("failed to audit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));

_exit:
  tjsonDelete(pJson);
  taosMemoryFreeClear(buf);
}

void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, char *detail,
                       int32_t len, double duration, int64_t affectedRows) {
  if (!tsEnableAudit || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  if(len > AUDIT_DETAIL_MAX){
    uWarn("can't record total audit since detail is too long, len:%d, operation:%s, target1:%s, target2:%s", 
            len, operation, target1, target2);
  }
  if(detail == NULL || len == 0){
    uWarn("audit detail shound not be null, len:%d", len);
  }

  int32_t min = len >= AUDIT_DETAIL_MAX ? AUDIT_DETAIL_MAX : len + 1;
  // buf will be freed when send audit records in batch
  char* buf = taosMemoryMalloc(min);
  if(buf == NULL){
    uError("failed to audit since can't alloc a tmp buf");
    return;
  }

  memset(buf, 0, min);
  if(detail != NULL && len > 0){
    if(len >= AUDIT_DETAIL_MAX){
      memcpy(buf, detail, min - 1);
    }
    else{
      memcpy(buf, detail, len);
    }
  }

  // record will be freed when send audit records in batch
  SAuditRecord *record = taosMemoryMalloc(sizeof(SAuditRecord));
  if(record == NULL){
    uError("failed to audit since can't alloc a audit record");
    return;
  }

  if(pReq != NULL && RPC_MSG_USER(pReq)[0] != 0){
    tstrncpy(record->user, RPC_MSG_USER(pReq), sizeof(record->user));
  }
  uTrace("audit record user:%s, len:%" PRId32, record->user, (int32_t)strlen(record->user));

  record->curTime = taosGetTimestampNs();

  sprintf(record->strClusterId, "%" PRId64, clusterId);

  if (pReq != NULL) {
    SIpAddr *pAddr = &pReq->info.conn.cliAddr;
    sprintf(record->clientAddress, "%s:%d", IP_ADDR_STR(pAddr), pAddr->port);
  }

  strcpy(record->operation, operation);
  strcpy(record->target1, target1);
  strcpy(record->target2, target2);
  record->detail = buf;
  record->duration = duration;
  record->affectedRows = affectedRows;

  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_GOTO(taosThreadMutexLock(&tsAudit.recordLock), &lino, _fail_cleanup);
  if (taosArrayPush(tsAudit.records, &record) == NULL) {
    TAOS_CHECK_GOTO(taosThreadMutexUnlock(&tsAudit.recordLock), &lino, _fail_cleanup);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _fail_cleanup);
  }
  TAOS_CHECK_GOTO(taosThreadMutexUnlock(&tsAudit.recordLock), &lino, _exit);

  return;

_fail_cleanup:
  if (buf != NULL) {
    taosMemoryFree(buf);
  }
  if (record != NULL) {
    taosMemoryFree(record);
  }
_exit:
  uError("failed to audit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
}

void auditSendRecordsInBatchImp() {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosThreadMutexLock(&tsAudit.recordLock) != 0) {
    uError("failed to send audit in batch since failed to lock");
    return;
  }

  int setSize = taosArrayGetSize(tsAudit.records);
  if(setSize == 0){
    (void)taosThreadMutexUnlock(&tsAudit.recordLock);
    return;
  }

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) {
    uError("failed to send audit in batch since failed to create json object");
    (void)taosThreadMutexUnlock(&tsAudit.recordLock);
    return;
  }

  SJson *items = tjsonAddArrayToObject(pJson, "records");
  if(items == NULL){
    code = TSDB_CODE_AUDIT_FAIL_GENERATE_JSON;
    lino = __LINE__;
    goto _error;
  }

  for (int i = 0; i < setSize; i++) {
    SAuditRecord *pRecord = *(SAuditRecord **)taosArrayPop(tsAudit.records);

    SJson *item = tjsonCreateObject();
    if(item == NULL){
      code = TSDB_CODE_AUDIT_FAIL_GENERATE_JSON;
      lino = __LINE__;
      goto _error;
    }

    TAOS_CHECK_GOTO(tjsonAddItemToArray(items, item), &lino, _error);

    TAOS_CHECK_GOTO(tjsonAddIntegerToObject(item, "timestamp", pRecord->curTime), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "cluster_id", pRecord->strClusterId), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "user", pRecord->user), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "operation", pRecord->operation), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "client_add", pRecord->clientAddress), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "db", pRecord->target1), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "resource", pRecord->target2), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddStringToObject(item, "details", pRecord->detail), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(item, "affected_rows", pRecord->affectedRows), &lino, _error);
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(item, "duration", pRecord->duration), &lino, _error);

    taosMemoryFree(pRecord->detail);
    taosMemoryFree(pRecord);
  }

  (void)taosThreadMutexUnlock(&tsAudit.recordLock);

  char *pCont = tjsonToString(pJson);

  if (pCont != NULL) {
    if (tsAuditSaveInSelf)
      code = auditSendToVnode(pCont);
    else
      code = auditSendToKeeper(tsAuditBatchUri, pCont, setSize);

    taosMemoryFree(pCont);

    if (code != 0) uError("failed to send audit msg since %s", tstrerror(code));
  } else {
    uError("failed to send audit msg since failed format to json string");
  }

  goto _exit;

_error:
  uError("failed to audit, %s at %s:%d since %s", __func__, __FILE__, lino, tstrerror(code));
  (void)taosThreadMutexUnlock(&tsAudit.recordLock);

_exit:
  tjsonDelete(pJson);
}