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
#include "tutil.h"

extern SGrantStatus  gStatus;
extern SGrantUniqObj grantObj;
extern int32_t       mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg);

typedef struct SAuthHBTask {
  SMnode *pMnode;
} SAuthHBTask;

static TdThread      gAuthHBThread;
static bool          gAuthHBThreadInit = false;
static bool          gAuthHBThreadStop = false;
static TdThreadCond  gAuthHBCond;
static TdThreadMutex gAuthHBMutex;
static bool          gAuthHBPending = false;

static const int64_t AUTH_DES_KEY = 0x656E6967444554LL;  // "TDengin" in little endian

static int32_t padTo8Bytes(const char *pInput, int32_t inputLen, char **ppPadded, int32_t *pPaddedLen) {
  int32_t remainder = inputLen % 8;
  int32_t paddingLen = (remainder == 0) ? 0 : (8 - remainder);
  int32_t totalLen = inputLen + paddingLen;

  char *padded = taosMemoryCalloc(1, totalLen + 1);
  if (!padded) {
    uError("failed to allocate memory for padding");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(padded, pInput, inputLen);
  for (int32_t i = 0; i < paddingLen; i++) {
    padded[inputLen + i] = (char)paddingLen;
  }

  *ppPadded = padded;
  *pPaddedLen = totalLen;
  return TSDB_CODE_SUCCESS;
}

static int32_t removePadding(char *pData, int32_t dataLen, int32_t *pRealLen) {
  if (dataLen == 0 || dataLen % 8 != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  uint8_t paddingLen = (uint8_t)pData[dataLen - 1];
  if (paddingLen == 0 || paddingLen > 8) {
    *pRealLen = dataLen;
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 1; i <= paddingLen; i++) {
    if ((uint8_t)pData[dataLen - i] != paddingLen) {
      *pRealLen = dataLen;
      return TSDB_CODE_SUCCESS;
    }
  }

  *pRealLen = dataLen - paddingLen;
  return TSDB_CODE_SUCCESS;
}

int32_t encryptAuthMessage(const char *pPlainText, int32_t plainLen, char **ppCipherText, int32_t *pCipherLen) {
  if (!pPlainText || plainLen <= 0 || !ppCipherText || !pCipherLen) {
    return TSDB_CODE_INVALID_PARA;
  }

  char   *paddedData = NULL;
  int32_t paddedLen = 0;
  char   *encrypted = NULL;
  char   *base64Output = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  code = padTo8Bytes(pPlainText, plainLen, &paddedData, &paddedLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  encrypted = taosDesEncode(AUTH_DES_KEY, paddedData, paddedLen);
  taosMemoryFree(paddedData);

  if (!encrypted) {
    uError("failed to DES encrypt data, error:%s", tstrerror(terrno));
    return terrno;
  }

  if (base64_encode((unsigned char *)encrypted, paddedLen, &base64Output) != 0 || !base64Output) {
    uError("failed to base64 encode encrypted data");
    taosMemoryFree(encrypted);
    return TSDB_CODE_FAILED;
  }
  taosMemoryFree(encrypted);

  *ppCipherText = base64Output;
  *pCipherLen = strlen(base64Output);

  uDebug("DES encrypted: plainLen=%d, paddedLen=%d, base64Len=%d", plainLen, paddedLen, *pCipherLen);
  return TSDB_CODE_SUCCESS;
}

int32_t decryptAuthMessage(const char *pCipherText, int32_t cipherLen, char **ppPlainText, int32_t *pPlainLen) {
  if (!pCipherText || cipherLen <= 0 || !ppPlainText || !pPlainLen) {
    return TSDB_CODE_INVALID_PARA;
  }

  uint8_t *decodedData = NULL;
  char    *decrypted = NULL;
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  actualLen = 0;

  code = base64_decode(pCipherText, cipherLen, &actualLen, &decodedData);
  if (code != TSDB_CODE_SUCCESS || !decodedData || actualLen <= 0) {
    uError("failed to base64 decode cipher text, code:%d, actualLen=%d", code, actualLen);
    return code != TSDB_CODE_SUCCESS ? code : TSDB_CODE_FAILED;
  }

  if (actualLen % 8 != 0) {
    uError("invalid decoded length for DES decryption: %d (must be multiple of 8)", actualLen);
    taosMemoryFree(decodedData);
    return TSDB_CODE_INVALID_MSG;
  }

  decrypted = taosDesDecode(AUTH_DES_KEY, (char *)decodedData, actualLen);
  taosMemoryFree(decodedData);

  if (!decrypted) {
    uError("failed to DES decrypt data, error:%s", tstrerror(terrno));
    return terrno;
  }

  int32_t realLen = 0;
  code = removePadding(decrypted, actualLen, &realLen);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(decrypted);
    return code;
  }

  decrypted[realLen] = '\0';

  *ppPlainText = decrypted;
  *pPlainLen = realLen;

  uDebug("DES decrypted: cipherLen=%d, plainLen=%d", actualLen, realLen);
  return TSDB_CODE_SUCCESS;
}

int32_t mndAuthReqDataToJson(SAuthReqData *pData, SJson *pJson) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pData || !pJson) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "ts", pData->ts));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "auth_time", pData->auth_time));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "auth_status", pData->auth_status));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "auth_code", pData->auth_code));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "auth_usage", pData->auth_usage));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "auth_updated", pData->auth_updated));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "machine_code", pData->machine_code));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "fqdn", pData->fqdn));
  TAOS_CHECK_EXIT(tjsonAddStringToObject(pJson, "first_ep", pData->first_ep));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "create_time", pData->create_time));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "boot_time", pData->boot_time));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "authReqInterval", pData->authReqInterval));
  TAOS_CHECK_EXIT(tjsonAddIntegerToObject(pJson, "expireDays", pData->expireDays));

_exit:
  TAOS_RETURN(code);
}

static int32_t mndSendAuthReq(SMnode *pMnode, int32_t contLen, void *pCont) {
  int32_t code = 0;

  if (strlen(tsAuthReqUrl) == 0) {
    uWarn("auth request URL not configured, skip auth request");
    rpcFreeCont(pCont);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  char     server[TSDB_FQDN_LEN] = {0};
  uint16_t port = tsServerPort;

  const char *portStart = strchr(tsAuthReqUrl, ':');
  if (portStart) {
    int hostLen = portStart - tsAuthReqUrl;
    if (hostLen >= TSDB_FQDN_LEN) hostLen = TSDB_FQDN_LEN - 1;
    strncpy(server, tsAuthReqUrl, hostLen);
    server[hostLen] = '\0';
    port = (uint16_t)atoi(portStart + 1);
  } else {
    tstrncpy(server, tsAuthReqUrl, TSDB_FQDN_LEN);
  }

  SRpcMsg rpcMsg = {.pCont = pCont,
                    .contLen = contLen,
                    .msgType = TDMT_MND_AUTH_CHECK,
                    .info.noResp = 0,
                    .info.ahandle = (void *)pMnode};

  SEpSet epSet = {.numOfEps = 1};
  tstrncpy(epSet.eps[0].fqdn, server, TSDB_FQDN_LEN);
  epSet.eps[0].port = port;

  uDebug("send async auth request to %s:%" PRIu16, server, port);

  if ((code = tmsgSendReq(&epSet, &rpcMsg)) != 0) {
    uWarn("failed to send async auth request to %s:%" PRIu16 " since %s", server, port, tstrerror(code));
    TAOS_RETURN(code);
  }

  uDebug("async auth request sent successfully to %s:%" PRIu16, server, port);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static void *authHBProcessThread(void *param) {
  setThreadName("auth-hb");
  taosSetCpuAffinity(THREAD_CAT_MANAGEMENT);
  SMnode *pMnode = (SMnode *)param;

  uInfo("auth heartbeat process thread started");

  while (!gAuthHBThreadStop) {
    taosThreadMutexLock(&gAuthHBMutex);

    while (!gAuthHBPending && !gAuthHBThreadStop) {
      taosThreadCondWait(&gAuthHBCond, &gAuthHBMutex);
    }

    if (gAuthHBThreadStop) {
      taosThreadMutexUnlock(&gAuthHBMutex);
      break;
    }

    gAuthHBPending = false;
    taosThreadMutexUnlock(&gAuthHBMutex);

    if (!pMnode || !tsAuthReq) {
      continue;
    }

    int32_t      code = 0;
    SAuthReqData authReqData = {0};
    SJson       *pJson = NULL;
    char        *pCont = NULL;
    
    grantRetrieveGrantInfo(pMnode);
    code = mndCollectClusterInfo(pMnode, &authReqData);
    if (code != 0) {
      uError("failed to collect cluster info in auth hb thread, code:%d", code);
      continue;
    }

    pJson = tjsonCreateObject();
    if (!pJson) {
      uError("failed to create json object in auth hb thread");
      continue;
    }

    code = mndAuthReqDataToJson(&authReqData, pJson);
    if (code != 0) {
      tjsonDelete(pJson);
      uError("failed to convert auth req data to json, code:%d", code);
      continue;
    }

    if (grantObj.clusterId[0] == 0) {
      grantSetClusterId(pMnode, grantObj.clusterId);
    }

    if (tjsonAddStringToObject(pJson, "clusterId", grantObj.clusterId) != 0) {
      tjsonDelete(pJson);
      uError("failed to add clusterId to json");
      continue;
    }

    pCont = tjsonToString(pJson);
    tjsonDelete(pJson);

    if (!pCont) {
      uError("failed to convert json to string in auth hb thread");
      continue;
    }

    int32_t contLen = strlen(pCont);

    char   *pEncrypted = NULL;
    int32_t encryptedLen = 0;
    code = encryptAuthMessage(pCont, contLen, &pEncrypted, &encryptedLen);
    taosMemoryFree(pCont);

    if (code != TSDB_CODE_SUCCESS || !pEncrypted) {
      uError("failed to encrypt auth request, code:%d", code);
      continue;
    }

    void *pRpcCont = rpcMallocCont(encryptedLen + 1);
    if (!pRpcCont) {
      taosMemoryFree(pEncrypted);
      uError("failed to allocate rpc content in auth hb thread");
      continue;
    }

    memcpy(pRpcCont, pEncrypted, encryptedLen);
    ((char *)pRpcCont)[encryptedLen] = '\0';
    taosMemoryFree(pEncrypted);

    code = mndSendAuthReq(pMnode, encryptedLen + 1, pRpcCont);
    if (code != 0) {
      uError("failed to send auth request in auth hb thread, code:%d", code);
    } else {
      uDebug("auth heartbeat processed successfully in background thread");
    }
  }

  uInfo("auth heartbeat process thread stopped");
  return NULL;
}

// transfer auth heartbeat from mnode to auth server
static int32_t mndProcessAuthHB(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  if (!pMnode) {
    TAOS_RETURN(TSDB_CODE_INVALID_PTR);
  }

  if (!tsAuthReq) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  if (gStatus.grantState == GRANT_STATE_GRANTED) {
    tsAuthReqHBInterval = tsAuthReqInterval;
  } else {
    tsAuthReqHBInterval = 5;
  }

  taosThreadMutexLock(&gAuthHBMutex);
  if (!gAuthHBPending) {
    gAuthHBPending = true;
    taosThreadCondSignal(&gAuthHBCond);
    uDebug("auth heartbeat task submitted to background thread");
  } else {
    uDebug("auth heartbeat task already pending, skip");
  }
  taosThreadMutexUnlock(&gAuthHBMutex);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

// authClient process response from authServer
static int32_t mndProcessAuthCheckRsp(SRpcMsg *pRsp) {
  int32_t code = 0;
  int32_t lino = 0;
  SJson  *pRspJson = NULL;
  char   *activeCode = NULL;

  SMnode *pMnode = pRsp->info.node;

  if (!pMnode) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  if (pRsp->code != 0) {
    uError("authClient receive RPC error, code:%d(%s)", pRsp->code, tstrerror(pRsp->code));
    TAOS_CHECK_EXIT(pRsp->code);
  }

  if (!pRsp->pCont || pRsp->contLen <= 0) {
    uError("invalid authServer response, empty content");
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  char   *pDecrypted = NULL;
  int32_t decryptedLen = 0;
  code = decryptAuthMessage((char *)pRsp->pCont, pRsp->contLen - 1, &pDecrypted, &decryptedLen);
  if (code != TSDB_CODE_SUCCESS || !pDecrypted) {
    uError("failed to decrypt auth response, code:%d", code);
    TAOS_CHECK_EXIT(code);
  }

  uInfo("receive auth response, encrypted length: %d, decrypted length: %d", pRsp->contLen, decryptedLen);

  pRspJson = tjsonParse(pDecrypted);
  if (!pRspJson) {
    uError("failed to parse authServer response JSON after decryption");
    taosMemoryFree(pDecrypted);
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  taosMemoryFree(pDecrypted);

  int32_t rspCode = 0;
  TAOS_UNUSED(tjsonGetIntValue(pRspJson, "code", &rspCode));

  if (rspCode == TSDB_CODE_GRANT_NO_UPDATE_NEEDED) {
    tsAuthReqHBInterval = tsAuthReqInterval;
    uDebug("cluster is not within expiration time, no update needed");
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  if (rspCode != TSDB_CODE_SUCCESS) {
    char message[256] = {0};
    TAOS_UNUSED(tjsonGetStringValue(pRspJson, "message", message));
    uError("auth check failed, code:%d(%s), message:%s", rspCode, tstrerror(rspCode),
           message[0] ? message : "unknown error");
    TAOS_CHECK_EXIT(rspCode);
  }

  // parse activeCode
  char activeCodeBuf[TSDB_CLUSTER_VALUE_LEN] = {0};
  code = tjsonGetStringValue(pRspJson, "activeCode", activeCodeBuf);
  if (code != 0 || activeCodeBuf[0] == 0) {
    uError("activeCode not found in auth response");
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }

  int32_t activeLen = strlen(activeCodeBuf);
  activeCode = taosMemoryMalloc(activeLen + 1);
  if (!activeCode) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  tstrncpy(activeCode, activeCodeBuf, activeLen + 1);

  SJson *pCheckInfo = tjsonGetObjectItem(pRspJson, "checkInfo");
  if (pCheckInfo) {
    int64_t recvChecksum = 0;
    char    checkStr[256] = {0};

    TAOS_UNUSED(tjsonGetBigIntValue(pCheckInfo, "checksum", &recvChecksum));
    TAOS_UNUSED(tjsonGetStringValue(pCheckInfo, "checkStr", checkStr));

    if (recvChecksum != 0 && checkStr[0] != 0) {
      TSCKSUM calcChecksum = taosCalcChecksum(0, (const uint8_t *)checkStr, strlen(checkStr));
      if (calcChecksum != recvChecksum) {
        uWarn("response checksum mismatch, recv:%" PRId64 ", calc:%" PRId64, (int64_t)recvChecksum,
              (int64_t)calcChecksum);
      } else {
        uDebug("response checksum verified successfully");
      }
    }
  }

  SMCfgClusterReq cfgReq = {0};
  tstrncpy(cfgReq.config, "grant", sizeof(cfgReq.config));
  tstrncpy(cfgReq.value, activeCode, sizeof(cfgReq.value));

  code = mndProcessConfigGrantReq(pMnode, NULL, &cfgReq);
  if (code != 0) {
    uError("failed to apply active code since %s", tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  uInfo("activate cluster successfully");
  // change auth heartbeat interval 5s to configured interval
  tsAuthReqHBInterval = tsAuthReqInterval;

_exit:
  if (pRspJson) tjsonDelete(pRspJson);
  taosMemoryFreeClear(activeCode);

  if (code < 0) {
    uError("failed to process auth check response at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t initAuthClient(SMnode *pMnode) {
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH_HB_TIMER, mndProcessAuthHB);         // pullup auth msg
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH_CHECK_RSP, mndProcessAuthCheckRsp);  // process auth rsp

  gAuthHBThreadStop = false;
  gAuthHBPending = false;
  gAuthHBThreadInit = false;

  taosThreadMutexInit(&gAuthHBMutex, NULL);
  taosThreadCondInit(&gAuthHBCond, NULL);

  TdThreadAttr attr;
  taosThreadAttrInit(&attr);
  int32_t code = taosThreadCreate(&gAuthHBThread, &attr, authHBProcessThread, pMnode);
  taosThreadAttrDestroy(&attr);

  if (code == 0) {
    gAuthHBThreadInit = true;
    uInfo("auth heartbeat process thread created successfully");
  } else {
    uError("failed to create auth heartbeat process thread, code:%d", code);
  }
  return code;
}

void cleanupAuthClient() {
  if (gAuthHBThreadInit) {
    taosThreadMutexLock(&gAuthHBMutex);
    gAuthHBThreadStop = true;
    taosThreadCondSignal(&gAuthHBCond);
    taosThreadMutexUnlock(&gAuthHBMutex);

    taosThreadJoin(gAuthHBThread, NULL);
    gAuthHBThreadInit = false;
    uInfo("auth heartbeat process thread stopped");
  }

  taosThreadMutexDestroy(&gAuthHBMutex);
  taosThreadCondDestroy(&gAuthHBCond);
  uInfo("auth client cleaned up");
}