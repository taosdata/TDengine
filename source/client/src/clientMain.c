/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "catalog.h"
#include "extConnector.h"
#include "clientLog.h"
#include "clientMonitor.h"
#include "clientSession.h"
#include "clientStmt.h"
#include "clientStmt2.h"
#include "clientWlr.h"
#include "functionMgt.h"
#include "os.h"
#include "parser.h"
#include "query.h"
#include "scheduler.h"
#include "tcompare.h"
#include "tconv.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmisce.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "ttime.h"
#include "tversion.h"
#include "version.h"

#define CLIENT_CLEANUP_WAIT_TIMEOUT_MS 10000

#ifdef TAOSD_INTEGRATED
extern void shellStopDaemon();
#endif

static void instanceRpcGlobalCleanup(void);

static int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt, SSqlCallbackWrapper *pWrapper);
static int     taos_fetch_block_s_impl(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows);
static int     taos_fetch_raw_block_impl(TAOS_RES *res, int *numOfRows, void **pData);
static int     taos_select_db_impl(TAOS *taos, const char *db);
static int32_t wlrEncodeSingleMultiBind(TAOS_MULTI_BIND *bind, uint8_t *buf, size_t bufLen);

static int32_t waitRefSetToBaseCount(int32_t rsetId, const char *name, int64_t startMs, int64_t timeoutMs) {
  if (rsetId < 0) {
    return TSDB_CODE_SUCCESS;
  }

  while (true) {
    int32_t count = 0;
    int32_t code = taosGetRefSetCount(rsetId, &count);
    if (code != TSDB_CODE_SUCCESS) {
      tscWarn("failed to inspect %s ref pool:%d before cleanup, code:%s", name, rsetId, tstrerror(code));
      return code;
    }

    if (count <= 1) {
      return TSDB_CODE_SUCCESS;
    }

    if (timeoutMs >= 0 && taosGetTimestampMs() - startMs >= timeoutMs) {
      tscWarn("timeout waiting for %s ref pool:%d to drain, count:%d", name, rsetId, count);
      return TSDB_CODE_TIMEOUT_ERROR;
    }

    taosMsleep(1);
  }
}

int taos_options(TSDB_OPTION option, const void *arg, ...) {
  if (arg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  static int32_t lock = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      (void)sched_yield();
    }
  }

  int ret = taos_options_imp(option, (const char *)arg);
  atomic_store_32(&lock, 0);
  return ret;
}

#if !defined(WINDOWS) && !defined(TD_ASTRA)
static void freeTz(void *p) {
  timezone_t tz = *(timezone_t *)p;
  tzfree(tz);
}
#endif

int32_t tzInit() {
#if !defined(WINDOWS) && !defined(TD_ASTRA)
  pTimezoneMap = taosHashInit(0, MurmurHash3_32, false, HASH_ENTRY_LOCK);
  if (pTimezoneMap == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTimezoneMap, freeTz);

  pTimezoneNameMap = taosHashInit(0, taosIntHash_64, false, HASH_ENTRY_LOCK);
  if (pTimezoneNameMap == NULL) {
    return terrno;
  }
#endif
  return 0;
}

void tzCleanup() {
#if !defined(WINDOWS) && !defined(TD_ASTRA)
  taosHashCleanup(pTimezoneMap);
  taosHashCleanup(pTimezoneNameMap);
#endif
}

static void buildSessionTimezoneName(const char* inputName,
                                     const char* normalizedName,
                                     char* sessionName,
                                     int32_t sessionNameLen) {
  size_t inputLen = strlen(inputName);
  bool isZulu = inputLen == 1 &&
                (inputName[0] == 'Z' || inputName[0] == 'z');
  bool isBareOffset = inputName[0] == '+' || inputName[0] == '-';
  bool isIanaName = strchr(inputName, '/') != NULL;

  if (isZulu || isBareOffset || isIanaName) {
    tstrncpy(sessionName, inputName, sessionNameLen);
    return;
  }

  bool isUtcName = strncasecmp(inputName, "UTC", 3) == 0 &&
                   (inputLen == 3 ||
                    (inputLen > 3 &&
                     (inputName[3] == '+' || inputName[3] == '-')));
  if (isUtcName) {
    (void)snprintf(sessionName, sessionNameLen, "UTC%s", inputName + 3);
    return;
  }

  tstrncpy(sessionName, normalizedName, sessionNameLen);
}

#if !defined(WINDOWS) && !defined(TD_ASTRA)
static timezone_t setConnectionTz(const char* val, char* sessionName,
                                  int32_t sessionNameLen) {
  char       normalizedName[TD_TIMEZONE_LEN] = {0};
  timezone_t tz = NULL;
  int32_t code = taosValidateAndNormalizeTimezone(
      val, normalizedName, sizeof(normalizedName), NULL);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("%s invalid timezone '%s', code:0x%x", __func__, val, code);
    terrno = code;
    return NULL;
  }

  buildSessionTimezoneName(val, normalizedName, sessionName, sessionNameLen);
  timezone_t* tmp = taosHashGet(pTimezoneMap, sessionName,
                                strlen(sessionName));
  if (tmp != NULL && *tmp != NULL) {
    return *tmp;
  }

  code = taosValidateTimezone(sessionName, &tz);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("%s failed to create timezone '%s', code:0x%x", __func__,
             sessionName, code);
    terrno = code;
    return NULL;
  }

  tscDebug("set timezone to %s", sessionName);

  code = taosHashPut(pTimezoneMap, sessionName, strlen(sessionName),
                     &tz, sizeof(timezone_t));
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_DUP_KEY) {
    tscError("%s put timezone to tz map error:%d", __func__, code);
    tzfree(tz);
    terrno = code;
    return NULL;
  }
  if (code == TSDB_CODE_DUP_KEY) {
    tmp = taosHashGet(pTimezoneMap, sessionName, strlen(sessionName));
    if (tmp == NULL || *tmp == NULL) {
      tscError("%s failed to get concurrently cached timezone '%s'",
               __func__, sessionName);
      tzfree(tz);
      terrno = TSDB_CODE_DUP_KEY;
      return NULL;
    }

    timezone_t cachedTz = *tmp;
    tzfree(tz);
    tz = cachedTz;
  }

  time_t tx1 = taosGetTimestampSec();
  char   output[TD_TIMEZONE_LEN] = {0};
  code = taosFormatTimezoneStr(tx1, sessionName, tz, output);
  if (code == 0) {
    code = taosHashPut(pTimezoneNameMap, &tz, sizeof(timezone_t), output,
                       strlen(output) + 1);
  }
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_DUP_KEY) {
    tscError("failed to put timezone %s to map", sessionName);
  }

  return tz;
}
#endif

int32_t tscPrepareSessionTimezone(const char* timezoneName,
                                  bool explicitTimezone,
                                  SPreparedTimezone* pPrepared) {
  if (pPrepared == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pPrepared = (SPreparedTimezone){0};
  if (timezoneName == NULL || timezoneName[0] == '\0') {
    return TSDB_CODE_SUCCESS;
  }

  char sessionName[TD_TIMEZONE_LEN] = {0};
#if !defined(WINDOWS) && !defined(TD_ASTRA)
  timezone_t timezone = setConnectionTz(timezoneName, sessionName,
                                        sizeof(sessionName));
  if (timezone == NULL) {
    return terrno;
  }

  pPrepared->timezone = timezone;
#else
  char normalizedName[TD_TIMEZONE_LEN] = {0};
  int32_t code = taosValidateAndNormalizeTimezone(
      timezoneName, normalizedName, sizeof(normalizedName), NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  buildSessionTimezoneName(timezoneName, normalizedName, sessionName,
                           sizeof(sessionName));
#endif
  tstrncpy(pPrepared->timezoneName, sessionName,
           sizeof(pPrepared->timezoneName));
  pPrepared->timezoneExplicit = explicitTimezone;
  return TSDB_CODE_SUCCESS;
}

void tscCommitSessionTimezone(STscObj* pObj, const SPreparedTimezone* pPrepared) {
  if (pObj == NULL || pPrepared == NULL) {
    return;
  }

#if !defined(WINDOWS) && !defined(TD_ASTRA)
  pObj->optionInfo.timezone = pPrepared->timezone;
#endif
  tstrncpy(pObj->optionInfo.timezoneName, pPrepared->timezoneName,
           sizeof(pObj->optionInfo.timezoneName));
  pObj->optionInfo.timezoneExplicit = pPrepared->timezoneExplicit;
}

int32_t tscSetSessionTimezone(STscObj* pObj, const char* timezoneName, bool explicitTimezone) {
  if (pObj == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SPreparedTimezone prepared = {0};
  int32_t code = tscPrepareSessionTimezone(timezoneName, explicitTimezone, &prepared);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  tscCommitSessionTimezone(pObj, &prepared);
  return TSDB_CODE_SUCCESS;
}

/* Snapshot the current client-level timezone (tsTimezoneStr) into the
 * session-level timezone (pObj->optionInfo.timezone) when the connection is
 * established. ALTER LOCAL prepares the validated session state before changing
 * the client-level setting, then commits it after the change succeeds. */
int32_t tscInitSessionTimezone(STscObj *pObj) {
#if defined(TD_ASTRA)
  (void)pObj;
  return TSDB_CODE_SUCCESS;
#else
#ifndef WINDOWS
  if (pTimezoneMap == NULL) {
    /* tzInit() not yet called; skip silently */
    return TSDB_CODE_SUCCESS;
  }
#endif

  /* Extract the IANA name from tsTimezoneStr.
   * The string has the form "Name (Abbrev, +HHMM)". */
  char        tzName[TD_TIMEZONE_LEN] = {0};
  const char *pSpace = strchr(tsTimezoneStr, ' ');
  int32_t     nameLen = pSpace ? (int32_t)(pSpace - tsTimezoneStr) : (int32_t)strlen(tsTimezoneStr);
  if (nameLen <= 0 || nameLen >= TD_TIMEZONE_LEN) {
    tscWarn("tscInitSessionTimezone: unexpected tsTimezoneStr format: '%s', "
            "skip session-level init", tsTimezoneStr);
    return TSDB_CODE_SUCCESS;  /* malformed; leave session tz unset */
  }
  tstrncpy(tzName, tsTimezoneStr, nameLen + 1);

  int32_t code = tscSetSessionTimezone(pObj, tzName, false);
  if (code != TSDB_CODE_SUCCESS) {
    /* Non-fatal: session tz stays NULL; timestamp parsing falls back to
     * getGlobalDefaultTZ() at query time (live global tz, not a snapshot). */
    tscWarn("tscInitSessionTimezone: setConnectionTz failed for '%s', "
            "session-level timezone not snapshotted", tzName);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
#endif
}

// Get connection's session timezone (opaque handle for C API bindings).
// Returns an opaque pointer to internal timezone_t.
/*
 * Return the timezone snapshot captured when the result was created.
 * The returned handle is owned by pTimezoneMap; caller must NOT tzfree().
 * Returns NULL for META/RAW/BATCH_META results or NULL input.
 */
void *taos_get_result_tz(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    return pRequest->body.resInfo.timezone;
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *info = tmqGetCurResInfo(res);
    return info->timezone;
  }
  return NULL;
}

static int32_t setConnectionOption(TAOS *taos, TSDB_OPTION_CONNECTION option, const char *val) {
  if (taos == NULL) {
    return terrno = TSDB_CODE_INVALID_PARA;
  }

#ifdef WINDOWS
  if (option == TSDB_OPTION_CONNECTION_TIMEZONE) {
    return terrno = TSDB_CODE_NOT_SUPPORTED_IN_WINDOWS;
  }
#endif

  if (option < TSDB_OPTION_CONNECTION_CLEAR || option >= TSDB_MAX_OPTIONS_CONNECTION) {
    return terrno = TSDB_CODE_INVALID_PARA;
  }

  int32_t code = taos_init();
  // initialize global config
  if (code != 0) {
    return terrno = code;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }

  if (option == TSDB_OPTION_CONNECTION_CLEAR) {
    val = NULL;
  }

#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if (option == TSDB_OPTION_CONNECTION_CHARSET || option == TSDB_OPTION_CONNECTION_CLEAR) {
    if (val != NULL) {
      if (!taosValidateEncodec(val)) {
        code = terrno;
        goto END;
      }
      void *tmp = taosConvInit(val);
      if (tmp == NULL) {
        code = terrno;
        goto END;
      }
      pObj->optionInfo.charsetCxt = tmp;
    } else {
      pObj->optionInfo.charsetCxt = NULL;
    }
  }
#endif
  if (option == TSDB_OPTION_CONNECTION_TIMEZONE || option == TSDB_OPTION_CONNECTION_CLEAR) {
#if !defined(WINDOWS) && !defined(TD_ASTRA)
    if (val != NULL) {
      if (val[0] == 0) {
        val = "UTC";
      }
      code = tscSetSessionTimezone(pObj, val, true);
      if (code != TSDB_CODE_SUCCESS) {
        goto END;
      }
    } else {
      code = tscSetSessionTimezone(pObj, NULL, false);
    }
#endif
  }

  if (option == TSDB_OPTION_CONNECTION_USER_APP || option == TSDB_OPTION_CONNECTION_CLEAR) {
    if (val != NULL) {
      tstrncpy(pObj->optionInfo.userApp, val, sizeof(pObj->optionInfo.userApp));
      pObj->optionInfo.userAppId = taosAppNameToId(val);
    } else {
      pObj->optionInfo.userApp[0] = 0;
      pObj->optionInfo.userAppId = TD_APP_UNKNOWN;
    }
  }

  if (option == TSDB_OPTION_CONNECTION_CONNECTOR_INFO || option == TSDB_OPTION_CONNECTION_CLEAR) {
    if (val != NULL) {
      tstrncpy(pObj->optionInfo.cInfo, val, sizeof(pObj->optionInfo.cInfo));
    } else {
      pObj->optionInfo.cInfo[0] = 0;
    }
  }

  if (option == TSDB_OPTION_CONNECTION_USER_IP || option == TSDB_OPTION_CONNECTION_CLEAR) {
    SIpRange dualIp = {0};
    if (val != NULL) {
      pObj->optionInfo.userIp = taosInetAddr(val);
      SIpAddr addr = {0};
      code = taosGetIpFromFqdn(tsEnableIpv6, val, &addr);
      if (code == 0) {
        code = tIpStrToUint(&addr, &pObj->optionInfo.userDualIp);
      }
      if (code != 0) {
        tscError("ipv6 flag %d failed to convert user ip %s to dual ip since %s", tsEnableIpv6 ? 1 : 0, val,
                 tstrerror(code));
        pObj->optionInfo.userIp = INADDR_NONE;
        pObj->optionInfo.userDualIp = dualIp;
        code = 0;
      }
    } else {
      pObj->optionInfo.userIp = INADDR_NONE;
      pObj->optionInfo.userDualIp = dualIp;
    }
  }

END:
  releaseTscObj(*(int64_t *)taos);
  return terrno = code;
}

/* Stable spelling for the file. Deliberately not the enum's numeric value alone:
 * the number is what replay applies, the name is what makes a trace readable
 * without the header at hand. */
static const char *wlrConnOptName(TSDB_OPTION_CONNECTION option, char *unknown, size_t unknownLen) {
  switch (option) {
    case TSDB_OPTION_CONNECTION_CLEAR:
      return "clear";
    case TSDB_OPTION_CONNECTION_CHARSET:
      return "charset";
    case TSDB_OPTION_CONNECTION_TIMEZONE:
      return "timezone";
    case TSDB_OPTION_CONNECTION_USER_IP:
      return "userIp";
    case TSDB_OPTION_CONNECTION_USER_APP:
      return "userApp";
    case TSDB_OPTION_CONNECTION_CONNECTOR_INFO:
      return "connectorInfo";
    default:
      (void)snprintf(unknown, unknownLen, "unknown(%d)", (int32_t)option);
      return unknown;
  }
}

int taos_options_connection(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...) {
  /* Connection options change how subsequent statements are interpreted (charset,
   * timezone), so replay has to apply them in the same order relative to the
   * queries that follow -- and with the same option and value, which is why both
   * are recorded rather than just the fact that an option was set. */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  char               wlrUnknownOpt[32] = {0};
  wlrArgs.apiType = WLR_API_OPTIONS_CONNECTION;
  wlrArgs.connOptSet = true;
  wlrArgs.connOptType = (int32_t)option;
  wlrArgs.connOptName = wlrConnOptName(option, wlrUnknownOpt, sizeof(wlrUnknownOpt));
  wlrArgs.connOptValue = (option == TSDB_OPTION_CONNECTION_CLEAR) ? NULL : (const char *)arg;
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  if (wlrRecorderState() == WLR_STATE_RUNNING) (void)wlrApiBegin(&wlrArgs, &wlrTok);

  int32_t wlrCode = setConnectionOption(taos, option, (const char *)arg);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = wlrCode;
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

// this function may be called by user or system, or by both simultaneously.
void taos_cleanup(void) {
  tscInfo("start to cleanup client environment");
  if (!beginAsyncWorkShutdown()) {
    return;
  }

  int64_t cleanupStartMs = taosGetTimestampMs();

  // Stop recording first: it drains its queue, writes SESSION_STOP with the list
  // of calls that never completed, and flushes. Doing this before the request ref
  // pool drains means a still-running query is recorded as unfinished, which is
  // the intended signal rather than a defect (DS 6.4, FS 4.7).
  wlrRecorderClose();

  if (TSDB_CODE_SUCCESS !=
      waitRefSetToBaseCount(clientReqRefPool, "request", cleanupStartMs, CLIENT_CLEANUP_WAIT_TIMEOUT_MS)) {
    tscWarn("request ref pool did not drain cleanly before cleanup continues");
  }

  monitorClose();
  tscStopCrashReport();

  hbMgrCleanUp();

  extConnectorModuleDestroy();
  catalogDestroy();
  schedulerDestroy();

  fmFuncMgtDestroy();
  qCleanupKeywordsTable();

#if !defined(WINDOWS) && !defined(TD_ASTRA)
  tzCleanup();
#endif
  tmqMgmtClose();
  writeRawCleanup();

  int32_t id = clientReqRefPool;
  clientReqRefPool = -1;
  taosCloseRef(id);

  if (TSDB_CODE_SUCCESS !=
      waitRefSetToBaseCount(clientConnRefPool, "connection", cleanupStartMs, CLIENT_CLEANUP_WAIT_TIMEOUT_MS)) {
    tscWarn("connection ref pool did not drain cleanly before cleanup continues");
  }

  id = clientConnRefPool;
  clientConnRefPool = -1;
  taosCloseRef(id);

  nodesDestroyAllocatorSet();
  cleanupAppInfo();
  instanceRpcGlobalCleanup();
  rpcCleanup();
  tscDebug("rpc cleanup");

  if (TSDB_CODE_SUCCESS != cleanupTaskQueue()) {
    tscWarn("failed to cleanup task queue");
  }

  sessMgtDestroy();

  taosConvDestroy();
  DestroyRegexCache();
#ifdef TAOSD_INTEGRATED
  shellStopDaemon();
#endif
  // The embedded daemon (TAOSD_INTEGRATED) shares the nodes library, and so this cache, with
  // the client: its plans can borrow cached handles until the daemon thread has been stopped
  // and joined, so the cache is released only after that. Local requests and connections were
  // drained above.
  nodesCleanupTimezoneCache();
  tscInfo("all local resources released");
  taosCleanupCfg();
#ifndef TAOSD_INTEGRATED
  if (!wlrRecorderReleaseLog()) taosCloseLog();
#endif
}

static setConfRet taos_set_config_imp(const char *config) {
  setConfRet ret = {SET_CONF_RET_SUCC, {0}};
  // TODO: need re-implementation
  return ret;
}

setConfRet taos_set_config(const char *config) {
  // TODO  pthread_mutex_lock(&setConfMutex);
  setConfRet ret = taos_set_config_imp(config);
  //  pthread_mutex_unlock(&setConfMutex);
  return ret;
}

TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port) {
  tscInfo("try to connect to %s:%u, user:%s db:%s", ip, port, user, db);
  if (user == NULL) {
    user = TSDB_DEFAULT_USER;
  }

  if (pass == NULL) {
    pass = TSDB_DEFAULT_PASS;
  }

  /*
   * The driver env -- and with it the recorder -- is normally initialized deeper in,
   * by taos_connect_by_auth. That is too late for the very first connect of a
   * process: its BEGIN would run while the recorder is still OFF, so the connect
   * would go unrecorded AND, because the logical connection ID used to be minted
   * only for a recorded BEGIN, that connection would keep wlrConnId = 0 for its
   * whole life. Every later event on it then omitted CONN_ID and the shipped
   * decoder rejected the segment (TS API007, FS 4.5).
   *
   * taos_init() is taosThreadOnce-guarded and idempotent, so hoisting it here just
   * moves the one-time setup ahead of the hook. A failure is deliberately ignored:
   * taos_connect_internal performs the same init and owns the error path, so
   * reporting it here would duplicate that with no added information.
   */
  (void)taos_init();

  // Recorder: endpoint/user/db/port exactly as the caller supplied them. `pass` is
  // deliberately not passed in -- the recorder has no way to reach a credential it
  // never receives (FS 6.2).
  //
  // ip is recorded raw, including NULL. NULL means "resolve from firstEp", which is
  // a different fact from any particular address: substituting the resolved value
  // would pin replay to the recording host's config instead of the target's, and
  // would put a value in the trace that the application never passed.
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_CONNECT(&wlrTok, ip, user, db, port);

  STscObj *pObj = NULL;
  int32_t  code = taos_connect_internal(ip, user, pass, NULL, db, port, CONN_TYPE__QUERY, &pObj);
  if (TSDB_CODE_SUCCESS == code) {
    int64_t *rid = taosMemoryCalloc(1, sizeof(int64_t));
    if (NULL == rid) {
      tscError("out of memory when taos connect to %s:%u, user:%s db:%s", ip, port, user, db);
      /* Explicit rather than terrno: the connect above succeeded, so terrno holds
       * whatever that path last set. Naming the code keeps the recorded value
       * correct, matching the sibling connect variants in clientImpl.c. */
      WLR_API_END_CODE(&wlrTok, TSDB_CODE_OUT_OF_MEMORY);
      taos_close_internal(pObj);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    *rid = pObj->id;

    /*
     * Mint the logical connection ID here so every later event on this connection
     * can reference it.
     *
     * Mint while the recorder is RUNNING even when this BEGIN was nested and skipped:
     * later outermost queries on that connection still need a non-zero CONN_ID. Also
     * mint when this BEGIN was recorded, because shutdown may move the manager out of
     * RUNNING while the real connect is in flight; its successful RETURN must not
     * carry CONN_ID=0 (TS API007).
     */
    wlrConnectComplete(&wlrTok, &pObj->wlrConnId);

    return (TAOS *)rid;
  } else {
    terrno = code;
  }

  WLR_API_END_CODE(&wlrTok, code);
  return NULL;
}

void taos_set_option(OPTIONS *options, const char *key, const char *value) {
  if (options == NULL || key == NULL || value == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    tscError("taos_set_option invalid parameter, options: %p, key: %p, value: %p", options, key, value);
    return;
  }

  size_t count = (size_t)options->count;
  size_t len = sizeof(options->keys) / sizeof(options->keys[0]);
  if (count >= len) {
    terrno = TSDB_CODE_INVALID_PARA;
    tscError("taos_set_option overflow, count: %zu, reached capacity: %zu", count, len);
    return;
  }

  options->keys[count] = key;
  options->values[count] = value;
  options->count = (uint16_t)(count + 1);
}

static int set_connection_option_or_close(TAOS *taos, TSDB_OPTION_CONNECTION option, const char *value) {
  if (value == NULL) return TSDB_CODE_SUCCESS;
  int code = taos_options_connection(taos, option, value);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("failed to set option(%d): %s", (int)option, value);
    taos_close(taos);
    terrno = code;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

TAOS *taos_connect_with(const OPTIONS *options) {
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t    port = 0;

  const char *charset = NULL;
  const char *timezone = NULL;
  const char *userIp = NULL;
  const char *userApp = NULL;
  const char *connectorInfo = NULL;

  typedef struct {
    const char             *key;
    TSDB_OPTION_CONNECTION  option;
    const char            **value;
  } SConnectOptionBinding;
  SConnectOptionBinding connectionOptions[] = {
      {"charset", TSDB_OPTION_CONNECTION_CHARSET, &charset},
      {"timezone", TSDB_OPTION_CONNECTION_TIMEZONE, &timezone},
      {"userIp", TSDB_OPTION_CONNECTION_USER_IP, &userIp},
      {"userApp", TSDB_OPTION_CONNECTION_USER_APP, &userApp},
      {"connectorInfo", TSDB_OPTION_CONNECTION_CONNECTOR_INFO, &connectorInfo},
  };
  const int32_t connectionOptionCount =
      (int32_t)(sizeof(connectionOptions) / sizeof(connectionOptions[0]));

  if (options && options->count > 0) {
    size_t count = (size_t)options->count;
    for (size_t i = 0; i < count; ++i) {
      const char *key = options->keys[i];
      const char *value = options->values[i];
      if (key == NULL || value == NULL) {
        tscWarn("taos_connect_with option key or value is NULL, index: %zu", i);
        continue;
      }

      if (strcmp(key, "ip") == 0) {
        ip = value;
      } else if (strcmp(key, "user") == 0) {
        user = value;
      } else if (strcmp(key, "pass") == 0) {
        pass = value;
      } else if (strcmp(key, "db") == 0) {
        db = value;
      } else if (strcmp(key, "port") == 0) {
        port = (uint16_t)taosStr2Int32(value, NULL, 10);
      } else {
        bool matched = false;
        for (int32_t j = 0; j < connectionOptionCount; ++j) {
          if (strcmp(key, connectionOptions[j].key) == 0) {
            *connectionOptions[j].value = value;
            matched = true;
            break;
          }
        }
        if (!matched) tscWarn("taos_connect_with unknown option key: %s", key);
      }
    }
  }

  /* The recorder is initialized by taos_init. As with taos_connect, it must run
   * before the BEGIN hook so the process's first connect_with call is captured. */
  (void)taos_init();

  /*
   * One event for the one call the application made. The inner taos_connect and
   * each set_connection_option_or_close are nested, so the depth rule suppresses
   * their own events (FS 4.1) -- which means every option the bag carried has to
   * ride along here, or replay would reconnect without them and silently
   * misinterpret every statement that followed (charset, timezone).
   *
   * Only the options actually present are recorded. A key absent from the bag is
   * not the same fact as a key explicitly set to NULL, and the inner helper skips
   * NULLs anyway.
   */
  SWlrConnOptIn wlrOpts[sizeof(connectionOptions) / sizeof(connectionOptions[0])];
  int32_t       wlrOptCount = 0;
  for (int32_t i = 0; i < connectionOptionCount; ++i) {
    if (*connectionOptions[i].value != NULL) {
      wlrOpts[wlrOptCount].type = connectionOptions[i].option;
      wlrOpts[wlrOptCount].name = connectionOptions[i].key;
      wlrOpts[wlrOptCount].value = *connectionOptions[i].value;
      ++wlrOptCount;
    }
  }

  uint8_t *wlrOptBuf = NULL;
  int32_t wlrOptLen = 0;
  bool wlrOptListOmitted = false;
  if (wlrOptCount > 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    const int32_t wlrOptCapacity = 4096;
    wlrOptBuf = taosMemoryMalloc(wlrOptCapacity);
    wlrOptLen = wlrOptBuf != NULL ? wlrEncodeConnOptList(wlrOptBuf, wlrOptCapacity, wlrOpts, wlrOptCount) : 0;
    if (wlrOptLen <= 0 || wlrOptLen > wlrOptCapacity) {
      tscWarn("WLR: connection option list exceeds capture capacity; recording an omission marker");
      wlrOptLen = 0;
      wlrOptListOmitted = true;
    }
  }

  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_CONNECT_WITH;
  wlrArgs.endpoint = ip;
  wlrArgs.user = user != NULL ? user : "root";
  wlrArgs.db = db;
  wlrArgs.port = port;
  wlrArgs.connOptList = (wlrOptLen > 0) ? wlrOptBuf : NULL;
  wlrArgs.connOptListLen = wlrOptLen;
  wlrArgs.connOptListOmitted = wlrOptListOmitted;
  (void)wlrApiBegin(&wlrArgs, &wlrTok);
  taosMemoryFree(wlrOptBuf);

  TAOS *taos = taos_connect(ip, user, pass, db, port);
  if (taos == NULL) {
    WLR_API_END_CODE(&wlrTok, terrno);
    return NULL;
  }

  /* The conn ID was minted by the inner taos_connect; read it back so this event's
   * RETURN carries the same one every later event on the connection references. */
  uint64_t wlrConnId = 0;
  STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
  if (pWlrObj != NULL) {
    wlrConnId = pWlrObj->wlrConnId;
    releaseTscObj(*(int64_t *)taos);
  }

  int32_t wlrCode = TSDB_CODE_SUCCESS;
  bool optionFailed = false;
  for (int32_t i = 0; i < connectionOptionCount; ++i) {
    if (set_connection_option_or_close(taos, connectionOptions[i].option, *connectionOptions[i].value) !=
        TSDB_CODE_SUCCESS) {
      optionFailed = true;
      break;
    }
  }
  if (optionFailed) {
    /* The helper already closed the connection, so there is nothing to return but
     * the failure -- and no live conn ID to report with it. */
    wlrCode = terrno;
    WLR_API_END_CODE(&wlrTok, wlrCode);
    return NULL;
  }

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = TSDB_CODE_SUCCESS;
    wlrRet.logicalConnId = wlrConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return taos;
}

TAOS *taos_connect_with_dsn(const char *dsn) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  tscError("taos_connect_with_dsn not supported");
  return NULL;
}

int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type) {
  if (taos == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }

  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->passInfo.fp = fp;
      pObj->passInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_WHITELIST_VER: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->whiteListInfo.fp = fp;
      pObj->whiteListInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_USER_DROPPED: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->userDroppedInfo.fp = fp;
      pObj->userDroppedInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_DATETIME_WHITELIST_VER: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->dateTimeWhiteListInfo.fp = fp;
      pObj->dateTimeWhiteListInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    case TAOS_NOTIFY_TOKEN: {
      TSC_ERR_RET(taosThreadMutexLock(&pObj->mutex));
      pObj->tokenNotifyInfo.fp = fp;
      pObj->tokenNotifyInfo.param = param;
      TSC_ERR_RET(taosThreadMutexUnlock(&pObj->mutex));
      break;
    }
    default: {
      terrno = TSDB_CODE_INVALID_PARA;
      releaseTscObj(*(int64_t *)taos);
      return terrno;
    }
  }

  releaseTscObj(*(int64_t *)taos);
  return 0;
}

typedef struct SFetchWhiteListInfo {
  int64_t                     connId;
  __taos_async_whitelist_fn_t userCbFn;
  void                       *userParam;
} SFetchWhiteListInfo;

int32_t fetchWhiteListCallbackFn(void *param, SDataBuf *pMsg, int32_t code) {
  SFetchWhiteListInfo *pInfo = (SFetchWhiteListInfo *)param;
  TAOS                *taos = &pInfo->connId;
  if (code != TSDB_CODE_SUCCESS) {
    pInfo->userCbFn(pInfo->userParam, code, taos, 0, NULL);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    return code;
  }

  SGetUserIpWhiteListRsp wlRsp;
  if (TSDB_CODE_SUCCESS != tDeserializeSGetUserIpWhiteListRsp(pMsg->pData, pMsg->len, &wlRsp)) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    tFreeSGetUserIpWhiteListRsp(&wlRsp);
    return terrno;
  }

  uint64_t *pWhiteLists = taosMemoryMalloc(wlRsp.numWhiteLists * sizeof(uint64_t));
  if (pWhiteLists == NULL) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    taosMemoryFree(pInfo);
    tFreeSGetUserIpWhiteListRsp(&wlRsp);
    return terrno;
  }

  for (int i = 0; i < wlRsp.numWhiteLists; ++i) {
    pWhiteLists[i] = ((uint64_t)wlRsp.pWhiteLists[i].mask << 32) | wlRsp.pWhiteLists[i].ip;
  }

  pInfo->userCbFn(pInfo->userParam, code, taos, wlRsp.numWhiteLists, pWhiteLists);

  taosMemoryFree(pWhiteLists);
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pInfo);
  tFreeSGetUserIpWhiteListRsp(&wlRsp);
  return code;
}

void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param) {
  if (NULL == taos) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    return;
  }

  int64_t connId = *(int64_t *)taos;

  STscObj *pTsc = acquireTscObj(connId);
  if (NULL == pTsc) {
    fp(param, TSDB_CODE_TSC_DISCONNECTED, taos, 0, NULL);
    return;
  }

  SGetUserWhiteListReq req;
  (void)memcpy(req.user, pTsc->user, TSDB_USER_LEN);
  int32_t msgLen = tSerializeSGetUserWhiteListReq(NULL, 0, &req);
  if (msgLen < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  void *pReq = taosMemoryMalloc(msgLen);
  if (pReq == NULL) {
    fp(param, terrno, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  if (tSerializeSGetUserWhiteListReq(pReq, msgLen, &req) < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  SFetchWhiteListInfo *pParam = taosMemoryMalloc(sizeof(SFetchWhiteListInfo));
  if (pParam == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pParam->connId = connId;
  pParam->userCbFn = fp;

  pParam->userParam = param;
  SMsgSendInfo *pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pSendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = msgLen, .handle = NULL};
  pSendInfo->requestId = generateRequestId();
  pSendInfo->requestObjRefId = 0;
  pSendInfo->param = pParam;
  pSendInfo->fp = fetchWhiteListCallbackFn;
  pSendInfo->msgType = TDMT_MND_GET_USER_IP_WHITELIST;

  SEpSet epSet = getEpSet_s(&pTsc->pAppInfo->mgmtEp);
  if (TSDB_CODE_SUCCESS != asyncSendMsgToServer(pTsc->pAppInfo->pTransporter, &epSet, NULL, pSendInfo)) {
    tscWarn("failed to async send msg to server");
  }
  releaseTscObj(connId);
  return;
}

typedef struct SFetchIpWhiteListInfo {
  int64_t connId;
  bool    supportNeg;
  void   *userParam;

  __taos_async_ip_whitelist_fn_t userCbFn;
} SFetchIpWhiteListInfo;

int32_t fetchIpWhiteListCallbackFn(void *param, SDataBuf *pMsg, int32_t code) {
  int32_t lino = 0;
  char  **pWhiteLists = NULL;

  SGetUserIpWhiteListRsp wlRsp = {0};

  SFetchIpWhiteListInfo *pInfo = (SFetchIpWhiteListInfo *)param;
  TAOS                  *taos = &pInfo->connId;

  if (code != TSDB_CODE_SUCCESS) {
    pInfo->userCbFn(pInfo->userParam, code, taos, 0, NULL);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  if ((code = tDeserializeSGetUserIpWhiteListDualRsp(pMsg->pData, pMsg->len, &wlRsp)) != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  pWhiteLists = taosMemoryMalloc(wlRsp.numWhiteLists * sizeof(char *));
  if (pWhiteLists == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  int32_t numWhiteLists = 0;
  for (int32_t i = 0; i < wlRsp.numWhiteLists; i++) {
    SIpRange *pIpRange = &wlRsp.pWhiteListsDual[i];
    if (!pInfo->supportNeg && pIpRange->neg) {
      continue;
    }
    SIpAddr ipAddr = {0};

    code = tIpUintToStr(pIpRange, &ipAddr);
    TAOS_CHECK_GOTO(code, &lino, _error);

    char *ip = taosMemCalloc(1, IP_RESERVE_CAP);
    if (ip == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
    if (ipAddr.type == 0) {
      if (pInfo->supportNeg) {
        snprintf(ip, IP_RESERVE_CAP, "%c %s/%d", pIpRange->neg ? '-' : '+', ipAddr.ipv4, ipAddr.mask);
      } else {
        snprintf(ip, IP_RESERVE_CAP, "%s/%d", ipAddr.ipv4, ipAddr.mask);
      }
    } else {
      if (ipAddr.ipv6[0] == 0) {
        memcpy(ipAddr.ipv6, "::", 2);
      }
      if (pInfo->supportNeg) {
        snprintf(ip, IP_RESERVE_CAP, "%c %s/%d", pIpRange->neg ? '-' : '+', ipAddr.ipv6, ipAddr.mask);
      } else {
        snprintf(ip, IP_RESERVE_CAP, "%s/%d", ipAddr.ipv6, ipAddr.mask);
      }
    }
    pWhiteLists[numWhiteLists++] = ip;
  }

  pInfo->userCbFn(pInfo->userParam, code, taos, numWhiteLists, pWhiteLists);
_error:
  if (pWhiteLists != NULL) {
    for (int32_t i = 0; i < numWhiteLists; i++) {
      taosMemFree(pWhiteLists[i]);
    }
    taosMemoryFree(pWhiteLists);
  }
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pInfo);
  tFreeSGetUserIpWhiteListDualRsp(&wlRsp);
  return code;
}

static void taosFetchIpWhiteList(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param, bool supportNeg) {
  if (NULL == taos) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    return;
  }
  int64_t connId = *(int64_t *)taos;

  STscObj *pTsc = acquireTscObj(connId);
  if (NULL == pTsc) {
    fp(param, TSDB_CODE_TSC_DISCONNECTED, taos, 0, NULL);
    return;
  }

  SGetUserWhiteListReq req;
  (void)memcpy(req.user, pTsc->user, TSDB_USER_LEN);
  int32_t msgLen = tSerializeSGetUserWhiteListReq(NULL, 0, &req);
  if (msgLen < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  void *pReq = taosMemoryMalloc(msgLen);
  if (pReq == NULL) {
    fp(param, terrno, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  if (tSerializeSGetUserWhiteListReq(pReq, msgLen, &req) < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  SFetchIpWhiteListInfo *pParam = taosMemoryMalloc(sizeof(SFetchIpWhiteListInfo));
  if (pParam == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pParam->connId = connId;
  pParam->supportNeg = supportNeg;
  pParam->userCbFn = fp;
  pParam->userParam = param;

  SMsgSendInfo *pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pSendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = msgLen, .handle = NULL};
  pSendInfo->requestId = generateRequestId();
  pSendInfo->requestObjRefId = 0;
  pSendInfo->param = pParam;
  pSendInfo->fp = fetchIpWhiteListCallbackFn;
  pSendInfo->msgType = TDMT_MND_GET_USER_IP_WHITELIST_DUAL;

  SEpSet epSet = getEpSet_s(&pTsc->pAppInfo->mgmtEp);
  if (TSDB_CODE_SUCCESS != asyncSendMsgToServer(pTsc->pAppInfo->pTransporter, &epSet, NULL, pSendInfo)) {
    tscWarn("failed to async send msg to server");
  }
  releaseTscObj(connId);
  return;
}

void taos_fetch_whitelist_dual_stack_a(TAOS *taos, __taos_async_whitelist_dual_stack_fn_t fp, void *param) {
  taosFetchIpWhiteList(taos, fp, param, false);
}

void taos_fetch_ip_whitelist_a(TAOS *taos, __taos_async_ip_whitelist_fn_t fp, void *param) {
  taosFetchIpWhiteList(taos, fp, param, true);
}

typedef struct SFetchDateTimeWhiteListInfo {
  int64_t                              connId;
  void                                *userParam;
  __taos_async_datetime_whitelist_fn_t userCbFn;
} SFetchDateTimeWhiteListInfo;

static const char *weekdays[] = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
int32_t            fetchDateTimeWhiteListCallbackFn(void *param, SDataBuf *pMsg, int32_t code) {
  int32_t lino = 0;
  char  **pWhiteLists = NULL;

  SUserDateTimeWhiteList wlRsp = {0};

  SFetchDateTimeWhiteListInfo *pInfo = (SFetchDateTimeWhiteListInfo *)param;
  TAOS                        *taos = &pInfo->connId;

  if (code != TSDB_CODE_SUCCESS) {
    pInfo->userCbFn(pInfo->userParam, code, taos, 0, NULL);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  if ((code = tDeserializeSUserDateTimeWhiteList(pMsg->pData, pMsg->len, &wlRsp)) != TSDB_CODE_SUCCESS) {
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  pWhiteLists = taosMemoryMalloc(wlRsp.numWhiteLists * sizeof(char *));
  if (pWhiteLists == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  int32_t numWhiteLists = 0;
  for (int32_t i = 0; i < wlRsp.numWhiteLists; i++) {
    SDateTimeWhiteListItem *item = &wlRsp.pWhiteLists[i];

    char *p = taosMemCalloc(1, 128);
    if (p == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }

    int duration = item->duration / 60;

    if (item->absolute) {
      struct STm tm;
      (void)taosTs2Tm(item->start, TSDB_TIME_PRECISION_SECONDS, &tm, NULL);
      snprintf(p, 128, "%c %04d-%02d-%02d %02d:%02d %d", item->neg ? '-' : '+', tm.tm.tm_year + 1900, tm.tm.tm_mon + 1,
                          tm.tm.tm_mday, tm.tm.tm_hour, tm.tm.tm_min, duration);
    } else {
      int day = item->start / 86400;
      int hour = (item->start % 86400) / 3600;
      int minute = (item->start % 3600) / 60;
      snprintf(p, 128, "%c %s %02d:%02d %d", item->neg ? '-' : '+', weekdays[day], hour, minute, duration);
    }
    pWhiteLists[numWhiteLists++] = p;
  }

  pInfo->userCbFn(pInfo->userParam, code, taos, numWhiteLists, pWhiteLists);
_error:
  if (pWhiteLists != NULL) {
    for (int32_t i = 0; i < numWhiteLists; i++) {
      taosMemFree(pWhiteLists[i]);
    }
    taosMemoryFree(pWhiteLists);
  }
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pInfo);
  tFreeSUserDateTimeWhiteList(&wlRsp);
  return code;
}

void taos_fetch_datetime_whitelist_a(TAOS *taos, __taos_async_datetime_whitelist_fn_t fp, void *param) {
  if (NULL == taos) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    return;
  }
  int64_t connId = *(int64_t *)taos;

  STscObj *pTsc = acquireTscObj(connId);
  if (NULL == pTsc) {
    fp(param, TSDB_CODE_TSC_DISCONNECTED, taos, 0, NULL);
    return;
  }

  SGetUserWhiteListReq req;
  (void)memcpy(req.user, pTsc->user, TSDB_USER_LEN);
  int32_t msgLen = tSerializeSGetUserWhiteListReq(NULL, 0, &req);
  if (msgLen < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  void *pReq = taosMemoryMalloc(msgLen);
  if (pReq == NULL) {
    fp(param, terrno, taos, 0, NULL);
    releaseTscObj(connId);
    return;
  }

  if (tSerializeSGetUserWhiteListReq(pReq, msgLen, &req) < 0) {
    fp(param, TSDB_CODE_INVALID_PARA, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  SFetchDateTimeWhiteListInfo *pParam = taosMemoryMalloc(sizeof(SFetchDateTimeWhiteListInfo));
  if (pParam == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pParam->connId = connId;
  pParam->userCbFn = fp;
  pParam->userParam = param;

  SMsgSendInfo *pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) {
    fp(param, terrno, taos, 0, NULL);
    taosMemoryFree(pParam);
    taosMemoryFree(pReq);
    releaseTscObj(connId);
    return;
  }

  pSendInfo->msgInfo = (SDataBuf){.pData = pReq, .len = msgLen, .handle = NULL};
  pSendInfo->requestId = generateRequestId();
  pSendInfo->requestObjRefId = 0;
  pSendInfo->param = pParam;
  pSendInfo->fp = fetchDateTimeWhiteListCallbackFn;
  pSendInfo->msgType = TDMT_MND_GET_USER_DATETIME_WHITELIST;

  SEpSet epSet = getEpSet_s(&pTsc->pAppInfo->mgmtEp);
  if (TSDB_CODE_SUCCESS != asyncSendMsgToServer(pTsc->pAppInfo->pTransporter, &epSet, NULL, pSendInfo)) {
    tscWarn("failed to async send msg to server");
  }
  releaseTscObj(connId);
  return;
}

void taos_close_internal(void *taos) {
  if (taos == NULL) {
    return;
  }
  int32_t code = 0;

  STscObj *pTscObj = (STscObj *)taos;
  tscDebug("conn:0x%" PRIx64 ", try to close connection, numOfReq:%d", pTscObj->id, pTscObj->numOfReqs);

  SSessParam para = {.type = SESSION_PER_USER, .value = -1, .noCheck = 1};

  code = tscUpdateSessMetric(pTscObj, &para);
  if (code != TSDB_CODE_SUCCESS) {
    tscWarn("conn:0x%" PRIx64 ", failed to update user:%s metric when close connection, code:%d", pTscObj->id,
            pTscObj->user, code);
  }

  code = tscUnrefSessMetric(pTscObj);
  if (TSDB_CODE_SUCCESS != taosRemoveRef(clientConnRefPool, pTscObj->id)) {
    tscError("conn:0x%" PRIx64 ", failed to remove ref from conn pool", pTscObj->id);
  }
}

void taos_close(TAOS *taos) {
  if (taos == NULL) {
    return;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    taosMemoryFree(taos);
    return;
  }

  /* Read the logical ID while the object is still alive: after
   * taos_close_internal it is gone. Recording close matters for replay because a
   * connection's lifetime bounds which calls may share it (DS 6.7). */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_CLOSE;
  wlrArgs.logicalConnId = pObj->wlrConnId;
  (void)wlrApiBegin(&wlrArgs, &wlrTok);

  taos_close_internal(pObj);
  releaseTscObj(*(int64_t *)taos);
  taosMemoryFree(taos);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

/**
 * @brief Begin a client-side transaction.
 *
 * Pre-flight check: acquire the connection mutex, verify no transaction is
 * already in progress (txnState == IDLE), then mark txnState as
 * UTXN_STAGE_BEGIN_PENDING before releasing the mutex.  This sentinel blocks
 * any concurrent taos_txn_begin() call on the same connection before the RPC
 * round-trip completes.
 *
 * The actual BEGIN message is sent via taos_query("BEGIN"), which goes through
 * the normal SQL parser -> execDdlQuery -> processBeginTxnRsp path - the same
 * path used when the user issues `taos_query(conn, "BEGIN")` directly.  On
 * success processBeginTxnRsp promotes txnState to UTXN_STAGE_ACTIVE; on
 * failure it resets txnState back to UTXN_STAGE_IDLE.
 */
#ifdef TD_ENTERPRISE
/*
 * Open a recording token for a txn boundary call.
 *
 * All three boundaries reach the server through taos_query("BEGIN"/"COMMIT"/
 * "ROLLBACK"). Without a hook here that inner query is what gets recorded, so the
 * file shows a plain QUERY and loses which txn API the application called. With
 * one, the inner query and its taos_free_result see depth > 0 and stay silent
 * (FS 4.1, TS RE001).
 */
static void wlrTxnBegin(TAOS *taos, uint16_t wlrApiType, SWorkloadCallToken *pTok, uint64_t *pConnId) {
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = wlrApiType;
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  *pConnId = wlrArgs.logicalConnId;
  (void)wlrApiBegin(&wlrArgs, pTok);
}

static int wlrTxnEnd(SWorkloadCallToken *pTok, int32_t code, uint64_t connId) {
  if (pTok->recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = code;
    wlrRet.logicalConnId = connId;
    wlrApiReturn(pTok, &wlrRet);
  } else {
    wlrApiReturn(pTok, NULL);
  }
  return code;
}
#endif

int taos_txn_begin(TAOS *taos) {
#ifdef TD_ENTERPRISE
  if (taos == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SWorkloadCallToken wlrTok = {0};
  uint64_t           wlrConnId = 0;
  wlrTxnBegin(taos, WLR_API_TXN_BEGIN, &wlrTok, &wlrConnId);

  int64_t  connId = *(int64_t *)taos;
  STscObj *pTscObj = acquireTscObj(connId);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return wlrTxnEnd(&wlrTok, terrno, wlrConnId);
  }

  // Check-and-reserve under mutex: prevents concurrent BEGIN attempts.
  (void)taosThreadMutexLock(&pTscObj->mutex);
  if (pTscObj->txnState != UTXN_STAGE_IDLE) {
    (void)taosThreadMutexUnlock(&pTscObj->mutex);
    releaseTscObj(connId);
    return wlrTxnEnd(&wlrTok, TSDB_CODE_TXN_ALREADY_IN_PROGRESS, wlrConnId);
  }
  atomic_store_8(&pTscObj->txnState, UTXN_STAGE_BEGIN_PENDING);
  (void)taosThreadMutexUnlock(&pTscObj->mutex);
  releaseTscObj(connId);

  // Delegate to the SQL path; processBeginTxnRsp handles the full state
  // transition (IDLE on failure, ACTIVE on success).
  TAOS_RES *res = taos_query(taos, "BEGIN");
  int32_t   code = taos_errno(res);
  taos_free_result(res);
  return wlrTxnEnd(&wlrTok, code, wlrConnId);
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

#ifdef TD_ENTERPRISE
// Common implementation for COMMIT and ROLLBACK: validates txn state then
// issues the SQL via taos_query (which handles the async MNode STrans response).
static int tscTxnEndImpl(TAOS *taos, const char *sql, uint16_t wlrApiType) {
  if (taos == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SWorkloadCallToken wlrTok = {0};
  uint64_t           wlrConnId = 0;
  wlrTxnBegin(taos, wlrApiType, &wlrTok, &wlrConnId);

  int64_t  connId = *(int64_t *)taos;
  STscObj *pTscObj = acquireTscObj(connId);
  if (NULL == pTscObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s, sql:%s", __func__, sql);
    return wlrTxnEnd(&wlrTok, terrno, wlrConnId);
  }

  (void)taosThreadMutexLock(&pTscObj->mutex);

  // If the MNode forcibly rolled back this txn due to inactivity timeout, handle locally.
  if (pTscObj->txnState == UTXN_STAGE_TIMEOUT_KILLED) {
    bool isRollback = (strcmp(sql, "ROLLBACK") == 0);
    (void)taosThreadMutexUnlock(&pTscObj->mutex);
    if (isRollback) {
      // Local cleanup only - do not send ROLLBACK RPC to MNode since it already
      // completed the rollback.  Just clear client-side txn state.
      tscResetTxnState(pTscObj);
      releaseTscObj(connId);
      return wlrTxnEnd(&wlrTok, 0, wlrConnId);
    }
    // COMMIT (or any other end-of-txn call) is rejected: the txn is already gone.
    releaseTscObj(connId);
    return wlrTxnEnd(&wlrTok, TSDB_CODE_TXN_TIMEOUT_KILLED, wlrConnId);
  }

  if (pTscObj->txnState != UTXN_STAGE_ACTIVE) {
    tscWarn("conn:0x%" PRIx64 ", txn:%" PRIu64 " not in progress, current state:%d, sql:%s", pTscObj->id,
            pTscObj->txnId, pTscObj->txnState, sql);
    (void)taosThreadMutexUnlock(&pTscObj->mutex);
    releaseTscObj(connId);
    return wlrTxnEnd(&wlrTok, TSDB_CODE_TXN_NOT_IN_PROGRESS, wlrConnId);
  }
  (void)taosThreadMutexUnlock(&pTscObj->mutex);
  releaseTscObj(connId);

  TAOS_RES *res = taos_query(taos, sql);
  int32_t   code = taos_errno(res);
  taos_free_result(res);
  return wlrTxnEnd(&wlrTok, code, wlrConnId);
}
#endif

int taos_txn_commit(TAOS *taos) {
#ifdef TD_ENTERPRISE
  return tscTxnEndImpl(taos, "COMMIT", WLR_API_TXN_COMMIT);
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int taos_txn_rollback(TAOS *taos) {
#ifdef TD_ENTERPRISE
  return tscTxnEndImpl(taos, "ROLLBACK", WLR_API_TXN_ROLLBACK);
#else
  return TSDB_CODE_OPS_NOT_SUPPORT;
#endif
}

int taos_errno(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return terrno;
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    return 0;
  }

  return ((SRequestObj *)res)->code;
}

const char *taos_errstr(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    if (*(taosGetErrMsg()) == 0) {
      return (const char *)tstrerror(terrno);
    } else {
      (void)snprintf(taosGetErrMsgReturn(), ERR_MSG_LEN, "%s", taosGetErrMsg());
      return (const char *)taosGetErrMsgReturn();
    }
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    return "success";
  }

  SRequestObj *pRequest = (SRequestObj *)res;
  if (NULL != pRequest->msgBuf && (strlen(pRequest->msgBuf) > 0 || pRequest->code == TSDB_CODE_RPC_FQDN_ERROR)) {
    return pRequest->msgBuf;
  } else {
    return (const char *)tstrerror(pRequest->code);
  }
}

void taos_free_result(TAOS_RES *res) {
  if (NULL == res) {
    return;
  }

  tscTrace("res:%p, will be freed", res);

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    tscDebug("QID:0x%" PRIx64 ", call taos_free_result to free query, res:%p", pRequest->requestId, res);

    // Read the logical ID before the request is destroyed; afterwards the struct
    // is invalid. A zero ID means this result was never handed to the user (an
    // internal request being cleaned up), so there is nothing to record.
    uint64_t wlrResId = pRequest->wlrResultId;
    SWorkloadCallToken wlrTok = {0};
    if (wlrResId != 0) {
      SWorkloadBeginArgs wlrArgs = {0};
      wlrArgs.apiType = WLR_API_FREE_RESULT;
      wlrArgs.logicalResultId = wlrResId;
      (void)wlrApiBegin(&wlrArgs, &wlrTok);
    }

    destroyRequest(pRequest);

    if (wlrTok.recorded) {
      SWorkloadReturnArgs wlrRet = {0};
      wlrRet.logicalResultId = wlrResId;
      wlrApiReturn(&wlrTok, &wlrRet);
      wlrResultFree(wlrResId);
    } else {
      wlrApiReturn(&wlrTok, NULL);
    }
    return;
  }

  SMqRspObj *pRsp = (SMqRspObj *)res;
  if (TD_RES_TMQ(res)) {
    tDeleteMqDataRsp(&pRsp->dataRsp);
    doFreeReqResultInfo(&pRsp->resInfo);
  } else if (TD_RES_TMQ_METADATA(res)) {
    tDeleteSTaosxRsp(&pRsp->dataRsp);
    doFreeReqResultInfo(&pRsp->resInfo);
  } else if (TD_RES_TMQ_META(res)) {
    tDeleteMqMetaRsp(&pRsp->metaRsp);
  } else if (TD_RES_TMQ_BATCH_META(res)) {
    tDeleteMqBatchMetaRsp(&pRsp->batchMetaRsp);
  } else if (TD_RES_TMQ_RAW(res)) {
    tDeleteMqRawDataRsp(&pRsp->dataRsp);
  }
  taosMemoryFree(pRsp);
}

void taos_kill_query(TAOS *taos) {
  if (NULL == taos) {
    return;
  }

  int64_t  rid = *(int64_t *)taos;
  STscObj *pTscObj = acquireTscObj(rid);

  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  if (wlrRecorderState() == WLR_STATE_RUNNING) {
    /* Part of the workload, not incidental: a client that cancels its own in-flight
     * queries stresses the server differently from one that waits for them. */
    wlrArgs.apiType = WLR_API_KILL_QUERY;
    if (pTscObj != NULL) wlrArgs.logicalConnId = pTscObj->wlrConnId;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  if (pTscObj) {
    stopAllRequests(pTscObj->pRequests);
  }
  releaseTscObj(rid);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else if (wlrRecorderState() == WLR_STATE_RUNNING) {
    wlrApiReturn(&wlrTok, NULL);
  }
}

int taos_field_count(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->numOfCols;
}

int taos_num_fields(TAOS_RES *res) { return taos_field_count(res); }

TAOS_FIELD *taos_fetch_fields(TAOS_RES *res) {
  if (taos_num_fields(res) == 0 || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->userFields;
}

TAOS_RES *taos_query(TAOS *taos, const char *sql) {
  /* The logical connection ID must be on the BEGIN frame, not just RETURN: a query
   * that never returns produces no RETURN at all, and that hung case is the whole
   * reason this feature exists. Without it replay could not tell which connection
   * to reissue the statement on. Resolving it costs a ref acquire, paid only while
   * recording is on. */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_QUERY;
  wlrArgs.sql = sql;
  wlrArgs.sqlLen = (sql == NULL) ? 0 : (int32_t)strlen(sql);
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  if (wlrRecorderState() == WLR_STATE_RUNNING && wlrArgs.logicalConnId != 0) {
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  TAOS_RES *res = taosQueryImpl(taos, sql, false, TD_REQ_FROM_APP);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = taos_errno(res);
    /* Carried from the BEGIN args rather than read off the result: CONN_ID is
     * required on a query RETURN (kQueryReturnTags), and a failed query has no
     * SRequestObj to take it from. Leaving it at 0 omitted the tag and made every
     * failed query's RETURN fail its own schema. The result branch below overwrites
     * it with the same value when a request does exist. */
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    // Mint the result ID only for a real query result: TMQ and raw result types
    // are out of scope this phase and must not consume IDs.
    if (res != NULL && TD_RES_QUERY(res)) {
      SRequestObj *pReq = (SRequestObj *)res;
      if (pReq->wlrResultId == 0) pReq->wlrResultId = wlrNextResultId();
      wlrRet.logicalResultId = pReq->wlrResultId;
      wlrRet.qid = (int64_t)pReq->requestId;
      wlrRet.affectedRows = taos_affected_rows64(res);
      if (pReq->pTscObj != NULL) wlrRet.logicalConnId = pReq->pTscObj->wlrConnId;
    }
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return res;
}
TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqid) {
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_QUERY_WITH_REQID;
  wlrArgs.sql = sql;
  wlrArgs.sqlLen = (sql == NULL) ? 0 : (int32_t)strlen(sql);
  /* Preserved rather than regenerated: the caller chose this ID to correlate its own
   * logs with server traces, and replacing it would break that correspondence. */
  wlrArgs.reqid = reqid;
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  if (wlrRecorderState() == WLR_STATE_RUNNING && wlrArgs.logicalConnId != 0) {
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  TAOS_RES *res = taosQueryImplWithReqid(taos, sql, false, reqid);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = taos_errno(res);
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    if (res != NULL && TD_RES_QUERY(res)) {
      SRequestObj *pReq = (SRequestObj *)res;
      if (pReq->wlrResultId == 0) pReq->wlrResultId = wlrNextResultId();
      wlrRet.logicalResultId = pReq->wlrResultId;
      wlrRet.qid = (int64_t)pReq->requestId;
      wlrRet.affectedRows = taos_affected_rows64(res);
      if (pReq->pTscObj != NULL) wlrRet.logicalConnId = pReq->pTscObj->wlrConnId;
    }
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return res;
}

TAOS_FIELD_E *taos_fetch_fields_e(TAOS_RES *res) {
  if (taos_num_fields(res) == 0 || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }
  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->fields;
}

TAOS_ROW taos_fetch_row_impl(TAOS_RES *res) {
  if (res == NULL) {
    return NULL;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    if (pRequest->killed) {
      tscInfo("query has been killed, cannot fetch more rows.");
      pRequest->code = TSDB_CODE_TSC_QUERY_KILLED;
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
      return NULL;
    }

    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
        CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
      }
      return NULL;
    }

    if (pRequest->inCallback) {
      tscError("cannot call taos_fetch_row before query callback ends.");
      terrno = TSDB_CODE_TSC_INVALID_OPERATION;
      return NULL;
    }

    return doAsyncFetchRows(pRequest, true, true);
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SMqRspObj      *msg = ((SMqRspObj *)res);
    SReqResultInfo *pResultInfo = NULL;
    if (msg->resIter == -1) {
      if (tmqGetNextResInfo(res, true, &pResultInfo) != 0) {
        return NULL;
      }
    } else {
      pResultInfo = tmqGetCurResInfo(res);
    }

    if (pResultInfo->current < pResultInfo->numOfRows) {
      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    } else {
      if (tmqGetNextResInfo(res, true, &pResultInfo) != 0) {
        return NULL;
      }

      doSetOneRowPtr(pResultInfo);
      pResultInfo->current += 1;
      return pResultInfo->row;
    }
  } else if (TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  } else {
    tscError("invalid result passed to taos_fetch_row");
    terrno = TSDB_CODE_TMQ_INVALID_DATA;
    return NULL;
  }
}

int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  return taos_print_row_with_size(str, INT32_MAX, row, fields, num_fields);
}
int taos_print_row_with_size(char *str, uint32_t size, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int32_t len = 0;
  for (int i = 0; i < num_fields; ++i) {
    // the buffer holds size - 1 characters at most plus the terminator: stop
    // before writing anything once it is full. len may already have jumped
    // past size through the return value of snprintf(), and the "size - len"
    // below would underflow in that case
    if ((uint32_t)len + 1 >= size) {
      break;
    }

    if (i > 0) {
      str[len++] = ' ';
    }

    if (row[i] == NULL) {
      len += snprintf(str + len, size - len, "%s", TSDB_DATA_NULL_STR);
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += snprintf(str + len, size - len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += snprintf(str + len, size - len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += snprintf(str + len, size - len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += snprintf(str + len, size - len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += snprintf(str + len, size - len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += snprintf(str + len, size - len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += snprintf(str + len, size - len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += snprintf(str + len, size - len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += snprintf(str + len, size - len, "%.*g", FLT_DIG, fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += snprintf(str + len, size - len, "%.*g", DBL_DIG, dv);
      } break;

      case TSDB_DATA_TYPE_VARBINARY: {
        void    *data = NULL;
        uint32_t tmp = 0;
        int32_t  charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (taosAscii2Hex(row[i], charLen, &data, &tmp) < 0) {
          break;
        }
        uint32_t copyLen = TMIN(size - len - 1, tmp);
        (void)memcpy(str + len, data, copyLen);
        len += copyLen;
        taosMemoryFree(data);
      } break;
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_GEOMETRY: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY || fields[i].type == TSDB_DATA_TYPE_VARBINARY ||
            fields[i].type == TSDB_DATA_TYPE_GEOMETRY) {
          if (charLen > fields[i].bytes || charLen < 0) {
            tscError("taos_print_row error binary. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        } else {
          if (charLen > fields[i].bytes * TSDB_NCHAR_SIZE || charLen < 0) {
            tscError("taos_print_row error. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        }

        uint32_t copyLen = TMIN(size - len - 1, charLen);
        (void)memcpy(str + len, row[i], copyLen);
        len += copyLen;
      } break;
      case TSDB_DATA_TYPE_BLOB:
      case TSDB_DATA_TYPE_MEDIUMBLOB: {
        void    *data = NULL;
        uint32_t tmp = 0;
        int32_t  charLen = blobDataLen((char *)row[i] - BLOBSTR_HEADER_SIZE);
        if (taosAscii2Hex(row[i], charLen, &data, &tmp) < 0) {
          break;
        }

        uint32_t copyLen = TMIN(size - len - 1, tmp);
        (void)memcpy(str + len, data, copyLen);
        len += copyLen;

        taosMemoryFree(data);
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += snprintf(str + len, size - len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += snprintf(str + len, size - len, "%d", *((int8_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_DECIMAL64:
      case TSDB_DATA_TYPE_DECIMAL: {
        uint32_t decimalLen = strlen(row[i]);
        uint32_t copyLen = TMIN(size - len - 1, decimalLen);
        (void)memcpy(str + len, row[i], copyLen);
        len += copyLen;
      } break;
      default:
        break;
    }
  }
  if (len < size) {
    str[len] = 0;
  }

  return len;
}

int *taos_fetch_lengths(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return pResInfo->length;
}

TAOS_ROW *taos_result_block(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  if (taos_is_update_query(res)) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  return &pResInfo->row;
}

// todo intergrate with tDataTypes
const char *taos_data_type(int type) {
  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      return "TSDB_DATA_TYPE_NULL";
    case TSDB_DATA_TYPE_BOOL:
      return "TSDB_DATA_TYPE_BOOL";
    case TSDB_DATA_TYPE_TINYINT:
      return "TSDB_DATA_TYPE_TINYINT";
    case TSDB_DATA_TYPE_SMALLINT:
      return "TSDB_DATA_TYPE_SMALLINT";
    case TSDB_DATA_TYPE_INT:
      return "TSDB_DATA_TYPE_INT";
    case TSDB_DATA_TYPE_BIGINT:
      return "TSDB_DATA_TYPE_BIGINT";
    case TSDB_DATA_TYPE_FLOAT:
      return "TSDB_DATA_TYPE_FLOAT";
    case TSDB_DATA_TYPE_DOUBLE:
      return "TSDB_DATA_TYPE_DOUBLE";
    case TSDB_DATA_TYPE_VARCHAR:
      return "TSDB_DATA_TYPE_VARCHAR";
      //    case TSDB_DATA_TYPE_BINARY:          return "TSDB_DATA_TYPE_VARCHAR";
    case TSDB_DATA_TYPE_TIMESTAMP:
      return "TSDB_DATA_TYPE_TIMESTAMP";
    case TSDB_DATA_TYPE_NCHAR:
      return "TSDB_DATA_TYPE_NCHAR";
    case TSDB_DATA_TYPE_JSON:
      return "TSDB_DATA_TYPE_JSON";
    case TSDB_DATA_TYPE_GEOMETRY:
      return "TSDB_DATA_TYPE_GEOMETRY";
    case TSDB_DATA_TYPE_UTINYINT:
      return "TSDB_DATA_TYPE_UTINYINT";
    case TSDB_DATA_TYPE_USMALLINT:
      return "TSDB_DATA_TYPE_USMALLINT";
    case TSDB_DATA_TYPE_UINT:
      return "TSDB_DATA_TYPE_UINT";
    case TSDB_DATA_TYPE_UBIGINT:
      return "TSDB_DATA_TYPE_UBIGINT";
    case TSDB_DATA_TYPE_VARBINARY:
      return "TSDB_DATA_TYPE_VARBINARY";
    case TSDB_DATA_TYPE_DECIMAL:
      return "TSDB_DATA_TYPE_DECIMAL";
    case TSDB_DATA_TYPE_BLOB:
      return "TSDB_DATA_TYPE_BLOB";
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      return "TSDB_DATA_TYPE_MEDIUMBLOB";
    default:
      return "UNKNOWN";
  }
}

const char *taos_get_client_info() { return td_version; }

// return int32_t
int taos_affected_rows(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return (int)pResInfo->numOfRows;
}

// return int64_t
int64_t taos_affected_rows64(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  SRequestObj    *pRequest = (SRequestObj *)res;
  SReqResultInfo *pResInfo = &pRequest->body.resInfo;
  return pResInfo->numOfRows;
}

int taos_result_precision(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return TSDB_TIME_PRECISION_MILLI;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    return pRequest->body.resInfo.precision;
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *info = tmqGetCurResInfo(res);
    return info->precision;
  }
  return TSDB_TIME_PRECISION_MILLI;
}

int taos_select_db(TAOS *taos, const char *db) {
  if (taos == NULL || db == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  /* Records one select_db event. The internal taos_query("use db") and its
   * taos_free_result see depth > 0 and record nothing, so replay issues one call
   * instead of three (FS 4.1, TS RE001). */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_SELECT_DB;
  wlrArgs.db = db;
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  (void)wlrApiBegin(&wlrArgs, &wlrTok);

  int32_t wlrCode = taos_select_db_impl(taos, db);
  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = wlrCode;
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

static int taos_select_db_impl(TAOS *taos, const char *db) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (pObj == NULL) {
    releaseTscObj(*(int64_t *)taos);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  if (db == NULL || strlen(db) == 0) {
    releaseTscObj(*(int64_t *)taos);
    tscError("invalid parameter for %s", db == NULL ? "db is NULL" : "db is empty");
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  char sql[256] = {0};
  (void)snprintf(sql, tListLen(sql), "use %s", db);

  TAOS_RES *pRequest = taos_query(taos, sql);
  int32_t   code = taos_errno(pRequest);

  taos_free_result(pRequest);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

void taos_stop_query(TAOS_RES *res) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_METADATA(res) ||
      TD_RES_TMQ_BATCH_META(res)) {
    return;
  }

  uint64_t wlrResId = TD_RES_QUERY(res) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  if (wlrRecorderState() == WLR_STATE_RUNNING) {
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STOP_QUERY;
    wlrArgs.logicalResultId = wlrResId;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  stopAllQueries((SRequestObj *)res);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = 0;
    wlrRet.logicalResultId = wlrResId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return true;
  }
  SReqResultInfo *pResultInfo = tscGetCurResInfo(res);
  if (col >= pResultInfo->numOfCols || col < 0 || row >= pResultInfo->numOfRows || row < 0) {
    return true;
  }

  SResultColumn *pCol = &pResultInfo->pCol[col];
  if (IS_VAR_DATA_TYPE(pResultInfo->fields[col].type)) {
    return (pCol->offset[row] == -1);
  } else {
    return colDataIsNull_f(pCol, row);
  }
}

bool taos_is_update_query(TAOS_RES *res) { return taos_num_fields(res) == 0; }

/*
 * Public entry. The body lives in taos_fetch_row_impl so internal callers can use
 * it without producing an event: postSubQueryFetchCb runs on a driver completion
 * thread where TLS depth is 0, so the depth rule cannot suppress it and only an
 * internal entry point can (DS 6.6, TS RE014).
 */
TAOS_ROW taos_fetch_row(TAOS_RES *res) {
  uint64_t wlrResId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_ROW, wlrResId);

  TAOS_ROW row = taos_fetch_row_impl(res);

  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, taos_errno(res), wlrResId, row != NULL ? 1 : 0);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return row;
}

int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows) {
  uint64_t           wlrResId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_BLOCK, wlrResId);

  int32_t numOfRows = 0;
  int32_t wlrCode =
      (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res))
          ? 0
          : taos_fetch_block_s_impl(res, &numOfRows, rows);

  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, wlrCode, wlrResId, numOfRows);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }

  /* Set last: this is the call's only error channel, and the recording path
   * above runs code of its own that could otherwise clobber it. */
  terrno = wlrCode;
  return numOfRows;
}

int taos_fetch_block_s(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  /* taos_fetch_block calls through here, so when the user entered via that symbol
   * this begin sees depth > 0 and records nothing -- the outer call already did
   * (FS 4.1). */
  uint64_t wlrResId = TD_RES_QUERY(res) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_BLOCK_S, wlrResId);
  int32_t wlrCode = taos_fetch_block_s_impl(res, numOfRows, rows);
  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, wlrCode, wlrResId, numOfRows != NULL ? *numOfRows : 0);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

static int taos_fetch_block_s_impl(TAOS_RES *res, int *numOfRows, TAOS_ROW *rows) {
  if (res == NULL || numOfRows == NULL || rows == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (TD_RES_QUERY(res)) {
    SRequestObj *pRequest = (SRequestObj *)res;
    (*rows) = NULL;
    (*numOfRows) = 0;

    if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
        pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
      return pRequest->code;
    }

    (void)doAsyncFetchRows(pRequest, false, true);

    // TODO refactor
    SReqResultInfo *pResultInfo = &pRequest->body.resInfo;
    pResultInfo->current = pResultInfo->numOfRows;

    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return pRequest->code;
  } else if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *pResultInfo = NULL;
    int32_t         code = tmqGetNextResInfo(res, true, &pResultInfo);
    if (code != 0) return code;

    pResultInfo->current = pResultInfo->numOfRows;
    (*rows) = pResultInfo->row;
    (*numOfRows) = pResultInfo->numOfRows;
    return 0;
  } else {
    tscError("taos_fetch_block_s invalid res type");
    return TSDB_CODE_TMQ_INVALID_DATA;
  }
}

int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData) {
  uint64_t wlrResId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_RAW_BLOCK, wlrResId);
  int32_t wlrCode = taos_fetch_raw_block_impl(res, numOfRows, pData);
  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, wlrCode, wlrResId, numOfRows != NULL ? *numOfRows : 0);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

static int taos_fetch_raw_block_impl(TAOS_RES *res, int *numOfRows, void **pData) {
  if (numOfRows == NULL || pData == NULL) return TSDB_CODE_INVALID_PARA;
  *numOfRows = 0;
  *pData = NULL;

  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return 0;
  }

  if (TD_RES_TMQ(res) || TD_RES_TMQ_METADATA(res)) {
    SReqResultInfo *pResultInfo = NULL;
    int32_t         code = tmqGetNextResInfo(res, false, &pResultInfo);
    if (code != 0) {
      (*numOfRows) = 0;
      return 0;
    }

    pResultInfo->current = pResultInfo->numOfRows;
    (*numOfRows) = pResultInfo->numOfRows;
    (*pData) = (void *)pResultInfo->pData;
    return 0;
  }

  SRequestObj *pRequest = (SRequestObj *)res;

  if (pRequest->type == TSDB_SQL_RETRIEVE_EMPTY_RESULT || pRequest->type == TSDB_SQL_INSERT ||
      pRequest->code != TSDB_CODE_SUCCESS || taos_num_fields(res) == 0) {
    if (pRequest->code == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
    }
    return pRequest->code;
  }

  (void)doAsyncFetchRows(pRequest, false, false);

  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  pResultInfo->current = pResultInfo->numOfRows;
  (*numOfRows) = pResultInfo->numOfRows;
  (*pData) = (void *)pResultInfo->pData;

  return pRequest->code;
}

int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex) {
  if (res == NULL || TD_RES_TMQ_RAW(res) || TD_RES_TMQ_META(res) || TD_RES_TMQ_BATCH_META(res)) {
    return NULL;
  }

  int32_t numOfFields = taos_num_fields(res);
  if (columnIndex < 0 || columnIndex >= numOfFields || numOfFields == 0) {
    return NULL;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  TAOS_FIELD     *pField = &pResInfo->userFields[columnIndex];
  if (!IS_VAR_DATA_TYPE(pField->type)) {
    return NULL;
  }

  return pResInfo->pCol[columnIndex].offset;
}

int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows) {
  if (res == NULL || result == NULL || rows == NULL || *rows <= 0 || columnIndex < 0 || TD_RES_TMQ_META(res) ||
      TD_RES_TMQ_RAW(res) || TD_RES_TMQ_BATCH_META(res)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t numOfFields = taos_num_fields(res);
  if (columnIndex >= numOfFields || numOfFields == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SReqResultInfo *pResInfo = tscGetCurResInfo(res);
  TAOS_FIELD     *pField = &pResInfo->userFields[columnIndex];
  SResultColumn  *pCol = &pResInfo->pCol[columnIndex];

  if (*rows > pResInfo->numOfRows) {
    *rows = pResInfo->numOfRows;
  }
  if (IS_VAR_DATA_TYPE(pField->type)) {
    for (int i = 0; i < *rows; i++) {
      if (pCol->offset[i] == -1) {
        result[i] = true;
      } else {
        result[i] = false;
      }
    }
  } else {
    for (int i = 0; i < *rows; i++) {
      if (colDataIsNull_f(pCol, i)) {
        result[i] = true;
      } else {
        result[i] = false;
      }
    }
  }
  return 0;
}

int taos_validate_sql(TAOS *taos, const char *sql) {
  /* The internal taos_free_result below sees depth > 0 and records nothing, so this
   * produces one validate_sql event rather than a validate plus a stray free
   * (FS 4.1, TS RE008). */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_VALIDATE_SQL;
  wlrArgs.sql = sql;
  wlrArgs.sqlLen = (sql == NULL) ? 0 : (int32_t)strlen(sql);
  if (taos != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(*(int64_t *)taos);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(*(int64_t *)taos);
    }
  }
  if (wlrArgs.logicalConnId != 0) (void)wlrApiBegin(&wlrArgs, &wlrTok);

  TAOS_RES *pObj = taosQueryImpl(taos, sql, true, TD_REQ_FROM_APP);

  int code = taos_errno(pObj);

  taos_free_result(pObj);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = code;
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return code;
}

void taos_reset_current_db(TAOS *taos) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return;
  }

  /* Recorded because it changes which database later unqualified statements resolve
   * against: replaying the queries without this would run them in the wrong db. */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_RESET_CURRENT_DB;
  wlrArgs.logicalConnId = pTscObj->wlrConnId;
  (void)wlrApiBegin(&wlrArgs, &wlrTok);

  resetConnectDB(pTscObj);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }

  releaseTscObj(*(int64_t *)taos);
}

const char *taos_get_server_info(TAOS *taos) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  releaseTscObj(*(int64_t *)taos);

  return pTscObj->sDetailVer;
}

int taos_get_current_db(TAOS *taos, char *database, int len, int *required) {
  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  int code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pTscObj->mutex);
  if (database == NULL || len <= 0) {
    if (required != NULL) *required = strlen(pTscObj->db) + 1;
    TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
  } else if (len < strlen(pTscObj->db) + 1) {
    tstrncpy(database, pTscObj->db, len);
    if (required) *required = strlen(pTscObj->db) + 1;
    TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
  } else {
    tstrncpy(database, pTscObj->db, len);
    code = 0;
  }
_return:
  (void)taosThreadMutexUnlock(&pTscObj->mutex);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

// buffer is allocated by caller, len is in/out parameter, input is buffer length, output is actual length.
// because this is a general purpose api, buffer is not null-terminated string even for string info, and
// the return length is the actual length of the info, not including null-terminator.
int taos_get_connection_info(TAOS *taos, TSDB_CONNECTION_INFO info, char *buffer, int *len) {
  if (len == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  STscObj *pTscObj = acquireTscObj(*(int64_t *)taos);
  if (pTscObj == NULL) {
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  int code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pTscObj->mutex);

  switch (info) {
    case TSDB_CONNECTION_INFO_USER: {
      int userLen = strlen(pTscObj->user);
      if (buffer == NULL || *len < userLen) {
        *len = userLen;
        TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
      } else {
        *len = userLen;
        (void)memcpy(buffer, pTscObj->user, userLen);
      }
      break;
    }

    case TSDB_CONNECTION_INFO_TOKEN: {
      int tokenLen = strlen(pTscObj->tokenName);
      if (tokenLen == 0) {
        *len = 0;
      } else if (buffer == NULL || *len < tokenLen) {
        *len = tokenLen;
        TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
      } else {
        *len = tokenLen;
        (void)memcpy(buffer, pTscObj->tokenName, tokenLen);
      }
      break;
    }

    default:
      TSC_ERR_JRET(TSDB_CODE_INVALID_PARA);
  }

_return:
  (void)taosThreadMutexUnlock(&pTscObj->mutex);
  releaseTscObj(*(int64_t *)taos);
  return code;
}

void destorySqlCallbackWrapper(SSqlCallbackWrapper *pWrapper) {
  if (NULL == pWrapper) {
    return;
  }
  destoryCatalogReq(pWrapper->pCatalogReq);
  taosMemoryFree(pWrapper->pCatalogReq);
  qDestroyParseContext(pWrapper->pParseCtx);
  taosMemoryFree(pWrapper);
}

void destroyCtxInRequest(SRequestObj *pRequest) {
  schedulerFreeJob(&pRequest->body.queryJob, 0);
  qDestroyQuery(pRequest->pQuery);
  pRequest->pQuery = NULL;
  destorySqlCallbackWrapper(pRequest->pWrapper);
  pRequest->pWrapper = NULL;
}

static void stashFirstExtSourceNameFromCatalogReq(SRequestObj *pRequest, const SCatalogReq *pCatalogReq) {
  if (pRequest == NULL || pCatalogReq == NULL || pCatalogReq->pExtSourceCheck == NULL ||
      pRequest->extSourceName[0] != '\0') {
    return;
  }

  if (taosArrayGetSize(pCatalogReq->pExtSourceCheck) <= 0) {
    return;
  }

  const char *srcName = (const char *)taosArrayGet(pCatalogReq->pExtSourceCheck, 0);
  if (srcName != NULL && srcName[0] != '\0') {
    tstrncpy(pRequest->extSourceName, srcName, TSDB_EXT_SOURCE_NAME_LEN);
  }
}

static void doAsyncQueryFromAnalyse(SMetaData *pResultMeta, void *param, int32_t code) {
  SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
  SRequestObj         *pRequest = pWrapper->pRequest;
  SQuery              *pQuery = pRequest->pQuery;

  qDebug("req:0x%" PRIx64 ", start to semantic analysis, QID:0x%" PRIx64, pRequest->self, pRequest->requestId);

  int64_t analyseStart = taosGetTimestampUs();
  pRequest->metric.ctgCostUs = analyseStart - pRequest->metric.ctgStart;
  pWrapper->pParseCtx->parseOnly = pRequest->parseOnly;

  if (TSDB_CODE_SUCCESS == code) {
    code = qAnalyseSqlSemantic(pWrapper->pParseCtx, pWrapper->pCatalogReq, pResultMeta, pQuery);
  }

  stashFirstExtSourceNameFromCatalogReq(pRequest, pWrapper->pCatalogReq);

  if (TSDB_CODE_SUCCESS == code) {
    // FH-10: stash the first ext source name for error-driven cache management
    if (pWrapper->pCatalogReq != NULL &&
        taosArrayGetSize(pWrapper->pCatalogReq->pExtSourceCheck) > 0) {
      const char* srcName = (const char*)taosArrayGet(pWrapper->pCatalogReq->pExtSourceCheck, 0);
      if (srcName != NULL) {
        tstrncpy(pRequest->extSourceName, srcName, TSDB_EXT_SOURCE_NAME_LEN);
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = sqlSecurityCheckASTLevel(pRequest, pQuery);
  }

  pRequest->metric.analyseCostUs += taosGetTimestampUs() - analyseStart;

  if (pRequest->parseOnly) {
    (void)memcpy(&pRequest->parseMeta, pResultMeta, sizeof(*pResultMeta));
    (void)memset(pResultMeta, 0, sizeof(*pResultMeta));
  }

  handleQueryAnslyseRes(pWrapper, pResultMeta, code);
}

int32_t cloneCatalogReq(SCatalogReq **ppTarget, SCatalogReq *pSrc) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SCatalogReq *pTarget = taosMemoryCalloc(1, sizeof(SCatalogReq));
  if (pTarget == NULL) {
    code = terrno;
  } else {
    pTarget->pDbVgroup = taosArrayDup(pSrc->pDbVgroup, NULL);
    pTarget->pDbCfg = taosArrayDup(pSrc->pDbCfg, NULL);
    pTarget->pDbInfo = taosArrayDup(pSrc->pDbInfo, NULL);
    pTarget->pTableMeta = taosArrayDup(pSrc->pTableMeta, NULL);
    pTarget->pTableHash = taosArrayDup(pSrc->pTableHash, NULL);
    pTarget->pUdf = taosArrayDup(pSrc->pUdf, NULL);
    pTarget->pIndex = taosArrayDup(pSrc->pIndex, NULL);
    pTarget->pUser = taosArrayDup(pSrc->pUser, NULL);
    pTarget->pTableIndex = taosArrayDup(pSrc->pTableIndex, NULL);
    pTarget->pTableCfg = taosArrayDup(pSrc->pTableCfg, NULL);
    pTarget->pTableTag = taosArrayDup(pSrc->pTableTag, NULL);
    pTarget->pView = taosArrayDup(pSrc->pView, NULL);
    pTarget->pTableTSMAs = taosArrayDup(pSrc->pTableTSMAs, NULL);
    pTarget->pTSMAs = taosArrayDup(pSrc->pTSMAs, NULL);
    pTarget->pVStbRefDbs = taosArrayDup(pSrc->pVStbRefDbs, NULL);
    pTarget->qNodeRequired = pSrc->qNodeRequired;
    pTarget->dNodeRequired = pSrc->dNodeRequired;
    pTarget->svrVerRequired = pSrc->svrVerRequired;
    pTarget->forceUpdate = pSrc->forceUpdate;
    pTarget->forceUpdateTbMetaOnly = pSrc->forceUpdateTbMetaOnly;
    pTarget->cloned = true;
    pTarget->pExtSourceCheck = taosArrayDup(pSrc->pExtSourceCheck, NULL);
    pTarget->pExtTableMeta = taosArrayDup(pSrc->pExtTableMeta, NULL);

    *ppTarget = pTarget;
  }

  return code;
}

void handleSubQueryFromAnalyse(SSqlCallbackWrapper *pWrapper, SMetaData *pResultMeta, SNode *pRoot) {
  SRequestObj         *pNewRequest = NULL;
  SSqlCallbackWrapper *pNewWrapper = NULL;
  int32_t              code = buildPreviousRequest(pWrapper->pRequest, pWrapper->pRequest->sqlstr, &pNewRequest);
  if (code) {
    handleQueryAnslyseRes(pWrapper, pResultMeta, code);
    return;
  }

  pNewRequest->pQuery = NULL;
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode **)&pNewRequest->pQuery);
  if (pNewRequest->pQuery) {
    pNewRequest->pQuery->pRoot = pRoot;
    pRoot = NULL;
    pNewRequest->pQuery->execStage = QUERY_EXEC_STAGE_ANALYSE;
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = prepareAndParseSqlSyntax(&pNewWrapper, pNewRequest, CLIENT_META_REFRESH_NONE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = cloneCatalogReq(&pNewWrapper->pCatalogReq, pWrapper->pCatalogReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    doAsyncQueryFromAnalyse(pResultMeta, pNewWrapper, code);
    nodesDestroyNode(pRoot);
  } else {
    handleQueryAnslyseRes(pWrapper, pResultMeta, code);
    return;
  }
}

void handleQueryAnslyseRes(SSqlCallbackWrapper *pWrapper, SMetaData *pResultMeta, int32_t code) {
  SRequestObj *pRequest = pWrapper->pRequest;
  SQuery      *pQuery = pRequest->pQuery;

  if (code == TSDB_CODE_SUCCESS && pQuery->pPrevRoot) {
    SNode *prevRoot = pQuery->pPrevRoot;
    pQuery->pPrevRoot = NULL;
    handleSubQueryFromAnalyse(pWrapper, pResultMeta, prevRoot);
    return;
  }

  if (code == TSDB_CODE_SUCCESS) {
    pRequest->stableQuery = pQuery->stableQuery;
    if (pQuery->pRoot) {
      pRequest->stmtType = pQuery->pRoot->type;
      markPassAlterSelf(pRequest, pQuery);
      if (nodeType(pQuery->pRoot) == QUERY_NODE_DELETE_STMT) {
        pRequest->secureDelete = ((SDeleteStmt *)pQuery->pRoot)->secureDelete;
      }
    }

    if (pQuery->haveResultSet) {
      code = setResSchemaInfo(&pRequest->body.resInfo, pQuery->pResSchema, pQuery->numOfResCols, pQuery->pResExtSchema,
                              pRequest->stmtBindVersion > 0);
      setResPrecision(&pRequest->body.resInfo, pQuery->precision);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    TSWAP(pRequest->dbList, (pQuery)->pDbList);
    TSWAP(pRequest->tableList, (pQuery)->pTableList);
    TSWAP(pRequest->targetTableList, (pQuery)->pTargetTableList);

    launchAsyncQuery(pRequest, pQuery, pResultMeta, pWrapper);
  } else {
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    qDestroyQuery(pRequest->pQuery);
    pRequest->pQuery = NULL;

    if (NEED_CLIENT_HANDLE_EXT_ERROR(code) && pRequest->stmtBindVersion == 0) {
      pRequest->code = code;
      handleExtSourceError(pRequest, code);
      return;
    }

    if (NEED_CLIENT_HANDLE_ERROR(code) &&
        (pRequest->stmtBindVersion == 0 || (pRequest->stmtBindVersion == 2 && pRequest->literal_by_stmt2))) {
      // NOTE: also cover literal statement by stmt2
      tscDebug("req:0x%" PRIx64 ", client retry to handle the error, code:%d - %s, tryCount:%d, QID:0x%" PRIx64,
               pRequest->self, code, tstrerror(code), pRequest->retry, pRequest->requestId);
      restartAsyncQuery(pRequest, code);
      return;
    }

    // return to app directly
    tscError("req:0x%" PRIx64 ", error occurs, code:%s, return to user app, QID:0x%" PRIx64, pRequest->self,
             tstrerror(code), pRequest->requestId);
    pRequest->code = code;
    returnToUser(pRequest);
  }
}

static int32_t getAllMetaAsync(SSqlCallbackWrapper *pWrapper, catalogCallback fp) {
  SRequestConnInfo conn = {.pTrans = pWrapper->pParseCtx->pTransporter,
                           .requestId = pWrapper->pParseCtx->requestId,
                           .requestObjRefId = pWrapper->pParseCtx->requestRid,
                           .mgmtEps = pWrapper->pParseCtx->mgmtEpSet,
                           .txnId = pWrapper->pParseCtx->txnId};

  pWrapper->pRequest->metric.ctgStart = taosGetTimestampUs();

  return catalogAsyncGetAllMeta(pWrapper->pParseCtx->pCatalog, &conn, pWrapper->pCatalogReq, fp, pWrapper,
                                &pWrapper->pRequest->body.queryJob);
}

static void doAsyncQueryFromParse(SMetaData *pResultMeta, void *param, int32_t code);

static int32_t phaseAsyncQuery(SSqlCallbackWrapper *pWrapper) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SRequestObj *pRequest = pWrapper->pRequest;

  switch (pRequest->pQuery->execStage) {
    case QUERY_EXEC_STAGE_PARSE: {
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_CATALOG);
      code = getAllMetaAsync(pWrapper, doAsyncQueryFromParse);
      break;
    }
    case QUERY_EXEC_STAGE_ANALYSE: {
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_CATALOG);
      code = getAllMetaAsync(pWrapper, doAsyncQueryFromAnalyse);
      break;
    }
    case QUERY_EXEC_STAGE_SCHEDULE: {
      CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_SCHEDULE);
      launchAsyncQuery(pRequest, pRequest->pQuery, NULL, pWrapper);
      break;
    }
    default:
      break;
  }
  return code;
}

static void doAsyncQueryFromParse(SMetaData *pResultMeta, void *param, int32_t code) {
  SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
  SRequestObj         *pRequest = pWrapper->pRequest;
  SQuery              *pQuery = pRequest->pQuery;

  pRequest->metric.ctgCostUs += taosGetTimestampUs() - pRequest->metric.ctgStart;
  qDebug("req:0x%" PRIx64 ", continue parse query, QID:0x%" PRIx64 ", code:%s", pRequest->self, pRequest->requestId,
         tstrerror(code));

  if (code == TSDB_CODE_SUCCESS) {
    // pWrapper->pCatalogReq->forceUpdate = false;
    code = qContinueParseSql(pWrapper->pParseCtx, pWrapper->pCatalogReq, pResultMeta, pQuery);
  }

  stashFirstExtSourceNameFromCatalogReq(pRequest, pWrapper->pCatalogReq);

  if (TSDB_CODE_SUCCESS == code) {
    code = phaseAsyncQuery(pWrapper);
  }

  if (TSDB_CODE_SUCCESS != code) {
    tscError("req:0x%" PRIx64 ", error happens, code:%d - %s, QID:0x%" PRIx64, pWrapper->pRequest->self, code,
             tstrerror(code), pWrapper->pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    if (NEED_CLIENT_HANDLE_EXT_ERROR(code) && pRequest->stmtBindVersion == 0) {
      pRequest->code = code;
      handleExtSourceError(pRequest, code);
      return;
    }
    if (pRequest->stmtBindVersion == 0 && clientGetMetaRefreshScope(code) == CLIENT_META_REFRESH_FULL) {
      restartAsyncQuery(pRequest, code);
      return;
    }
    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void continueInsertFromCsv(SSqlCallbackWrapper *pWrapper, SRequestObj *pRequest) {
  int32_t code = qParseSqlSyntax(pWrapper->pParseCtx, &pRequest->pQuery, pWrapper->pCatalogReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = phaseAsyncQuery(pWrapper);
  }

  if (TSDB_CODE_SUCCESS != code) {
    tscError("req:0x%" PRIx64 ", error happens, code:%d - %s, QID:0x%" PRIx64, pWrapper->pRequest->self, code,
             tstrerror(code), pWrapper->pRequest->requestId);
    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param) {
  int64_t connId = *(int64_t *)taos;

  /* An async API has three moments, not two: BEGIN, the API returning to the user,
   * and the request completing (DS 6.8). BEGIN and RETURN are recorded here; the
   * third, REQUEST_DONE, is emitted from doRequestCallback, which is the only place
   * that runs once per completion regardless of which driver thread finishes it.
   *
   * The token is handed to the request through thread-local pending state rather than
   * per-connection state: several async queries can be in flight on one connection at
   * once, so a per-connection slot would tie a DONE to the wrong call. */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_QUERY_A;
  wlrArgs.sql = sql;
  wlrArgs.sqlLen = (sql == NULL) ? 0 : (int32_t)strlen(sql);
  if (wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(connId);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(connId);
    }
  }
  if (wlrArgs.logicalConnId != 0) (void)wlrApiBegin(&wlrArgs, &wlrTok);
  wlrSetPendingAsyncCall(&wlrTok);

  taosAsyncQueryImpl(connId, sql, fp, param, false, TD_REQ_FROM_APP, true);

  /* Clear unconditionally: the driver's early-return paths (invalid args, request
   * build failure) never reach the take, and a leftover token would be picked up by
   * the next request built on this thread. Clearing after the call is safe because
   * the take already happened if a request was created. */
  wlrSetPendingAsyncCall(NULL);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid) {
  int64_t connId = *(int64_t *)taos;

  /* Same three-moment shape as taos_query_a, with the caller's reqid preserved. */
  SWorkloadCallToken wlrTok = {0};
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_QUERY_A_WITH_REQID;
  wlrArgs.sql = sql;
  wlrArgs.sqlLen = (sql == NULL) ? 0 : (int32_t)strlen(sql);
  wlrArgs.reqid = reqid;
  if (wlrRecorderState() == WLR_STATE_RUNNING) {
    STscObj *pWlrObj = acquireTscObj(connId);
    if (pWlrObj != NULL) {
      wlrArgs.logicalConnId = pWlrObj->wlrConnId;
      releaseTscObj(connId);
    }
  }
  if (wlrArgs.logicalConnId != 0) (void)wlrApiBegin(&wlrArgs, &wlrTok);
  wlrSetPendingAsyncCall(&wlrTok);
  taosAsyncQueryImplWithReqid(NULL, connId, sql, fp, param, false, reqid, true);
  wlrSetPendingAsyncCall(NULL); /* see taos_query_a: covers the driver's early returns */
  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.logicalConnId = wlrArgs.logicalConnId;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

int32_t createParseContext(const SRequestObj *pRequest, SParseContext **pCxt, SSqlCallbackWrapper *pWrapper) {
  const STscObj *pTscObj = pRequest->pTscObj;

  *pCxt = taosMemoryCalloc(1, sizeof(SParseContext));
  if (*pCxt == NULL) {
    return terrno;
  }

  void *charsetCxt = taosResolveCharsetCxt(pTscObj->optionInfo.charsetCxt);

  **pCxt = (SParseContext){.requestId = pRequest->requestId,
                           .requestRid = pRequest->self,
                           .acctId = pTscObj->acctId,
                           .db = pRequest->pDb,
                           .topicQuery = false,
                           .pSql = pRequest->sqlstr,
                           .sqlLen = pRequest->sqlLen,
                           .pMsg = pRequest->msgBuf,
                           .msgLen = ERROR_MSG_BUF_DEFAULT_SIZE,
                           .pTransporter = pTscObj->pAppInfo->pTransporter,
                           .pStmtCb = NULL,
                           .pUser = pTscObj->user,
                           .userId = pTscObj->userId,
                           .pEffectiveUser = pRequest->effectiveUser,
                           .isSuperUser = (0 == strcmp(pTscObj->user, TSDB_DEFAULT_USER)),
                           .enableSysInfo = pTscObj->sysInfo,
                           .privInfo = pWrapper->pParseCtx ? pWrapper->pParseCtx->privInfo : 0,
                           .sodInitial = pTscObj->pAppInfo->serverCfg.sodInitial,
                           .async = true,
                           .svrVer = pTscObj->sVer,
                           .nodeOffline = (pTscObj->pAppInfo->onlineDnodes < pTscObj->pAppInfo->totalDnodes),
                           .allocatorId = pRequest->allocatorRefId,
                           .parseSqlFp = clientParseSql,
                           .parseSqlParam = pWrapper,
                           .setQueryFp = setQueryRequest,
                           .timezone = pTscObj->optionInfo.timezone,
                           .charsetCxt = charsetCxt,
                           // .txnId / .txnDdlRemaining are filled in below, under
                           // pTscObj->mutex: they are the pair that governs the txn DDL
                           // budget and are written (and zeroed together) by the txn
                           // lifecycle under that lock. Deriving the per-vgroup quota from
                           // an unlocked read could see a half-updated txnId/txnDdlCount pair.
                           .pTxnVgSet = pTscObj->pTxnVgSet,
                           .pTxnTableMeta = pTscObj->pTxnTableMeta,
                           .pTxnSuidMap = pTscObj->pTxnSuidMap,
                           .firstDayOfWeek = pTscObj->optionInfo.firstDayOfWeek};
  tstrncpy((*pCxt)->timezoneName, pTscObj->optionInfo.timezoneName, sizeof((*pCxt)->timezoneName));
  int8_t biMode = atomic_load_8(&((STscObj *)pTscObj)->biMode);
  (*pCxt)->biMode = biMode;
  (*pCxt)->minSecLevel = pTscObj->minSecLevel;
  (*pCxt)->maxSecLevel = pTscObj->maxSecLevel;
  (*pCxt)->macMode = pTscObj->pAppInfo->serverCfg.macActive;

  // Inject active external source context so 1-seg table refs can be resolved after USE ext_source
  (void)taosThreadMutexLock(&((STscObj *)pTscObj)->mutex);
  // Snapshot the txn identity and the remaining budget under the same lock that the BEGIN
  // handler and tscResetTxnState use to write (and zero) them. txnDdlCount is also charged
  // from the scheduler completion callback, so deriving the per-vgroup quota from an unlocked
  // read could see a half-updated txnId/txnDdlCount pair.
  (*pCxt)->txnId = pTscObj->txnId;
  (*pCxt)->txnDdlRemaining = txnDdlRemainingOps(pTscObj);
  tstrncpy((*pCxt)->currentExtSource, pTscObj->extSource, sizeof((*pCxt)->currentExtSource));
  tstrncpy((*pCxt)->currentExtNs1,    pTscObj->extNs1,    sizeof((*pCxt)->currentExtNs1));
  tstrncpy((*pCxt)->currentExtNs2,    pTscObj->extNs2,    sizeof((*pCxt)->currentExtNs2));
  tstrncpy((*pCxt)->timezoneName,     pTscObj->optionInfo.timezoneName, sizeof((*pCxt)->timezoneName));
  (void)taosThreadMutexUnlock(&((STscObj *)pTscObj)->mutex);
  return TSDB_CODE_SUCCESS;
}

EClientMetaRefreshScope clientGetMetaRefreshScope(int32_t code) {
  if (NEED_CLIENT_REFRESH_VG_ERROR(code) || TSDB_CODE_SCH_DATA_SRC_EP_MISS == code) {
    return CLIENT_META_REFRESH_FULL;
  }

  if (NEED_CLIENT_RM_TBLMETA_ERROR(code) || TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER == code ||
      TSDB_CODE_MND_INVALID_SCHEMA_VER == code) {
    return CLIENT_META_REFRESH_TABLE;
  }

  return CLIENT_META_REFRESH_NONE;
}

int32_t prepareAndParseSqlSyntax(SSqlCallbackWrapper **ppWrapper, SRequestObj *pRequest,
                                 EClientMetaRefreshScope refreshScope) {
  int32_t              code = TSDB_CODE_SUCCESS;
  STscObj             *pTscObj = pRequest->pTscObj;
  SSqlCallbackWrapper *pWrapper = taosMemoryCalloc(1, sizeof(SSqlCallbackWrapper));
  if (pWrapper == NULL) {
    code = terrno;
  } else {
    pWrapper->pRequest = pRequest;
    pRequest->pWrapper = pWrapper;
    *ppWrapper = pWrapper;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createParseContext(pRequest, &pWrapper->pParseCtx, pWrapper);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pWrapper->pParseCtx->mgmtEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
    code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pWrapper->pParseCtx->pCatalog);
  }

  if (TSDB_CODE_SUCCESS == code && NULL == pRequest->pQuery) {
    int64_t syntaxStart = taosGetTimestampUs();

    pWrapper->pCatalogReq = taosMemoryCalloc(1, sizeof(SCatalogReq));
    if (pWrapper->pCatalogReq == NULL) {
      code = terrno;
    } else {
      pWrapper->pCatalogReq->forceUpdate = (CLIENT_META_REFRESH_NONE != refreshScope);
      pWrapper->pCatalogReq->forceUpdateTbMetaOnly = (CLIENT_META_REFRESH_TABLE == refreshScope);
      TSC_ERR_RET(qnodeRequired(pRequest, &pWrapper->pCatalogReq->qNodeRequired));
      code = qParseSqlSyntax(pWrapper->pParseCtx, &pRequest->pQuery, pWrapper->pCatalogReq);
    }

    pRequest->metric.parseCostUs += taosGetTimestampUs() - syntaxStart;
  }

  return code;
}

static bool tscIsInternalExtLifecycleCode(int32_t code) {
  switch (code) {
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_FUNC:
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OP:
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_TYPE:
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_CLAUSE:
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY:
    case TSDB_CODE_EXT_PUSHDOWN_FAILED:
    case TSDB_CODE_EXT_PUSHDOWN_BASELINE:
    case TSDB_CODE_EXT_PUSHDOWN_BAILOUT_BROKEN:
      return true;
    default:
      return false;
  }
}

static int32_t tscSanitizeExtCodeForUser(int32_t code) {
  if (tscIsInternalExtLifecycleCode(code)) {
    return TSDB_CODE_EXT_PUSHDOWN_FAILED;
  }
  return code;
}

static void doAsyncExec(SRequestObj *pRequest, int32_t code) {
  SSqlCallbackWrapper *pWrapper = pRequest->pWrapper;

  if (TSDB_CODE_SUCCESS == code) {
    pRequest->stmtType = pRequest->pQuery->pRoot->type;
#ifdef TD_ENTERPRISE
    bool handled = false;
    code = tscCheckTxnState(pRequest, pWrapper, &handled);
    if (handled) return;  // ROLLBACK early-exit: callback already dispatched
    if (TSDB_CODE_SUCCESS == code) {
#endif
      markPassAlterSelf(pRequest, pRequest->pQuery);
      code = phaseAsyncQuery(pWrapper);
#ifdef TD_ENTERPRISE
    }
#endif
  }

  if (TSDB_CODE_SUCCESS != code) {
    if (NULL != pRequest->msgBuf && strlen(pRequest->msgBuf) > 0) {
      tscError("req:0x%" PRIx64 ", error happens, code:%d - %s, QID:0x%" PRIx64, pRequest->self, code, pRequest->msgBuf,
               pRequest->requestId);
    } else {
      tscError("req:0x%" PRIx64 ", error happens, code:%d - %s, QID:0x%" PRIx64, pRequest->self, code, tstrerror(code),
               pRequest->requestId);
    }

    destorySqlCallbackWrapper(pWrapper);
    pRequest->pWrapper = NULL;
    qDestroyQuery(pRequest->pQuery);
    pRequest->pQuery = NULL;

    if (NEED_CLIENT_HANDLE_ERROR(code) && pRequest->stmtBindVersion == 0) {
      EClientMetaRefreshScope retryScope = clientGetMetaRefreshScope(code);
      tscDebug("req:0x%" PRIx64 ", client retry to handle the error, code:%d - %s, tryCount:%d, QID:0x%" PRIx64,
               pRequest->self, code, tstrerror(code), pRequest->retry, pRequest->requestId);
      int32_t refreshCode = refreshMeta(pRequest->pTscObj, pRequest, code);
      if (refreshCode != 0) {
        tscWarn("req:0x%" PRIx64 ", refresh meta failed, code:%d - %s, QID:0x%" PRIx64, pRequest->self,
                refreshCode, tstrerror(refreshCode), pRequest->requestId);
      }
      pRequest->prevCode = (TSDB_CODE_SUCCESS == refreshCode) ? code : refreshCode;
      doAsyncQuery(pRequest, retryScope);
      return;
    }

    terrno = code;
    pRequest->code = code;
    doRequestCallback(pRequest, code);
  }
}

void doAsyncQuery(SRequestObj *pRequest, EClientMetaRefreshScope refreshScope) {
  SSqlCallbackWrapper *pWrapper = NULL;
  int32_t              code = TSDB_CODE_SUCCESS;

  CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_PARSE);

  if (pRequest->retry++ > REQUEST_TOTAL_EXEC_TIMES) {
    code = tscSanitizeExtCodeForUser(pRequest->prevCode);
    terrno = code;
    pRequest->code = code;
    tscDebug("req:0x%" PRIx64 ", call sync query cb with code:%s", pRequest->self, tstrerror(code));
    doRequestCallback(pRequest, code);
    return;
  }

  // The same request object may be reused across fallback/retry paths. Clear any
  // stale server-side error text before the next parse/dispatch attempt so later
  // parser or planner failures do not inherit unrelated auth/network messages.
  if (pRequest->msgBuf != NULL && pRequest->msgBufLen > 0) {
    pRequest->msgBuf[0] = '\0';
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = prepareAndParseSqlSyntax(&pWrapper, pRequest, refreshScope);
  }

  if (pRequest->literal_by_stmt2) {
    TAOS_STMT2* stmt = pRequest->literal_by_stmt2;
    STscStmt2* pStmt = (STscStmt2*)stmt;
    if (pStmt->ctx.prepared == 0) {
      // NOTE: preparing stage for literal statement by stmt2
      doRequestCallback(pRequest, code);
      return;
    }
  }

  doAsyncExec(pRequest, code);
}

void taosAsyncExecLiteral(TAOS_STMT2 *stmt) {
  STscStmt2* pStmt = (STscStmt2*)stmt;
  doAsyncExec(pStmt->exec.pRequest, pStmt->ctx.code);
}

void restartAsyncQuery(SRequestObj *pRequest, int32_t code) {
  tscInfo("restart request:%s p:%p", pRequest->sqlstr, pRequest);
  SRequestObj *pUserReq = pRequest;
  (void)acquireRequest(pRequest->self);
  while (pUserReq) {
    if (pUserReq->self == pUserReq->relation.userRefId || pUserReq->relation.userRefId == 0) {
      break;
    } else {
      int64_t nextRefId = pUserReq->relation.nextRefId;
      (void)releaseRequest(pUserReq->self);
      if (nextRefId) {
        pUserReq = acquireRequest(nextRefId);
      }
    }
  }
  bool hasSubRequest = pUserReq != pRequest || pRequest->relation.prevRefId != 0;
  if (pUserReq) {
    destroyCtxInRequest(pUserReq);
    // Ext pushdown lifecycle has its own bounded attempt counter; reset generic
    // parse/refresh retry so fallback restarts are not cut off prematurely.
    if (tscIsInternalExtLifecycleCode(code)) {
      pUserReq->retry = 0;
    }
    pUserReq->prevCode = code;
    (void)memset(&pUserReq->relation, 0, sizeof(pUserReq->relation));
  } else {
    tscError("User req is missing");
    (void)removeFromMostPrevReq(pRequest);
    return;
  }
  if (hasSubRequest)
    (void)removeFromMostPrevReq(pRequest);
  else
    (void)releaseRequest(pUserReq->self);
  doAsyncQuery(pUserReq, clientGetMetaRefreshScope(code));
}

typedef struct SAsyncFetchParam {
  SRequestObj      *pReq;
  __taos_async_fn_t fp;
  void             *param;
  SWorkloadCallToken wlrToken;
} SAsyncFetchParam;

static void wlrAsyncFetchCallback(void *opaque, void *res, int32_t value) {
  SAsyncFetchParam *param = opaque;
  SWorkloadDoneArgs done = {0};
  done.code = value < 0 ? value : TSDB_CODE_SUCCESS;
  done.logicalResultId = param->pReq->wlrResultId;
  done.fetchRows = value > 0 ? value : 0;
  wlrAsyncDone(&param->wlrToken, &done);

  SWlrCallbackScope scope;
  wlrCallbackScopeBegin(param->wlrToken.callId, &scope);
  param->fp(param->param, res, value);
  wlrCallbackScopeEnd(&scope);
  taosMemoryFree(param);
}

static int32_t doAsyncFetch(void *pParam) {
  SAsyncFetchParam *param = pParam;
  /* taosAsyncFetchImpl owns completion through the callback even when dispatch
   * returns an error. Some scheduler failures invoke the callback synchronously
   * before returning that error, so this frame must never touch or free param after
   * the call. */
  (void)taosAsyncFetchImpl(param->pReq, wlrAsyncFetchCallback, param);
  return TSDB_CODE_SUCCESS;
}

/*
 * Async fetch recording.
 *
 * The heap-owned callback context carries the token until the driver invokes the
 * user callback, allowing DONE to contain the callback result and parent nested calls.
 *
 * taos_fetch_raw_block_a has its own hook and reaches the shared body through
 * taos_fetch_rows_a_impl, so the two API types stay distinct in the file instead
 * of both logging as FETCH_ROWS_A (FS 4.1).
 */
static int32_t taos_fetch_rows_a_impl(TAOS_RES *res, __taos_async_fn_t fp, void *param,
                                      const SWorkloadCallToken *wlrToken);
static int32_t taos_fetch_raw_block_a_impl(TAOS_RES *res, __taos_async_fn_t fp, void *param,
                                           const SWorkloadCallToken *wlrToken);

static void invokeSynchronousFetchCallback(__taos_async_fn_t fp, void *param, TAOS_RES *res, int32_t code,
                                           uint64_t parentCallId) {
  SWlrCallbackScope scope;
  wlrCallbackScopeBegin(parentCallId, &scope);
  fp(param, res, code);
  wlrCallbackScopeEnd(&scope);
}

void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  uint64_t wlrResId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_ROWS_A, wlrResId);
  int32_t wlrCode = taos_fetch_rows_a_impl(res, fp, param, &wlrTok);
  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, wlrCode, wlrResId, 0);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

static int32_t taos_fetch_rows_a_impl(TAOS_RES *res, __taos_async_fn_t fp, void *param,
                                      const SWorkloadCallToken *wlrToken) {
  if (res == NULL || fp == NULL) {
    tscError("taos_fetch_rows_a invalid paras");
    if (wlrToken->recorded) {
      SWorkloadDoneArgs done = {
          .code = TSDB_CODE_INVALID_PARA,
          .logicalResultId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0};
      wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    }
    return TSDB_CODE_INVALID_PARA;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_fetch_rows_a res is NULL");
    SWorkloadDoneArgs done = {.code = TSDB_CODE_APP_ERROR};
    wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    invokeSynchronousFetchCallback(fp, param, res, TSDB_CODE_APP_ERROR, wlrToken->callId);
    return TSDB_CODE_APP_ERROR;
  }

  SRequestObj *pRequest = res;

  // Each fetch call sets phase to IN_PROGRESS

  if (TSDB_SQL_RETRIEVE_EMPTY_RESULT == pRequest->type) {
    CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
    SWorkloadDoneArgs done = {.logicalResultId = pRequest->wlrResultId};
    wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    invokeSynchronousFetchCallback(fp, param, res, 0, wlrToken->callId);
    return TSDB_CODE_SUCCESS;
  }

  SAsyncFetchParam *pParam = taosMemoryCalloc(1, sizeof(SAsyncFetchParam));
  if (!pParam) {
    CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_RETURNED);
    SWorkloadDoneArgs done = {.code = terrno, .logicalResultId = pRequest->wlrResultId};
    wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    invokeSynchronousFetchCallback(fp, param, res, terrno, wlrToken->callId);
    return terrno;
  }

  CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_IN_PROGRESS);
  pParam->pReq = pRequest;
  pParam->fp = fp;
  pParam->param = param;
  pParam->wlrToken = *wlrToken;
  int32_t code = taosAsyncExec(doAsyncFetch, pParam, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    SWorkloadDoneArgs done = {.code = code, .logicalResultId = pRequest->wlrResultId};
    wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    taosMemoryFree(pParam);
    invokeSynchronousFetchCallback(fp, param, res, code, wlrToken->callId);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param) {
  uint64_t wlrResId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_FETCH(&wlrTok, WLR_API_FETCH_RAW_BLOCK_A, wlrResId);
  int32_t wlrCode = taos_fetch_raw_block_a_impl(res, fp, param, &wlrTok);
  if (wlrTok.recorded) {
    WLR_API_END_FETCH(&wlrTok, wlrCode, wlrResId, 0);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
}

static int32_t taos_fetch_raw_block_a_impl(TAOS_RES *res, __taos_async_fn_t fp, void *param,
                                           const SWorkloadCallToken *wlrToken) {
  if (res == NULL || fp == NULL) {
    tscError("taos_fetch_raw_block_a invalid paras");
    if (wlrToken->recorded) {
      SWorkloadDoneArgs done = {
          .code = TSDB_CODE_INVALID_PARA,
          .logicalResultId = (res != NULL && TD_RES_QUERY(res)) ? ((SRequestObj *)res)->wlrResultId : 0};
      wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    }
    return TSDB_CODE_INVALID_PARA;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_fetch_raw_block_a res is NULL");
    if (wlrToken->recorded) {
      SWorkloadDoneArgs done = {.code = TSDB_CODE_APP_ERROR};
      wlrAsyncDone((SWorkloadCallToken *)wlrToken, &done);
    }
    return TSDB_CODE_APP_ERROR;
  }
  SRequestObj    *pRequest = res;
  SReqResultInfo *pResultInfo = &pRequest->body.resInfo;

  CLIENT_UPDATE_REQUEST_PHASE_IF_CHANGED(pRequest, QUERY_PHASE_FETCH_IN_PROGRESS);
  // set the current block is all consumed
  pResultInfo->convertUcs4 = false;

  // it is a local executed query, no need to do async fetch
  /* Straight to the impl: the public symbol carries its own recording hook, and going
   * through it would open a nested FETCH_ROWS_A call inside this one. */
  return taos_fetch_rows_a_impl(pRequest, fp, param, wlrToken);
}

const void *taos_get_raw_block(TAOS_RES *res) {
  if (res == NULL) {
    tscError("taos_get_raw_block invalid paras");
    return NULL;
  }
  if (!TD_RES_QUERY(res)) {
    tscError("taos_get_raw_block res is NULL");
    return NULL;
  }
  SRequestObj *pRequest = res;

  return pRequest->body.resInfo.pData;
}

int taos_get_db_route_info(TAOS *taos, const char *db, TAOS_DB_ROUTE_INFO *dbInfo) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == dbInfo) {
    tscError("invalid input param, db:%p, dbInfo:%p", db, dbInfo);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_db_route_info";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return terrno;
  }

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)snprintf(dbFName, sizeof(dbFName), "%d.%s", pTscObj->acctId, db);

  code = catalogGetDBVgInfo(pCtg, &conn, dbFName, dbInfo);
  if (code) {
    goto _return;
  }

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == table || NULL == vgId) {
    tscError("invalid input param, db:%p, table:%p, vgId:%p", db, table, vgId);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_table_vgId";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  pRequest->syncQuery = true;

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  SName tableName = {0};
  toName(pTscObj->acctId, db, table, &tableName);

  SVgroupInfo vgInfo;
  code = catalogGetTableHashVgroup(pCtg, &conn, &tableName, &vgInfo);
  if (code) {
    goto _return;
  }

  *vgId = vgInfo.vgId;

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  if (NULL == db || NULL == table || NULL == vgId || tableNum <= 0) {
    tscError("invalid input param, db:%p, table:%p, vgId:%p, tbNum:%d", db, table, vgId, tableNum);
    terrno = TSDB_CODE_TSC_INVALID_INPUT;
    return terrno;
  }

  int64_t      connId = *(int64_t *)taos;
  SRequestObj *pRequest = NULL;
  char        *sql = "taos_get_table_vgId";
  int32_t      code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  pRequest->syncQuery = true;

  STscObj  *pTscObj = pRequest->pTscObj;
  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  code = catalogGetTablesHashVgId(pCtg, &conn, pTscObj->acctId, db, table, tableNum, vgId);
  if (code) {
    goto _return;
  }

_return:

  terrno = code;

  destroyRequest(pRequest);
  return code;
}

int taos_load_table_info(TAOS *taos, const char *tableNameList) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return terrno;
  }

  int64_t       connId = *(int64_t *)taos;
  const int32_t MAX_TABLE_NAME_LENGTH = 12 * 1024 * 1024;  // 12MB list
  int32_t       code = 0;
  SRequestObj  *pRequest = NULL;
  SCatalogReq   catalogReq = {0};

  if (NULL == tableNameList) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t length = (int32_t)strlen(tableNameList);
  if (0 == length) {
    return TSDB_CODE_SUCCESS;
  } else if (length > MAX_TABLE_NAME_LENGTH) {
    tscError("tableNameList too long, length:%d, maximum allowed:%d", length, MAX_TABLE_NAME_LENGTH);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  char *sql = "taos_load_table_info";
  code = buildRequest(connId, sql, strlen(sql), NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _return;
  }

  pRequest->syncQuery = true;

  STscObj *pTscObj = pRequest->pTscObj;
  code = transferTableNameList(tableNameList, pTscObj->acctId, pTscObj->db, &catalogReq.pTableMeta);
  if (code) {
    goto _return;
  }

  SCatalog *pCtg = NULL;
  code = catalogGetHandle(pTscObj->pAppInfo->clusterId, &pCtg);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  SRequestConnInfo conn = {
      .pTrans = pTscObj->pAppInfo->pTransporter, .requestId = pRequest->requestId, .requestObjRefId = pRequest->self};

  conn.mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

  code = catalogAsyncGetAllMeta(pCtg, &conn, &catalogReq, syncCatalogFn, pRequest->body.interParam, NULL);
  if (code) {
    goto _return;
  }

  SSyncQueryParam *pParam = pRequest->body.interParam;
  code = tsem_wait(&pParam->sem);
  if (code) {
    tscError("tsem wait failed, code:%d - %s", code, tstrerror(code));
    goto _return;
  }
_return:
  destoryCatalogReq(&catalogReq);
  destroyRequest(pRequest);
  return code;
}

TAOS_STMT *taos_stmt_init(TAOS *taos) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, 0, NULL);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  } else if (wlrRecorderState() == WLR_STATE_RUNNING) {
    /* Mint the logical statement ID so every later event on this handle can be
     * correlated, then record the init itself (DS 6.9). */
    ((STscStmt *)pStmt)->wlrStmtId = wlrNextStmtId();
    SWorkloadCallToken wlrTok = {0};
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_INIT;
    wlrArgs.logicalStmtId = ((STscStmt *)pStmt)->wlrStmtId;
    wlrArgs.logicalConnId = pObj->wlrConnId;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
    if (wlrTok.recorded) {
      WLR_API_END_STMT(&wlrTok, 0, wlrArgs.logicalStmtId);
    }
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, reqid, NULL);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  } else if (wlrRecorderState() == WLR_STATE_RUNNING) {
    ((STscStmt *)pStmt)->wlrStmtId = wlrNextStmtId();
    SWorkloadCallToken wlrTok = {0};
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_INIT_WITH_REQID;
    wlrArgs.logicalStmtId = ((STscStmt *)pStmt)->wlrStmtId;
    wlrArgs.logicalConnId = pObj->wlrConnId;
    /* The caller-supplied reqid is the whole point of this variant, and replay must
     * reissue it verbatim so the application's own logs still correlate with server
     * traces (FS 4.5) -- the same reason taos_query_with_reqid records it. Without this
     * the event was indistinguishable from a plain taos_stmt_init and replay had to
     * mint a fresh one. */
    wlrArgs.reqid = reqid;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
    if (wlrTok.recorded) {
      WLR_API_END_STMT(&wlrTok, 0, wlrArgs.logicalStmtId);
    }
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

TAOS_STMT *taos_stmt_init_with_options(TAOS *taos, TAOS_STMT_OPTIONS *options) {
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT *pStmt = stmtInit(pObj, options->reqId, options);
  if (NULL == pStmt) {
    tscError("stmt init failed, errcode:%s", terrstr());
  } else if (wlrRecorderState() == WLR_STATE_RUNNING) {
    ((STscStmt *)pStmt)->wlrStmtId = wlrNextStmtId();
    SWorkloadCallToken wlrTok = {0};
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_INIT_WITH_OPTIONS;
    wlrArgs.logicalStmtId = ((STscStmt *)pStmt)->wlrStmtId;
    wlrArgs.logicalConnId = pObj->wlrConnId;
    wlrArgs.reqid = options->reqId;
    wlrArgs.stmtInitOptionsSet = true;
    if (options->singleStbInsert) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_SINGLE_STB_INSERT;
    if (options->singleTableBindOnce) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
    if (wlrTok.recorded) {
      WLR_API_END_STMT(&wlrTok, 0, wlrArgs.logicalStmtId);
    }
  }
  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length) {
  if (stmt == NULL || sql == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  SWorkloadCallToken wlrTok = {0};
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT_PREPARE, wlrSid, sql, length);
  int32_t wlrCode = stmtPrepare(stmt, sql, length);
  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags) {
  if (stmt == NULL || name == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  /* Calls the internal stmtSetTbName/stmtSetTbTags rather than the public wrappers, so
   * no nested public event is produced and this needs its own. Records the table name;
   * tag values are intentionally omitted because their count cannot be obtained safely. */
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    /* The tag count is deliberately NOT queried. stmtGetTagFields switches statement
     * status and creates/recreates exec.pRequest -- freeing it outright on error -- so
     * calling it here would let recording perturb the very insert it observes, and no
     * non-destructive source for the count exists. The table name still records, which is what routes
     * the row on replay; the tag values do not (FS 4.8). */
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_SET_TBNAME_TAGS;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.tbname = name;
    wlrArgs.bindDesc = NULL;
    wlrArgs.bindDescLen = 0;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  int32_t code = stmtSetTbName(stmt, name);
  if (code == 0 && tags != NULL) {
    code = stmtSetTbTags(stmt, tags);
  }

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, code, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return code;
}

/* Shared by taos_stmt_set_tbname and its taos_stmt_set_sub_tbname alias. The alias
 * used to delegate to the public symbol, which recorded it as STMT_SET_TBNAME and
 * lost which one the application called; the API type is a parameter instead. */
static int stmtSetTbNameRecorded(TAOS_STMT *stmt, const char *name, uint16_t wlrApiType) {
  if (stmt == NULL || name == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = wlrApiType;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.tbname = name;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  int32_t wlrCode = stmtSetTbName(stmt, name);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name) {
  return stmtSetTbNameRecorded(stmt, name, WLR_API_STMT_SET_TBNAME);
}

int taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags) {
  if (stmt == NULL || tags == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    /* Tag count deliberately not queried: stmtGetTagFields mutates statement status
     * and the pending request, so it must not run from a capture hook. See the note
     * in taos_stmt_set_tbname_tags. */
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_SET_TAGS;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.bindDesc = NULL;
    wlrArgs.bindDescLen = 0;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  int32_t wlrCode = stmtSetTbTags(stmt, tags);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_set_sub_tbname(TAOS_STMT *stmt, const char *name) {
  return stmtSetTbNameRecorded(stmt, name, WLR_API_STMT_SET_SUB_TBNAME);
}

int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (stmt == NULL || NULL == fieldNum) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetTagFields(stmt, fieldNum, fields);
}

int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields) {
  if (stmt == NULL || NULL == fieldNum) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetColFields(stmt, fieldNum, fields);
}

// let stmt to reclaim TAOS_FIELD_E that was allocated by `taos_stmt_get_tag_fields`/`taos_stmt_get_col_fields`
void taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields) {
  (void)stmt;
  if (!fields) return;
  taosMemoryFree(fields);
}


static int32_t wlrEncodeSingleMultiBind(TAOS_MULTI_BIND *bind, uint8_t *buf, size_t bufLen) {
  SWlrBindEncodeCol col = {0};
  int32_t     num;
  uint32_t    width;
  uint8_t    *packedValues = NULL;

  if (bind == NULL || buf == NULL) return 0;
  if (bind->num <= 0) return 0;
  num = bind->num;
  col.type = (uint16_t)bind->buffer_type;
  col.elemCount = num;
  col.isNull = (const uint8_t *)bind->is_null;
  col.lengths = bind->length;
  col.values = (const uint8_t *)bind->buffer;
  width = wlrTypeFixedWidth(col.type);
  if (width == 0) {
    uint32_t total = 0;
    if (bind->length == NULL) {
      if (bind->is_null == NULL) return 0;
      for (int32_t row = 0; row < num; ++row) {
        if (bind->is_null[row] == 0) return 0;
      }
    }
    if (num > 1 && (uintptr_t)bind->buffer_length > UINTPTR_MAX / (uintptr_t)(num - 1)) return 0;
    for (int32_t row = 0; row < num; ++row) {
      if (bind->is_null != NULL && bind->is_null[row] != 0) continue;
      if (bind->length[row] < 0 || (uintptr_t)bind->length[row] > bind->buffer_length ||
          UINT32_MAX - total < (uint32_t)bind->length[row]) {
        return 0;
      }
      total += (uint32_t)bind->length[row];
    }
    if (total > 0 && bind->buffer == NULL) return 0;
    col.valueBytes = total;
    if (wlrBindEncodedSize(&col, 1) > bufLen) return 0;
    if (total > 0) {
      packedValues = (uint8_t *)taosMemoryMalloc(total);
      if (packedValues == NULL) return 0;
      uint32_t packedOffset = 0;
      for (int32_t row = 0; row < num; ++row) {
        if (bind->is_null != NULL && bind->is_null[row] != 0) continue;
        uint32_t rowLen = (uint32_t)bind->length[row];
        if (rowLen > 0) {
          memcpy(packedValues + packedOffset,
                 (const uint8_t *)bind->buffer + (uintptr_t)row * bind->buffer_length, rowLen);
          packedOffset += rowLen;
        }
      }
    }
    col.values = packedValues;
  } else {
    if ((uint64_t)width * (uint32_t)num > UINT32_MAX) return 0;
    col.valueBytes = width * (uint32_t)num;
    if (bind->buffer == NULL) {
      if (bind->is_null == NULL) return 0;
      for (int32_t row = 0; row < num; ++row) {
        if (bind->is_null[row] == 0) return 0;
      }
      packedValues = (uint8_t *)taosMemoryCalloc(1, col.valueBytes);
      if (packedValues == NULL) return 0;
      col.values = packedValues;
    }
  }
  if (width != 0 && wlrBindEncodedSize(&col, 1) > bufLen) {
    taosMemoryFree(packedValues);
    return 0;
  }
  int32_t written = wlrEncodeBind(buf, bufLen, &col, 1);
  taosMemoryFree(packedValues);
  return written > 0 ? written : 0;
}

static uint32_t wlrSingleMultiBindSize(const TAOS_MULTI_BIND *bind) {
  if (bind == NULL || bind->num <= 0) return 0;
  SWlrBindEncodeCol col = {0};
  col.type = (uint16_t)bind->buffer_type;
  col.elemCount = bind->num;
  col.isNull = (const uint8_t *)bind->is_null;
  col.lengths = bind->length;
  col.values = (const uint8_t *)bind->buffer;
  uint32_t width = wlrTypeFixedWidth(col.type);
  if (width == 0) {
    if (bind->length == NULL && bind->is_null == NULL) return 0;
    uint64_t total = 0;
    for (int32_t row = 0; row < bind->num; ++row) {
      if (bind->is_null != NULL && bind->is_null[row] != 0) continue;
      if (bind->length == NULL) return 0;
      if (bind->length[row] < 0 || (uintptr_t)bind->length[row] > bind->buffer_length) return 0;
      total += (uint32_t)bind->length[row];
      if (total > UINT32_MAX) return 0;
    }
    col.valueBytes = (uint32_t)total;
  } else {
    uint64_t total = (uint64_t)width * (uint32_t)bind->num;
    if (total > UINT32_MAX) return 0;
    col.valueBytes = (uint32_t)total;
  }
  if (col.valueBytes > 0 && col.values == NULL) col.values = (const uint8_t *)bind;
  return wlrBindEncodedSize(&col, 1);
}

#ifdef WLR_TEST_HOOKS
uint32_t wlrTestSingleMultiBindSize(void *bind) {
  return wlrSingleMultiBindSize((const TAOS_MULTI_BIND *)bind);
}
#endif

#ifdef WLR_TEST_HOOKS
int32_t wlrTestEncodeSingleMultiBind(void *bind, uint8_t *buf, size_t bufLen) {
  return wlrEncodeSingleMultiBind((TAOS_MULTI_BIND *)bind, buf, bufLen);
}
#endif

int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (bind->num > 1) {
    tscError("invalid bind number %d for %s", bind->num, __FUNCTION__);
    terrno = TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    /* bind points to the first element of the statement's column array. The
     * single-row restriction means each column has num <= 1, not that there is
     * only one column. The public API provides no column count, and querying it
     * from this hook can mutate statement state. Do not guess one column and
     * record a silently truncated payload; omit bind bytes so replay marks this
     * call as skipped rather than applying an incomplete bind shape. */
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_BIND_PARAM;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.bindDesc = NULL;
    wlrArgs.bindDescLen = 0;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  int32_t wlrCode = stmtBindBatch(stmt, bind, -1);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (bind->num <= 0 || bind->num > INT16_MAX) {
    tscError("invalid bind num %d", bind->num);
    terrno = TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR;
    return terrno;
  }

  int32_t insert = 0;
  int32_t code = stmtIsInsert(stmt, &insert);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("stmt insert failed, errcode:%s", tstrerror(code));
    return code;
  }
  if (0 == insert && bind->num > 1) {
    tscError("only one row data allowed for query");
    terrno = TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR;
    return terrno;
  }

  /* TAOS_MULTI_BIND carries no terminator and this hook cannot query the parsed
   * statement without perturbing its execution state. The event is therefore
   * recorded without bind bytes rather than guessing the caller's array length. */
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    /* The column count is deliberately NOT queried here. stmtGetColFields switches
     * statement status, may free and recreate exec.pRequest, and frees it outright on
     * error. That would let recording perturb the bind it is observing, which is never an
     * acceptable trade for a diagnostic (FS 4.8). No non-destructive source for the
     * count exists on the statement, so the event is recorded without bind bytes:
     * losing the payload is better than corrupting the call.
     *
     * The absent WLR_TAG_BIND_BYTES is itself the signal, and replay is built to read
     * it that way: replayBuildBinds returns <= 0 for a missing payload, and taosReplay
     * skips the call, counts it in "calls skipped", and exits REPLAY_EXIT_PARTIAL
     * rather than binding an invented shape. So a batch bind recorded this way is
     * visibly unreplayable, not silently mis-replayed -- which is the whole point of
     * refusing to guess. The STMT2 columnar paths do carry their payload. Both this
     * STMT1 full-batch form and taos_stmt_bind_param omit it because their column
     * count has no non-destructive source. */
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_BIND_PARAM_BATCH;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.bindDesc = NULL;
    wlrArgs.bindDescLen = 0;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
  }

  int32_t wlrCode = stmtBindBatch(stmt, bind, -1);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_bind_single_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind, int colIdx) {
  if (stmt == NULL || bind == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (colIdx < 0) {
    tscError("invalid bind column idx %d", colIdx);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t insert = 0;
  int32_t code = stmtIsInsert(stmt, &insert);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("stmt insert failed, errcode:%s", tstrerror(code));
    return code;
  }
  if (0 == insert && bind->num > 1) {
    tscError("only one row data allowed for query");
    terrno = TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR;
    return terrno;
  }

  /* This API explicitly targets one column, so unlike the full-row bind APIs its
   * descriptor count is known without inspecting statement state. */
  uint64_t           wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    uint32_t capacity = wlrSingleMultiBindSize(bind);
    uint8_t *wlrBuf = capacity > 0 && capacity <= WLR_MAX_FRAME_PAYLOAD
                          ? (uint8_t *)taosMemoryMalloc(capacity) : NULL;
    int32_t wlrLen = wlrBuf != NULL ? wlrEncodeSingleMultiBind(bind, wlrBuf, capacity) : 0;
    if (wlrBuf == NULL) {
      tscWarn("WLR: unable to allocate single-param bind capture buffer; recording without bind bytes");
    } else if (wlrLen <= 0) {
      tscWarn("WLR: single-param bind payload exceeds capture capacity; recording without bind bytes");
    }
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT_BIND_SINGLE_PARAM_BATCH;
    wlrArgs.logicalStmtId = wlrSid;
    wlrArgs.bindDesc = wlrLen > 0 ? wlrBuf : NULL;
    wlrArgs.bindDescLen = wlrLen;
    wlrArgs.bindColIdx = colIdx;
    wlrArgs.bindColIdxSet = true;
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
    taosMemoryFree(wlrBuf);
  }

  int32_t wlrCode = stmtBindBatch(stmt, bind, colIdx);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_add_batch(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  SWorkloadCallToken wlrTok = {0};
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT_ADD_BATCH, wlrSid, NULL, 0);
  int32_t wlrCode = stmtAddBatch(stmt);
  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_execute(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  SWorkloadCallToken wlrTok = {0};
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT_EXECUTE, wlrSid, NULL, 0);
  int32_t wlrCode = stmtExec(stmt);
  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = wlrCode;
    wlrRet.logicalStmtId = wlrSid;
    if (wlrCode == TSDB_CODE_SUCCESS) wlrRet.affectedRows = taos_stmt_affected_rows_once(stmt);
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert) {
  if (stmt == NULL || insert == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtIsInsert(stmt, insert);
}

int taos_stmt_num_params(TAOS_STMT *stmt, int *nums) {
  if (stmt == NULL || nums == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetParamNum(stmt, nums);
}

int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes) {
  if (stmt == NULL || type == NULL || NULL == bytes || idx < 0) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  return stmtGetParam(stmt, idx, type, bytes);
}

TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT_USE_RESULT, wlrSid, NULL, 0);

  TAOS_RES *res = stmtUseResult(stmt);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = taos_errno(res);
    wlrRet.logicalStmtId = wlrSid;
    /* This is where a statement result becomes visible to the user, so it gets a
     * logical result ID -- the same one taos_free_result will reference. The
     * internal taos_free_result(pStmt->exec.pRequest) calls inside clientStmt.c
     * leave wlrResultId zero and so record nothing (TS RE013). */
    if (res != NULL && TD_RES_QUERY(res)) {
      SRequestObj *pReq = (SRequestObj *)res;
      if (pReq->wlrResultId == 0) pReq->wlrResultId = wlrNextResultId();
      wlrRet.logicalResultId = pReq->wlrResultId;
    }
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return res;
}

char *taos_stmt_errstr(TAOS_STMT *stmt) { return (char *)stmtErrstr(stmt); }

int taos_stmt_affected_rows(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  return stmtAffectedRows(stmt);
}

int taos_stmt_affected_rows_once(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }

  return stmtAffectedRowsOnce(stmt);
}

int taos_stmt_close(TAOS_STMT *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  /* Read the logical ID before stmtClose: the handle is invalid afterwards. */
  uint64_t wlrSid = ((STscStmt *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT_CLOSE, wlrSid, NULL, 0);

  int32_t wlrCode = stmtClose(stmt);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option) {
  if (NULL == taos) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    tscError("invalid parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  TAOS_STMT2 *pStmt = stmtInit2(pObj, option);

  if (pStmt != NULL && wlrRecorderState() == WLR_STATE_RUNNING) {
    ((STscStmt2 *)pStmt)->wlrStmtId = wlrNextStmtId();
    SWorkloadCallToken wlrTok = {0};
    SWorkloadBeginArgs wlrArgs = {0};
    wlrArgs.apiType = WLR_API_STMT2_INIT;
    wlrArgs.logicalStmtId = ((STscStmt2 *)pStmt)->wlrStmtId;
    wlrArgs.logicalConnId = pObj->wlrConnId;
    wlrArgs.stmtInitOptionsSet = true;
    if (option != NULL) {
      wlrArgs.reqid = option->reqid;
      if (option->singleStbInsert) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_SINGLE_STB_INSERT;
      if (option->singleTableBindOnce) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE;
      if (option->asyncExecFn != NULL) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_ASYNC_CALLBACK;
      if (option->userdata != NULL) wlrArgs.stmtInitOptions |= WLR_STMT_OPT_USERDATA;
    }
    (void)wlrApiBegin(&wlrArgs, &wlrTok);
    if (wlrTok.recorded) {
      WLR_API_END_STMT(&wlrTok, 0, wlrArgs.logicalStmtId);
    }
  }

  releaseTscObj(*(int64_t *)taos);

  return pStmt;
}

int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length) {
  if (stmt == NULL || sql == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt2 *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT2_PREPARE, wlrSid, sql, length);
  int32_t wlrCode = stmtPrepare2(stmt, sql, length);
  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

static int32_t wlrStmt2RowColumnCount(STscStmt2 *stmt, int32_t colIdx) {
  if (colIdx >= 0) return 1;
  if (stmt->sql.sqlStr == NULL) return 0;
  if (stmt2IsSelect((TAOS_STMT2 *)stmt)) {
    if (stmt->sql.pQuery == NULL || stmt->sql.pQuery->pPlaceholderValues == NULL) return 0;
    return taosArrayGetSize(stmt->sql.pQuery->pPlaceholderValues);
  }
  if (stmt->sql.placeholderOfCols > 0) return stmt->sql.placeholderOfCols;
  SPureInsertParserCtx pureInsert = {0};
  if (qPureParseInsert(&pureInsert, stmt->sql.sqlStr) == TSDB_CODE_SUCCESS && pureInsert.nr_params > 1) {
    return pureInsert.nr_params - 1;
  }
  int32_t fieldCount = 0;
  int32_t colCount = 0;
  TAOS_FIELD_ALL *fields = NULL;
  if (taos_stmt2_get_fields((TAOS_STMT2 *)stmt, &fieldCount, &fields) == TSDB_CODE_SUCCESS && fields != NULL) {
    for (int32_t index = 0; index < fieldCount; ++index) {
      if (fields[index].field_type == TAOS_FIELD_COL) ++colCount;
    }
  }
  taos_stmt2_free_fields((TAOS_STMT2 *)stmt, fields);
  return colCount;
}

static uint32_t wlrEncodeStmt2RowBindv(const TAOS_STMT2_BINDV *bindv, int32_t colsPerTable,
                                       uint8_t *buf, uint32_t bufLen, uint8_t *scratch) {
  SWlrBindEncodeCol cols[128] = {0};
  uint32_t packedOffset = 0;
  if (bindv == NULL || bindv->bind_cols == NULL || bindv->count <= 0 || colsPerTable <= 0 ||
      bindv->count > 128 / colsPerTable) return 0;

  int32_t colCount = bindv->count * colsPerTable;
  for (int32_t table = 0; table < bindv->count; ++table) {
    if (bindv->bind_cols[table] == NULL) {
      for (int32_t column = 0; column < colsPerTable; ++column) {
        SWlrBindEncodeCol *col = &cols[table * colsPerTable + column];
        col->type = 0;
        col->flags = WLR_BIND_FLAG_ABSENT_TABLE;
      }
      continue;
    }
    for (int32_t column = 0; column < colsPerTable; ++column) {
      const TAOS_STMT2_BIND *bind = &bindv->bind_cols[table][column];
      SWlrBindEncodeCol *col = &cols[table * colsPerTable + column];
      if (bind->num <= 0) return 0;
      col->type = (uint16_t)bind->buffer_type;
      col->flags = WLR_BIND_FLAG_TRISTATE_NULL;
      col->elemCount = bind->num;
      col->isNull = (const uint8_t *)bind->is_null;
      col->lengths = bind->length;
      col->values = (const uint8_t *)bind->buffer;

      uint32_t width = wlrTypeFixedWidth(col->type);
      if (width == 0) {
        uint64_t valueBytes = 0;
        size_t sourceOffset = 0;
        uint32_t valueStart = packedOffset;
        for (int32_t row = 0; row < bind->num; ++row) {
          if (bind->is_null != NULL && bind->is_null[row] != 0) continue;
          if (bind->length == NULL || bind->length[row] < 0) return 0;
          size_t length = (size_t)bind->length[row];
          if (valueBytes + length > UINT32_MAX || SIZE_MAX - sourceOffset < length ||
              (length > 0 && bind->buffer == NULL)) return 0;
          if (buf != NULL) {
            if (packedOffset > bufLen || length > bufLen - packedOffset) return 0;
            if (length > 0) memcpy(scratch + packedOffset, (const uint8_t *)bind->buffer + sourceOffset, length);
            packedOffset += (uint32_t)length;
          }
          valueBytes += length;
          sourceOffset += length;
        }
        col->valueBytes = (uint32_t)valueBytes;
        if (buf != NULL) col->values = valueBytes > 0 ? scratch + valueStart : NULL;
      } else {
        uint64_t valueBytes = (uint64_t)width * (uint32_t)bind->num;
        if (valueBytes > UINT32_MAX) return 0;
        col->valueBytes = (uint32_t)valueBytes;
        if (bind->buffer == NULL) {
          if (bind->is_null == NULL) return 0;
          for (int32_t row = 0; row < bind->num; ++row) {
            if (bind->is_null[row] == 0) return 0;
          }
          if (buf == NULL) col->values = (const uint8_t *)bind;
          if (buf != NULL) {
            if (packedOffset > bufLen || valueBytes > bufLen - packedOffset) return 0;
            memset(scratch + packedOffset, 0, (size_t)valueBytes);
            col->values = scratch + packedOffset;
            packedOffset += (uint32_t)valueBytes;
          }
        }
      }
    }
  }

  uint32_t encodedSize = wlrBindEncodedSize(cols, colCount);
  if (buf == NULL) return encodedSize;
  if (encodedSize == 0 || encodedSize > bufLen) return 0;
  int32_t encoded = wlrEncodeBind(buf, bufLen, cols, colCount);
  return encoded > 0 ? (uint32_t)encoded : 0;
}

static uint32_t wlrEncodeStmt2Tags(const TAOS_STMT2_BINDV *bindv, int32_t tagsPerTable,
                                   uint8_t *buf, uint32_t bufLen, uint8_t *scratch) {
  if (bindv == NULL || bindv->tags == NULL || bindv->count <= 0 || bindv->count > 128 ||
      tagsPerTable <= 0 || tagsPerTable > 128) return 0;
  uint64_t size = 4;
  if (buf != NULL) {
    if (bufLen < size) return 0;
    wlrPutU32(buf, (uint32_t)bindv->count);
  }
  for (int32_t table = 0; table < bindv->count; ++table) {
    TAOS_STMT2_BIND *tagRow = bindv->tags[table];
    TAOS_STMT2_BIND scalarTags[128];
    if (tagRow != NULL) {
      for (int32_t tag = 0; tag < tagsPerTable; ++tag) {
        scalarTags[tag] = tagRow[tag];
        scalarTags[tag].num = 1;
      }
    }
    TAOS_STMT2_BIND *scalarRow = tagRow != NULL ? scalarTags : NULL;
    TAOS_STMT2_BINDV group = {.count = 1, .bind_cols = &scalarRow};
    uint32_t groupSize = tagRow != NULL ? wlrEncodeStmt2RowBindv(&group, tagsPerTable, NULL, 0, NULL) : 0;
    if (tagRow != NULL && (groupSize == 0 || groupSize > WLR_MAX_FRAME_PAYLOAD)) return 0;
    if (size + 4 + groupSize > WLR_MAX_FRAME_PAYLOAD) return 0;
    if (buf != NULL) {
      if (size + 4 + groupSize > bufLen) return 0;
      wlrPutU32(buf + size, groupSize);
      if (groupSize > 0 &&
          wlrEncodeStmt2RowBindv(&group, tagsPerTable, buf + size + 4,
                                 bufLen - (uint32_t)size - 4, scratch) != groupSize) return 0;
    }
    size += 4 + groupSize;
  }
  return (uint32_t)size;
}

#ifdef WLR_TEST_HOOKS
uint32_t wlrTestEncodeStmt2RowBindv(void *bindv, int32_t colsPerTable, uint8_t *buf, uint32_t bufLen) {
  uint8_t *scratch = buf != NULL ? (uint8_t *)taosMemoryMalloc(bufLen) : NULL;
  uint32_t size = buf == NULL || scratch != NULL
                      ? wlrEncodeStmt2RowBindv((const TAOS_STMT2_BINDV *)bindv, colsPerTable, buf, bufLen, scratch)
                      : 0;
  taosMemoryFree(scratch);
  return size;
}

uint32_t wlrTestEncodeStmt2Tags(void *bindv, int32_t tagsPerTable, uint8_t *buf, uint32_t bufLen) {
  uint8_t *scratch = buf != NULL ? (uint8_t *)taosMemoryMalloc(bufLen) : NULL;
  uint32_t size = buf == NULL || scratch != NULL
                      ? wlrEncodeStmt2Tags((const TAOS_STMT2_BINDV *)bindv, tagsPerTable, buf, bufLen, scratch)
                      : 0;
  taosMemoryFree(scratch);
  return size;
}
#endif

static void wlrStmt2RowBindBegin(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t colIdx,
                                 uint16_t apiType, SWorkloadCallToken *token) {
  STscStmt2 *statement = (STscStmt2 *)stmt;
  uint8_t *tbnameBuf = NULL;
  int32_t tbnameLen = 0;
  if (bindv != NULL && bindv->count > 0 && bindv->count <= 128) {
    uint32_t tbnameCapacity = wlrStmt2NamesEncodedSize((const char *const *)bindv->tbnames, bindv->count);
    if (tbnameCapacity <= WLR_MAX_FRAME_PAYLOAD) {
      tbnameBuf = (uint8_t *)taosMemoryMalloc(tbnameCapacity);
      if (tbnameBuf != NULL) {
        tbnameLen = wlrEncodeStmt2Names(tbnameBuf, tbnameCapacity, (const char *const *)bindv->tbnames,
                                      bindv->count);
        if (tbnameLen <= 0) tbnameLen = 0;
      }
    }
  }

  bool noColumns = bindv != NULL && bindv->count > 0 && bindv->count <= 128;
  for (int32_t table = 0; noColumns && bindv->bind_cols != NULL && table < bindv->count; ++table) {
    if (bindv->bind_cols[table] != NULL) noColumns = false;
  }
  int32_t colsPerTable = noColumns ? 0 : wlrStmt2RowColumnCount(statement, colIdx);
  uint32_t capacity = noColumns ? 4 : wlrEncodeStmt2RowBindv(bindv, colsPerTable, NULL, 0, NULL);
  uint8_t *bindBuf = capacity > 0 && capacity <= WLR_MAX_FRAME_PAYLOAD
                         ? (uint8_t *)taosMemoryMalloc((size_t)capacity * 2) : NULL;
  uint32_t bindLen = 0;
  if (bindBuf != NULL) {
    if (noColumns) {
      wlrPutU32(bindBuf, 0);
      bindLen = 4;
    } else {
      bindLen = wlrEncodeStmt2RowBindv(bindv, colsPerTable, bindBuf, capacity, bindBuf + capacity);
    }
  }
  bool hasTags = false;
  if (bindv != NULL && bindv->tags != NULL && bindv->count > 0) {
    for (int32_t table = 0; table < bindv->count; ++table) {
      if (bindv->tags[table] != NULL) hasTags = true;
    }
  }
  uint32_t tagCapacity = hasTags ? wlrEncodeStmt2Tags(bindv, statement->sql.placeholderOfTags, NULL, 0, NULL) : 0;
  uint8_t *tagBuf = tagCapacity > 0 && tagCapacity <= WLR_MAX_FRAME_PAYLOAD
                        ? (uint8_t *)taosMemoryMalloc((size_t)tagCapacity * 2) : NULL;
  uint32_t tagLen = tagBuf != NULL
                        ? wlrEncodeStmt2Tags(bindv, statement->sql.placeholderOfTags, tagBuf, tagCapacity,
                                             tagBuf + tagCapacity)
                        : 0;
  if (hasTags && tagLen == 0) bindLen = 0;
  if (tbnameLen == 0) bindLen = 0;

  SWorkloadBeginArgs args = {0};
  args.apiType = apiType;
  args.logicalStmtId = statement->wlrStmtId;
  args.bindDesc = bindLen > 0 ? bindBuf : NULL;
  args.bindDescLen = (int32_t)bindLen;
  args.tbnameList = tbnameLen > 0 ? tbnameBuf : NULL;
  args.tbnameListLen = tbnameLen;
  args.stmt2Tags = bindLen > 0 && tagLen > 0 ? tagBuf : NULL;
  args.stmt2TagsLen = bindLen > 0 ? (int32_t)tagLen : 0;
  if (bindv != NULL) {
    args.bindTableCount = bindv->count;
    args.bindTableCountSet = true;
  }
  args.bindColIdx = colIdx;
  args.bindColIdxSet = true;
  (void)wlrApiBegin(&args, token);
  taosMemoryFree(tbnameBuf);
  taosMemoryFree(bindBuf);
  taosMemoryFree(tagBuf);
}

static int32_t wlrEncodeStmt2ColumnBindvWithScratch(TAOS_STMT2_COLUMN_BINDV *bindv, uint8_t *buf,
                                                    size_t bufLen, uint8_t *packedValues) {
  SWlrBindEncodeCol cols[128];
  size_t            packedOffset = 0;
  int32_t           result = 0;
  int32_t           c;

  if (bindv == NULL || bindv->columns == NULL) return 0;
  if (bindv->num_columns > 128) {
    tscWarn("WLR: stmt2 column bind has %d columns; recording without bind bytes", bindv->num_columns);
    return 0;
  }
  if (bindv->num_columns <= 0) return 0;
  if (bindv->num_rows <= 0) return 0;

  if (packedValues == NULL) return 0;

  for (c = 0; c < bindv->num_columns; ++c) {
    TAOS_STMT2_COLUMN_BIND *b = &bindv->columns[c];
    int32_t                 num = bindv->num_rows;
    uint32_t                width;

    cols[c].type = (uint16_t)b->buffer_type;
    cols[c].flags = WLR_BIND_FLAG_TRISTATE_NULL;
    cols[c].elemCount = num;
    cols[c].isNull = (const uint8_t *)b->is_null;
    cols[c].lengths = b->length;
    cols[c].values = (const uint8_t *)b->buffer;

    width = wlrTypeFixedWidth((uint16_t)b->buffer_type);
    if (width == 0) {
      uint32_t total = 0;
      size_t   sourceOffset = 0;
      int32_t  e;
      if (b->length == NULL) goto _return; /* var-length with no lengths is unusable */
      for (e = 0; e < num; ++e) {
        if (b->length[e] < 0) goto _return;
        size_t elemLen = (size_t)b->length[e];
        if (b->is_null == NULL || b->is_null[e] == 0) {
          if (SIZE_MAX - sourceOffset < elemLen) goto _return;
          if (UINT32_MAX - total < elemLen || packedOffset > bufLen || elemLen > bufLen - packedOffset) {
            goto _return;
          }
          if (elemLen > 0 && b->buffer == NULL) goto _return;
          if (elemLen > 0) {
            memcpy(packedValues + packedOffset, (const uint8_t *)b->buffer + sourceOffset, elemLen);
          }
          packedOffset += elemLen;
          total += (uint32_t)elemLen;
          sourceOffset += elemLen;
        }
      }
      cols[c].values = (total > 0) ? packedValues + packedOffset - total : NULL;
      cols[c].valueBytes = total;
    } else {
      if ((uint64_t)width * (uint32_t)num > UINT32_MAX) goto _return;
      cols[c].valueBytes = width * (uint32_t)num;
      if (b->buffer == NULL) {
        if (b->is_null == NULL || packedOffset > bufLen || cols[c].valueBytes > bufLen - packedOffset) {
          goto _return;
        }
        for (int32_t row = 0; row < num; ++row) {
          if (b->is_null[row] == 0) goto _return;
        }
        memset(packedValues + packedOffset, 0, cols[c].valueBytes);
        cols[c].values = packedValues + packedOffset;
        packedOffset += cols[c].valueBytes;
      }
    }
  }

  if (wlrBindEncodedSize(cols, bindv->num_columns) > bufLen) goto _return;
  result = wlrEncodeBind(buf, bufLen, cols, bindv->num_columns);
  if (result < 0) result = 0;

_return:
  return result;
}

static uint32_t wlrStmt2ColumnBindvSize(const TAOS_STMT2_COLUMN_BINDV *bindv) {
  if (bindv == NULL || bindv->columns == NULL || bindv->num_columns <= 0 ||
      bindv->num_columns > 128 || bindv->num_rows <= 0) return 0;
  SWlrBindEncodeCol cols[128] = {0};
  for (int32_t colIdx = 0; colIdx < bindv->num_columns; ++colIdx) {
    const TAOS_STMT2_COLUMN_BIND *bind = &bindv->columns[colIdx];
    SWlrBindEncodeCol *col = &cols[colIdx];
    col->type = (uint16_t)bind->buffer_type;
    col->flags = WLR_BIND_FLAG_TRISTATE_NULL;
    col->elemCount = bindv->num_rows;
    col->isNull = (const uint8_t *)bind->is_null;
    col->lengths = bind->length;
    col->values = (const uint8_t *)bind->buffer;
    uint32_t width = wlrTypeFixedWidth(col->type);
    if (width == 0) {
      if (bind->length == NULL) return 0;
      uint64_t total = 0;
      for (int32_t row = 0; row < bindv->num_rows; ++row) {
        if (bind->length[row] < 0) return 0;
        if (bind->is_null == NULL || bind->is_null[row] == 0) total += (uint32_t)bind->length[row];
        if (total > UINT32_MAX) return 0;
      }
      col->valueBytes = (uint32_t)total;
    } else {
      uint64_t total = (uint64_t)width * (uint32_t)bindv->num_rows;
      if (total > UINT32_MAX) return 0;
      col->valueBytes = (uint32_t)total;
    }
    if (col->valueBytes > 0 && col->values == NULL) col->values = (const uint8_t *)bind;
  }
  return wlrBindEncodedSize(cols, bindv->num_columns);
}

#ifdef WLR_TEST_HOOKS
uint32_t wlrTestStmt2ColumnBindvSize(void *bindv) {
  return wlrStmt2ColumnBindvSize((const TAOS_STMT2_COLUMN_BINDV *)bindv);
}
#endif

#ifdef WLR_TEST_HOOKS
static int32_t wlrEncodeStmt2ColumnBindv(TAOS_STMT2_COLUMN_BINDV *bindv, uint8_t *buf, size_t bufLen) {
  uint8_t *packedValues = (uint8_t *)taosMemoryMalloc(bufLen);
  if (packedValues == NULL) return 0;
  int32_t result = wlrEncodeStmt2ColumnBindvWithScratch(bindv, buf, bufLen, packedValues);
  taosMemoryFree(packedValues);
  return result;
}

int32_t wlrTestEncodeStmt2ColumnBindv(void *bindv, uint8_t *buf, size_t bufLen) {
  return wlrEncodeStmt2ColumnBindv((TAOS_STMT2_COLUMN_BINDV *)bindv, buf, bufLen);
}
#endif

/*
 * Open a recording token for a columnar bind. Columnar mode has no tbnames vector
 * (the table name is fixed in the SQL or carried in a tbname column), so unlike the
 * row-mode hook there is no separate table-name list to record.
 */
static void wlrColumnBindBegin(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv, uint16_t wlrApiType,
                               SWorkloadCallToken *pTok, uint64_t *pSid) {
  uint64_t wlrSid = (stmt != NULL) ? ((STscStmt2 *)stmt)->wlrStmtId : 0;
  *pSid = wlrSid;
  if (wlrSid == 0 || bindv == NULL || wlrRecorderState() != WLR_STATE_RUNNING) return;

  size_t captureCapacity = wlrStmt2ColumnBindvSize(bindv);
  uint8_t *wlrBuf = captureCapacity > 0 && captureCapacity <= WLR_MAX_FRAME_PAYLOAD
                        ? (uint8_t *)taosMemoryMalloc(captureCapacity * 2) : NULL;
  int32_t wlrLen =
      (wlrBuf != NULL)
          ? wlrEncodeStmt2ColumnBindvWithScratch(bindv, wlrBuf, captureCapacity,
                                                 wlrBuf + captureCapacity)
          : 0;

  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = wlrApiType;
  wlrArgs.logicalStmtId = wlrSid;
  wlrArgs.bindDesc = (wlrLen > 0) ? wlrBuf : NULL;
  wlrArgs.bindDescLen = wlrLen;
  if (bindv != NULL) {
    wlrArgs.bindTableCount = bindv->num_tables;
    wlrArgs.bindTableCountSet = true;
  }
  (void)wlrApiBegin(&wlrArgs, pTok);
  taosMemoryFree(wlrBuf);
}

static int32_t stmt2BindError(STscStmt2 *pStmt, int32_t code) {
  // A new binding error takes precedence over the previous execution result.
  SRequestObj *pRequest = pStmt->exec.pRequest;
  if (pRequest != NULL && pRequest->msgBuf != NULL) {
    pRequest->msgBuf[0] = '\0';
  }
  pStmt->errCode = code;
  terrno = pStmt->errCode;
  return terrno;
}

/*
 * Public entry. The body is in the impl because the bind path has many early
 * returns; wrapping here records exactly one event per user call without threading a
 * token through every failure branch.
 */
int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt2 *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    wlrStmt2RowBindBegin(stmt, bindv, col_idx, WLR_API_STMT2_BIND_PARAM, &wlrTok);
  }

  int32_t wlrCode = stmt2BindParamImpl(stmt, bindv, col_idx);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int stmt2BindParamImpl(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscStmt2 *pStmt = (STscStmt2 *)stmt;
  int32_t    code = TSDB_CODE_SUCCESS;
  STMT2_DLOG_E("start to bind param");

  if (stmtIsLiteral(pStmt)) {
    return stmtBindLiteral2(stmt);
  }

  // check query bind number
  bool isQuery = (STMT_TYPE_QUERY == pStmt->sql.type || (pStmt->sql.type == 0 && stmt2IsSelect(stmt)));
  if (atomic_load_8((int8_t *)&pStmt->asyncBindParam.asyncBindNum) > 1) {
    STMT2_ELOG_E("async bind param is still working, please try again later");
    terrno = TSDB_CODE_TSC_STMT_API_ERROR;
    return terrno;
  }

  if (pStmt->options.asyncExecFn && !pStmt->execSemWaited) {
    if (tsem_wait(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("bind param wait asyncExecSem failed");
    }
    pStmt->execSemWaited = true;
  }

  if (isQuery) {
    if (bindv->count != 1 || bindv->bind_cols[0]->num != 1) {
      STMT2_ELOG_E("query only support one table and one row bind");
      return stmt2BindError(pStmt, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
    }
  }

  for (int i = 0; i < bindv->count; ++i) {
    SVCreateTbReq *pCreateTbReq = NULL;
    if (!isQuery) {
      STMT2_TLOG("start to bind %dth table", i);
      if (bindv->tbnames && bindv->tbnames[i]) {
        code = stmtSetTbName2(stmt, bindv->tbnames[i]);
        if (code) {
          terrno = code;
          STMT2_ELOG("set tbname failed, code:%s", stmt2Errstr(stmt));
          return terrno;
        }
      }

      bool tableExists = stmt2TableExistsInCache(stmt);
      if (bindv->tags && bindv->tags[i]) {
        if (!tableExists) {
          code = stmtSetTbTags2(stmt, bindv->tags[i], &pCreateTbReq);
        }
        if (code == TSDB_CODE_SUCCESS) {
          code = stmt2CacheRetryTags(stmt, bindv->tags[i], false);
        }
      } else if (pStmt->bInfo.tbNameFlag & IS_FIXED_TAG) {
        // The fixed-tag template is initialized once per statement. Cached child tables then avoid cloning it
        // into every normal submit, while retaining enough information to auto-create on a retry.
        if (!pStmt->sql.fixValueTags || !tableExists) {
          code = stmtCheckTags2(stmt, &pCreateTbReq);
        }
        if (code == TSDB_CODE_SUCCESS) {
          code = stmt2CacheRetryTags(stmt, NULL, true);
        }
        if (code == TSDB_CODE_SUCCESS && tableExists && pCreateTbReq != NULL) {
          tdDestroySVCreateTbReq(pCreateTbReq);
          taosMemoryFreeClear(pCreateTbReq);
        }
      } else if (pStmt->sql.autoCreateTbl && !tableExists) {
        code = stmtSetTbTags2(stmt, NULL, &pCreateTbReq);
      }

      if (code) {
        terrno = code;
        STMT2_ELOG("set tags failed, code:%s", stmt2Errstr(stmt));
        if (pCreateTbReq) {
          tdDestroySVCreateTbReq(pCreateTbReq);
          taosMemoryFreeClear(pCreateTbReq);
        }
        return terrno;
      }
    }

    if (bindv->bind_cols && bindv->bind_cols[i]) {
      TAOS_STMT2_BIND *bind = bindv->bind_cols[i];

      if (bind->num <= 0 || bind->num > INT16_MAX) {
        STMT2_ELOG("bind num:%d must > 0 and < INT16_MAX", bind->num);
        if (pCreateTbReq) {
          tdDestroySVCreateTbReq(pCreateTbReq);
          taosMemoryFreeClear(pCreateTbReq);
        }
        return stmt2BindError(pStmt, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
      }

      code = stmtBindBatch2(stmt, bind, col_idx, pCreateTbReq);
      if (TSDB_CODE_SUCCESS != code) {
        terrno = code;
        STMT2_ELOG("bind batch failed, code:%s", stmt2Errstr(stmt));
        if (pCreateTbReq) {
          tdDestroySVCreateTbReq(pCreateTbReq);
          taosMemoryFreeClear(pCreateTbReq);
        }
        return terrno;
      }
    } else if (pCreateTbReq != NULL) {
      tdDestroySVCreateTbReq(pCreateTbReq);
      taosMemoryFree(pCreateTbReq);
    }
  }

  return code;
}

// Internal common implementation for async bind (both row and columnar)
static int stmtBindParamAImpl(TAOS_STMT2 *stmt, void *bindv, int32_t col_idx,
                              __taos_async_fn_t fp, void *param, bool is_columnar,
                              const SWorkloadCallToken *wlrToken, ThreadArgs **wlrArgsOut) {
  if (wlrArgsOut != NULL) *wlrArgsOut = NULL;
  if (stmt == NULL || bindv == NULL || fp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscStmt2 *pStmt = (STscStmt2 *)stmt;

  if (stmtIsLiteral(pStmt)) {
    return stmtBindLiteral2(stmt);
  }

  ThreadArgs *args = (ThreadArgs *)taosMemoryMalloc(sizeof(ThreadArgs));
  if (args == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  args->stmt = stmt;
  args->fp = fp;
  args->param = param;
  args->wlrCallId = wlrToken->callId;
  args->wlrBeginMonoNs = wlrToken->beginMonoNs;
  args->wlrStmtId = pStmt->wlrStmtId;
  args->wlrApiType = wlrToken->apiType;
  args->wlrReturnReady = false;
  args->is_columnar = is_columnar;
  if (is_columnar) {
    args->column_bindv = (TAOS_STMT2_COLUMN_BINDV *)bindv;
  } else {
    args->bindv = (TAOS_STMT2_BINDV *)bindv;
    args->col_idx = col_idx;
  }

  (void)taosThreadMutexLock(&(pStmt->asyncBindParam.mutex));
  if (atomic_load_8((int8_t *)&pStmt->asyncBindParam.asyncBindNum) > 0) {
    (void)taosThreadMutexUnlock(&(pStmt->asyncBindParam.mutex));
    taosMemoryFree(args);
    tscError("async bind param is still working, please try again later");
    terrno = TSDB_CODE_TSC_STMT_API_ERROR;
    return terrno;
  }
  (void)atomic_add_fetch_8(&pStmt->asyncBindParam.asyncBindNum, 1);
  (void)taosThreadMutexUnlock(&(pStmt->asyncBindParam.mutex));

  int code_s = taosStmt2AsyncBind(stmtAsyncBindThreadFunc, (void *)args);
  if (code_s != TSDB_CODE_SUCCESS) {
    terrno = code_s;
    (void)taosThreadMutexLock(&(pStmt->asyncBindParam.mutex));
    (void)atomic_sub_fetch_8(&pStmt->asyncBindParam.asyncBindNum, 1);
    (void)taosThreadCondSignal(&(pStmt->asyncBindParam.waitCond));
    (void)taosThreadMutexUnlock(&(pStmt->asyncBindParam.mutex));
    taosMemoryFree(args);
    tscError("async bind failed, code:%d , %s", code_s, tstrerror(code_s));
  } else if (wlrToken->callId != 0 && wlrArgsOut != NULL) {
    *wlrArgsOut = args;
  }

  return code_s;
}

static void wlrStmtAsyncDone(const SWorkloadCallToken *token, uint64_t stmtId, int32_t code) {
  SWorkloadDoneArgs done = {.code = code, .logicalStmtId = stmtId};
  wlrAsyncDone((SWorkloadCallToken *)token, &done);
}

/* Records dispatch/return here; the async bind thread records DONE immediately
 * before it invokes the user's callback. */
int taos_stmt2_bind_param_a(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx, __taos_async_fn_t fp,
                            void *param) {
  uint64_t           wlrSid = (stmt != NULL) ? ((STscStmt2 *)stmt)->wlrStmtId : 0;
  SWorkloadCallToken wlrTok = {0};
  if (wlrSid != 0 && wlrRecorderState() == WLR_STATE_RUNNING) {
    wlrStmt2RowBindBegin(stmt, bindv, col_idx, WLR_API_STMT2_BIND_PARAM_A, &wlrTok);
  }

  ThreadArgs *wlrArgs = NULL;
  int32_t wlrCode = stmtBindParamAImpl(stmt, bindv, col_idx, fp, param, false, &wlrTok,
                                       &wlrArgs);
  /* Literal binds return before allocating a worker; dispatch failures mean no
   * worker was started. All other successful calls emit DONE on that worker. */
  if (wlrTok.recorded &&
      (wlrCode != TSDB_CODE_SUCCESS || stmtIsLiteral((STscStmt2 *)stmt))) {
    wlrStmtAsyncDone(&wlrTok, wlrSid, wlrCode);
  }

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  if (wlrArgs != NULL) {
    STscStmt2 *pStmt = (STscStmt2 *)stmt;
    (void)taosThreadMutexLock(&pStmt->asyncBindParam.mutex);
    wlrArgs->wlrReturnReady = true;
    (void)taosThreadCondBroadcast(&pStmt->asyncBindParam.waitCond);
    (void)taosThreadMutexUnlock(&pStmt->asyncBindParam.mutex);
  }
  return wlrCode;
}

int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint64_t wlrSid = ((STscStmt2 *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  STscStmt2 *pWlrStmt = (STscStmt2 *)stmt;
  SWorkloadBeginArgs wlrArgs = {0};
  wlrArgs.apiType = WLR_API_STMT2_EXEC;
  wlrArgs.logicalStmtId = wlrSid;
  wlrArgs.stmt2Async = pWlrStmt->options.asyncExecFn != NULL;
  if (wlrSid != 0) (void)wlrApiBegin(&wlrArgs, &wlrTok);
  else wlrApiSkipToken(&wlrTok);
  int32_t wlrCode = stmtExec2(stmt, affected_rows, &wlrTok);
  if (wlrCode != TSDB_CODE_SUCCESS && pWlrStmt->options.asyncExecFn != NULL && wlrTok.recorded) {
    wlrStmtAsyncDone(&wlrTok, wlrSid, wlrCode);
    if (pWlrStmt->wlrExecCallId == wlrTok.callId) pWlrStmt->wlrExecCallId = 0;
  }

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = wlrCode;
    wlrRet.logicalStmtId = wlrSid;
    /* Recorded so replay can compare row counts without storing results (DS 6.11). */
    wlrRet.affectedRows = wlrCode == TSDB_CODE_SUCCESS ? pWlrStmt->exec.affectedRows : 0;
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt2_close(TAOS_STMT2 *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  /* Read the ID before stmtClose2 frees the handle. */
  uint64_t wlrSid = ((STscStmt2 *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT2_CLOSE, wlrSid, NULL, 0);

  int32_t wlrCode = stmtClose2(stmt);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert) {
  if (stmt == NULL || insert == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  *insert = stmt2IsInsert(stmt);
  return TSDB_CODE_SUCCESS;
}

int taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields) {
  if (stmt == NULL || count == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscStmt2 *pStmt = (STscStmt2 *)stmt;
  STMT2_DLOG_E("start to get fields");

  if (stmtIsLiteral(pStmt)) {
    if (count)  *count = 0;
    if (fields) *fields = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (STMT_TYPE_INSERT == pStmt->sql.type || STMT_TYPE_MULTI_INSERT == pStmt->sql.type ||
      (pStmt->sql.type == 0 && stmt2IsInsert(stmt))) {
    return stmtGetStbColFields2(stmt, count, fields);
  }
  if (STMT_TYPE_QUERY == pStmt->sql.type || (pStmt->sql.type == 0 && stmt2IsSelect(stmt))) {
    return stmtGetParamNum2(stmt, count);
  }

  tscError("Invalid sql for stmt %s", pStmt->sql.sqlStr);
  return TSDB_CODE_PAR_SYNTAX_ERROR;
}

DLL_EXPORT void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields) {
  (void)stmt;
  if (!fields) return;
  taosMemoryFree(fields);
}

TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt) {
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  uint64_t wlrSid = ((STscStmt2 *)stmt)->wlrStmtId;
  SWorkloadCallToken wlrTok = {0};
  WLR_API_BEGIN_STMT(&wlrTok, WLR_API_STMT2_RESULT, wlrSid, NULL, 0);

  TAOS_RES *res = stmtUseResult2(stmt);

  if (wlrTok.recorded) {
    SWorkloadReturnArgs wlrRet = {0};
    wlrRet.code = taos_errno(res);
    wlrRet.logicalStmtId = wlrSid;
    /* Where a STMT2 result becomes user-visible, so it gets the result ID that a
     * later taos_free_result will reference (TS RE013). */
    if (res != NULL && TD_RES_QUERY(res)) {
      SRequestObj *pReq = (SRequestObj *)res;
      if (pReq->wlrResultId == 0) pReq->wlrResultId = wlrNextResultId();
      wlrRet.logicalResultId = pReq->wlrResultId;
    }
    wlrApiReturn(&wlrTok, &wlrRet);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return res;
}

char *taos_stmt2_error(TAOS_STMT2 *stmt) { return (char *)stmt2Errstr(stmt); }

int taos_set_conn_mode(TAOS *taos, int mode, int value) {
  int32_t code = 0;
  if (taos == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }
  switch (mode) {
    case TAOS_CONN_MODE_BI:
      atomic_store_8(&pObj->biMode, value);
      break;
    default:
      tscError("not supported mode.");
      code = TSDB_CODE_INVALID_PARA;
  }
  releaseTscObj(*(int64_t *)taos);
  return code;
}

char *getBuildInfo() { return td_buildinfo; }

int32_t taos_connect_is_alive(TAOS *taos) {
  int32_t code = 0, lino = 0;
  if (taos == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  STscObj *pObj = acquireTscObj(*(int64_t *)taos);
  if (NULL == pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    tscError("invalid parameter for %s", __func__);
    return terrno;
  }

  code = tscCheckConnSessionMetric(pObj);
  TAOS_CHECK_GOTO(code, &lino, _error);

_error:
  releaseTscObj(*(int64_t *)taos);

  if (code != 0) {
    tscError("taos conn failed to check alive, code:%d - %s", code, tstrerror(code));
  }

  return code != 0 ? 0 : 1;
}
static int32_t buildInstanceRegisterSql(const SInstanceRegisterReq *req, char **ppSql, uint32_t *pLen) {
  const char *action = (req->expire < 0) ? "UNREGISTER" : "REGISTER";
  int32_t     len = 0;

  len += snprintf(NULL, 0, "%s INSTANCE '%s'", action, req->id);
  if (req->type[0] != 0) {
    len += snprintf(NULL, 0, " TYPE '%s'", req->type);
  }
  if (req->desc[0] != 0) {
    len += snprintf(NULL, 0, " DESC '%s'", req->desc);
  }
  if (req->expire >= 0) {
    len += snprintf(NULL, 0, " EXPIRE %d", req->expire);
  }

  char *sql = taosMemoryMalloc((size_t)len + 1);
  if (sql == NULL) {
    return terrno;
  }

  int32_t offset = snprintf(sql, (size_t)len + 1, "%s INSTANCE '%s'", action, req->id);
  if (req->type[0] != 0) {
    offset += snprintf(sql + offset, (size_t)len + 1 - (size_t)offset, " TYPE '%s'", req->type);
  }
  if (req->desc[0] != 0) {
    offset += snprintf(sql + offset, (size_t)len + 1 - (size_t)offset, " DESC '%s'", req->desc);
  }
  if (req->expire >= 0) {
    (void)snprintf(sql + offset, (size_t)len + 1 - (size_t)offset, " EXPIRE %d", req->expire);
  }

  *ppSql = sql;
  if (pLen != NULL) {
    *pLen = (uint32_t)len;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sendInstanceRegisterReq(STscObj *pObj, const SInstanceRegisterReq *req) {
  SRequestObj *pRequest = NULL;
  int32_t      code = createRequest(pObj->id, TDMT_MND_REGISTER_INSTANCE, 0, &pRequest);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  code = buildInstanceRegisterSql(req, &pRequest->sqlstr, (uint32_t *)&pRequest->sqlLen);
  if (code != TSDB_CODE_SUCCESS) {
    goto _cleanup;
  }

  int32_t msgLen = tSerializeSInstanceRegisterReq(NULL, 0, (SInstanceRegisterReq *)req);
  if (msgLen <= 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    goto _cleanup;
  }

  void *pMsg = taosMemoryMalloc(msgLen);
  if (pMsg == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    goto _cleanup;
  }

  if (tSerializeSInstanceRegisterReq(pMsg, msgLen, (SInstanceRegisterReq *)req) < 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    taosMemoryFree(pMsg);
    goto _cleanup;
  }

  pRequest->type = TDMT_MND_REGISTER_INSTANCE;
  pRequest->body.requestMsg = (SDataBuf){.pData = pMsg, .len = msgLen, .handle = NULL};

  SMsgSendInfo *pSend = buildMsgInfoImpl(pRequest);
  if (pSend == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    taosMemoryFree(pMsg);
    pRequest->body.requestMsg.pData = NULL;
    goto _cleanup;
  }

  SEpSet epSet = getEpSet_s(&pObj->pAppInfo->mgmtEp);
  code = asyncSendMsgToServer(pObj->pAppInfo->pTransporter, &epSet, NULL, pSend);
  if (code != TSDB_CODE_SUCCESS) {
    destroySendMsgInfo(pSend);
    pRequest->body.requestMsg = (SDataBuf){0};
    goto _cleanup;
  }

  code = tsem_wait(&pRequest->body.rspSem);
  if (code != TSDB_CODE_SUCCESS) {
    code = terrno != 0 ? terrno : code;
    goto _cleanup;
  }

  code = pRequest->code;
  terrno = code;

_cleanup:
  destroyRequest(pRequest);
  return code;
}

static bool instanceRegisterRpcRfp(int32_t code, tmsg_t msgType) {
  if (NEED_REDIRECT_ERROR(code)) {
    return true;
  } else if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY || code == TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE ||
             code == TSDB_CODE_SYN_WRITE_STALL || code == TSDB_CODE_SYN_PROPOSE_NOT_READY ||
             code == TSDB_CODE_SYN_RESTORING) {
    tscDebug("client msg type %s should retry since %s", TMSG_INFO(msgType), tstrerror(code));
    return true;
  } else {
    return false;
  }
}

/** Build epSet from firstEp and secondEp in config. pCfg must have valid firstEp. */
static int32_t instanceBuildEpSetFromCfg(SConfig *pCfg, SEpSet *pEpSet) {
  SConfigItem *pFirstEpItem = cfgGetItem(pCfg, "firstEp");
  if (pFirstEpItem == NULL || pFirstEpItem->str == NULL || pFirstEpItem->str[0] == 0) {
    return TSDB_CODE_CFG_NOT_FOUND;
  }
  SEp     firstEp = {0};
  int32_t code = taosGetFqdnPortFromEp(pFirstEpItem->str, &firstEp);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 1;
  tstrncpy(pEpSet->eps[0].fqdn, firstEp.fqdn, TSDB_FQDN_LEN);
  pEpSet->eps[0].port = firstEp.port;

  SConfigItem *pSecondEpItem = cfgGetItem(pCfg, "secondEp");
  if (pSecondEpItem != NULL && pSecondEpItem->str != NULL && pSecondEpItem->str[0] != 0) {
    SEp secondEp = {0};
    if (taosGetFqdnPortFromEp(pSecondEpItem->str, &secondEp) == TSDB_CODE_SUCCESS) {
      tstrncpy(pEpSet->eps[1].fqdn, secondEp.fqdn, TSDB_FQDN_LEN);
      pEpSet->eps[1].port = secondEp.port;
      pEpSet->numOfEps = 2;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/** Init and open instance RPC client. label e.g. "INST" or "LIST". Returns handle or NULL. */
static void *instanceOpenRpcClient(const char *label) {
  SRpcInit rpcInit = {0};
  rpcInit.label = (char *)label;
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = NULL;
  rpcInit.sessions = 16;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.compressSize = tsCompressMsgSize;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.rfp = instanceRegisterRpcRfp;
  rpcInit.retryMinInterval = tsRedirectPeriod;
  rpcInit.retryStepFactor = tsRedirectFactor;
  rpcInit.retryMaxInterval = tsRedirectMaxPeriod;
  rpcInit.retryMaxTimeout = tsMaxRetryWaitTime;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);
  rpcInit.connLimitNum = connLimitNum;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  rpcInit.readTimeout = tsReadTimeout;
  rpcInit.ipv6 = tsEnableIpv6;
  rpcInit.enableSSL = tsEnableTLS;

  memcpy(rpcInit.caPath, tsTLSCaPath, strlen(tsTLSCaPath));
  memcpy(rpcInit.certPath, tsTLSSvrCertPath, strlen(tsTLSSvrCertPath));
  memcpy(rpcInit.keyPath, tsTLSSvrKeyPath, strlen(tsTLSSvrKeyPath));
  memcpy(rpcInit.cliCertPath, tsTLSCliCertPath, strlen(tsTLSCliCertPath));
  memcpy(rpcInit.cliKeyPath, tsTLSCliKeyPath, strlen(tsTLSCliKeyPath));

  int32_t code = taosVersionStrToInt(td_version, &rpcInit.compatibilityVer);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("failed to convert taos version from str to int, errcode:%s", tstrerror(code));
    terrno = code;
    return NULL;
  }

  void *clientRpc = rpcOpen(&rpcInit);
  if (clientRpc == NULL) {
    tscError("failed to init instance rpc client since %s", tstrerror(terrno));
  }
  return clientRpc;
}

/** Client-side rate limit: fixed 100 calls per 1s (register + list combined, protect mnode). */
#define TSC_INSTANCE_API_RL_WINDOW_MS   1000
#define TSC_INSTANCE_API_RL_MAX_PER_SEC 100

static TdThreadOnce  gInstRlOnce = PTHREAD_ONCE_INIT;
static TdThreadMutex gInstRlMutex;
static int32_t       gInstRlMutexInited = 0;
static int64_t       gInstRlWindowStartMs = 0;
static int32_t       gInstRlCountInWindow = 0;

static void instRlMutexInit(void) {
  if (taosThreadMutexInit(&gInstRlMutex, NULL) == TSDB_CODE_SUCCESS) {
    gInstRlMutexInited = 1;
  }
}

/** Call before instance RPC; shared by register and list. */
static int32_t instanceApiRateLimitTry(void) {
  int32_t c = taosThreadOnce(&gInstRlOnce, instRlMutexInit);
  if (c != TSDB_CODE_SUCCESS) {
    terrno = c;
    return c;
  }
  if (!gInstRlMutexInited) {
    tscError("instance API rate limiter init failed, block request");
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }

  int64_t now = taosGetTimestampMs();
  (void)taosThreadMutexLock(&gInstRlMutex);
  /* Reset window if first use, window elapsed, or clock moved backwards (NTP). */
  if (gInstRlWindowStartMs == 0 || now < gInstRlWindowStartMs ||
      (now - gInstRlWindowStartMs) >= (int64_t)TSC_INSTANCE_API_RL_WINDOW_MS) {
    gInstRlWindowStartMs = now;
    gInstRlCountInWindow = 0;
  }
  if (gInstRlCountInWindow >= TSC_INSTANCE_API_RL_MAX_PER_SEC) {
    (void)taosThreadMutexUnlock(&gInstRlMutex);
    tscWarn("instance API rate limit exceeded (max %d calls per %d ms, register and list combined)",
            TSC_INSTANCE_API_RL_MAX_PER_SEC, TSC_INSTANCE_API_RL_WINDOW_MS);
    terrno = TSDB_CODE_TSC_INSTANCE_API_RATE_LIMIT;
    return TSDB_CODE_TSC_INSTANCE_API_RATE_LIMIT;
  }
  gInstRlCountInWindow++;
  (void)taosThreadMutexUnlock(&gInstRlMutex);
  return TSDB_CODE_SUCCESS;
}

/** Process-wide singleton: connectionless instance APIs share one rpcOpen; closed in taos_cleanup. */
static TdThreadOnce     gInstRpcOnce = PTHREAD_ONCE_INIT;
static TdThreadMutex    gInstRpcMutex;
static TdThreadCond     gInstRpcCond;
static volatile int32_t gInstRpcMutexReady = 0;
static volatile int32_t gInstRpcCondReady = 0;
static void            *gInstRpc = NULL;
static int32_t          gInstRpcRef = 0;
static int32_t          gInstRpcClosing = 0;

static void instRpcMutexInit(void) {
  if (taosThreadMutexInit(&gInstRpcMutex, NULL) == TSDB_CODE_SUCCESS) {
    if (taosThreadCondInit(&gInstRpcCond, NULL) == TSDB_CODE_SUCCESS) {
      gInstRpcCondReady = 1;
      gInstRpcMutexReady = 1;
      return;
    }
    (void)taosThreadMutexDestroy(&gInstRpcMutex);
  }
}

static int32_t instanceRpcAcquire(void **ppRpc) {
  int32_t code = taosThreadOnce(&gInstRpcOnce, instRpcMutexInit);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }
  if (!gInstRpcMutexReady || !gInstRpcCondReady) {
    tscError("instance RPC singleton not ready, block request");
    terrno = TSDB_CODE_TSC_INTERNAL_ERROR;
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  code = taosThreadMutexLock(&gInstRpcMutex);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (gInstRpcClosing) {
    (void)taosThreadMutexUnlock(&gInstRpcMutex);
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  if (gInstRpc == NULL) {
    gInstRpc = instanceOpenRpcClient("INST");
    if (gInstRpc == NULL) {
      code = terrno;
      (void)taosThreadMutexUnlock(&gInstRpcMutex);
      return code;
    }
    tscInfo("instance RPC singleton opened, handle:%p (search this line to count rpcOpen)", gInstRpc);
  }
  gInstRpcRef++;
  *ppRpc = gInstRpc;
  (void)taosThreadMutexUnlock(&gInstRpcMutex);
  return TSDB_CODE_SUCCESS;
}

static void instanceRpcRelease(void) {
  if (!gInstRpcMutexReady || !gInstRpcCondReady) {
    return;
  }
  (void)taosThreadMutexLock(&gInstRpcMutex);
  if (gInstRpcRef > 0) {
    gInstRpcRef--;
    if (gInstRpcClosing && gInstRpcRef == 0) {
      (void)taosThreadCondSignal(&gInstRpcCond);
    }
  }
  (void)taosThreadMutexUnlock(&gInstRpcMutex);
}

static void instanceRpcGlobalCleanup(void) {
  if (!gInstRpcMutexReady || !gInstRpcCondReady) {
    return;
  }
  (void)taosThreadMutexLock(&gInstRpcMutex);
  gInstRpcClosing = 1;
  while (gInstRpcRef > 0) {
    (void)taosThreadCondWait(&gInstRpcCond, &gInstRpcMutex);
  }
  if (gInstRpc != NULL) {
    tscInfo("instance RPC singleton closing, handle:%p (search this line to count rpcClose)", gInstRpc);
    rpcClose(gInstRpc);
    gInstRpc = NULL;
  }
  gInstRpcCondReady = 0;
  gInstRpcMutexReady = 0;
  (void)taosThreadMutexUnlock(&gInstRpcMutex);
}

int32_t taos_register_instance(const char *id, const char *type, const char *desc, int32_t expire) {
  if (id == NULL || id[0] == 0) {
    return terrno = TSDB_CODE_INVALID_PARA;
  }

  // Validate string lengths
  size_t idLen = strlen(id);
  if (idLen >= TSDB_INSTANCE_ID_LEN) {
    tscError("instance id length %zu exceeds limit %d", idLen, TSDB_INSTANCE_ID_LEN - 1);
    return terrno = TSDB_CODE_INVALID_PARA;
  }

  if (type != NULL && type[0] != 0) {
    size_t typeLen = strlen(type);
    if (typeLen >= TSDB_INSTANCE_TYPE_LEN) {
      tscError("instance type length %zu exceeds limit %d", typeLen, TSDB_INSTANCE_TYPE_LEN - 1);
      return terrno = TSDB_CODE_INVALID_PARA;
    }
  }

  if (desc != NULL && desc[0] != 0) {
    size_t descLen = strlen(desc);
    if (descLen >= TSDB_INSTANCE_DESC_LEN) {
      tscError("instance desc length %zu exceeds limit %d", descLen, TSDB_INSTANCE_DESC_LEN - 1);
      return terrno = TSDB_CODE_INVALID_PARA;
    }
  }

  int32_t code = taos_init();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SConfig *pCfg = taosGetCfg();
  if (pCfg == NULL) {
    return terrno = TSDB_CODE_CFG_NOT_FOUND;
  }

  SEpSet epSet = {0};
  code = instanceBuildEpSetFromCfg(pCfg, &epSet);
  if (code != TSDB_CODE_SUCCESS) {
    return terrno = code;
  }

  code = instanceApiRateLimitTry();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  void *clientRpc = NULL;
  code = instanceRpcAcquire(&clientRpc);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  SRpcMsg rpcMsg = {0};
  SRpcMsg rpcRsp = {0};

  // Prepare request
  SInstanceRegisterReq req = {0};
  tstrncpy(req.id, id, sizeof(req.id));
  if (type != NULL && type[0] != 0) {
    tstrncpy(req.type, type, sizeof(req.type));
  }
  if (desc != NULL && desc[0] != 0) {
    tstrncpy(req.desc, desc, sizeof(req.desc));
  }
  req.expire = expire;

  int32_t contLen = tSerializeSInstanceRegisterReq(NULL, 0, &req);
  if (contLen <= 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    goto _register_inst_end;
  }

  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    goto _register_inst_end;
  }

  if (tSerializeSInstanceRegisterReq(pCont, contLen, &req) < 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    rpcFreeCont(pCont);
    goto _register_inst_end;
  }

  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_REGISTER_INSTANCE;
  rpcMsg.info.ahandle = (void *)0x9528;  // Different magic number from server status
  rpcMsg.info.notFreeAhandle = 1;

  code = rpcSendRecv(clientRpc, &epSet, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to send instance register req since %s", tstrerror(code));
    goto _register_inst_end;
  }

  if (rpcRsp.code != 0) {
    code = rpcRsp.code;
    tscError("instance register failed, code:%s", tstrerror(code));
  } else {
    code = TSDB_CODE_SUCCESS;
  }

  if (rpcRsp.pCont != NULL) {
    rpcFreeCont(rpcRsp.pCont);
  }

_register_inst_end:
  instanceRpcRelease();
  terrno = code;
  return code;
}

int32_t taos_list_instances(const char *filter_type, char ***pList, int32_t *pCount) {
  if (pList == NULL || pCount == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = taos_init();
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  SConfig *pCfg = taosGetCfg();
  if (pCfg == NULL) {
    terrno = TSDB_CODE_CFG_NOT_FOUND;
    return TSDB_CODE_CFG_NOT_FOUND;
  }

  SEpSet epSet = {0};
  code = instanceBuildEpSetFromCfg(pCfg, &epSet);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  code = instanceApiRateLimitTry();
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  void *clientRpc = NULL;
  code = instanceRpcAcquire(&clientRpc);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  SRpcMsg rpcMsg = {0};
  SRpcMsg rpcRsp = {0};

  SInstanceListReq req = {0};
  if (filter_type != NULL && filter_type[0] != 0) {
    tstrncpy(req.filter_type, filter_type, sizeof(req.filter_type));
  }

  int32_t contLen = tSerializeSInstanceListReq(NULL, 0, &req);
  if (contLen <= 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    goto _list_inst_end;
  }

  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    goto _list_inst_end;
  }

  if (tSerializeSInstanceListReq(pCont, contLen, &req) < 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_TSC_INTERNAL_ERROR;
    rpcFreeCont(pCont);
    goto _list_inst_end;
  }

  rpcMsg.pCont = pCont;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TDMT_MND_LIST_INSTANCES;
  rpcMsg.info.ahandle = (void *)0x9529;  // Different magic number from register
  rpcMsg.info.notFreeAhandle = 1;

  code = rpcSendRecv(clientRpc, &epSet, &rpcMsg, &rpcRsp);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to send instance list req since %s", tstrerror(code));
    goto _list_inst_end;
  }

  if (rpcRsp.code != 0) {
    code = rpcRsp.code;
    tscError("instance list failed, code:%s", tstrerror(code));
    if (rpcRsp.pCont != NULL) {
      rpcFreeCont(rpcRsp.pCont);
    }
    goto _list_inst_end;
  }

  if (rpcRsp.pCont != NULL && rpcRsp.contLen > 0) {
    SInstanceListRsp rsp = {0};
    code = tDeserializeSInstanceListRsp(rpcRsp.pCont, rpcRsp.contLen, &rsp);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("failed to deserialize instance list rsp, code:%s", tstrerror(code));
      if (rsp.ids != NULL) {
        for (int32_t i = 0; i < rsp.count; i++) {
          if (rsp.ids[i] != NULL) {
            taosMemoryFree(rsp.ids[i]);
          }
        }
        taosMemoryFree(rsp.ids);
        rsp.ids = NULL;
      }
      rsp.count = 0;
      rpcFreeCont(rpcRsp.pCont);
      goto _list_inst_end;
    }
    *pList = rsp.ids;
    *pCount = rsp.count;
  } else {
    *pList = NULL;
    *pCount = 0;
  }

  if (rpcRsp.pCont != NULL) {
    rpcFreeCont(rpcRsp.pCont);
  }
  code = TSDB_CODE_SUCCESS;

_list_inst_end:
  instanceRpcRelease();
  terrno = code;
  return code;
}

void taos_free_instances(char ***pList, int32_t count) {
  if (pList == NULL || *pList == NULL || count <= 0) {
    return;
  }

  // Free each string in the array
  for (int32_t i = 0; i < count; i++) {
    if ((*pList)[i] != NULL) {
      taosMemoryFree((*pList)[i]);
      (*pList)[i] = NULL;
    }
  }

  // Free the array itself
  taosMemoryFree(*pList);
  *pList = NULL;
}

static inline bool stmt2ColumnBindNeedsLength(int32_t type) {
  return IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64;
}

static int validateColumnBindMetadata(STscStmt2 *pStmt, TAOS_STMT2_COLUMN_BINDV *bindv) {
  for (int32_t colIdx = 0; colIdx < bindv->num_columns; ++colIdx) {
    TAOS_STMT2_COLUMN_BIND *colBind = &bindv->columns[colIdx];
    int32_t type = colBind->buffer_type;
    if (IS_INVALID_TYPE(type)) {
      STMT2_ELOG("Invalid column bind type, column index:%d, type:%d", colIdx, type);
      return TSDB_CODE_INVALID_PARA;
    }

    bool needsLength = stmt2ColumnBindNeedsLength(type);
    if (needsLength && colBind->length == NULL) {
      STMT2_ELOG("Column bind length is required, column index:%d, type:%d", colIdx, type);
      return TSDB_CODE_INVALID_PARA;
    }

    if (colBind->buffer != NULL) {
      continue;
    }
    if (colBind->is_null == NULL) {
      STMT2_ELOG("Column bind buffer is required for non-null values, column index:%d, type:%d", colIdx, type);
      return TSDB_CODE_INVALID_PARA;
    }
    for (int32_t row = 0; row < bindv->num_rows; ++row) {
      bool isNull = colBind->is_null[row];
      if (!isNull) {
        STMT2_ELOG("Column bind buffer is required for non-null values, column index:%d, type:%d", colIdx, type);
        return TSDB_CODE_INVALID_PARA;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

// Inline function: Validate tbname column for super table inserts
static inline int validateTbnameColumn(
    STscStmt2 *pStmt,
    TAOS_STMT2_COLUMN_BINDV *bindv,
    bool hasTbnameColumn,
    int *tbnameColIdx
) {
  if (!hasTbnameColumn) {
    *tbnameColIdx = -1;
    return TSDB_CODE_SUCCESS;
  }

  // Get tbname column index from cached information
  *tbnameColIdx = pStmt->sql.cachedTbnameColIdx;

  // Validate tbname column index
  if (*tbnameColIdx < 0 || *tbnameColIdx >= bindv->num_columns) {
    STMT2_ELOG("Invalid tbname column index: %d, num_columns: %d",
               *tbnameColIdx, bindv->num_columns);
    return TSDB_CODE_INVALID_PARA;
  }

  // Tbname column must be BINARY type
  if (bindv->columns[*tbnameColIdx].buffer_type != TSDB_DATA_TYPE_BINARY) {
    STMT2_ELOG("Tbname column (index %d) must be BINARY type, got %d",
               *tbnameColIdx, bindv->columns[*tbnameColIdx].buffer_type);
    return TSDB_CODE_INVALID_PARA;
  }

  // Tbname column must have length array (variable-length data)
  if (bindv->columns[*tbnameColIdx].length == NULL) {
    STMT2_ELOG("Tbname column (index %d) must have length array for variable-length data",
               *tbnameColIdx);
    return TSDB_CODE_INVALID_PARA;
  }

  if (bindv->columns[*tbnameColIdx].buffer == NULL) {
    STMT2_ELOG("Tbname column (index %d) must have non-NULL buffer", *tbnameColIdx);
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t row = 0; row < bindv->num_rows; ++row) {
    if (bindv->columns[*tbnameColIdx].is_null != NULL && bindv->columns[*tbnameColIdx].is_null[row]) {
      STMT2_ELOG("Tbname column (index %d) must not be NULL, row:%d", *tbnameColIdx, row);
      return TSDB_CODE_INVALID_PARA;
    }
    if (bindv->columns[*tbnameColIdx].length[row] <= 0) {
      STMT2_ELOG("Invalid tbname length, column index:%d, row:%d, length:%d",
                 *tbnameColIdx, row, bindv->columns[*tbnameColIdx].length[row]);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// Inline function: Process columns for a single table (used by query, super table, and normal table)
static inline int processColumnsForTable(
    STscStmt2 *pStmt,
    int tableIdx,
    int startRow,
    int numRows,
    int tbnameColIdx,
    TAOS_STMT2_COLUMN_BINDV *bindv,
    TAOS_STMT2_BINDV *rowBindv,
    int32_t *colOffsets
) {
  int code = TSDB_CODE_SUCCESS;

  // Get field information (unified for query and insert)
  int fieldNum = pStmt->sql.cachedFieldNum;
  TAOS_FIELD_ALL* pFields = pStmt->sql.cachedFields;

  // Initialize column indices
  int tagColIdx = 0;
  int dataColIdx = 0;
  int bindvColIdx = 0;

  // Process each field
  for (int fieldIdx = 0; fieldIdx < fieldNum; fieldIdx++) {
    TAOS_FIELD_ALL *field = &pFields[fieldIdx];

    // Skip the tbname field (only for super table with tbname column)
    if (fieldIdx == tbnameColIdx) {
      bindvColIdx++;
      continue;
    }

    // Skip non-TAG/COL/QUERY type fields
    if (field->field_type != TAOS_FIELD_TAG &&
        field->field_type != TAOS_FIELD_COL &&
        field->field_type != TAOS_FIELD_QUERY) {
      bindvColIdx++;
      continue;
    }

    if (bindvColIdx >= bindv->num_columns) {
      tscError("column bind count mismatch: field index %d requires column %d, but only %d provided",
               fieldIdx, bindvColIdx, bindv->num_columns);
      return TSDB_CODE_INVALID_PARA;
    }
    const TAOS_STMT2_COLUMN_BIND *colBind = &bindv->columns[bindvColIdx];

    // Determine bind destination based on field type
    TAOS_STMT2_BIND *bind;
    if (field->field_type == TAOS_FIELD_TAG) {
      if (rowBindv->tags == NULL || rowBindv->tags[tableIdx] == NULL) {
        tscError("tags array not allocated for table %d", tableIdx);
        return TSDB_CODE_INVALID_PARA;
      }
      bind = &rowBindv->tags[tableIdx][tagColIdx++];
    } else if (field->field_type == TAOS_FIELD_COL ||
               field->field_type == TAOS_FIELD_QUERY) {
      if (rowBindv->bind_cols == NULL || rowBindv->bind_cols[tableIdx] == NULL) {
        tscError("bind_cols array not allocated for table %d", tableIdx);
        return TSDB_CODE_INVALID_PARA;
      }
      bind = &rowBindv->bind_cols[tableIdx][dataColIdx++];
    } else {
      bindvColIdx++;
      continue;
    }

    // Set buffer type
    bind->buffer_type = colBind->buffer_type;
    bind->length = NULL;
    bind->is_null = NULL;

    // Determine number of rows: TAG has 1 row, COL/QUERY has numRows
    int colNumRows = (field->field_type == TAOS_FIELD_TAG) ? 1 : numRows;
    bind->num = colNumRows;

    // Check if variable length type
    bool isVarLength = IS_VAR_DATA_TYPE(colBind->buffer_type) ||
                      colBind->buffer_type == TSDB_DATA_TYPE_DECIMAL ||
                      colBind->buffer_type == TSDB_DATA_TYPE_DECIMAL64;

    // Set buffer, length, and is_null with offset
    if (isVarLength) {
      if (tbnameColIdx < 0) {
        for (int32_t row = startRow; row < startRow + colNumRows; ++row) {
          if (colBind->length[row] < 0) {
            STMT2_ELOG("Invalid column bind length, column index:%d, row:%d, length:%d",
                       bindvColIdx, row, colBind->length[row]);
            return TSDB_CODE_INVALID_PARA;
          }
        }
      }
      bind->buffer = colBind->buffer ? (char*)colBind->buffer + colOffsets[fieldIdx] : NULL;
      bind->length = colBind->length + startRow;
      bind->is_null = colBind->is_null ? (char*)colBind->is_null + startRow : NULL;
    } else {
      int32_t typeSize = tDataTypes[colBind->buffer_type].bytes;
      bind->buffer = colBind->buffer ? (char*)colBind->buffer + (int64_t)startRow * typeSize : NULL;
      bind->is_null = colBind->is_null ? (char*)colBind->is_null + startRow : NULL;
    }

    bindvColIdx++;
  }

  return code;
}

// Columnar binding implementation - converts columnar data to row format and calls taos_stmt2_bind_param
/* Public entry. Wrapped for the same reason as the row-mode bind: the impl has 16
 * early returns, and wrapping records exactly one event per user call without
 * threading a token through every failure branch. */
int taos_stmt2_bind_param_column(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv) {
  SWorkloadCallToken wlrTok = {0};
  uint64_t           wlrSid = 0;
  wlrColumnBindBegin(stmt, bindv, WLR_API_STMT2_BIND_PARAM_COLUMN, &wlrTok, &wlrSid);

  int32_t wlrCode = stmt2BindParamColumnImpl(stmt, bindv);

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  return wlrCode;
}

int stmt2BindParamColumnImpl(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv) {
  int code = TSDB_CODE_SUCCESS;
  if (stmt == NULL) {
    tscError("NULL parameter for %s", __FUNCTION__);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  STscStmt2 *pStmt = (STscStmt2 *)stmt;
  if (bindv == NULL) {
    return stmt2BindError(pStmt, TSDB_CODE_INVALID_PARA);
  }

  // Resource management variables
  int32_t *colOffsets = NULL;
  TAOS_STMT2_BINDV rowBindv;
  memset(&rowBindv, 0, sizeof(rowBindv));

  if (atomic_load_8((int8_t *)&pStmt->asyncBindParam.asyncBindNum) > 1) {
    terrno = TSDB_CODE_TSC_STMT_API_ERROR;
    return terrno;
  }
  // Finish the previous execution before updating its request diagnostics or field cache.
  if (pStmt->options.asyncExecFn && !pStmt->execSemWaited) {
    if (tsem_wait(&pStmt->asyncExecSem) != 0) {
      STMT2_ELOG_E("column bind wait asyncExecSem failed");
    }
    pStmt->execSemWaited = true;
  }

  if (pStmt->errCode != TSDB_CODE_SUCCESS) {
    terrno = pStmt->errCode;
    return pStmt->errCode;
  }

  if (bindv->num_columns < 1 || bindv->columns == NULL) {
    tscError("No columns in bindv");
    return stmt2BindError(pStmt, TSDB_CODE_INVALID_PARA);
  }

  STMT2_DLOG_E("start to bind param from columnar data");

  // Get number of rows
  int totalRows = bindv->num_rows;
  if (totalRows == 0) {
    STMT2_ELOG_E("No rows found in columnar data");
    code = TSDB_CODE_INVALID_PARA;
    goto _return;
  }

  code = stmtEnsureColumnFieldCache2(stmt);
  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_TSC_STMT_TBNAME_ERROR || code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      code = TSDB_CODE_TSC_STMT_API_ERROR;
    }
    if (code == TSDB_CODE_TSC_STMT_API_ERROR) {
      code = stmtBuildErrorMsgWithCode(pStmt, "Stmt API usage error", code);
    } else {
      pStmt->errCode = code;
    }
    STMT2_ELOG("failed to initialize column field cache, code:%s", tstrerror(code));
    goto _return;
  }

  // The number of input columns must strictly match placeholders derived at prepare.
  if (bindv->num_columns != pStmt->sql.cachedFieldNum) {
    STMT2_ELOG("column count mismatch, expected:%d, actual:%d",
               pStmt->sql.cachedFieldNum, bindv->num_columns);
    code = TSDB_CODE_PAR_INVALID_COLUMNS_NUM;
    goto _return;
  }

  code = validateColumnBindMetadata(pStmt, bindv);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  // Query keeps stmt2 row-bind semantics: each bind/exec handles exactly one parameter set.
  if (!pStmt->sql.cachedIsInsert && bindv->num_rows != 1) {
    STMT2_ELOG("query columnar bind only supports num_rows=1, actual:%d", bindv->num_rows);
    code = TSDB_CODE_INVALID_PARA;
    goto _return;
  }

  // Check if this is a super table insert (has tbname column)
  bool hasTbnameColumn = pStmt->sql.cachedIsInsert &&
                         pStmt->sql.cachedTbnameColIdx >= 0 &&
                         pStmt->sql.cachedHasTbnameColumn;

  // Validate tbname column (only for super table inserts)
  int tbnameColIdx = -1;
  code = validateTbnameColumn(pStmt, bindv, hasTbnameColumn, &tbnameColIdx);
  if (code != TSDB_CODE_SUCCESS) {
    goto _return;
  }

  int numTables = 0;
  if (hasTbnameColumn) {
    // Super table insert: use user-provided table count
    numTables = bindv->num_tables;
    if (numTables <= 0) {
      STMT2_ELOG("Invalid num_tables for super table insert: %d", numTables);
      code = TSDB_CODE_INVALID_PARA;
      goto _return;
    }
  } else {
    // Normal table insert and query: only one table
    numTables = 1;
  }

  // Use field information cached by column bind (unified for both query and insert)
  if (pStmt->sql.cachedFields == NULL) {
    tscError("stmt2:%p, Field information not cached. Call taos_stmt2_prepare first", pStmt);
    code = TSDB_CODE_INVALID_PARA;
    goto _return;
  }

  // Both query and insert use cached field information
  int fieldNum = pStmt->sql.cachedFieldNum;
  int numTagCols = 0;
  int numDataCols = 0;
  for (int i = 0; i < fieldNum; ++i) {
    if (pStmt->sql.cachedFields[i].field_type == TAOS_FIELD_TAG) {
      ++numTagCols;
    } else if (pStmt->sql.cachedFields[i].field_type == TAOS_FIELD_COL ||
               pStmt->sql.cachedFields[i].field_type == TAOS_FIELD_QUERY) {
      ++numDataCols;
    }
  }

  // Allocate row format bind structure
  rowBindv.count = numTables;
  rowBindv.tbnames = (char**)taosMemoryCalloc(numTables, sizeof(char*));
  rowBindv.bind_cols = (TAOS_STMT2_BIND**)taosMemoryCalloc(numTables, sizeof(TAOS_STMT2_BIND*));
  rowBindv.tags = (TAOS_STMT2_BIND**)taosMemoryCalloc(numTables, sizeof(TAOS_STMT2_BIND*));

  if (rowBindv.tbnames == NULL || rowBindv.bind_cols == NULL || rowBindv.tags == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  // Initialize running offsets for variable-length columns
  colOffsets = (int32_t*)taosMemoryCalloc(bindv->num_columns, sizeof(int32_t));
  if (colOffsets == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  // Process each table
  int tableIdx = 0;
  int startRow = 0;
  int32_t tbnameOffset = 0;

  if (hasTbnameColumn) {
    // Super table insert: group rows by tbname
    while (startRow < totalRows && tableIdx < numTables) {
      char *tbname = (char*)bindv->columns[tbnameColIdx].buffer + tbnameOffset;
      int32_t tbname_len = bindv->columns[tbnameColIdx].length[startRow];

      if (tbname == NULL || tbname_len == 0) {
        STMT2_ELOG("Table name at startRow %d is NULL or empty", startRow);
        code = TSDB_CODE_INVALID_PARA;
        goto _return;
      }

      // Find rows for this table (until tbname changes or we've processed all tables)
      int endRow = startRow + 1;
      int32_t nextTbnameOffset = tbnameOffset + bindv->columns[tbnameColIdx].length[startRow];

      while (endRow < totalRows) {
        char *next_tbname = (char*)bindv->columns[tbnameColIdx].buffer + nextTbnameOffset;
        int32_t next_tbname_len = bindv->columns[tbnameColIdx].length[endRow];

        if (tbname_len != next_tbname_len || memcmp(tbname, next_tbname, tbname_len) != 0) break;
        nextTbnameOffset += bindv->columns[tbnameColIdx].length[endRow];
        endRow++;
      }

      int numRows = endRow - startRow;

      // Store tbname
      rowBindv.tbnames[tableIdx] = (char*)taosMemoryMalloc(tbname_len + 1);
      if (rowBindv.tbnames[tableIdx] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _return;
      }
      memcpy(rowBindv.tbnames[tableIdx], tbname, tbname_len);
      rowBindv.tbnames[tableIdx][tbname_len] = '\0';

      // Allocate tag and bind arrays for this table
      if (numTagCols > 0) {
        rowBindv.tags[tableIdx] = (TAOS_STMT2_BIND*)taosMemoryMalloc(numTagCols * sizeof(TAOS_STMT2_BIND));
        if (rowBindv.tags[tableIdx] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _return;
        }
      }

      if (numDataCols > 0) {
        rowBindv.bind_cols[tableIdx] = (TAOS_STMT2_BIND*)taosMemoryMalloc(numDataCols * sizeof(TAOS_STMT2_BIND));
        if (rowBindv.bind_cols[tableIdx] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _return;
        }
      }

      // Process columns for this table using unified function
      code = processColumnsForTable(pStmt, tableIdx, startRow, numRows,
                                    tbnameColIdx, bindv, &rowBindv, colOffsets);
      if (code != TSDB_CODE_SUCCESS) {
        goto _return;
      }

      // Update column offsets for variable-length columns
      int bindvColIdx = 0;  // Reset to track actual bindv column index
      for (int fieldIdx = 0; fieldIdx < fieldNum; fieldIdx++) {
        // Skip tbname field
        if (fieldIdx == tbnameColIdx) {
          bindvColIdx++;
          continue;
        }
        const TAOS_STMT2_COLUMN_BIND *colBind = &bindv->columns[bindvColIdx++];
        if (colBind->length != NULL &&
            (IS_VAR_DATA_TYPE(colBind->buffer_type) ||
             colBind->buffer_type == TSDB_DATA_TYPE_DECIMAL ||
             colBind->buffer_type == TSDB_DATA_TYPE_DECIMAL64)) {
          for (int row = startRow; row < endRow; row++) {
            if (row < totalRows) {
               int32_t length = colBind->length[row];
               if (length < 0) {
                 STMT2_ELOG("Invalid column bind length, column index:%d, row:%d, length:%d",
                            bindvColIdx - 1, row, length);
                 code = TSDB_CODE_INVALID_PARA;
                 goto _return;
               }
               if (colBind->is_null == NULL || colBind->is_null[row] == 0) {
                 colOffsets[fieldIdx] += length;
               }
             }
          }
        }
      }

      // nextTbnameOffset already calculated in table switch detection loop
      tbnameOffset = nextTbnameOffset;

      tableIdx++;
      startRow = endRow;

      if (tableIdx >= numTables) break;
    }

    if (startRow != totalRows) {
      STMT2_ELOG("Only processed %d rows out of %d columnar rows", startRow, totalRows);
      code = TSDB_CODE_INVALID_PARA;
      goto _return;
    }
    rowBindv.count = tableIdx;
  } else {
    // Query / Normal table / Child table insert: no tbname column
    // - Query: all fields are QUERY type, numTagCols = 0
    // - Normal table (NTB): no tags, only data columns
    // - Child table (CTB with explicit name): has tags and data columns
    // All three cases use the same unified processing logic
    int numRows = totalRows;

    // No tbname to store (not a super table insert with auto-create)
    rowBindv.tbnames[0] = NULL;

    // Allocate tag and bind arrays
    if (numTagCols > 0) {
      rowBindv.tags[0] = (TAOS_STMT2_BIND*)taosMemoryMalloc(numTagCols * sizeof(TAOS_STMT2_BIND));
      if (rowBindv.tags[0] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _return;
      }
    }

    if (numDataCols > 0) {
      rowBindv.bind_cols[0] = (TAOS_STMT2_BIND*)taosMemoryMalloc(numDataCols * sizeof(TAOS_STMT2_BIND));
      if (rowBindv.bind_cols[0] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _return;
      }
    }

    // Process columns using unified function (handles query, NTB, and CTB)
    code = processColumnsForTable(pStmt, 0, 0, numRows, -1, bindv, &rowBindv, colOffsets);
    if (code != TSDB_CODE_SUCCESS) {
      goto _return;
    }

    tableIdx = 1;
  }

  // Call row-based bind function
  code = stmt2BindParamImpl(stmt, &rowBindv, -1);

_return:
  // Free resources
  if (colOffsets != NULL) {
    taosMemoryFree(colOffsets);
  }

  // Free tbnames
  if (rowBindv.tbnames != NULL) {
    for (int i = 0; i < tableIdx; i++) {
      if (rowBindv.tbnames[i] != NULL) {
        taosMemoryFree(rowBindv.tbnames[i]);
      }
    }
    taosMemoryFree(rowBindv.tbnames);
  }

  // Free bind_cols (free the structure array only; do not free pointees).
  if (rowBindv.bind_cols != NULL) {
    for (int i = 0; i < tableIdx; i++) {
      if (rowBindv.bind_cols[i] != NULL) {
        taosMemoryFree(rowBindv.bind_cols[i]);
      }
    }
    taosMemoryFree(rowBindv.bind_cols);
  }

  // Free tags (free the structure array only; do not free pointees).
  if (rowBindv.tags != NULL) {
    for (int i = 0; i < tableIdx; i++) {
      if (rowBindv.tags[i] != NULL) {
        taosMemoryFree(rowBindv.tags[i]);
      }
    }
    taosMemoryFree(rowBindv.tags);
  }

  if (code != TSDB_CODE_SUCCESS) {
    if (pStmt->errCode == TSDB_CODE_SUCCESS) {
      return stmt2BindError(pStmt, code);
    }
    terrno = code;
  } else {
    terrno = TSDB_CODE_SUCCESS;
  }
  return code;
}

/* Records dispatch/return here; the async bind thread records DONE immediately
 * before it invokes the user's callback. */
int taos_stmt2_bind_param_column_a(TAOS_STMT2 *stmt, TAOS_STMT2_COLUMN_BINDV *bindv, __taos_async_fn_t fp, void *param) {
  SWorkloadCallToken wlrTok = {0};
  uint64_t           wlrSid = 0;
  wlrColumnBindBegin(stmt, bindv, WLR_API_STMT2_BIND_PARAM_COLUMN_A, &wlrTok, &wlrSid);

  ThreadArgs *wlrArgs = NULL;
  int32_t wlrCode = stmtBindParamAImpl(stmt, bindv, 0, fp, param, true, &wlrTok,
                                       &wlrArgs);
  /* Keep DONE ownership mutually exclusive with stmtAsyncBindThreadFunc. */
  if (wlrTok.recorded &&
      (wlrCode != TSDB_CODE_SUCCESS || stmtIsLiteral((STscStmt2 *)stmt))) {
    wlrStmtAsyncDone(&wlrTok, wlrSid, wlrCode);
  }

  if (wlrTok.recorded) {
    WLR_API_END_STMT(&wlrTok, wlrCode, wlrSid);
  } else {
    wlrApiReturn(&wlrTok, NULL);
  }
  if (wlrArgs != NULL) {
    STscStmt2 *pStmt = (STscStmt2 *)stmt;
    (void)taosThreadMutexLock(&pStmt->asyncBindParam.mutex);
    wlrArgs->wlrReturnReady = true;
    (void)taosThreadCondBroadcast(&pStmt->asyncBindParam.waitCond);
    (void)taosThreadMutexUnlock(&pStmt->asyncBindParam.mutex);
  }
  return wlrCode;
}
