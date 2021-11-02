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

#ifndef TDENGINE_MONITOR_H
#define TDENGINE_MONITOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#define monSaveLogs(level, ...) {      \
  monSaveLog(level, __VA_ARGS__);      \
  monSaveDnodeLog(level, __VA_ARGS__); \
}

#define MON_MAX_HTTP_CODE 63

static const char *monHttpStatusCodeTable[MON_MAX_HTTP_CODE][2] = {
  {"HTTP_CODE_CONTINUE",                "100"},
  {"HTTP_CODE_SWITCHING_PROTOCOL",      "101"},
  {"HTTP_CODE_PROCESSING",              "102"},
  {"HTTP_CODE_EARLY_HINTS",             "103"},
  {"HTTP_CODE_OK",                      "200"},
  {"HTTP_CODE_CREATED",                 "201"},
  {"HTTP_CODE_ACCEPTED",                "202"},
  {"HTTP_CODE_NON_AUTHORITATIVE_INFO",  "203"},
  {"HTTP_CODE_NO_CONTENT",              "204"},
  {"HTTP_CODE_RESET_CONTENT",           "205"},
  {"HTTP_CODE_PARTIAL_CONTENT",         "206"},
  {"HTTP_CODE_MULTI_STATUS",            "207"},
  {"HTTP_CODE_ALREADY_REPORTED",        "208"},
  {"HTTP_CODE_IM_USED",                 "226"},
  {"HTTP_CODE_MULTIPLE_CHOICE",         "300"},
  {"HTTP_CODE_MOVED_PERMANENTLY",       "301"},
  {"HTTP_CODE_FOUND",                   "302"},
  {"HTTP_CODE_SEE_OTHER",               "303"},
  {"HTTP_CODE_NOT_MODIFIED",            "304"},
  {"HTTP_CODE_USE_PROXY",               "305"},
  {"HTTP_CODE_UNUSED",                  "306"},
  {"HTTP_CODE_TEMPORARY_REDIRECT",      "307"},
  {"HTTP_CODE_PERMANENT_REDIRECT",      "308"},
  {"HTTP_CODE_BAD_REQUEST",             "400"},
  {"HTTP_CODE_UNAUTHORIZED",            "401"},
  {"HTTP_CODE_PAYMENT_REQUIRED",        "402"},
  {"HTTP_CODE_FORBIDDEN",               "403"},
  {"HTTP_CODE_NOT_FOUND",               "404"},
  {"HTTP_CODE_METHOD_NOT_ALLOWED",      "405"},
  {"HTTP_CODE_NOT_ACCEPTABLE",          "406"},
  {"HTTP_CODE_PROXY_AUTH_REQUIRED",     "407"},
  {"HTTP_CODE_REQUEST_TIMEOUT",         "408"},
  {"HTTP_CODE_CONFLICT",                "409"},
  {"HTTP_CODE_GONE",                    "410"},
  {"HTTP_CODE_LENGTH_REQUIRED",         "411"},
  {"HTTP_CODE_PRECONDITION_FAILED",     "412"},
  {"HTTP_CODE_PAYLOAD_TOO_LARGE",       "413"},
  {"HTTP_CODE_URI_TOO_LARGE",           "414"},
  {"HTTP_CODE_UNSUPPORTED_MEDIA_TYPE",  "415"},
  {"HTTP_CODE_RANGE_NOT_SATISFIABLE",   "416"},
  {"HTTP_CODE_EXPECTATION_FAILED",      "417"},
  {"HTTP_CODE_IM_A_TEAPOT",             "418"},
  {"HTTP_CODE_MISDIRECTED_REQUEST",     "421"},
  {"HTTP_CODE_UNPROCESSABLE_ENTITY",    "422"},
  {"HTTP_CODE_LOCKED",                  "423"},
  {"HTTP_CODE_FAILED_DEPENDENCY",       "424"},
  {"HTTP_CODE_TOO_EARLY",               "425"},
  {"HTTP_CODE_UPGRADE_REQUIRED",        "426"},
  {"HTTP_CODE_PRECONDITION_REQUIRED",   "428"},
  {"HTTP_CODE_TOO_MANY_REQUESTS",       "429"},
  {"HTTP_CODE_REQ_HDR_FIELDS_TOO_LARGE","431"},
  {"HTTP_CODE_UNAVAIL_4_LEGAL_REASONS", "451"},
  {"HTTP_CODE_INTERNAL_SERVER_ERROR",   "500"},
  {"HTTP_CODE_NOT_IMPLEMENTED",         "501"},
  {"HTTP_CODE_BAD_GATEWAY",             "502"},
  {"HTTP_CODE_SERVICE_UNAVAILABLE",     "503"},
  {"HTTP_CODE_GATEWAY_TIMEOUT",         "504"},
  {"HTTP_CODE_HTTP_VER_NOT_SUPPORTED",  "505"},
  {"HTTP_CODE_VARIANT_ALSO_NEGOTIATES", "506"},
  {"HTTP_CODE_INSUFFICIENT_STORAGE",    "507"},
  {"HTTP_CODE_LOOP_DETECTED",           "508"},
  {"HTTP_CODE_NOT_EXTENDED",            "510"},
  {"HTTP_CODE_NETWORK_AUTH_REQUIRED",   "511"},
};

typedef struct {
  char *  acctId;
  int64_t currentPointsPerSecond;
  int64_t maxPointsPerSecond;
  int64_t totalTimeSeries;
  int64_t maxTimeSeries;
  int64_t totalStorage;
  int64_t maxStorage;
  int64_t totalQueryTime;
  int64_t maxQueryTime;
  int64_t totalInbound;
  int64_t maxInbound;
  int64_t totalOutbound;
  int64_t maxOutbound;
  int64_t totalDbs;
  int64_t maxDbs;
  int64_t totalUsers;
  int64_t maxUsers;
  int64_t totalStreams;
  int64_t maxStreams;
  int64_t totalConns;
  int64_t maxConns;
  int8_t  accessState;
} SAcctMonitorObj;

int32_t monInitSystem();
int32_t monStartSystem();
void    monStopSystem();
void    monCleanupSystem();
void    monSaveAcctLog(SAcctMonitorObj *pMonObj);
void    monSaveLog(int32_t level, const char *const format, ...);
void    monSaveDnodeLog(int32_t level, const char *const format, ...);
void    monExecuteSQL(char *sql);
typedef void (*MonExecuteSQLCbFP)(void *param, TAOS_RES *, int code);
void monExecuteSQLWithResultCallback(char *sql, MonExecuteSQLCbFP callback, void* param);
void    monIncQueryReqCnt();
void    monIncSubmitReqCnt();
int32_t monFetchQueryReqCnt();
int32_t monFetchSubmitReqCnt();
#ifdef __cplusplus
}
#endif

#endif
