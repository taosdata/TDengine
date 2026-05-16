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

#ifndef _TD_TMQTT_H_
#define _TD_TMQTT_H_

#undef malloc
#define malloc malloc
#undef free
#define free free
#undef realloc
#define alloc alloc

#include <stdbool.h>
#include <stdint.h>
#include "dnode/bnode/bnode.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MQTT_MGMT_LISTEN_PIPE_NAME_LEN 32
#ifdef _WIN32
#define MQTT_MGMT_LISTEN_PIPE_NAME_PREFIX "\\\\?\\pipe\\taosmqtt.sock"
#else
#define MQTT_MGMT_LISTEN_PIPE_NAME_PREFIX ".taosmqtt.sock."
#endif
#define MQTT_MGMT_DNODE_ID_ENV_NAME "DNODE_ID"

#define TAOS_UV_LIB_ERROR_RET(ret)              \
  do {                                          \
    if (0 != ret) {                             \
      terrno = TSDB_CODE_BNODE_UV_EXEC_FAILURE; \
      return TSDB_CODE_BNODE_UV_EXEC_FAILURE;   \
    }                                           \
  } while (0)

#define TAOS_UV_CHECK_ERRNO(CODE) \
  do {                            \
    if (0 != CODE) {              \
      terrln = __LINE__;          \
      terrno = (CODE);            \
      goto _exit;                 \
    }                             \
  } while (0)

#define TAOS_MQTT_MGMT_CHECK_PTR_RCODE(...)                                         \
  do {                                                                              \
    const void *ptrs[] = {__VA_ARGS__};                                             \
    for (int i = 0; i < sizeof(ptrs) / sizeof(ptrs[0]); ++i) {                      \
      if (ptrs[i] == NULL) {                                                        \
        bndError("taosmqtt %dth parameter invalid, NULL PTR.line:%d", i, __LINE__); \
        return TSDB_CODE_INVALID_PARA;                                              \
      }                                                                             \
    }                                                                               \
  } while (0)

#define TAOS_MQTT_MGMT_CHECK_PTR_RVOID(...)                                         \
  do {                                                                              \
    const void *ptrs[] = {__VA_ARGS__};                                             \
    for (int i = 0; i < sizeof(ptrs) / sizeof(ptrs[0]); ++i) {                      \
      if (ptrs[i] == NULL) {                                                        \
        bndError("taosmqtt %dth parameter invalid, NULL PTR.line:%d", i, __LINE__); \
        return;                                                                     \
      }                                                                             \
    }                                                                               \
  } while (0)

#define TAOS_MQTT_MGMT_CHECK_CONDITION(o, code)        \
  do {                                                 \
    if ((o) == false) {                                \
      bndError("Condition not met.line:%d", __LINE__); \
      return code;                                     \
    }                                                  \
  } while (0)

/**
 * start taosmqtt that serves mqtt function invocation under dnode startDnodeId
 * @param startDnodeId
 * @return
 */
int32_t mqttMgmtStartMqttd(int32_t startDnodeId);
/**
 * stop taosmqtt
 * @return
 */
void mqttMgmtStopMqttd(void);

/**
 * get taosmqtt pid
 *
 */
// int32_t mqttMgmtGetPid(int32_t* pMqttdPid);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TMQTT_H_
