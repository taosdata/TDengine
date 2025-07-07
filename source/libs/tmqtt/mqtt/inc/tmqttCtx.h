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

#ifndef _TD_TMQ_CTX_H_
#define _TD_TMQ_CTX_H_

// clang-format off
#include "uv.h"

#define ALLOW_FORBID_FUNC

#include "taos.h"
#include "thash.h"
#include "tmisce.h"
#include "tmqtt.h"
// clang-format on

#ifdef __cplusplus
extern "C" {
#endif

struct tmqtt;

struct tmq_ctx {
  TAOS         *conn;
  tmq_t        *tmq;
  char         *cid;
  tmq_list_t   *topic_list;
  struct tmqtt *context;
};

typedef struct {
  uv_loop_t  *loop;
  uv_pipe_t   ctrlPipe;
  uv_signal_t intrSignal;
  char        listenPipeName[PATH_MAX + MQTT_MGMT_LISTEN_PIPE_NAME_LEN + 2];
  uv_pipe_t   listeningPipe;

  void     *clientRpc;
  SCorEpSet mgmtEp;

  uv_mutex_t udfsMutex;
  SHashObj  *udfsHash;

  char    mqttDataDir[PATH_MAX];
  bool    printVersion;
  int32_t dnode_id;
} SMqttdContext;

extern SMqttdContext global;

bool tmq_ctx_auth(struct tmq_ctx *context, const char *username, const char *password);
bool tmq_ctx_topic_exists(struct tmq_ctx *context, const char *topic, const char *cid, const char *sn, bool earliest);
void tmq_ctx_poll_msgs(void);
bool tmq_ctx_unsub_topic(struct tmq_ctx *context, const char *topic_name, const char *cid, const char *sn);
void tmq_ctx_cleanup(struct tmq_ctx *context);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQ_CTX_H_*/
