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

#include "clientTmq.h"
#include "taos.h"
#include "tglobal.h"
#include "tmsg.h"

// ============================================================
// conf helper: parse bool config ("true" / "false")
// ============================================================
static tmq_conf_res_t parseBoolConf(const char* key, const char* value, int8_t* dest) {
  if (strcasecmp(value, "true") == 0) {
    *dest = true;
    return TMQ_CONF_OK;
  }
  if (strcasecmp(value, "false") == 0) {
    *dest = false;
    return TMQ_CONF_OK;
  }
  tqErrorC("invalid value for %s:%s", key, value);
  return TMQ_CONF_INVALID;
}

// ============================================================
// conf helper: parse integer config with range check
// ============================================================
static tmq_conf_res_t parseIntConf(const char* key, const char* value, int64_t min, int64_t max, int64_t* dest) {
  int64_t tmp;
  int32_t code = taosStr2int64(value, &tmp);
  if (code != 0 || tmp < min || tmp > max) {
    tqErrorC("invalid value for %s:%s", key, value);
    return TMQ_CONF_INVALID;
  }
  *dest = tmp;
  return TMQ_CONF_OK;
}

// ============================================================
// conf helper: duplicate string config
// ============================================================
static tmq_conf_res_t dupStringConf(const char* key, const char* value, char** dest) {
  char* tmp = taosStrdup(value);
  if (tmp == NULL) {
    tqErrorC("tmq_conf_set out of memory:%d", terrno);
    return TMQ_CONF_INVALID;
  }
  taosMemoryFree(*dest);
  *dest = tmp;
  return TMQ_CONF_OK;
}

// ============================================================
// tmq_conf lifecycle
// ============================================================
tmq_conf_t* tmq_conf_new() {
  tmq_conf_t* conf = taosMemoryCalloc(1, sizeof(tmq_conf_t));
  if (conf == NULL) {
    return conf;
  }

  conf->withTbName = false;
  conf->autoCommit = true;
  conf->enableWalMarker = false;
  conf->autoCommitInterval = DEFAULT_AUTO_COMMIT_INTERVAL;
  conf->resetOffset = TMQ_OFFSET__RESET_LATEST;
  conf->enableBatchMeta = false;
  conf->heartBeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL;
  conf->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
  conf->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
  conf->maxPollWaitTime = DEFAULT_MAX_POLL_WAIT_TIME;
  conf->minPollRows = DEFAULT_MIN_POLL_ROWS;

  return conf;
}

void tmq_conf_destroy(tmq_conf_t* conf) {
  if (conf) {
    taosMemoryFree(conf->ip);
    taosMemoryFree(conf->user);
    taosMemoryFree(conf->pass);
    taosMemoryFree(conf->token);
    taosMemoryFree(conf);
  }
}

tmq_conf_res_t tmq_conf_set(tmq_conf_t* conf, const char* key, const char* value) {
  if (conf == NULL || key == NULL || value == NULL) {
    tqErrorC("tmq_conf_set null, conf:%p key:%p value:%p", conf, key, value);
    return TMQ_CONF_INVALID;
  }

  if (strcasecmp(key, "group.id") == 0) {
    if (strchr(value, TMQ_SEPARATOR_CHAR) != NULL) {
      tqErrorC("invalid group.id:%s, can not contains ':'", value);
      return TMQ_CONF_INVALID;
    }
    tstrncpy(conf->groupId, value, TSDB_CGROUP_LEN);
    return TMQ_CONF_OK;
  }
  if (strcasecmp(key, "client.id") == 0) {
    tstrncpy(conf->clientId, value, TSDB_CLIENT_ID_LEN);
    return TMQ_CONF_OK;
  }
  if (strcasecmp(key, "enable.auto.commit") == 0) {
    return parseBoolConf(key, value, &conf->autoCommit);
  }
  if (strcasecmp(key, "enable.wal.marker") == 0) {
    return parseBoolConf(key, value, &conf->enableWalMarker);
  }
  if (strcasecmp(key, "auto.commit.interval.ms") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 0, INT32_MAX, &tmp);
    if (res == TMQ_CONF_OK) conf->autoCommitInterval = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "session.timeout.ms") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 6000, 1800000, &tmp);
    if (res == TMQ_CONF_OK) conf->sessionTimeoutMs = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "heartbeat.interval.ms") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 1000, (int64_t)conf->sessionTimeoutMs - 1, &tmp);
    if (res == TMQ_CONF_OK) conf->heartBeatIntervalMs = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "max.poll.interval.ms") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 1000, INT32_MAX, &tmp);
    if (res == TMQ_CONF_OK) conf->maxPollIntervalMs = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "auto.offset.reset") == 0) {
    if (strcasecmp(value, "none") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_NONE;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "earliest") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_EARLIEST;
      return TMQ_CONF_OK;
    } else if (strcasecmp(value, "latest") == 0) {
      conf->resetOffset = TMQ_OFFSET__RESET_LATEST;
      return TMQ_CONF_OK;
    } else {
      tqErrorC("invalid value for auto.offset.reset:%s", value);
      return TMQ_CONF_INVALID;
    }
  }
  if (strcasecmp(key, "msg.with.table.name") == 0) {
    return parseBoolConf(key, value, &conf->withTbName);
  }
  if (strcasecmp(key, "experimental.snapshot.enable") == 0) {
    return parseBoolConf(key, value, &conf->snapEnable);
  }
  if (strcasecmp(key, "td.connect.ip") == 0) {
    return dupStringConf(key, value, &conf->ip);
  }
  if (strcasecmp(key, "td.connect.user") == 0) {
    return dupStringConf(key, value, &conf->user);
  }
  if (strcasecmp(key, "td.connect.pass") == 0) {
    return dupStringConf(key, value, &conf->pass);
  }
  if (strcasecmp(key, "td.connect.token") == 0) {
    return dupStringConf(key, value, &conf->token);
  }
  if (strcasecmp(key, "td.connect.port") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 1, 65535, &tmp);
    if (res == TMQ_CONF_OK) conf->port = (uint16_t)tmp;
    return res;
  }
  if (strcasecmp(key, "enable.replay") == 0) {
    return parseBoolConf(key, value, &conf->replayEnable);
  }
  if (strcasecmp(key, "msg.consume.excluded") == 0) {
    int64_t tmp = 0;
    int32_t code = taosStr2int64(value, &tmp);
    conf->sourceExcluded = (0 == code && tmp != 0) ? TD_REQ_FROM_TAOX : 0;
    return TMQ_CONF_OK;
  }
  if (strcasecmp(key, "msg.consume.rawdata") == 0) {
    int64_t tmp = 0;
    int32_t code = taosStr2int64(value, &tmp);
    conf->rawData = (0 == code && tmp != 0) ? 1 : 0;
    return TMQ_CONF_OK;
  }
  if (strcasecmp(key, "fetch.max.wait.ms") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 1, INT32_MAX, &tmp);
    if (res == TMQ_CONF_OK) conf->maxPollWaitTime = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "min.poll.rows") == 0) {
    int64_t tmp;
    tmq_conf_res_t res = parseIntConf(key, value, 1, INT32_MAX, &tmp);
    if (res == TMQ_CONF_OK) conf->minPollRows = (int32_t)tmp;
    return res;
  }
  if (strcasecmp(key, "td.connect.db") == 0) {
    return TMQ_CONF_OK;
  }
  if (strcasecmp(key, "msg.enable.batchmeta") == 0) {
    int64_t tmp;
    int32_t code = taosStr2int64(value, &tmp);
    conf->enableBatchMeta = (0 == code && tmp != 0) ? true : false;
    return TMQ_CONF_OK;
  }

  tqErrorC("unknown key:%s", key);
  return TMQ_CONF_UNKNOWN;
}

void tmq_conf_set_auto_commit_cb(tmq_conf_t* conf, tmq_commit_cb* cb, void* param) {
  if (conf == NULL) return;
  conf->commitCb = cb;
  conf->commitCbUserParam = param;
}

// ============================================================
// tmq_list operations
// ============================================================
tmq_list_t* tmq_list_new() {
  return (tmq_list_t*)taosArrayInit(0, sizeof(void*));
}

int32_t tmq_list_append(tmq_list_t* list, const char* src) {
  if (list == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SArray* container = &list->container;
  if (src == NULL || src[0] == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  char* topic = taosStrdup(src);
  if (topic == NULL) return terrno;
  if (taosArrayPush(container, &topic) == NULL) {
    taosMemoryFree(topic);
    return terrno;
  }
  return 0;
}

void tmq_list_destroy(tmq_list_t* list) {
  if (list == NULL) return;
  SArray* container = &list->container;
  taosArrayDestroyP(container, taosMemFree);
}

int32_t tmq_list_get_size(const tmq_list_t* list) {
  if (list == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  const SArray* container = &list->container;
  return taosArrayGetSize(container);
}

char** tmq_list_to_c_array(const tmq_list_t* list) {
  if (list == NULL) {
    return NULL;
  }
  const SArray* container = &list->container;
  return container->pData;
}
