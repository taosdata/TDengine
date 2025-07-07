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

#include "tmqttCtx.h"

#include <stdio.h>

#include "cJSON.h"
#include "decimal.h"
#include "geometry/geosWrapper.h"
#include "tmqttBrokerInt.h"
#include "tmqttProto.h"
#include "ttlist.h"
#include "ttqMemory.h"

bool tmq_ctx_auth(struct tmq_ctx* context, const char* username, const char* password) {
  const char* host = "localhost";
  uint16_t    port = 6030;
  TAOS*       conn = NULL;

  for (int i = 0; i < global.mgmtEp.epSet.numOfEps; ++i) {
    host = global.mgmtEp.epSet.eps[i].fqdn;
    port = global.mgmtEp.epSet.eps[i].port;

    conn = taos_connect(host, username, password, NULL, port);
    if (conn) {
      context->conn = conn;

      break;
    }
  }

  if (!conn) {
    bndError("failed to connect to %s:%hu, error: 0x%x(%s)", host, port, taos_errno(NULL), taos_errstr(NULL));

    return false;
  }

  return true;
}

typedef struct {
  const char* enable_auto_commit;
  const char* auto_commit_interval_ms;
  const char* group_id;
  const char* client_id;
  const char* td_connect_host;
  const char* td_connect_port;
  const char* td_connect_user;
  const char* td_connect_pass;
  const char* auto_offset_reset;
} ConsumerConfig;

static void tmq_ctx_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  // count += 1;
  // fprintf(stdout, "tmq_commit_cb_print() code: %d, tmq: %p, param: %p, count: %d.\n", code, tmq, param, count);
  fprintf(stdout, "tmq_commit_cb_print() code: %d, tmq: %p, param: %p.\n", code, tmq, param);
}

static tmq_t* tmq_ctx_build_consumer(const ConsumerConfig* config) {
  tmq_conf_res_t code;
  tmq_t*         tmq = NULL;

  // create a configuration object
  tmq_conf_t* conf = tmq_conf_new();

  // set the configuration parameters
  code = tmq_conf_set(conf, "enable.auto.commit", config->enable_auto_commit);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.commit.interval.ms", config->auto_commit_interval_ms);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "group.id", config->group_id);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "client.id", config->client_id);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.ip", config->td_connect_host);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.port", config->td_connect_port);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.user", config->td_connect_user);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.pass", config->td_connect_pass);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.offset.reset", config->auto_offset_reset);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_ctx_commit_cb_print, NULL);

  tmq = tmq_consumer_new(conf, NULL, 0);

_end:
  // destroy the configuration object
  tmq_conf_destroy(conf);
  return tmq;
}

static bool tmq_ctx_init_consumer(struct tmq_ctx* context, const char* cid, ConsumerConfig* config) {
  if (!context->tmq) {
    context->tmq = tmq_ctx_build_consumer(config);
    if (!context->tmq) {
      bndError("failed to build consumer: %s %s", config->client_id, config->group_id);

      return false;
    }

    context->cid = ttq_strdup(cid);
    if (NULL == context->cid) {
      return TTQ_ERR_NOMEM;
    }
  }
  /*
  if (!context->topic_list) {
    // create a empty topic list
    context->topic_list = tmq_list_new();
    if (!context->topic_list) {
      bndError("failed to init topic list: %s %s", config->client_id, config->group_id);

      return false;
    }
  }
  */
  return true;
}

static tmq_list_t* tmq_ctx_append_topic(struct tmq_ctx* context, const char* topic_name, ConsumerConfig* config) {
  int32_t code = 0;
  char**  topic_names = NULL;
  int32_t topic_count = 0;

  // append topic name to the list
  tmq_list_t* topic_list = tmq_list_new();
  if (!topic_list) {
    bndError("failed to update topic list: %s %s", config->client_id, config->group_id);

    return NULL;
  }

  code = tmq_list_append(topic_list, topic_name);
  if (code) {
    // failed, destroy the list and return NULL
    tmq_list_destroy(topic_list);

    bndError("Failed to create topic_list, topic: %s, groupId: %s, clientId: %s, error: 0x%x(%s)", topic_name,
             config->group_id, config->client_id, code, tmq_err2str(code));
    return NULL;
  }

  if (context->topic_list) {
    topic_count = tmq_list_get_size(context->topic_list);
    topic_names = tmq_list_to_c_array(context->topic_list);

    for (int i = 0; i < topic_count; ++i) {
      code = tmq_list_append(topic_list, topic_names[i]);
      if (code) {
        tmq_list_destroy(topic_list);

        bndError("Failed to create topic_list, topic: %s, groupId: %s, clientId: %s, error: 0x%x(%s)", topic_name,
                 config->group_id, config->client_id, code, tmq_err2str(code));
        return NULL;
      }
    }
  }

  return topic_list;
}

static tmq_list_t* tmq_ctx_remove_topic(struct tmq_ctx* context, const char* topic_name) {
  int32_t code = 0;
  char**  topic_names = NULL;
  int32_t topic_count = 0;

  // append topic name to the list
  tmq_list_t* topic_list = tmq_list_new();
  if (!topic_list) {
    bndError("failed to remove topic: %s", topic_name);

    return NULL;
  }

  if (context->topic_list) {
    topic_count = tmq_list_get_size(context->topic_list);
    topic_names = tmq_list_to_c_array(context->topic_list);

    for (int i = 0; i < topic_count; ++i) {
      if (!strcmp(topic_names[i], topic_name)) {
        continue;
      }

      code = tmq_list_append(topic_list, topic_names[i]);
      if (code) {
        tmq_list_destroy(topic_list);

        bndError("Failed to create topic_list, topic: %s, error: 0x%x(%s)", topic_name, code, tmq_err2str(code));
        return NULL;
      }
    }
  }

  return topic_list;
}

bool tmq_ctx_topic_exists(struct tmq_ctx* context, const char* topic_name, const char* cid, const char* sn,
                          bool earliest) {
  int32_t     code = 0;
  char        group_id[256] = {0};
  char        consumer_client_id[64] = {0};
  char        port_str[16] = {0};
  tmq_list_t* topic_list = NULL;

  snprintf(consumer_client_id, sizeof(consumer_client_id), "_cid-%s-%d", cid, global.dnode_id);
  // snprintf(group_id, sizeof(group_id), "_xnd-gid-%d", global.dnode_id);
  snprintf(group_id, sizeof(group_id), "_bnd-gid-%s", sn ? sn : consumer_client_id);
  snprintf(port_str, sizeof(port_str), "%d", global.mgmtEp.epSet.eps[0].port);

  ConsumerConfig config = {.enable_auto_commit = "true",
                           .auto_commit_interval_ms = "10000",
                           .group_id = group_id,  // group_id,
                           .client_id = consumer_client_id,
                           .td_connect_host = global.mgmtEp.epSet.eps[0].fqdn,  // "localhost",
                           .td_connect_port = port_str,                         // "6030",
                           .td_connect_user = context->context->username,
                           .td_connect_pass = context->context->password,
                           .auto_offset_reset = earliest ? "earliest" : "latest"};

  if (!context->tmq && !tmq_ctx_init_consumer(context, cid, &config)) {
    return false;
  }

  // topic exists? and subscribe this topic
  topic_list = tmq_ctx_append_topic(context, topic_name, &config);
  if (NULL == topic_list) {
    bndError("Failed to append topic, topic: %s, groupId: %s, clientId: %s.\n", topic_name, config.group_id,
             config.client_id);

    return false;
  }

  if (code = tmq_subscribe(context->tmq, topic_list)) {
    bndError("Failed to subscribe topic_list, topic: %s, groupId: %s, clientId: %s, error: 0x%x(%s)", topic_name,
             config.group_id, config.client_id, code, tmq_err2str(code));

    tmq_list_destroy(topic_list);

    // close consumer if no succeeded topics, mnode may print spurious consumer not exist errors
    if (!context->topic_list || tmq_list_get_size(context->topic_list) <= 0) {
      tmq_list_destroy(context->topic_list);
      tmq_consumer_close(context->tmq);
      context->topic_list = NULL;
      context->tmq = NULL;
    }
    return false;
  }

  bndInfo("subscribed topic: %s.", topic_name);

  if (context->topic_list) {
    tmq_list_destroy(context->topic_list);
  }
  context->topic_list = topic_list;

  return true;
}

bool tmq_ctx_unsub_topic(struct tmq_ctx* context, const char* topic_name, const char* cid, const char* sn) {
  int32_t     code = 0;
  char        group_id[256] = {0};
  char        consumer_client_id[64] = {0};
  char        port_str[16] = {0};
  tmq_list_t* topic_list = NULL;

  snprintf(consumer_client_id, sizeof(consumer_client_id), "_cid-%s-%d", cid, global.dnode_id);
  // snprintf(group_id, sizeof(group_id), "_xnd-gid-%d", global.dnode_id);
  snprintf(group_id, sizeof(group_id), "_xnd-gid-%s", sn ? sn : consumer_client_id);
  snprintf(port_str, sizeof(port_str), "%d", global.mgmtEp.epSet.eps[0].port);

  ConsumerConfig config = {.enable_auto_commit = "true",
                           .auto_commit_interval_ms = "10000",
                           .group_id = group_id,
                           .client_id = consumer_client_id,
                           .td_connect_host = global.mgmtEp.epSet.eps[0].fqdn,
                           .td_connect_port = port_str,
                           .td_connect_user = context->context->username,
                           .td_connect_pass = context->context->password,
                           .auto_offset_reset = "latest"};

  if (!context->tmq && !tmq_ctx_init_consumer(context, cid, &config)) {
    return false;
  }

  code = tmq_commit_sync(context->tmq, NULL);
  if (code != 0) {
    if (!context->topic_list || tmq_list_get_size(context->topic_list) <= 0) {
      tmq_list_destroy(context->topic_list);
      tmq_consumer_close(context->tmq);
      context->topic_list = NULL;
      context->tmq = NULL;
    }

    return false;
  }

  topic_list = tmq_ctx_remove_topic(context, topic_name);
  if (NULL == topic_list) {
    bndError("Failed to remove topic, topic: %s.", topic_name);

    return false;
  }

  if (code = tmq_subscribe(context->tmq, topic_list)) {
    bndError("Failed to unsubscribe topic_list, topic: %s, error: 0x%x(%s)", topic_name, code, tmq_err2str(code));

    tmq_list_destroy(topic_list);

    if (!context->topic_list || tmq_list_get_size(context->topic_list) <= 0) {
      tmq_list_destroy(context->topic_list);
      tmq_consumer_close(context->tmq);
      context->topic_list = NULL;
      context->tmq = NULL;
    }
    return false;
  }

  bndInfo("unsubscribed topic: %s.", topic_name);

  if (context->topic_list) {
    tmq_list_destroy(context->topic_list);
  }
  context->topic_list = topic_list;

  if (!context->topic_list || tmq_list_get_size(context->topic_list) <= 0) {
    tmq_list_destroy(context->topic_list);
    tmq_consumer_close(context->tmq);
    context->topic_list = NULL;
    context->tmq = NULL;
  }

  return true;
}

void tmq_ctx_cleanup(struct tmq_ctx* context) {
  if (!context) return;

  if (context->conn) {
    taos_close(context->conn);
    context->conn = NULL;
  }

  if (context->topic_list) {
    tmq_list_destroy(context->topic_list);
    context->topic_list = NULL;
  }

  if (context->tmq) {
    tmq_consumer_close(context->tmq);
    context->tmq = NULL;
  }

  if (context->cid) {
    ttq_free(context->cid);
    context->cid = NULL;
  }
}

static int ttq_broker_publish(const char* clientid, const char* topic, int payloadlen, void* payload, int qos,
                              bool retain, tmqtt_property* properties) {
  struct tmqtt_message_v5* msg;

  if (topic == NULL || payloadlen < 0 || (payloadlen > 0 && payload == NULL) || qos < 0 || qos > 2) {
    return TTQ_ERR_INVAL;
  }

  msg = ttq_malloc(sizeof(struct tmqtt_message_v5));
  if (msg == NULL) return TTQ_ERR_NOMEM;

  msg->next = NULL;
  msg->prev = NULL;
  if (clientid) {
    msg->clientid = ttq_strdup(clientid);
    if (msg->clientid == NULL) {
      ttq_free(msg);
      return TTQ_ERR_NOMEM;
    }
  } else {
    msg->clientid = NULL;
  }
  msg->topic = ttq_strdup(topic);
  if (msg->topic == NULL) {
    ttq_free(msg->clientid);
    ttq_free(msg);
    return TTQ_ERR_NOMEM;
  }
  msg->payloadlen = payloadlen;
  msg->payload = payload;
  msg->qos = qos;
  msg->retain = retain;
  msg->properties = properties;

  DL_APPEND(db.plugin_msgs, msg);

  return TTQ_ERR_SUCCESS;
}

static cJSON* tmq_ctx_do_topic(const char* topic_name, const char* db_name, const char* tbl_name, int32_t vid,
                               cJSON** ajson) {
  const char* json_topic_name_key = "topic";
  const char* json_db_name_key = "db";
  const char* json_tbl_name_key = "tbl";
  const char* json_vid_name_key = "vid";
  const char* json_rows_key = "rows";

  cJSON* json = cJSON_CreateObject();
  if (!json) {
    bndError("json msg: out of memory.");
    return NULL;
  }

  if (NULL == cJSON_AddStringToObject(json, json_topic_name_key, topic_name)) {
    bndError("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  if (db_name) {
    if (NULL == cJSON_AddStringToObject(json, json_db_name_key, db_name)) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return NULL;
    }
  }

  if (tbl_name) {
    if (NULL == cJSON_AddStringToObject(json, json_tbl_name_key, tbl_name)) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return NULL;
    }
  }

  if (NULL == cJSON_AddNumberToObject(json, json_vid_name_key, vid)) {
    bndError("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  *ajson = cJSON_AddArrayToObject(json, json_rows_key);
  if (!ajson) {
    bndError("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  return json;
}

static int tmq_ctx_do_fields(cJSON* item, TAOS_FIELD* fields, int32_t field_count, TAOS_ROW row) {
  int rc = TTQ_ERR_SUCCESS;

  for (int i = 0; i < field_count; ++i) {
    if (!row[i]) {
      if (NULL == cJSON_AddNullToObject(item, fields[i].name)) {
        return TTQ_ERR_NOMEM;
      }

      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        if (NULL == cJSON_AddBoolToObject(item, fields[i].name, *((int8_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_TINYINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int8_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_UTINYINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint8_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int16_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint16_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_INT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int32_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_UINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint32_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int64_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint64_t*)row[i]))) {
          return TTQ_ERR_NOMEM;
        }
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float fv = GET_FLOAT_VAL(row[i]);

        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, fv)) {
          return TTQ_ERR_NOMEM;
        }
      } break;
      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = GET_DOUBLE_VAL(row[i]);

        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, dv)) {
          return TTQ_ERR_NOMEM;
        }
      } break;
      case TSDB_DATA_TYPE_VARBINARY: {
        void*    data = NULL;
        uint32_t tmp = 0;
        int32_t  charLen = varDataLen((char*)row[i] - VARSTR_HEADER_SIZE);
        if (taosAscii2Hex(row[i], charLen, &data, &tmp) < 0) {
          break;
        }

        if (NULL == cJSON_AddStringToObject(item, fields[i].name, data)) {
          taosMemoryFree(data);
          return TTQ_ERR_NOMEM;
        }

        taosMemoryFree(data);
      } break;
      case TSDB_DATA_TYPE_BINARY:  // TSDB_DATA_TYPE_VARCHAR
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t charLen = varDataLen((char*)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY || fields[i].type == TSDB_DATA_TYPE_VARBINARY ||
            fields[i].type == TSDB_DATA_TYPE_GEOMETRY) {
          if (charLen > fields[i].bytes || charLen < 0) {
            bndError("json msg error: binary. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        } else {
          if (charLen > fields[i].bytes * TSDB_NCHAR_SIZE || charLen < 0) {
            bndError("json msg error: charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        }

        char* data = taosMemoryCalloc(1, charLen + 1);
        if (!data) {
          return TTQ_ERR_NOMEM;
        }
        (void)memcpy(data, row[i], charLen);

        // if (NULL == cJSON_AddStringToObject(item, fields[i].name, row[i])) {
        if (NULL == cJSON_AddStringToObject(item, fields[i].name, data)) {
          taosMemoryFree(data);
          return TTQ_ERR_NOMEM;
        }
        taosMemoryFree(data);

      } break;
      case TSDB_DATA_TYPE_GEOMETRY: {
        char* outputWKT = NULL;

        rc = initCtxAsText();
        if (rc != TSDB_CODE_SUCCESS) {
          return TTQ_ERR_NOMEM;
        }

        rc = doAsText(row[i], fields[i].bytes, &outputWKT);
        if (rc != TSDB_CODE_SUCCESS) {
          return TTQ_ERR_NOMEM;
        }

        if (NULL == cJSON_AddStringToObject(item, fields[i].name, outputWKT)) {
          geosFreeBuffer(outputWKT);
          return TTQ_ERR_NOMEM;
        }

        geosFreeBuffer(outputWKT);
      } break;
      case TSDB_DATA_TYPE_DECIMAL64:
      case TSDB_DATA_TYPE_DECIMAL: {
        if (NULL == cJSON_AddStringToObject(item, fields[i].name, row[i])) {
          return TTQ_ERR_NOMEM;
        }
      } break;
      default:
        bndError("json do_fields: invalid data type: %hhd", fields[i].type);

        // ignore invalid data types rc = TTQ_ERR_INVAL;
        break;
    }
  }

  return rc;
}

static int tmq_ctx_do_props(tmqtt_property** props) {
  const uint32_t expire_interval = 0;
  const uint8_t  payload_format_indicator = 1;
  const char*    content_type = "TDengineJsonV1.0";  // "application/json"
  int            rc = TTQ_ERR_SUCCESS;

  rc = tmqtt_property_add_int32(props, MQTT_PROP_MESSAGE_EXPIRY_INTERVAL, expire_interval);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add property expiry interval: out of memory.");

    return rc;
  }

  rc = tmqtt_property_add_byte(props, MQTT_PROP_PAYLOAD_FORMAT_INDICATOR, payload_format_indicator);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add property payload format indicator: out of memory.");

    return rc;
  }

  rc = tmqtt_property_add_string(props, MQTT_PROP_CONTENT_TYPE, content_type);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add property content type: out of memory.");

    return rc;
  }

  return rc;
}

static void tmq_ctx_do_msg(struct tmqtt* ctxt, TAOS_RES* msg) {
  char*           msg_buf = NULL;
  int             msg_len = -1;
  int32_t         rows = 0;
  int             rc;
  const char*     topic_name = tmq_get_topic_name(msg);
  struct tmq_ctx* context = &ctxt->tmq_context;
  cJSON*          ajson = NULL;
  const char*     db_name = tmq_get_db_name(msg);
  const char*     tb_name = tmq_get_table_name(msg);
  int32_t         vgroup_id = tmq_get_vgroup_id(msg);

  cJSON* json = tmq_ctx_do_topic(topic_name, db_name, tb_name, vgroup_id, &ajson);
  if (!json) {
    bndError("json msg: out of memory.");
    return;
  }

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(msg)) != NULL) {
    int32_t field_count = taos_num_fields(msg);
    if (!field_count) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return;
    }

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    if (!fields) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return;
    }

    cJSON* item = cJSON_CreateObject();
    if (!item) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return;
    }
    if (!cJSON_AddItemToArray(ajson, item)) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return;
    }

    int rc = tmq_ctx_do_fields(item, fields, field_count, row);
    if (rc) {
      bndError("json msg: out of memory.");
      cJSON_Delete(json);
      return;
    }

    rows++;
  }

  char* data = cJSON_PrintUnformatted(json);
  if (!data) {
    bndError("json msg: out of memory.");
    cJSON_Delete(json);
    return;
  }
  cJSON_Delete(json);

  tmqtt_property* props = NULL;

  rc = tmq_ctx_do_props(&props);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add properties: out of memory.");
    ttq_free(data);
    return;
  }

  const bool retain = false;
  const int  qos = 1;
  int        data_len = strlen(data);

  rc = ttq_broker_publish(context->cid, topic_name, data_len, data, qos, retain, props);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add property: out of memory.");
    ttq_free(data);
  }
}

static void tmq_ctx_do_msg_meta(struct tmqtt* ctxt, TAOS_RES* msg) {
  int             rc;
  const char*     topic_name = tmq_get_topic_name(msg);
  struct tmq_ctx* context = &ctxt->tmq_context;

  char* data = tmq_get_json_meta(msg);
  if (!data) {
    bndError("json msg: out of memory.");
    return;
  }

  tmqtt_property* props = NULL;

  rc = tmq_ctx_do_props(&props);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add properties: out of memory.");
    tmq_free_json_meta(data);
    return;
  }

  const bool retain = false;
  const int  qos = 1;
  int        data_len = strlen(data);

  rc = ttq_broker_publish(context->cid, topic_name, data_len, data, qos, retain, props);
  if (rc != TTQ_ERR_SUCCESS) {
    bndError("json msg/add property: out of memory.");
    tmq_free_json_meta(data);
  }
}

static void tmq_ctx_poll_single(struct tmqtt* ctxt) {
  int32_t         timeout = 1000;  // poll timeout
  struct tmq_ctx* context = &ctxt->tmq_context;

  // poll message from taosc
  TAOS_RES* tmqmsg = tmq_consumer_poll(context->tmq, timeout);
  if (tmqmsg) {
    // data processing this msg
    tmq_res_t msg_type = tmq_get_res_type(tmqmsg);
    if (TMQ_RES_DATA == msg_type) {
      tmq_ctx_do_msg(ctxt, tmqmsg);
    } else if (TMQ_RES_TABLE_META == msg_type || TMQ_RES_METADATA == msg_type) {
      tmq_ctx_do_msg_meta(ctxt, tmqmsg);
    } else {
      // ignore rawdata
    }

    // free the message
    taos_free_result(tmqmsg);
  }
}

void tmq_ctx_poll_msgs(void) {
  // 1, traverse client contexts
  struct tmqtt *ctxt, *ctxt_tmp;

  HASH_ITER(hh_id, db.contexts_by_id, ctxt, ctxt_tmp) {
    if (ctxt && ctxt->tmq_context.tmq) {
      // 2, poll single tmq
      tmq_ctx_poll_single(ctxt);
    }
  }
}
