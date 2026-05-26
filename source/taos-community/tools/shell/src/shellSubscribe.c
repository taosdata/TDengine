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

#include "shellInt.h"

#define SHELL_SUB_DEFAULT_POLL_TIMEOUT 500
#define SHELL_SUB_DEFAULT_OFFSET       "latest"
#define SHELL_SUB_MAX_TOPIC_LEN        192
#define SHELL_SUB_MAX_GROUP_LEN        192
#define SHELL_SUB_MAX_CLIENT_LEN       192

typedef struct {
  char    topic[SHELL_SUB_MAX_TOPIC_LEN];
  char    groupId[SHELL_SUB_MAX_GROUP_LEN];
  char    clientId[SHELL_SUB_MAX_CLIENT_LEN];
  char    offset[16];
  int64_t pollTimeoutMs;
  int64_t maxRows;
} SShellSubArgs;

static void shellPrintSubscribeHelp(void) {
  (void)printf(
    "Usage: subscribe <topic> -g <group_id> [options];\r\n"
    "\r\n"
    "Subscribe to a TMQ topic and print received data in real-time.\r\n"
    "\r\n"
    "Required:\r\n"
    "  <topic>          Topic name to subscribe to\r\n"
    "  -g <group_id>    Consumer group ID\r\n"
    "\r\n"
    "Options:\r\n"
    "  -c <client_id>   Client ID (default: auto-generated)\r\n"
    "  -o <offset>      Auto offset reset: 'earliest' or 'latest' (default: latest)\r\n"
    "  -n <count>       Max rows to receive before exiting (default: unlimited)\r\n"
    "  -t <timeout_ms>  Poll timeout in milliseconds (default: 1000)\r\n"
    "  -h               Show this help message\r\n"
    "\r\n"
    "Examples:\r\n"
    "  subscribe my_topic -g group1;\r\n"
    "  subscribe my_topic -g group1 -o earliest -n 100;\r\n"
    "  subscribe my_topic -g group1 -t 500;\r\n"
    "\r\n"
    "Press Ctrl+C to stop subscribing.\r\n");
}

static int32_t shellParseSubscribeArgs(char *command, SShellSubArgs *pArgs) {
  memset(pArgs, 0, sizeof(SShellSubArgs));
  pArgs->pollTimeoutMs = SHELL_SUB_DEFAULT_POLL_TIMEOUT;
  tstrncpy(pArgs->offset, SHELL_SUB_DEFAULT_OFFSET, sizeof(pArgs->offset));
  pArgs->maxRows = 0;

  // skip "subscribe" keyword
  char *p = command;
  while (*p == ' ' || *p == '\t') p++;
  // skip "subscribe"
  p += strlen("subscribe");
  while (*p == ' ' || *p == '\t') p++;

  // check for -h
  if (*p == '-' && *(p + 1) == 'h' && (*(p + 2) == '\0' || *(p + 2) == ' ' || *(p + 2) == '\t' || *(p + 2) == ';')) {
    shellPrintSubscribeHelp();
    return -1;
  }

  // first non-option argument is the topic name
  if (*p == '\0' || *p == ';') {
    shellPrintSubscribeHelp();
    return -1;
  }

  // parse topic name (first positional arg)
  if (*p != '-') {
    char *start = p;
    while (*p != '\0' && *p != ' ' && *p != '\t' && *p != ';') p++;
    int32_t len = (int32_t)(p - start);
    if (len >= SHELL_SUB_MAX_TOPIC_LEN) {
      (void)printf("Error: topic name too long (max %d).\r\n", SHELL_SUB_MAX_TOPIC_LEN - 1);
      return -1;
    }
    tstrncpy(pArgs->topic, start, len + 1);
  }

  // parse options
  while (*p != '\0' && *p != ';') {
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0' || *p == ';') break;

    if (*p == '-') {
      char opt = *(p + 1);
      p += 2;
      while (*p == ' ' || *p == '\t') p++;

      char *valStart = p;
      while (*p != '\0' && *p != ' ' && *p != '\t' && *p != ';') p++;
      int32_t valLen = (int32_t)(p - valStart);

      switch (opt) {
        case 'g': {
          if (valLen == 0 || valLen >= SHELL_SUB_MAX_GROUP_LEN) {
            (void)printf("Error: invalid group_id.\r\n");
            return -1;
          }
          tstrncpy(pArgs->groupId, valStart, valLen + 1);
          break;
        }
        case 'c': {
          if (valLen == 0 || valLen >= SHELL_SUB_MAX_CLIENT_LEN) {
            (void)printf("Error: invalid client_id.\r\n");
            return -1;
          }
          tstrncpy(pArgs->clientId, valStart, valLen + 1);
          break;
        }
        case 'o': {
          if (valLen == 0 || valLen >= (int32_t)sizeof(pArgs->offset)) {
            (void)printf("Error: invalid offset (use 'latest' or 'earliest').\r\n");
            return -1;
          }
          tstrncpy(pArgs->offset, valStart, valLen + 1);
          break;
        }
        case 'n': {
          char buf[32] = {0};
          if (valLen == 0 || valLen >= 32) {
            (void)printf("Error: invalid count.\r\n");
            return -1;
          }
          tstrncpy(buf, valStart, valLen + 1);
          pArgs->maxRows = atoll(buf);
          break;
        }
        case 't': {
          char buf[32] = {0};
          if (valLen == 0 || valLen >= 32) {
            (void)printf("Error: invalid timeout.\r\n");
            return -1;
          }
          tstrncpy(buf, valStart, valLen + 1);
          pArgs->pollTimeoutMs = atoll(buf);
          if (pArgs->pollTimeoutMs <= 0) pArgs->pollTimeoutMs = SHELL_SUB_DEFAULT_POLL_TIMEOUT;
          break;
        }
        case 'h': {
          shellPrintSubscribeHelp();
          return -1;
        }
        default:
          (void)printf("Warning: unknown option '-%c', ignored.\r\n", opt);
          break;
      }
    } else {
      // skip unknown positional args
      while (*p != '\0' && *p != ' ' && *p != '\t' && *p != ';') p++;
    }
  }

  if (pArgs->topic[0] == '\0') {
    (void)printf("Error: topic name is required.\r\n");
    return -1;
  }
  if (pArgs->groupId[0] == '\0') {
    (void)printf("Error: group_id is required. Use -g <group_id>.\r\n");
    return -1;
  }

  return 0;
}

int32_t shellSubscribe(char *command) {
  SShellSubArgs subArgs;
  if (shellParseSubscribeArgs(command, &subArgs) != 0) {
    return 0;
  }

  // create tmq conf
  tmq_conf_t *conf = tmq_conf_new();
  if (conf == NULL) {
    (void)printf("Error: failed to create tmq conf.\r\n");
    return 0;
  }

  tmq_conf_res_t res;
  res = tmq_conf_set(conf, "group.id", subArgs.groupId);
  if (res != TMQ_CONF_OK) {
    (void)printf("Error: failed to set group.id.\r\n");
    tmq_conf_destroy(conf);
    return 0;
  }

  if (subArgs.clientId[0] != '\0') {
    res = tmq_conf_set(conf, "client.id", subArgs.clientId);
    if (res != TMQ_CONF_OK) {
      (void)printf("Error: failed to set client.id.\r\n");
      tmq_conf_destroy(conf);
      return 0;
    }
  }

  res = tmq_conf_set(conf, "auto.offset.reset", subArgs.offset);
  if (res != TMQ_CONF_OK) {
    (void)printf("Error: failed to set auto.offset.reset to '%s'.\r\n", subArgs.offset);
    tmq_conf_destroy(conf);
    return 0;
  }

  res = tmq_conf_set(conf, "enable.auto.commit", "true");
  if (res != TMQ_CONF_OK) {
    (void)printf("Error: failed to set enable.auto.commit.\r\n");
    tmq_conf_destroy(conf);
    return 0;
  }

  // use existing connection's info for td.connect.ip, td.connect.port, etc.
  if (shell.args.host != NULL) {
    tmq_conf_set(conf, "td.connect.ip", shell.args.host);
  }
  if (shell.args.port > 0) {
    char portStr[16] = {0};
    (void)snprintf(portStr, sizeof(portStr), "%d", shell.args.port);
    tmq_conf_set(conf, "td.connect.port", portStr);
  }
  if (shell.args.user != NULL) {
    tmq_conf_set(conf, "td.connect.user", shell.args.user);
  }
  if (shell.args.password[0] != '\0') {
    tmq_conf_set(conf, "td.connect.pass", shell.args.password);
  }

  // create consumer
  char errStr[512] = {0};
  tmq_t *tmq = tmq_consumer_new(conf, errStr, sizeof(errStr));
  tmq_conf_destroy(conf);
  if (tmq == NULL) {
    (void)printf("Error: failed to create consumer: %s\r\n", errStr);
    return 0;
  }

  // create topic list and subscribe
  tmq_list_t *topicList = tmq_list_new();
  if (topicList == NULL) {
    (void)printf("Error: failed to create topic list.\r\n");
    tmq_consumer_close(tmq);
    return 0;
  }

  int32_t code = tmq_list_append(topicList, subArgs.topic);
  if (code != 0) {
    (void)printf("Error: failed to append topic: %s\r\n", tmq_err2str(code));
    tmq_list_destroy(topicList);
    tmq_consumer_close(tmq);
    return 0;
  }

  code = tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);
  if (code != 0) {
    (void)printf("Error: failed to subscribe: %s\r\n", tmq_err2str(code));
    tmq_consumer_close(tmq);
    return 0;
  }

  (void)printf("Subscribing to topic [%s], group [%s], offset [%s] ...\r\n",
               subArgs.topic, subArgs.groupId, subArgs.offset);
  (void)printf("Press Ctrl+C to stop.\r\n\r\n");

  // poll loop
  int64_t totalRows = 0;
  bool    headerPrinted = false;
  int32_t width[TSDB_MAX_COLUMNS] = {0};

  shellCmdkilled = false;

  while (!shellCmdkilled && !shell.exit) {
    TAOS_RES *pRes = tmq_consumer_poll(tmq, subArgs.pollTimeoutMs);
    if (pRes == NULL) {
      continue;
    }

    // check message type
    tmq_res_t msgType = tmq_get_res_type(pRes);
    if (msgType == TMQ_RES_TABLE_META || msgType == TMQ_RES_METADATA) {
      // print meta info as JSON
      char *jsonMeta = tmq_get_json_meta(pRes);
      if (jsonMeta != NULL) {
        (void)printf("Meta: %s\r\n", jsonMeta);
        tmq_free_json_meta(jsonMeta);
      }
      taos_free_result(pRes);
      continue;
    }

    // TMQ_RES_DATA - fetch rows
    // For TMQ results, fields info is only available after the first taos_fetch_row
    TAOS_ROW row;
    while ((row = taos_fetch_row(pRes)) != NULL) {
      if (shellCmdkilled) break;

      int32_t     numFields = taos_field_count(pRes);
      TAOS_FIELD *fields = taos_fetch_fields(pRes);
      int32_t     precision = taos_result_precision(pRes);
      int32_t    *length = taos_fetch_lengths(pRes);

      if (!headerPrinted && fields != NULL && numFields > 0) {
        for (int32_t i = 0; i < numFields; i++) {
          width[i] = shellCalcColWidth(fields + i, precision);
        }
        shellPrintHeader(fields, width, numFields);
        headerPrinted = true;
      }

      if (fields != NULL && numFields > 0) {
        for (int32_t i = 0; i < numFields; i++) {
          putchar(' ');
          shellPrintField((const char *)row[i], fields + i, width[i],
                          length ? length[i] : 0, precision);
          putchar(' ');
          putchar('|');
        }
        putchar('\r');
        putchar('\n');
      }
      totalRows++;

      if (subArgs.maxRows > 0 && totalRows >= subArgs.maxRows) {
        shellCmdkilled = true;
        break;
      }
    }

    taos_free_result(pRes);
  }

  (void)printf("\r\nUnsubscribed. Total rows received: %" PRId64 "\r\n\r\n", totalRows);

  // cleanup
  tmq_unsubscribe(tmq);
  tmq_consumer_close(tmq);
  shellCmdkilled = false;

  return 0;
}
