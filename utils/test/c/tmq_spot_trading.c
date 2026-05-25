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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "cJSON.h"
#include "taos.h"

static const char *kTopicName = "topic_spot_trading";
static const char *kExpectedSubtable = "realtime_20260524_20260520_19_11";

// Expected tag name -> value pairs for the auto-created child table.
// Validates the fix for tag name / tag value alignment when the INSERT
// tag binding order differs from the stable TAGS schema column order.
typedef struct {
  const char *name;
  const char *value;  // expected substring
} TagExpect;

static const TagExpect kExpectedTags[] = {
    {"tab", "2026spot-trade-20260524"},
    {"company_code", "91320811MADR1RFF2J01"},
    {"company_name", "GuangJing-Energy-Company-Long-Name"},
    {"un_name", "GuangJing-Energy-UnName"},
    {"unit_alias_name", "GuangJing-Energy-UnitAlias"},
    {"un_id", "19"},
};
static const int kExpectedTagsNum = (int)(sizeof(kExpectedTags) / sizeof(kExpectedTags[0]));

static int g_ctbVerified = 0;
static int g_tagErrors = 0;

// Verify a single create-child-table JSON object: {tableName, using, tags:[...]}
static void verify_ctb_json(cJSON *item) {
  cJSON *tableName = cJSON_GetObjectItem(item, "tableName");
  if (!cJSON_IsString(tableName)) return;
  if (strcmp(tableName->valuestring, kExpectedSubtable) != 0) return;

  cJSON *tags = cJSON_GetObjectItem(item, "tags");
  if (!cJSON_IsArray(tags)) {
    fprintf(stderr, "ERROR: ctb '%s' meta has no tags array\n", kExpectedSubtable);
    g_tagErrors++;
    return;
  }

  printf(">>> verifying tag name/value pairing for ctb: %s\n", kExpectedSubtable);
  int arrSize = cJSON_GetArraySize(tags);
  for (int e = 0; e < kExpectedTagsNum; ++e) {
    const TagExpect *expect = &kExpectedTags[e];
    int found = 0;
    for (int i = 0; i < arrSize; ++i) {
      cJSON *t = cJSON_GetArrayItem(tags, i);
      cJSON *name = cJSON_GetObjectItem(t, "name");
      cJSON *value = cJSON_GetObjectItem(t, "value");
      if (!cJSON_IsString(name) || !cJSON_IsString(value)) continue;
      if (strcmp(name->valuestring, expect->name) != 0) continue;
      found = 1;
      if (strstr(value->valuestring, expect->value) == NULL) {
        fprintf(stderr,
                "ERROR: tag '%s' value mismatch. expected substring '%s', got '%s'\n",
                expect->name, expect->value, value->valuestring);
        g_tagErrors++;
      } else {
        printf("  OK: tag '%s' value '%s'\n", expect->name, value->valuestring);
      }
      break;
    }
    if (!found) {
      fprintf(stderr, "ERROR: tag '%s' not found in meta\n", expect->name);
      g_tagErrors++;
    }
  }
  g_ctbVerified = 1;
}

// Recursively walk a JSON tree looking for ctb meta objects (have "using"
// and "tableName" and "tags").
static void walk_for_ctb(cJSON *node) {
  if (node == NULL) return;
  if (cJSON_IsObject(node)) {
    cJSON *using = cJSON_GetObjectItem(node, "using");
    cJSON *tableName = cJSON_GetObjectItem(node, "tableName");
    cJSON *tags = cJSON_GetObjectItem(node, "tags");
    if (cJSON_IsString(using) && cJSON_IsString(tableName) && cJSON_IsArray(tags)) {
      verify_ctb_json(node);
    }
    for (cJSON *c = node->child; c != NULL; c = c->next) {
      walk_for_ctb(c);
    }
  } else if (cJSON_IsArray(node)) {
    int n = cJSON_GetArraySize(node);
    for (int i = 0; i < n; ++i) {
      walk_for_ctb(cJSON_GetArrayItem(node, i));
    }
  }
}

static void msg_process(TAOS_RES *msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  int32_t resType = tmq_get_res_type(msg);
  printf("res type: %d\n", resType);
  if (resType == TMQ_RES_TABLE_META || resType == TMQ_RES_METADATA) {
    char *result = tmq_get_json_meta(msg);
    if (result != NULL) {
      printf("meta result: %s\n", result);
      cJSON *root = cJSON_Parse(result);
      if (root != NULL) {
        walk_for_ctb(root);
        cJSON_Delete(root);
      } else {
        fprintf(stderr, "WARN: cJSON_Parse failed for meta\n");
      }
      tmq_free_json_meta(result);
    } else {
      printf("meta result: <null>\n");
    }
  }
}

static void tmq_commit_cb_print(tmq_t *tmq, int32_t code, void *param) {
  printf("commit %d tmq %p param %p\n", code, tmq, param);
}

static tmq_t *build_consumer(void) {
  tmq_conf_t *conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg_spot_trading");
  tmq_conf_set(conf, "client.id", "client_spot_trading");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t *tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

static tmq_list_t *build_topic_list(void) {
  tmq_list_t *topic_list = tmq_list_new();
  tmq_list_append(topic_list, kTopicName);
  return topic_list;
}

static void basic_consume_loop(tmq_t *tmq, tmq_list_t *topics) {
  int32_t code = tmq_subscribe(tmq, topics);
  if (code != 0) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(code));
    return;
  }

  int32_t cnt = 0;
  int32_t emptyPolls = 0;
  while (1) {
    TAOS_RES *tmqmessage = tmq_consumer_poll(tmq, 3000);
    if (tmqmessage) {
      cnt++;
      printf("\n========== message %d ==========\n", cnt);
      msg_process(tmqmessage);
      taos_free_result(tmqmessage);
      emptyPolls = 0;
    } else {
      emptyPolls++;
      if (emptyPolls >= 3) {
        break;
      }
    }
  }
  printf("\n=== total messages consumed: %d ===\n", cnt);

  code = tmq_consumer_close(tmq);
  if (code) {
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  } else {
    fprintf(stderr, "%% Consumer closed\n");
  }
}

int main(int argc, char *argv[]) {
  if (argc > 1) {
    kTopicName = argv[1];
  }
  printf("subscribe topic: %s\n", kTopicName);

  tmq_t      *tmq = build_consumer();
  tmq_list_t *topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);

  if (!g_ctbVerified) {
    fprintf(stderr, "ERROR: did not receive create-child-table meta for '%s'\n",
            kExpectedSubtable);
    return 1;
  }
  if (g_tagErrors != 0) {
    fprintf(stderr, "ERROR: tag verification failed with %d error(s)\n", g_tagErrors);
    return 2;
  }
  printf("=== tag name/value pairing verified successfully ===\n");
  return 0;
}
