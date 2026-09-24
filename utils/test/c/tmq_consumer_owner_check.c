#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "taos.h"
#include "taoserror.h"
#include "types.h"
#include "clientTmq.h"
#include "osMemory.h"
#include "osSemaphore.h"
#include "osTime.h"

static const char *kPassword = "Test7059!";

static char database[64];
static char topic[64];
static char group[64];
static char ownerUser[64];
static char attackerUser[64];

static int32_t execSql(TAOS *taos, const char *format, ...) {
  char    sql[512];
  va_list arguments;
  va_start(arguments, format);
  int32_t length = vsnprintf(sql, sizeof(sql), format, arguments);
  va_end(arguments);
  if (length < 0 || length >= (int32_t)sizeof(sql)) {
    fprintf(stderr, "SQL formatting failed, length:%d\n", length);
    return -1;
  }
  TAOS_RES *result = taos_query(taos, sql);
  int32_t   code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "SQL execution failed, code:0x%x(%s)\n", code,
            taos_errstr(result));
  }
  taos_free_result(result);
  return code;
}

static tmq_t *createConsumer(const char *user, const char *authToken) {
  tmq_conf_t *conf = tmq_conf_new();
  if (conf == NULL) {
    fprintf(stderr, "Consumer configuration allocation failed for %s\n", user);
    return NULL;
  }

  tmq_conf_res_t groupCode = tmq_conf_set(conf, "group.id", group);
  tmq_conf_res_t userCode = tmq_conf_set(conf, "td.connect.user", user);
  const char *password = strcmp(user, "root") == 0 ? "taosdata" : kPassword;
  tmq_conf_res_t passCode = tmq_conf_set(conf, "td.connect.pass", password);
  if (groupCode != TMQ_CONF_OK || userCode != TMQ_CONF_OK || passCode != TMQ_CONF_OK) {
    fprintf(stderr, "Consumer configuration failed for %s, group:%d user:%d pass:%d\n",
            user, groupCode, userCode, passCode);
    tmq_conf_destroy(conf);
    return NULL;
  }

  if (tmq_conf_set(conf, "enable.auto.commit", "false") != TMQ_CONF_OK ||
      tmq_conf_set(conf, "auto.offset.reset", "earliest") != TMQ_CONF_OK ||
      tmq_conf_set(conf, "experimental.snapshot.enable", "false") != TMQ_CONF_OK ||
      (authToken != NULL &&
       tmq_conf_set(conf, "td.connect.token", authToken) != TMQ_CONF_OK)) {
    fprintf(stderr, "Consumer recovery configuration failed for %s\n", user);
    tmq_conf_destroy(conf);
    return NULL;
  }

  char   error[256] = {0};
  tmq_t *consumer = tmq_consumer_new(conf, error, sizeof(error));
  tmq_conf_destroy(conf);
  if (consumer == NULL) {
    fprintf(stderr, "Failed to create consumer for %s: %s, code:0x%x(%s)\n", user, error,
            taos_errno(NULL), taos_errstr(NULL));
  }
  return consumer;
}

static int32_t consumeValue(tmq_t *consumer, int32_t expectedValue,
                            int32_t *vgId) {
  int64_t deadline = taosGetTimestampMs() + 20000;
  while (taosGetTimestampMs() < deadline) {
    TAOS_RES *result = tmq_consumer_poll(consumer, 1000);
    if (result == NULL) {
      if (taos_errno(NULL) != 0) {
        fprintf(stderr, "Recovery poll failed: %s\n", taos_errstr(NULL));
        return -1;
      }
      continue;
    }

    int32_t rowCount = 0;
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(result)) != NULL) {
      int32_t value = 0;
      if (taos_num_fields(result) != 2 || row[1] == NULL) {
        fprintf(stderr, "Recovery row has an unexpected schema or NULL value\n");
        taos_free_result(result);
        return -1;
      }
      memcpy(&value, row[1], sizeof(value));
      if (value != expectedValue) {
        fprintf(stderr, "Recovery value:%d, expected:%d\n", value, expectedValue);
        taos_free_result(result);
        return -1;
      }
      ++rowCount;
    }
    int32_t code = taos_errno(result);
    if (code == 0 && rowCount == 1) {
      *vgId = tmq_get_vgroup_id(result);
      code = tmq_commit_sync(consumer, result);
    }
    taos_free_result(result);
    if (code != 0 || rowCount > 1) {
      fprintf(stderr, "Recovery consume/commit failed, rows:%d, code:0x%x\n",
              rowCount, code);
      return -1;
    }
    if (rowCount == 1) {
      printf("Recovery value:%d consumed and committed, vgId:%d\n",
             expectedValue, *vgId);
      return 0;
    }
  }
  fprintf(stderr, "Recovery poll timed out waiting for value:%d\n", expectedValue);
  return -1;
}

static int32_t checkTokenRecovery(TAOS *taos, tmq_list_t *topics) {
  int32_t code = -1;
  int32_t vgId = 0;
  char tokenName[80] = {0};
  char sql[256] = {0};
  char *authToken = NULL;
  tmq_t *legacy = NULL;
  tmq_t *recovered = NULL;
  snprintf(tokenName, sizeof(tokenName), "token_%s", ownerUser);
  snprintf(sql, sizeof(sql), "create token %s from user %s", tokenName, ownerUser);
  TAOS_RES *result = taos_query(taos, sql);
  if (taos_errno(result) == 0) {
    TAOS_ROW row = taos_fetch_row(result);
    int *lengths = taos_fetch_lengths(result);
    if (row != NULL && row[0] != NULL && lengths != NULL && lengths[0] > 0) {
      authToken = taosMemCalloc((int64_t)lengths[0] + 1, 1);
      if (authToken != NULL) {
        memcpy(authToken, row[0], lengths[0]);
      }
    }
  }
  taos_free_result(result);
  if (authToken == NULL) {
    fprintf(stderr, "Recovery token creation failed\n");
    goto END;
  }

  if (execSql(taos, "create table %s.recovery (ts timestamp, value int)", database) != 0 ||
      execSql(taos, "insert into %s.recovery values (1700000000000, 1)", database) != 0) {
    goto END;
  }
  legacy = createConsumer("root", NULL);
  recovered = createConsumer("root", authToken);
  if (legacy == NULL || recovered == NULL ||
      strcmp(recovered->user, ownerUser) != 0 ||
      tmq_subscribe(legacy, topics) != 0 ||
      consumeValue(legacy, 1, &vgId) != 0) {
    fprintf(stderr, "Legacy owner recovery setup failed\n");
    goto END;
  }
  int64_t committed = tmq_committed(legacy, topic, vgId);
  if (committed < 0) {
    fprintf(stderr, "Legacy committed offset is invalid:%" PRId64 "\n", committed);
    goto END;
  }

  int64_t newConsumerId = recovered->consumerId;
  recovered->consumerId = legacy->consumerId;
  int32_t rejected = syncAskEp(recovered);
  recovered->consumerId = newConsumerId;
  if (rejected != TSDB_CODE_MND_CONSUMER_NOT_EXIST ||
      atomic_load_8(&recovered->status) != TMQ_CONSUMER_STATUS__CLOSED ||
      syncAskEp(legacy) != 0) {
    fprintf(stderr, "Legacy owner rejection changed ownership or did not close the handle\n");
    goto END;
  }
  printf("Legacy owner mismatch rejected; recreating token consumer, group:%s\n", group);
  int32_t closeCode = tmq_consumer_close(recovered);
  if (closeCode != 0) {
    fprintf(stderr, "Rejected consumer close failed:0x%x\n", closeCode);
    goto END;
  }
  recovered = NULL;
  closeCode = tmq_consumer_close(legacy);
  if (closeCode != 0) {
    fprintf(stderr, "Legacy consumer close failed:0x%x\n", closeCode);
    goto END;
  }
  legacy = NULL;

  recovered = createConsumer("root", authToken);
  if (recovered == NULL || tmq_subscribe(recovered, topics) != 0 ||
      syncAskEp(recovered) != 0 ||
      tmq_committed(recovered, topic, vgId) != committed) {
    fprintf(stderr, "Recreated token consumer did not retain its committed offset\n");
    goto END;
  }
  if (execSql(taos, "insert into %s.recovery values (1700000000001, 2)", database) != 0 ||
      consumeValue(recovered, 2, &vgId) != 0) {
    goto END;
  }
  printf("Token consumer recreation preserved the group offset and resumed consumption\n");
  code = 0;

END:
  if (recovered != NULL) {
    (void)tmq_consumer_close(recovered);
  }
  if (legacy != NULL) {
    (void)tmq_consumer_close(legacy);
  }
  (void)execSql(taos, "drop token if exists %s", tokenName);
  taosMemFree(authToken);
  return code;
}

static int32_t prepare(TAOS *taos) {
  if (execSql(taos, "create database %s vgroups 1 wal_retention_period 3600", database) != 0 ||
      execSql(taos, "create topic %s as database %s", topic, database) != 0 ||
      execSql(taos, "create user %s pass '%s'", ownerUser, kPassword) != 0 ||
      execSql(taos, "create user %s pass '%s'", attackerUser, kPassword) != 0 ||
      execSql(taos, "grant use on database %s to %s", database, ownerUser) != 0 ||
      execSql(taos, "grant subscribe on topic %s.%s to %s", database, topic, ownerUser) != 0 ||
      execSql(taos, "grant use on database %s to %s", database, attackerUser) != 0 ||
      execSql(taos, "grant subscribe on topic %s.%s to %s", database, topic, attackerUser) != 0) {
    return -1;
  }
  printf("Consumer owner test setup complete, database:%s, topic:%s\n", database, topic);
  return 0;
}

static void cleanup(TAOS *taos) {
  (void)execSql(taos, "drop topic if exists %s", topic);
  (void)execSql(taos, "drop database if exists %s", database);
  (void)execSql(taos, "drop user if exists %s", ownerUser);
  (void)execSql(taos, "drop user if exists %s", attackerUser);
}

int main(int argc, char *argv[]) {
  int32_t     exitCode = EXIT_FAILURE;
  TAOS       *taos = taos_connect(NULL, "root", "taosdata", NULL, 0);
  tmq_t      *owner = NULL;
  tmq_t      *attacker = NULL;
  tmq_list_t *topics = NULL;
  tmq_list_t *emptyTopics = NULL;

  int64_t suffix = taosGetTimestampMs() % 1000000;
  int32_t pid = taosGetPId() % 1000;
  snprintf(database, sizeof(database), "id_%" PRId64 "_%d", suffix, pid);
  snprintf(topic, sizeof(topic), "it_%" PRId64 "_%d", suffix, pid);
  snprintf(group, sizeof(group), "ig_%" PRId64 "_%d", suffix, pid);
  snprintf(ownerUser, sizeof(ownerUser), "io_%" PRId64 "_%d", suffix, pid);
  snprintf(attackerUser, sizeof(attackerUser), "ia_%" PRId64 "_%d", suffix, pid);

  if (taos == NULL || prepare(taos) != 0) {
    if (taos == NULL) {
      fprintf(stderr, "Admin connection failed: %s\n", taos_errstr(NULL));
    }
    goto END;
  }

  topics = tmq_list_new();
  emptyTopics = tmq_list_new();
  if (topics == NULL || emptyTopics == NULL ||
      tmq_list_append(topics, topic) != 0) {
    fprintf(stderr, "Consumer or topic list initialization failed\n");
    goto END;
  }

  if (argc == 2 && strcmp(argv[1], "--token-recovery") == 0) {
    if (checkTokenRecovery(taos, topics) == 0) {
      exitCode = EXIT_SUCCESS;
    }
    goto END;
  }
  if (argc != 1) {
    fprintf(stderr, "Unexpected consumer owner test arguments\n");
    goto END;
  }
  owner = createConsumer(ownerUser, NULL);
  attacker = createConsumer(attackerUser, NULL);
  if (owner == NULL || attacker == NULL) {
    goto END;
  }

  int32_t code = tmq_subscribe(owner, topics);
  if (code != 0) {
    fprintf(stderr, "Owner subscribe failed: 0x%x(%s)\n", code,
            tmq_err2str(code));
    goto END;
  }

  code = syncAskEp(owner);
  if (code != 0) {
    fprintf(stderr, "Owner ask ep failed: 0x%x(%s)\n", code,
            tmq_err2str(code));
    goto END;
  }

  int64_t attackerConsumerId = attacker->consumerId;
  attacker->consumerId = owner->consumerId;
  code = syncAskEp(attacker);
  attacker->consumerId = attackerConsumerId;
  if (code != TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
    fprintf(stderr,
            "Cross-user ask ep returned 0x%x(%s), expected 0x%x(%s)\n",
            code, tmq_err2str(code), TSDB_CODE_MND_CONSUMER_NOT_EXIST,
            tmq_err2str(TSDB_CODE_MND_CONSUMER_NOT_EXIST));
    goto END;
  }

  attacker->consumerId = owner->consumerId;
  code = tmq_subscribe(attacker, topics);
  attacker->consumerId = attackerConsumerId;
  if (code != TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
    fprintf(stderr,
            "Cross-user subscribe returned 0x%x(%s), expected 0x%x(%s)\n",
            code, tmq_err2str(code), TSDB_CODE_MND_CONSUMER_NOT_EXIST,
            tmq_err2str(TSDB_CODE_MND_CONSUMER_NOT_EXIST));
    goto END;
  }

  attacker->consumerId = owner->consumerId;
  code = tmq_subscribe(attacker, emptyTopics);
  attacker->consumerId = attackerConsumerId;
  if (code != TSDB_CODE_MND_CONSUMER_NOT_EXIST) {
    fprintf(stderr,
            "Cross-user unsubscribe returned 0x%x(%s), expected 0x%x(%s)\n",
            code, tmq_err2str(code), TSDB_CODE_MND_CONSUMER_NOT_EXIST,
            tmq_err2str(TSDB_CODE_MND_CONSUMER_NOT_EXIST));
    goto END;
  }

  printf("Owner requests succeeded and cross-user consumer operations were "
         "rejected, consumer:0x%" PRIx64 "\n",
         owner->consumerId);
  exitCode = EXIT_SUCCESS;

END:
  if (attacker != NULL) {
    (void)tmq_consumer_close(attacker);
  }
  if (owner != NULL) {
    (void)tmq_consumer_close(owner);
  }
  tmq_list_destroy(emptyTopics);
  tmq_list_destroy(topics);
  if (taos != NULL) {
    cleanup(taos);
    taos_close(taos);
  }
  return exitCode;
}
