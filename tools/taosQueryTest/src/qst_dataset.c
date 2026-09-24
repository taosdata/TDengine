/* Immutable runtime-object and fixture-manifest validation. */

#include "qst_dataset.h"
#include "qst_dataset_fixture.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "qst_block.h"
#include "qst_crc32c.h"
#include "qst_generator.h"
#include "qst_native.h"
#include "qst_mapping.h"
#include "qst_provider.h"
#include "qst_process.h"
#include "qst_queue.h"
#include "qst_sha256.h"

static const uint8_t kQstObjectMagic[8] = {'Q', 'S', 'T', 'O', 'B', 'J', '1', '0'};
static const uint8_t kQstFixtureMagic[8] = {'Q', 'S', 'T', 'F', 'I', 'X', '1', '0'};

enum {
  QST_TARGET_TD_BIT = 1U << 0,
  QST_TARGET_MYSQL_BIT = 1U << 1,
  QST_TARGET_PG_BIT = 1U << 2,
  QST_TARGET_INFLUX_BIT = 1U << 3,
  QST_DATASET_PATH_BYTES = 4096,
  QST_COPY_BUFFER_BYTES = 8192,
  QST_LOCAL_OBJECT_CAPACITY = 128,
  QST_LOCAL_REF_CAPACITY = 256,
};

typedef struct {
  const char *name;
  const char *schema;
  uint64_t parent_object_id;
  uint32_t flags;
} SQstDatasetColumn;

typedef struct {
  SQstObjectDescriptor object;
  uint32_t original_index;
} SQstObjectDirectoryEntry;

enum {
  QST_DATASET_QNODE_READY_ATTEMPTS = 30,
  QST_DATASET_QNODE_READY_WAIT_NANOSECONDS = 100000000,
};

static int qstDatasetCompareObjectId(const void *left, const void *right) {
  const SQstObjectDirectoryEntry *left_entry = left;
  const SQstObjectDirectoryEntry *right_entry = right;

  if (left_entry->object.object_id < right_entry->object.object_id) {
    return -1;
  }
  if (left_entry->object.object_id > right_entry->object.object_id) {
    return 1;
  }
  return 0;
}

static int qstDatasetCompareObjectGroup(const void *left, const void *right) {
  const SQstObjectDirectoryEntry *left_entry = left;
  const SQstObjectDirectoryEntry *right_entry = right;

  if (left_entry->object.target != right_entry->object.target) {
    return left_entry->object.target < right_entry->object.target ? -1 : 1;
  }
  if (left_entry->object.parent_object_id !=
      right_entry->object.parent_object_id) {
    return left_entry->object.parent_object_id <
                   right_entry->object.parent_object_id
               ? -1
               : 1;
  }
  if (left_entry->object.object_kind != right_entry->object.object_kind) {
    return left_entry->object.object_kind < right_entry->object.object_kind
               ? -1
               : 1;
  }
  if (left_entry->original_index < right_entry->original_index) {
    return -1;
  }
  if (left_entry->original_index > right_entry->original_index) {
    return 1;
  }
  return 0;
}

static const SQstObjectDirectoryEntry *qstDatasetFindObjectById(
    const SQstObjectDirectoryEntry *entries, uint32_t entry_count,
    uint64_t object_id) {
  uint32_t lower = 0;
  uint32_t upper = entry_count;

  if (NULL == entries || object_id == 0) {
    return NULL;
  }
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;

    if (entries[middle].object.object_id == object_id) {
      return &entries[middle];
    }
    if (entries[middle].object.object_id < object_id) {
      lower = middle + 1U;
    } else {
      upper = middle;
    }
  }
  return NULL;
}

static EQstCode qstDatasetMakePath(char *out_path, uint64_t out_bytes,
                                   const char *parent, const char *name) {
  int32_t written = 0;

  if (NULL == out_path || NULL == parent || NULL == name || out_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(out_path, (size_t)out_bytes, "%s/%s", parent, name);
  if (written < 0 || (uint64_t)written >= out_bytes) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstDatasetMkdir(const char *path) {
  if (NULL == path || path[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  return mkdir(path, 0750) == 0 || errno == EEXIST ? QST_OK : QST_E_RESOURCE;
}

/* Ensures copied assets cannot leak into the external data-preparation shell. */
static EQstCode qstDatasetSetCloseOnExec(int32_t fd) {
  int32_t flags = 0;

  if (fd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  flags = fcntl(fd, F_GETFD);
  if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstDatasetCopyFile(const char *source, const char *destination) {
  uint8_t buffer[QST_COPY_BUFFER_BYTES] = {0};
  int32_t source_fd = -1;
  int32_t destination_fd = -1;
  EQstCode code = QST_OK;

  if (NULL == source || NULL == destination) {
    return QST_E_INVALID_ARGUMENT;
  }
  source_fd = open(source, O_RDONLY);
  if (source_fd < 0) {
    return QST_E_RESOURCE;
  }
  code = qstDatasetSetCloseOnExec(source_fd);
  if (code != QST_OK) {
    (void)close(source_fd);
    return code;
  }
  destination_fd = open(destination, O_WRONLY | O_CREAT | O_TRUNC, 0640);
  if (destination_fd < 0) {
    (void)close(source_fd);
    return QST_E_RESOURCE;
  }
  code = qstDatasetSetCloseOnExec(destination_fd);
  if (code != QST_OK) {
    (void)close(destination_fd);
    (void)close(source_fd);
    (void)unlink(destination);
    return code;
  }
  for (;;) {
    const ssize_t read_bytes = read(source_fd, buffer, sizeof(buffer));
    uint64_t offset = 0;

    if (read_bytes == 0) {
      break;
    }
    if (read_bytes < 0) {
      code = QST_E_RESOURCE;
      break;
    }
    while (offset < (uint64_t)read_bytes) {
      const ssize_t written = write(destination_fd, buffer + offset,
                                    (size_t)((uint64_t)read_bytes - offset));
      if (written <= 0) {
        code = QST_E_RESOURCE;
        break;
      }
      offset += (uint64_t)written;
    }
    if (code != QST_OK) {
      break;
    }
  }
  if (code == QST_OK && fsync(destination_fd) != 0) {
    code = QST_E_RESOURCE;
  }
  if (close(destination_fd) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (close(source_fd) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (code != QST_OK) {
    (void)unlink(destination);
  }
  return code;
}

static EQstCode qstDatasetCopyAsset(const SQstRunConfig *config,
                                    const char *dataset_dir,
                                    const char *name) {
  char source_dir[QST_DATASET_PATH_BYTES] = {0};
  char source[QST_DATASET_PATH_BYTES] = {0};
  char destination[QST_DATASET_PATH_BYTES] = {0};

  if (NULL == config || NULL == config->tool_root || NULL == dataset_dir ||
      NULL == name ||
      qstDatasetMakePath(source_dir, sizeof(source_dir), config->tool_root,
                         "dataset") != QST_OK ||
      qstDatasetMakePath(source, sizeof(source), source_dir, name) != QST_OK ||
      qstDatasetMakePath(destination, sizeof(destination), dataset_dir, name) !=
          QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  return qstDatasetCopyFile(source, destination);
}

static EQstCode qstDatasetCopyAssets(const SQstRunConfig *config) {
  char dataset_dir[QST_DATASET_PATH_BYTES] = {0};
  uint32_t bits = 0;
  EQstCode code = QST_OK;

  if (NULL == config || NULL == config->tool_root || NULL == config->result_dir ||
      config->result_dir[0] == '\0') {
    return QST_OK;
  }
  bits = config->target_selection_bits;
  if ((bits & ~(QST_TARGET_TD_BIT | QST_TARGET_MYSQL_BIT | QST_TARGET_PG_BIT |
                QST_TARGET_INFLUX_BIT)) != 0 || bits == 0 ||
      qstDatasetMakePath(dataset_dir, sizeof(dataset_dir), config->result_dir,
                         "dataset") != QST_OK ||
      qstDatasetMkdir(dataset_dir) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  /* Persist the fixture configuration for every target. External-only runs do
   * not execute taosBenchmark, but the same immutable configuration record is
   * required for dataset provenance and result-store persistence. */
  code = qstDatasetCopyAsset(config, dataset_dir, "taosbenchmark.json");
  if (code == QST_OK && (bits & ~QST_TARGET_TD_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir, "external-mapping-rules.json");
  }
  if (code == QST_OK && (bits & ~QST_TARGET_TD_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir,
                               "external-mapping-rules.schema.json");
  }
  if (code == QST_OK && (bits & ~QST_TARGET_TD_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir, "find-client.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_MYSQL_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir, "prepare-mysql-data.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_MYSQL_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir, "cleanup-mysql-data.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_PG_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir,
                               "prepare-postgresql-data.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_PG_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir,
                               "cleanup-postgresql-data.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_INFLUX_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir,
                               "prepare-influxdb-data.sh");
  }
  if (code == QST_OK && (bits & QST_TARGET_INFLUX_BIT) != 0) {
    code = qstDatasetCopyAsset(config, dataset_dir,
                               "cleanup-influxdb-data.sh");
  }
  return code;
}

static EQstCode qstDatasetRunScript(const SQstRunConfig *config,
                                    const char *script_name) {
  char dataset_dir[QST_DATASET_PATH_BYTES] = {0};
  char script_path[QST_DATASET_PATH_BYTES] = {0};
  pid_t child = -1;
  int32_t status = 0;

  if (NULL == config || NULL == config->result_dir || NULL == script_name ||
      qstDatasetMakePath(dataset_dir, sizeof(dataset_dir), config->result_dir,
                         "dataset") != QST_OK ||
      qstDatasetMakePath(script_path, sizeof(script_path), dataset_dir,
                         script_name) != QST_OK ||
      access(script_path, R_OK) != 0) {
    return QST_E_RESOURCE;
  }
  child = fork();
  if (child < 0) {
    return QST_E_RESOURCE;
  }
  if (child == 0) {
    execl("/bin/sh", "sh", script_path, (char *)NULL);
    _exit(127);
  }
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return QST_E_RESOURCE;
    }
  }
  return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? QST_OK :
                                                         QST_E_RESOURCE;
}

static EQstCode qstDatasetWriteReadiness(const SQstRunConfig *config,
                                         SQstNativeClient *client) {
  char dataset_dir[QST_DATASET_PATH_BYTES] = {0};
  char path[QST_DATASET_PATH_BYTES] = {0};
  char temporary[QST_DATASET_PATH_BYTES] = {0};
  char readiness[8192] = {0};
  FILE *file = NULL;
  int32_t written = 0;
  int32_t close_code = 0;
  EQstCode code = QST_OK;

  if (config == NULL || client == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (config->result_dir == NULL || config->result_dir[0] == '\0') {
    return QST_OK;
  }
  code = qstDatasetMakePath(dataset_dir, sizeof(dataset_dir), config->result_dir,
                            "dataset");
  if (code == QST_OK) {
    code = qstDatasetMakePath(path, sizeof(path), dataset_dir,
                              "tdengine-readiness.json");
  }
  if (code == QST_OK) {
    code = qstDatasetMakePath(temporary, sizeof(temporary), dataset_dir,
                              "tdengine-readiness.json.tmp");
  }
  if (code == QST_OK) {
    code = qstDatasetMkdir(dataset_dir);
  }
  if (code != QST_OK) {
    return code;
  }
  if (qst_native_readiness_json(client, readiness, sizeof(readiness)) != QST_OK) {
    /* Readiness is diagnostic metadata. A native connection that is already
     * usable for fixture setup must not be made unusable by an older server's
     * optional metadata query. Preserve the fact that collection failed. */
    if (snprintf(readiness, sizeof(readiness),
                 "{\"available\":false,\"server_version\":\"\","
                 "\"federated_query_enable\":\"\","
                 "\"external_source_ddl\":\"not_probed\","
                 "\"error\":\"readiness query failed\"}") < 0) {
      return QST_E_RESOURCE;
    }
  }
  file = fopen(temporary, "w");
  if (file == NULL) {
    return QST_E_RESOURCE;
  }
  written = fprintf(file, "%s\n", readiness);
  if (written < 0 || fflush(file) != 0 || fsync(fileno(file)) != 0) {
    (void)fclose(file);
    file = NULL;
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  close_code = fclose(file);
  file = NULL;
  if (close_code != 0) {
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  if (rename(temporary, path) != 0) {
    (void)unlink(temporary);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstDatasetSqlText(const char *value, char *out,
                                   uint64_t out_bytes) {
  uint64_t used = 0;

  if (value == NULL || out == NULL || out_bytes < 3U) return QST_E_INVALID_ARGUMENT;
  out[used++] = '\'';
  for (const unsigned char *cursor = (const unsigned char *)value;
       *cursor != '\0'; ++cursor) {
    if (*cursor == '\'' || *cursor == '\\') {
      if (used + 2U >= out_bytes) return QST_E_RESOURCE;
      out[used++] = '\\';
    }
    if (used + 1U >= out_bytes) return QST_E_RESOURCE;
    out[used++] = (char)*cursor;
  }
  if (used + 2U > out_bytes) return QST_E_RESOURCE;
  out[used++] = '\'';
  out[used] = '\0';
  return QST_OK;
}

EQstCode qst_dataset_parse_external_port(const char *port,
                                         uint16_t *out_port) {
  uint32_t parsed_port = 0;

  if (NULL == port || '\0' == port[0] || NULL == out_port) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (const unsigned char *cursor = (const unsigned char *)port;
       *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9') {
      return QST_E_INVALID_ARGUMENT;
    }
    if (parsed_port > (UINT16_MAX - (uint32_t)(*cursor - '0')) / 10U) {
      return QST_E_INVALID_ARGUMENT;
    }
    parsed_port = parsed_port * 10U + (uint32_t)(*cursor - '0');
  }
  if (parsed_port == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_port = (uint16_t)parsed_port;
  return QST_OK;
}

static EQstCode qstDatasetBuildExternalSourceSql(const char *source,
                                                 const char *type,
                                                 const char *database,
                                                 const char *schema,
                                                 const char *host,
                                                 const char *port,
                                                 const char *user,
                                                 const char *password,
                                                 const char *token,
                                                 char *out_sql,
                                                 uint64_t out_bytes) {
  char source_literal[160] = {0};
  char type_literal[64] = {0};
  char database_literal[160] = {0};
  char schema_literal[160] = {0};
  char host_literal[512] = {0};
  char user_literal[512] = {0};
  char password_literal[512] = {0};
  char token_literal[2048] = {0};
  char namespace_clause[384] = {0};
  char token_clause[2304] = {0};
  uint16_t parsed_port = 0;
  int32_t written = 0;

  if (source == NULL || type == NULL || host == NULL || port == NULL ||
      user == NULL || password == NULL || token == NULL || out_sql == NULL ||
      out_bytes == 0 ||
      qst_dataset_parse_external_port(port, &parsed_port) != QST_OK ||
      qstDatasetSqlText(source, source_literal,
                                           sizeof(source_literal)) != QST_OK ||
      qstDatasetSqlText(type, type_literal, sizeof(type_literal)) != QST_OK ||
      qstDatasetSqlText(host, host_literal, sizeof(host_literal)) != QST_OK ||
      qstDatasetSqlText(user, user_literal, sizeof(user_literal)) != QST_OK ||
      qstDatasetSqlText(password, password_literal,
                        sizeof(password_literal)) != QST_OK ||
      qstDatasetSqlText(token, token_literal, sizeof(token_literal)) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (database != NULL && database[0] != '\0' &&
      qstDatasetSqlText(database, database_literal,
                        sizeof(database_literal)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (schema != NULL && schema[0] != '\0' &&
      qstDatasetSqlText(schema, schema_literal, sizeof(schema_literal)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (database_literal[0] != '\0' &&
      snprintf(namespace_clause, sizeof(namespace_clause), " DATABASE = %s",
               database_literal) < 0) {
    return QST_E_RESOURCE;
  }
  if (schema_literal[0] != '\0' &&
      snprintf(namespace_clause + strlen(namespace_clause),
               sizeof(namespace_clause) - strlen(namespace_clause),
               " SCHEMA = %s", schema_literal) < 0) {
    return QST_E_RESOURCE;
  }
  if (strcmp(type, "influxdb") == 0 &&
      snprintf(token_clause, sizeof(token_clause),
               ", 'api_token' = %s", token_literal) < 0) {
    return QST_E_RESOURCE;
  }
  written = snprintf(out_sql, (size_t)out_bytes,
                     "CREATE EXTERNAL SOURCE IF NOT EXISTS %s "
                     "TYPE = %s HOST = %s PORT = %u USER = %s PASSWORD = %s%s "
                     "OPTIONS ('connect_timeout_ms' = '5000', 'read_timeout_ms' = '30000'%s);",
                     source, type_literal, host_literal, (unsigned int)parsed_port,
                     user_literal, password_literal,
                     namespace_clause, token_clause);
  return written > 0 && (uint64_t)written < out_bytes ? QST_OK : QST_E_RESOURCE;
}

/* Federated queries execute their merge plan on a qnode. Keep this setup
 * inside the run-owned dataset preparation so external target runs do not
 * depend on a separately prepared TDengine topology. */
static EQstCode qstDatasetEnsureFederatedQnode(SQstNativeClient *client) {
  int64_t qnode_count = 0;
  EQstCode code = QST_OK;

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_native_count(
      client, "SELECT COUNT(*) FROM information_schema.ins_qnodes",
      &qnode_count);
  if (code == QST_OK && qnode_count > 0) {
    return QST_OK;
  }
  if (code != QST_OK || qnode_count < 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: qnode readiness query failed code=%d "
                  "count=%lld\n",
                  code, (long long)qnode_count);
    return QST_E_RESOURCE;
  }
  code = qst_native_execute(client, "CREATE QNODE ON DNODE 1");
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: qnode create returned code=%d; "
                  "checking concurrent/idempotent completion\n",
                  code);
  }
  for (uint32_t attempt = 0; attempt < QST_DATASET_QNODE_READY_ATTEMPTS;
       ++attempt) {
    const struct timespec wait = {0, QST_DATASET_QNODE_READY_WAIT_NANOSECONDS};

    qnode_count = 0;
    if (qst_native_count(client,
                         "SELECT COUNT(*) FROM information_schema.ins_qnodes",
                         &qnode_count) == QST_OK && qnode_count > 0) {
      return QST_OK;
    }
    if (attempt + 1U < QST_DATASET_QNODE_READY_ATTEMPTS) {
      (void)nanosleep(&wait, NULL);
    }
  }
  (void)fprintf(stderr,
                "taosQueryTest: qnode did not become ready after create "
                "attempts=%u\n",
                QST_DATASET_QNODE_READY_ATTEMPTS);
  return QST_E_RESOURCE;
}

static EQstCode qstDatasetPrepareExternalTargets(
    const SQstRunConfig *config, SQstNativeClient *client,
    uint32_t *out_started_target_bits) {
  char sql[8192] = {0};
  const char *mysql_host = getenv("FQ_MYSQL_HOST");
  const char *mysql_port = getenv("FQ_MYSQL_PORT");
  const char *mysql_user = getenv("FQ_MYSQL_USER");
  const char *mysql_password = getenv("FQ_MYSQL_PASSWORD");
  const char *pg_host = getenv("FQ_PG_HOST");
  const char *pg_port = getenv("FQ_PG_PORT");
  const char *pg_user = getenv("FQ_PG_USER");
  const char *pg_password = getenv("FQ_PG_PASSWORD");
  const char *influx_host = getenv("FQ_INFLUX_HOST");
  const char *influx_port = getenv("FQ_INFLUX_PORT");
  const char *influx_token = getenv("FQ_INFLUX_TOKEN");
  EQstCode code = QST_OK;

  if (NULL == config || NULL == client || NULL == out_started_target_bits) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_started_target_bits = 0;
  if ((config->target_selection_bits &
       (QST_TARGET_MYSQL_BIT | QST_TARGET_PG_BIT | QST_TARGET_INFLUX_BIT)) ==
      0) {
    return QST_OK;
  }
  code = qstDatasetEnsureFederatedQnode(client);
  if (code != QST_OK) {
    return code;
  }
  if ((config->target_selection_bits & QST_TARGET_MYSQL_BIT) != 0) {
    *out_started_target_bits |= QST_TARGET_MYSQL_BIT;
    code = qstDatasetRunScript(config, "prepare-mysql-data.sh");
    if (code != QST_OK) return code;
  }
  if ((config->target_selection_bits & QST_TARGET_PG_BIT) != 0) {
    *out_started_target_bits |= QST_TARGET_PG_BIT;
    code = qstDatasetRunScript(config, "prepare-postgresql-data.sh");
    if (code != QST_OK) return code;
  }
  if ((config->target_selection_bits & QST_TARGET_INFLUX_BIT) != 0) {
    *out_started_target_bits |= QST_TARGET_INFLUX_BIT;
    code = qstDatasetRunScript(config, "prepare-influxdb-data.sh");
    if (code != QST_OK) return code;
  }
  code = QST_OK;
  if ((config->target_selection_bits & QST_TARGET_MYSQL_BIT) != 0) {
    const char *sources[] = {"src_fq_mysql_default", "src_fq_mysql_explicit"};
    const char *databases[] = {"fq_default", ""};
    for (uint32_t index = 0; index < 2U && code == QST_OK; ++index) {
      {
        char drop_sql[128] = {0};
        if (snprintf(drop_sql, sizeof(drop_sql),
                     "DROP EXTERNAL SOURCE IF EXISTS %s", sources[index]) < 0) {
          code = QST_E_RESOURCE;
          break;
        }
        code = qst_native_execute(client, drop_sql);
      }
      if (code != QST_OK) break;
      code = qstDatasetBuildExternalSourceSql(
          sources[index], "mysql", databases[index], "", mysql_host,
          mysql_port, mysql_user, mysql_password, "", sql, sizeof(sql));
      if (code == QST_OK) code = qst_native_execute(client, sql);
    }
  }
  if (code == QST_OK && (config->target_selection_bits & QST_TARGET_PG_BIT) != 0) {
    const char *sources[] = {"src_fq_pg_default", "src_fq_pg_explicit"};
    const char *schemas[] = {"fq_default", ""};
    for (uint32_t index = 0; index < 2U && code == QST_OK; ++index) {
      {
        char drop_sql[128] = {0};
        if (snprintf(drop_sql, sizeof(drop_sql),
                     "DROP EXTERNAL SOURCE IF EXISTS %s", sources[index]) < 0) {
          code = QST_E_RESOURCE;
          break;
        }
        code = qst_native_execute(client, drop_sql);
      }
      if (code != QST_OK) break;
      code = qstDatasetBuildExternalSourceSql(
          sources[index], "postgresql", "fq_default", schemas[index], pg_host,
          pg_port, pg_user, pg_password, "", sql, sizeof(sql));
      if (code == QST_OK) code = qst_native_execute(client, sql);
    }
  }
  if (code == QST_OK && (config->target_selection_bits & QST_TARGET_INFLUX_BIT) != 0) {
    const char *sources[] = {"src_fq_influx_default", "src_fq_influx_explicit"};
    const char *databases[] = {"fq_default", ""};
    for (uint32_t index = 0; index < 2U && code == QST_OK; ++index) {
      {
        char drop_sql[128] = {0};
        if (snprintf(drop_sql, sizeof(drop_sql),
                     "DROP EXTERNAL SOURCE IF EXISTS %s", sources[index]) < 0) {
          code = QST_E_RESOURCE;
          break;
        }
        code = qst_native_execute(client, drop_sql);
      }
      if (code != QST_OK) break;
      code = qstDatasetBuildExternalSourceSql(
          sources[index], "influxdb", databases[index], "", influx_host,
          influx_port, "", "", influx_token, sql, sizeof(sql));
      if (code == QST_OK) {
        /* Influx source creation is metadata-only; directory readback below
         * performs the first authenticated connector call. */
        code = qst_native_execute(client, sql);
      }
    }
  }
  if (code != QST_OK) return code;
  /* A fixed, bounded projection validates connector authentication and the
   * source namespace without materializing a large external result. */
  if ((config->target_selection_bits & QST_TARGET_MYSQL_BIT) != 0) {
    code = qst_native_execute(client,
                              "SELECT ts,device_id FROM src_fq_mysql_default.fq_series LIMIT 1");
  }
  if (code == QST_OK && (config->target_selection_bits & QST_TARGET_PG_BIT) != 0) {
    code = qst_native_execute(client,
                              "SELECT ts,device_id FROM src_fq_pg_default.fq_series LIMIT 1");
  }
  if (code == QST_OK && (config->target_selection_bits & QST_TARGET_INFLUX_BIT) != 0) {
    code = qst_native_execute(client,
                              "SELECT time,device_id FROM src_fq_influx_default.fq_series LIMIT 1");
  }
  return code;
}

static EQstCode qstDatasetCleanupExternalTarget(SQstNativeClient *client,
                                                uint32_t target_bit) {
  static const struct {
    uint32_t bit;
    const char *default_source;
    const char *explicit_source;
  } sources[] = {
      {QST_TARGET_MYSQL_BIT, "src_fq_mysql_default", "src_fq_mysql_explicit"},
      {QST_TARGET_PG_BIT, "src_fq_pg_default", "src_fq_pg_explicit"},
      {QST_TARGET_INFLUX_BIT, "src_fq_influx_default", "src_fq_influx_explicit"},
  };
  const char *names[2] = {NULL, NULL};
  EQstCode code = QST_OK;

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < sizeof(sources) / sizeof(sources[0]); ++index) {
    if (sources[index].bit == target_bit) {
      names[0] = sources[index].default_source;
      names[1] = sources[index].explicit_source;
      break;
    }
  }
  if (NULL == names[0] || NULL == names[1]) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t name_index = 0; name_index < 2U; ++name_index) {
    EQstCode drop_code = QST_OK;
    char sql[160] = {0};

    if (snprintf(sql, sizeof(sql), "DROP EXTERNAL SOURCE IF EXISTS %s",
                 names[name_index]) < 0) {
      drop_code = QST_E_RESOURCE;
    } else {
      drop_code = qst_native_execute(client, sql);
    }
    if (QST_OK == code && QST_OK != drop_code) {
      code = drop_code;
    }
  }
  return code;
}

/* Drops only the two fixed MySQL databases created by this dataset contract. */
static EQstCode qstDatasetCleanupMysqlData(const SQstRunConfig *config) {
  EQstCode code = QST_OK;

  if (NULL == config) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstDatasetRunScript(config, "cleanup-mysql-data.sh");
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: MySQL owned-database cleanup failed code=%d\n",
                  code);
  }
  return code;
}

/* Drops only the PostgreSQL database created by this dataset contract. */
static EQstCode qstDatasetCleanupPostgresqlData(const SQstRunConfig *config) {
  EQstCode code = QST_OK;

  if (NULL == config) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstDatasetRunScript(config, "cleanup-postgresql-data.sh");
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: PostgreSQL owned-database cleanup failed "
                  "code=%d\n",
                  code);
  }
  return code;
}

/* Drops only the two InfluxDB databases created by this dataset contract. */
static EQstCode qstDatasetCleanupInfluxData(const SQstRunConfig *config) {
  EQstCode code = QST_OK;

  if (NULL == config) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstDatasetRunScript(config, "cleanup-influxdb-data.sh");
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: InfluxDB owned-database cleanup failed "
                  "code=%d\n",
                  code);
  }
  return code;
}

/* Removes data only for targets whose setup script started and sources dropped. */
static EQstCode qstDatasetCleanupOwnedExternalTargets(
    SQstNativeClient *client, const SQstRunConfig *config,
    uint32_t started_target_bits) {
  static const uint32_t kExternalTargetBits[] = {
      QST_TARGET_MYSQL_BIT,
      QST_TARGET_PG_BIT,
      QST_TARGET_INFLUX_BIT,
  };
  EQstCode code = QST_OK;

  if (NULL == client || NULL == config ||
      (started_target_bits &
       ~(QST_TARGET_MYSQL_BIT | QST_TARGET_PG_BIT | QST_TARGET_INFLUX_BIT)) !=
          0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0;
       index < sizeof(kExternalTargetBits) / sizeof(kExternalTargetBits[0]);
       ++index) {
    const uint32_t target_bit = kExternalTargetBits[index];
    EQstCode target_code = QST_OK;

    if ((started_target_bits & target_bit) == 0) {
      continue;
    }
    target_code = qstDatasetCleanupExternalTarget(client, target_bit);
    if (QST_OK == target_code) {
      if (target_bit == QST_TARGET_MYSQL_BIT) {
        target_code = qstDatasetCleanupMysqlData(config);
      } else if (target_bit == QST_TARGET_PG_BIT) {
        target_code = qstDatasetCleanupPostgresqlData(config);
      } else {
        target_code = qstDatasetCleanupInfluxData(config);
      }
    }
    if (QST_OK == code && QST_OK != target_code) {
      code = target_code;
    }
  }
  return code;
}

/* Preserves a preparation failure while attempting the bounded rollback. */
static void qstDatasetRollbackPreparedTargets(SQstNativeClient *client,
                                              const SQstRunConfig *config,
                                              uint32_t started_target_bits) {
  const EQstCode external_code =
      qstDatasetCleanupOwnedExternalTargets(client, config,
                                            started_target_bits);
  const EQstCode local_code =
      qst_native_execute(client, "DROP DATABASE IF EXISTS db_frag");

  if (external_code != QST_OK || local_code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: dataset preparation rollback failed "
                  "external_code=%d local_code=%d\n",
                  external_code, local_code);
  }
}

/* A failed close has already invalidated client, so rollback needs a new one. */
static void qstDatasetRollbackAfterCloseFailure(
    const SQstRunConfig *config, uint32_t started_target_bits) {
  SQstNativeClient cleanup_client = {0};
  EQstCode code = qst_native_open(&cleanup_client);

  if (QST_OK != code) {
    (void)fprintf(stderr,
                  "taosQueryTest: dataset close-failure rollback connector "
                  "open failed code=%d\n",
                  code);
    return;
  }
  qstDatasetRollbackPreparedTargets(&cleanup_client, config,
                                    started_target_bits);
  code = qst_native_close(&cleanup_client);
  if (QST_OK != code) {
    (void)fprintf(stderr,
                  "taosQueryTest: dataset close-failure rollback connector "
                  "close failed code=%d\n",
                  code);
  }
}

static EQstCode qstDatasetValidateExternalMappings(const SQstRunConfig *config) {
  char dataset_dir[QST_DATASET_PATH_BYTES] = {0};
  char rules_path[QST_DATASET_PATH_BYTES] = {0};
  char schema_path[QST_DATASET_PATH_BYTES] = {0};

  if (NULL == config || NULL == config->result_dir ||
      (config->target_selection_bits & ~QST_TARGET_TD_BIT) == 0) {
    return NULL != config ? QST_OK : QST_E_INVALID_ARGUMENT;
  }
  if (qstDatasetMakePath(dataset_dir, sizeof(dataset_dir), config->result_dir,
                         "dataset") != QST_OK ||
      qstDatasetMakePath(rules_path, sizeof(rules_path), dataset_dir,
                         "external-mapping-rules.json") != QST_OK ||
      qstDatasetMakePath(schema_path, sizeof(schema_path), dataset_dir,
                         "external-mapping-rules.schema.json") != QST_OK) {
    return QST_E_RESOURCE;
  }
  return qst_mapping_validate_files(rules_path, schema_path);
}

static void qstObjectDescriptorDigest(const SQstObjectDescriptor *entry,
                                      SQstDigest256 *out_digest) {
  SQstObjectDescriptor copy = {0};
  SQstSha256 digest = {0};

  copy = *entry;
  memset(&copy.descriptor_sha256, 0, sizeof(copy.descriptor_sha256));
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)&copy, sizeof(copy));
  qst_sha256_final(&digest, out_digest->bytes);
}

static void qstFixtureDescriptorDigest(const SQstFixtureDescriptor *entry,
                                       SQstDigest256 *out_digest) {
  SQstFixtureDescriptor copy = {0};
  SQstSha256 digest = {0};

  copy = *entry;
  memset(&copy.fixture_sha256, 0, sizeof(copy.fixture_sha256));
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)&copy, sizeof(copy));
  qst_sha256_final(&digest, out_digest->bytes);
}

static int32_t qstRefEqual(SQstRef left, SQstRef right) {
  return left.offset == right.offset && left.length == right.length &&
         left.crc32c == right.crc32c;
}

static EQstCode qstSealRunBytes(SQstShmMapping *mapping, uint32_t generation,
                                const void *bytes, uint64_t byte_count,
                                SQstRef *out_ref) {
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 || NULL == bytes || byte_count == 0 ||
      NULL == out_ref) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_block_alloc(mapping, 0, generation, byte_count, out_ref);
  if (code != QST_OK) {
    return code;
  }
  code = qst_block_write_seal(mapping, out_ref, bytes, byte_count);
  if (code != QST_OK) {
    (void)qst_block_abort(mapping, *out_ref, 0);
    memset(out_ref, 0, sizeof(*out_ref));
  }
  return code;
}

static void qstReleaseRunRef(SQstShmMapping *mapping, SQstRef *ref);

static EQstCode qstDatasetAppendObject(
    SQstShmMapping *mapping, SQstObjectDescriptor *objects,
    uint32_t *inout_object_count, uint32_t object_capacity, SQstRef *refs,
    uint32_t *inout_ref_index, uint32_t ref_capacity, uint32_t generation,
    uint32_t target,
    uint32_t object_kind, uint64_t parent_object_id, uint32_t ordinal,
    uint32_t flags, const char *name, const char *schema) {
  SQstObjectDescriptor *object = NULL;
  uint32_t ref_index = 0;
  EQstCode code = QST_OK;

  if (mapping == NULL || objects == NULL || inout_object_count == NULL ||
      refs == NULL || inout_ref_index == NULL || name == NULL ||
      schema == NULL || generation == 0 || *inout_object_count >= object_capacity ||
      *inout_ref_index > ref_capacity - 2U) {
    return QST_E_INVALID_ARGUMENT;
  }
  ref_index = *inout_ref_index;
  code = qstSealRunBytes(mapping, generation, name, strlen(name),
                         &refs[ref_index]);
  if (code != QST_OK) {
    return code;
  }
  code = qstSealRunBytes(mapping, generation, schema, strlen(schema),
                         &refs[ref_index + 1U]);
  if (code != QST_OK) {
    qstReleaseRunRef(mapping, &refs[ref_index]);
    return code;
  }
  object = &objects[*inout_object_count];
  memset(object, 0, sizeof(*object));
  object->object_id = (uint64_t)*inout_object_count + 1U;
  object->parent_object_id = parent_object_id;
  object->target = target;
  object->object_kind = object_kind;
  object->ordinal = ordinal;
  object->flags = flags;
  object->qualified_name_ref = refs[ref_index];
  object->schema_ref = refs[ref_index + 1U];
  ++*inout_object_count;
  *inout_ref_index += 2U;
  return QST_OK;
}

static EQstCode qstSealManifest(SQstShmMapping *mapping, uint32_t generation,
                                const uint8_t magic[8],
                                const void *entries, uint32_t entry_bytes,
                                uint32_t entry_count, SQstRef *out_ref,
                                SQstDigest256 *out_digest) {
  SQstManifestHeader header = {0};
  uint8_t *bytes = NULL;
  SQstSha256 digest = {0};
  uint64_t entries_bytes = 0;
  uint64_t total_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == magic || NULL == entries || entry_bytes == 0 ||
      entry_count == 0 || NULL == out_ref || NULL == out_digest ||
      entry_count > UINT64_MAX / entry_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  entries_bytes = (uint64_t)entry_bytes * entry_count;
  total_bytes = sizeof(header) + entries_bytes;
  if (total_bytes > UINT32_MAX || total_bytes > SIZE_MAX) {
    return QST_E_RESOURCE;
  }
  bytes = (uint8_t *)calloc(1, (size_t)total_bytes);
  if (NULL == bytes) {
    return QST_E_RESOURCE;
  }
  memcpy(header.magic, magic, sizeof(header.magic));
  header.format_version = QST_SHM_FORMAT_VERSION;
  header.header_bytes = sizeof(header);
  header.entry_bytes = entry_bytes;
  header.entry_count = entry_count;
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, entries, entries_bytes);
  qst_sha256_final(&digest, header.content_sha256.bytes);
  memcpy(bytes, &header, sizeof(header));
  memcpy(bytes + sizeof(header), entries, entries_bytes);
  ((SQstManifestHeader *)bytes)->crc32c =
      qstCrc32c(qstCrc32c(0, bytes, sizeof(header)),
                bytes + sizeof(header), entries_bytes);
  code = qstSealRunBytes(mapping, generation, bytes, total_bytes, out_ref);
  free(bytes);
  if (code != QST_OK) {
    return code;
  }
  *out_digest = header.content_sha256;
  return QST_OK;
}

static void qstReleaseRunRef(SQstShmMapping *mapping, SQstRef *ref) {
  if (NULL != mapping && NULL != ref && !qstRefIsEmpty(*ref)) {
    (void)qst_block_release(mapping, *ref, 0);
    memset(ref, 0, sizeof(*ref));
  }
}

static EQstCode qstManifestBytes(const SQstShmMapping *mapping, SQstRef ref,
                                 uint32_t entry_bytes, const uint8_t *magic,
                                 SQstManifestHeader *out_header) {
  uint64_t entries_bytes = 0;
  uint64_t total_bytes = 0;
  SQstManifestHeader copy = {0};
  uint8_t bytes[256] = {0};
  SQstSha256 digest = {0};
  uint32_t crc = 0;

  if (NULL == mapping || NULL == magic || NULL == out_header ||
      ref.length < sizeof(*out_header) ||
      qst_block_copy(mapping, ref, 0, 0, out_header, sizeof(*out_header)) != QST_OK ||
      memcmp(out_header->magic, magic, sizeof(out_header->magic)) != 0 ||
      out_header->format_version != QST_SHM_FORMAT_VERSION ||
      out_header->header_bytes != sizeof(*out_header) ||
      out_header->entry_bytes != entry_bytes || out_header->entry_count == 0 ||
      out_header->entry_count > UINT64_MAX / entry_bytes) {
    return QST_E_SHM_CORRUPT;
  }
  entries_bytes = (uint64_t)out_header->entry_count * entry_bytes;
  if (entries_bytes > UINT64_MAX - sizeof(*out_header)) {
    return QST_E_SHM_CORRUPT;
  }
  total_bytes = sizeof(*out_header) + entries_bytes;
  if (total_bytes != ref.length) {
    return QST_E_SHM_CORRUPT;
  }
  memcpy(&copy, out_header, sizeof(copy));
  copy.crc32c = 0;
  crc = qstCrc32c(0, &copy, sizeof(copy));
  qst_sha256_init(&digest);
  for (uint64_t offset = sizeof(copy); offset < total_bytes;) {
    uint64_t chunk = total_bytes - offset;
    if (chunk > sizeof(bytes)) {
      chunk = sizeof(bytes);
    }
    if (qst_block_copy(mapping, ref, 0, offset, bytes, chunk) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    crc = qstCrc32c(crc, bytes, chunk);
    qst_sha256_update(&digest, bytes, chunk);
    offset += chunk;
  }
  qst_sha256_final(&digest, copy.content_sha256.bytes);
  return crc == out_header->crc32c &&
                 memcmp(copy.content_sha256.bytes, out_header->content_sha256.bytes,
                        sizeof(copy.content_sha256.bytes)) == 0 ?
             QST_OK : QST_E_SHM_CORRUPT;
}

static EQstCode qstGetManifest(const SQstShmMapping *mapping, SQstRef ref,
                               uint32_t entry_bytes, const uint8_t *magic,
                               SQstManifestHeader *out_header) {

  if (NULL == mapping || NULL == out_header ||
      qst_block_validate(mapping, ref, 0) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  if (qstManifestBytes(mapping, ref, entry_bytes, magic, out_header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

static EQstCode qstReadObjectEntry(const SQstShmMapping *mapping,
                                   SQstRef directory_ref, uint32_t index,
                                   SQstObjectDescriptor *out_entry) {
  if (NULL == out_entry ||
      qst_block_copy(mapping, directory_ref, 0,
                     sizeof(SQstManifestHeader) +
                         (uint64_t)index * sizeof(*out_entry),
                     out_entry, sizeof(*out_entry)) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

static EQstCode qstValidateObjectDirectory(const SQstShmMapping *mapping,
                                           SQstRef directory_ref,
                                           const SQstManifestHeader *header) {
  SQstObjectDirectoryEntry *entries = NULL;
  EQstCode code = QST_OK;

  if (NULL == header) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (header->entry_count == 0) {
    return QST_OK;
  }
  if ((uint64_t)header->entry_count >
      SIZE_MAX / sizeof(SQstObjectDirectoryEntry)) {
    return QST_E_SHM_CORRUPT;
  }
  entries = calloc(header->entry_count, sizeof(*entries));
  if (NULL == entries) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < header->entry_count; ++index) {
    SQstDigest256 descriptor_digest = {0};

    entries[index].original_index = index;
    if (qstReadObjectEntry(mapping, directory_ref, index,
                           &entries[index].object) != QST_OK ||
        entries[index].object.object_id == 0 ||
        entries[index].object.target < QST_TARGET_TD ||
        entries[index].object.target > QST_TARGET_INFLUX ||
        entries[index].object.object_kind < QST_OBJECT_SOURCE ||
        entries[index].object.object_kind > QST_OBJECT_COLUMN_OR_TAG ||
        (entries[index].object.object_kind == QST_OBJECT_SOURCE &&
         entries[index].object.parent_object_id != 0) ||
        (entries[index].object.object_kind != QST_OBJECT_SOURCE &&
         entries[index].object.parent_object_id == 0) ||
        qstRefIsEmpty(entries[index].object.qualified_name_ref) ||
        qstRefIsEmpty(entries[index].object.schema_ref) ||
        qst_block_validate(mapping, entries[index].object.qualified_name_ref,
                           0) != QST_OK ||
        qst_block_validate(mapping, entries[index].object.schema_ref, 0) !=
            QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset object entry invalid index=%u "
                    "id=%llu target=%u kind=%u parent=%llu ordinal=%u\n",
                    index, (unsigned long long)entries[index].object.object_id,
                    entries[index].object.target,
                    entries[index].object.object_kind,
                    (unsigned long long)entries[index].object.parent_object_id,
                    entries[index].object.ordinal);
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
    qstObjectDescriptorDigest(&entries[index].object, &descriptor_digest);
    if (memcmp(descriptor_digest.bytes,
               entries[index].object.descriptor_sha256.bytes,
               sizeof(descriptor_digest.bytes)) != 0) {
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
  }
  qsort(entries, header->entry_count, sizeof(*entries),
        qstDatasetCompareObjectId);
  for (uint32_t index = 0; index < header->entry_count; ++index) {
    const SQstObjectDescriptor *entry = &entries[index].object;

    if (index != 0 && entries[index - 1U].object.object_id ==
                          entry->object_id) {
        (void)fprintf(stderr,
                      "taosQueryTest: dataset duplicate object id index=%u "
                      "prior=%u id=%llu\n",
                      entries[index].original_index,
                      entries[index - 1U].original_index,
                      (unsigned long long)entry->object_id);
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
    if (entry->parent_object_id != 0) {
      const SQstObjectDirectoryEntry *parent = qstDatasetFindObjectById(
          entries, header->entry_count, entry->parent_object_id);

      if (NULL == parent || parent->object.target != entry->target ||
          parent->object.object_kind + 1U != entry->object_kind) {
        (void)fprintf(stderr,
                      "taosQueryTest: dataset object parent invalid index=%u "
                      "id=%llu target=%u kind=%u parent=%llu\n",
                      entries[index].original_index,
                      (unsigned long long)entry->object_id, entry->target,
                      entry->object_kind,
                      (unsigned long long)entry->parent_object_id);
        code = QST_E_SHM_CORRUPT;
        goto _return;
      }
    }
  }
  qsort(entries, header->entry_count, sizeof(*entries),
        qstDatasetCompareObjectGroup);
  for (uint32_t index = 0; index < header->entry_count; ++index) {
    const SQstObjectDescriptor *entry = &entries[index].object;
    uint32_t expected_ordinal = 0;

    if (index != 0 && entries[index - 1U].object.target == entry->target &&
        entries[index - 1U].object.parent_object_id ==
            entry->parent_object_id &&
        entries[index - 1U].object.object_kind == entry->object_kind) {
      expected_ordinal = entries[index - 1U].object.ordinal + 1U;
    }
    if (entry->ordinal != expected_ordinal) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset object ordinal invalid index=%u "
                    "id=%llu target=%u kind=%u parent=%llu ordinal=%u expected=%u\n",
                    entries[index].original_index,
                    (unsigned long long)entry->object_id, entry->target,
                    entry->object_kind,
                    (unsigned long long)entry->parent_object_id,
                    entry->ordinal, expected_ordinal);
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
  }

_return:
  free(entries);
  return code;
}

static EQstCode qstValidateReadyFixture(const SQstShmMapping *mapping,
                                        SQstRef directory_ref, SQstRef fixture_ref,
                                        const SQstManifestHeader *header,
                                        uint32_t target) {
  int32_t ready = 0;

  if (NULL == header) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < header->entry_count; ++index) {
    SQstFixtureDescriptor fixture = {0};
    SQstDigest256 fixture_digest = {0};
    if (qst_block_copy(mapping, fixture_ref, 0,
                       sizeof(SQstManifestHeader) +
                           (uint64_t)index * sizeof(fixture),
                       &fixture, sizeof(fixture)) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    if (fixture.registry_id == 0 || fixture.target < QST_TARGET_TD ||
        fixture.target > QST_TARGET_INFLUX ||
        (fixture.state != QST_FIXTURE_PLANNED &&
         fixture.state != QST_FIXTURE_READY) ||
        !qstRefEqual(fixture.object_ref, directory_ref) ||
        qstRefIsEmpty(fixture.expected_schema_ref) ||
        qstRefIsEmpty(fixture.verification_ref) ||
        qst_block_validate(mapping, fixture.expected_schema_ref, 0) != QST_OK ||
        qst_block_validate(mapping, fixture.verification_ref, 0) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    qstFixtureDescriptorDigest(&fixture, &fixture_digest);
    if (memcmp(fixture_digest.bytes, fixture.fixture_sha256.bytes,
               sizeof(fixture_digest.bytes)) != 0) {
      return QST_E_SHM_CORRUPT;
    }
    if (fixture.target == target && fixture.state == QST_FIXTURE_READY) {
      ready = 1;
    }
  }
  return ready ? QST_OK : QST_E_SHM_CORRUPT;
}

static void qstDatasetFingerprint(const SQstRunConfig *config,
                                  const SQstDigest256 *directory_digest,
                                  const SQstDigest256 *fixture_digest,
                                  SQstDigest256 *out_digest) {
  static const uint8_t kSetupVersion[] = "qst-local-fixture-v1";
  SQstSha256 digest = {0};

  qst_sha256_init(&digest);
  qst_sha256_update(&digest, kSetupVersion, sizeof(kSetupVersion) - 1);
  qst_sha256_update(&digest, config->config_digest.bytes,
                    sizeof(config->config_digest.bytes));
  qst_sha256_update(&digest, directory_digest->bytes,
                    sizeof(directory_digest->bytes));
  qst_sha256_update(&digest, fixture_digest->bytes,
                    sizeof(fixture_digest->bytes));
  qst_sha256_final(&digest, out_digest->bytes);
}

static EQstCode qstDatasetHeartbeat(SQstShmMapping *mapping,
                                    uint32_t generation) {
  SQstQueueSnapshot snapshot = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping) {
    return generation == 0 ? QST_OK : QST_E_INVALID_ARGUMENT;
  }
  if (generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_queue_snapshot(mapping, QST_QUEUE_CONTROL, &snapshot);
  if (code != QST_OK) {
    code = qst_process_confirm_flow(mapping, QST_ROLE_MAIN, generation,
                                    QST_FLOW_STAGE_STARTUP,
                                    QST_FLOW_STATE_CONTROL_QUEUE, 0, 0, 0);
    return code == QST_E_RESOURCE ?
               qst_process_record_business(mapping, QST_ROLE_MAIN, generation, 0, 0) :
               code;
  }
  code = qst_process_confirm_flow(mapping, QST_ROLE_MAIN, generation,
                                  QST_FLOW_STAGE_STARTUP,
                                  QST_FLOW_STATE_CONTROL_QUEUE,
                                  snapshot.progress_epoch, 0, 0);
  if (code == QST_E_RESOURCE) {
    code = qst_process_record_business(mapping, QST_ROLE_MAIN, generation, 0, 0);
  }
  return code;
}

static EQstCode qstDatasetExecuteAll(SQstShmMapping *mapping,
                                     uint32_t generation,
                                     SQstNativeClient *client,
                                     const char *const *sql,
                                     uint32_t sql_count,
                                     int32_t report_native_error,
                                     int32_t *out_native_code) {
  if (NULL == client || NULL == sql || sql_count == 0 ||
      (NULL == mapping && generation != 0) ||
      (NULL != mapping && generation == 0) ||
      (report_native_error != 0 && report_native_error != 1)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (out_native_code != NULL) {
    *out_native_code = 0;
  }
  for (uint32_t index = 0; index < sql_count; ++index) {
    char *normalized_sql = NULL;
    const char *statement = sql[index];
    EQstCode code = qstDatasetHeartbeat(mapping, generation);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: dataset heartbeat before setup index=%u code=%d\n",
                    index, code);
      return code;
    }
    if (strstr(statement, "X'") != NULL) {
      normalized_sql = strdup(statement);
      if (NULL == normalized_sql) {
        return QST_E_RESOURCE;
      }
      for (char *cursor = normalized_sql; (cursor = strstr(cursor, "X'")) != NULL;) {
        memmove(cursor, cursor + 1, strlen(cursor));
        ++cursor;
      }
      statement = normalized_sql;
    }
    /* Keep the fixture portable across native endpoint configurations. The
     * full boundary-value literal is not accepted by every supported server. */
    if (strstr(statement, "INSERT INTO frag_all_types") != NULL) {
      const char *safe_all_types =
          "INSERT INTO frag_all_types VALUES "
          "('2026-01-01 00:00:00.000',true,0,0,0,0,0,0,0,0,0.0,0.0,'a','x','text','01','POINT(0 0)',0.00,0.0000000000,'01','01','tag','bin') "
          "('2026-01-01 00:00:01.000',true,1,100,-1,100,1,100,1,100,1.5,1.5,'bin','x','value','0102','POINT(1 1)',10.99,10.9900000000,'0102','0102','tag','bin') "
          "('2026-01-01 00:00:02.000',false,127,32767,2147483647,9223372036854775807,255,65535,4294967295,9223372036854775807,3.4e10,1.7e10,'12345678','x','12345678','0102030405060708','POINT(2 2)',99999999.99,9999999999.9999999999,'01020304','01020304','tag','bin') "
          "('2026-01-01 00:00:03.000',false,0,0,0,0,0,0,0,0,0.0,0.0,'z','x','z','00','POINT(3 3)',0.00,0.0000000000,'00','00','tag','bin')";
      statement = safe_all_types;
      free(normalized_sql);
      normalized_sql = NULL;
    }
    if (strstr(statement, "1969-12-31") != NULL) {
      if (NULL == normalized_sql) {
        normalized_sql = strdup(statement);
      }
      if (NULL == normalized_sql) {
        return QST_E_RESOURCE;
      }
      {
        char *timestamp = strstr(normalized_sql, "1969-12-31 23:59:59.999");
        if (NULL != timestamp) {
          memcpy(timestamp, "1970-01-01 00:00:00.000", 23);
        }
      }
      statement = normalized_sql;
    }
    code = report_native_error != 0 ?
               qst_native_execute(client, statement) :
               qst_native_execute_silent(client, statement, out_native_code);
    free(normalized_sql);
    if (code != QST_OK) {
      if (report_native_error != 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: dataset setup statement index=%u code=%d\n",
                      index, code);
      }
      return code;
    }
    code = qstDatasetHeartbeat(mapping, generation);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: dataset heartbeat after setup index=%u code=%d\n",
                    index, code);
      return code;
    }
  }
  return QST_OK;
}

static EQstCode qstDatasetVerifyCount(SQstShmMapping *mapping,
                                      uint32_t generation,
                                      SQstNativeClient *client,
                                      const char *sql, int64_t expected_count) {
  int64_t actual_count = 0;
  EQstCode code = qstDatasetHeartbeat(mapping, generation);

  if (code == QST_OK) {
    code = qst_native_count(client, sql, &actual_count);
  }

  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset count verification code=%d expected=%lld actual=%lld\n",
                  code, (long long)expected_count, (long long)actual_count);
    return code;
  }
  code = actual_count == expected_count ? QST_OK : QST_E_RESOURCE;
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset count mismatch expected=%lld actual=%lld\n",
                  (long long)expected_count, (long long)actual_count);
  }
  if (code == QST_OK) {
    code = qstDatasetHeartbeat(mapping, generation);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: dataset heartbeat after count code=%d\n",
                    code);
    }
  }
  return code;
}

EQstCode qst_dataset_materialize_local_fixture(SQstNativeClient *client) {
  static const char *const setup_sql[] = {
      "DROP DATABASE IF EXISTS db_frag",
      "CREATE DATABASE db_frag PRECISION 'ms'",
      "USE db_frag",
      "CREATE TABLE fq_local_series (ts TIMESTAMP, device_id INT, reading DOUBLE, active BOOL, region VARCHAR(16), note NCHAR(64), jdoc NCHAR(256), payload VARBINARY(8))",
      "CREATE TABLE fq_local_device (ts TIMESTAMP, device_id INT, device_name VARCHAR(32), tier VARCHAR(16))",
      "CREATE TABLE fq_local_series_empty (ts TIMESTAMP, device_id INT, reading DOUBLE)",
      "CREATE TABLE fq_local_precision (ts TIMESTAMP, local_note NCHAR(16))",
      "CREATE STABLE frag_stb_all_types (ts TIMESTAMP,c_bool BOOL,c_tiny TINYINT,c_small SMALLINT,c_int INT,c_big BIGINT,c_utiny TINYINT UNSIGNED,c_usmall SMALLINT UNSIGNED,c_uint INT UNSIGNED,c_ubig BIGINT UNSIGNED,c_float FLOAT,c_double DOUBLE,c_binary BINARY(8),c_nchar NCHAR(8),c_varchar VARCHAR(8),c_varbinary VARBINARY(64),c_geometry GEOMETRY(64),c_decimal_10_2 DECIMAL(10,2),c_decimal_20_10 DECIMAL(20,10),c_mediumblob VARBINARY(64),c_blob VARBINARY(64),tag_nchar NCHAR(8),tag_binary BINARY(8)) TAGS (jdoc JSON)",
      "CREATE TABLE frag_all_types USING frag_stb_all_types TAGS ('{\"k\":1}')",
      "CREATE TABLE frag_plain (ts TIMESTAMP,c_int INT,c_varchar VARCHAR(8))",
      "INSERT INTO fq_local_series VALUES ('2026-01-01 00:00:00.123',1001,10.5,true,'north','alpha','{\"k\":\"v\",\"n\":1}','0102') ('2026-01-01 00:00:01.000',1001,11.0,false,'north',NULL,'{\"k\":\"w\",\"n\":2}','0304') ('2026-01-01 00:00:02.000',1002,20.25,true,'south','\xE4\xB8\xAD\xE6\x96\x87','{\"k\":\"\xE4\xB8\xAD\",\"n\":3}','0506') ('2026-01-01 00:00:03.000',1003,-1.5,true,'east','%_\\\\\\'','{\"k\":\"x\",\"n\":4}','0708')",
      "INSERT INTO fq_local_device VALUES ('2026-01-01 00:00:00.123',1001,'meter-a','gold') ('2026-01-01 00:00:01.000',1001,'meter-a','gold') ('2026-01-01 00:00:02.000',1002,'meter-b','silver') ('2026-01-01 00:00:03.000',1003,'meter-c','bronze')",
      "INSERT INTO fq_local_precision VALUES ('2026-01-01 00:00:10.123','ms') ('2026-01-01 00:00:10.000','zero')",
      "INSERT INTO frag_all_types VALUES ('1970-01-01 00:00:00.000',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) ('2026-01-01 00:00:00.000',true,0,0,0,0,0,0,0,0,0.0,0.0,'a','\xE4\xB8\xAD','text',X'01','POINT(0 0)',0.00,0.0000000000,X'01',X'01') ('2026-01-01 00:00:01.000',true,1,100,-1,100,1,100,1,100,1.5,1.5,'bin','\xE4\xB8\xAD\xE6\x96\x87','value',X'0102','POINT(1 1)',10.99,10.9900000000,X'0102',X'0102') ('2026-01-01 00:00:02.000',false,127,32767,2147483647,9223372036854775807,255,65535,4294967295,18446744073709551615,3.4e38,1.7e308,'12345678','\xE4\xB8\xAD\xE6\x96\x87','12345678',X'0102030405060708','POINT(2 2)',99999999.99,9999999999.9999999999,X'01020304',X'01020304')",
      "INSERT INTO frag_plain VALUES ('2026-01-01 00:00:00.000',0,'plain') ('2026-01-01 00:00:01.000',1,'second') ('2026-01-01 00:00:02.000',-1,'third') ('2026-01-01 00:00:03.000',2147483647,'max')",
  };
  static const char *const count_sql[] = {
      "SELECT COUNT(*) FROM db_frag.fq_local_series",
      "SELECT COUNT(*) FROM db_frag.fq_local_device",
      "SELECT COUNT(*) FROM db_frag.fq_local_series_empty",
      "SELECT COUNT(*) FROM db_frag.fq_local_precision",
      "SELECT COUNT(*) FROM db_frag.frag_all_types",
      "SELECT COUNT(*) FROM db_frag.frag_plain",
  };
  static const int64_t expected_count[] = {4, 4, 0, 2, 4, 4};
  EQstCode code = QST_OK;

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstDatasetExecuteAll(NULL, 0, client, setup_sql,
                              sizeof(setup_sql) / sizeof(setup_sql[0]), 1,
                              NULL);
  for (uint32_t index = 0;
       code == QST_OK && index < sizeof(count_sql) / sizeof(count_sql[0]);
       ++index) {
    code = qstDatasetVerifyCount(NULL, 0, client, count_sql[index],
                                 expected_count[index]);
  }
  return code;
}

EQstCode qst_dataset_materialize_local_fixture_silent(
    SQstNativeClient *client, int32_t *out_native_code) {
  static const char *const setup_sql[] = {
      "DROP DATABASE IF EXISTS db_frag",
      "CREATE DATABASE db_frag PRECISION 'ms'",
      "USE db_frag",
      "CREATE TABLE fq_local_series (ts TIMESTAMP, device_id INT, reading DOUBLE, active BOOL, region VARCHAR(16), note NCHAR(64), jdoc NCHAR(256), payload VARBINARY(8))",
      "CREATE TABLE fq_local_device (ts TIMESTAMP, device_id INT, device_name VARCHAR(32), tier VARCHAR(16))",
      "CREATE TABLE fq_local_series_empty (ts TIMESTAMP, device_id INT, reading DOUBLE)",
      "CREATE TABLE fq_local_precision (ts TIMESTAMP, local_note NCHAR(16))",
      "CREATE STABLE frag_stb_all_types (ts TIMESTAMP,c_bool BOOL,c_tiny TINYINT,c_small SMALLINT,c_int INT,c_big BIGINT,c_utiny TINYINT UNSIGNED,c_usmall SMALLINT UNSIGNED,c_uint INT UNSIGNED,c_ubig BIGINT UNSIGNED,c_float FLOAT,c_double DOUBLE,c_binary BINARY(8),c_nchar NCHAR(8),c_varchar VARCHAR(8),c_varbinary VARBINARY(64),c_geometry GEOMETRY(64),c_decimal_10_2 DECIMAL(10,2),c_decimal_20_10 DECIMAL(20,10),c_mediumblob VARBINARY(64),c_blob VARBINARY(64),tag_nchar NCHAR(8),tag_binary BINARY(8)) TAGS (jdoc JSON)",
      "CREATE TABLE frag_all_types USING frag_stb_all_types TAGS ('{\"k\":1}')",
      "CREATE TABLE frag_plain (ts TIMESTAMP,c_int INT,c_varchar VARCHAR(8))",
      "INSERT INTO fq_local_series VALUES ('2026-01-01 00:00:00.123',1001,10.5,true,'north','alpha','{\"k\":\"v\",\"n\":1}','0102') ('2026-01-01 00:00:01.000',1001,11.0,false,'north',NULL,'{\"k\":\"w\",\"n\":2}','0304') ('2026-01-01 00:00:02.000',1002,20.25,true,'south','\xE4\xB8\xAD\xE6\x96\x87','{\"k\":\"\xE4\xB8\xAD\",\"n\":3}','0506') ('2026-01-01 00:00:03.000',1003,-1.5,true,'east','%_\\\\\\'','{\"k\":\"x\",\"n\":4}','0708')",
      "INSERT INTO fq_local_device VALUES ('2026-01-01 00:00:00.123',1001,'meter-a','gold') ('2026-01-01 00:00:01.000',1001,'meter-a','gold') ('2026-01-01 00:00:02.000',1002,'meter-b','silver') ('2026-01-01 00:00:03.000',1003,'meter-c','bronze')",
      "INSERT INTO fq_local_precision VALUES ('2026-01-01 00:00:10.123','ms') ('2026-01-01 00:00:10.000','zero')",
      "INSERT INTO frag_all_types VALUES ('1970-01-01 00:00:00.000',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) ('2026-01-01 00:00:00.000',true,0,0,0,0,0,0,0,0,0.0,0.0,'a','\xE4\xB8\xAD','text',X'01','POINT(0 0)',0.00,0.0000000000,X'01',X'01') ('2026-01-01 00:00:01.000',true,1,100,-1,100,1,100,1,100,1.5,1.5,'bin','\xE4\xB8\xAD\xE6\x96\x87','value',X'0102','POINT(1 1)',10.99,10.9900000000,X'0102',X'0102') ('2026-01-01 00:00:02.000',false,127,32767,2147483647,9223372036854775807,255,65535,4294967295,18446744073709551615,3.4e38,1.7e308,'12345678','\xE4\xB8\xAD\xE6\x96\x87','12345678',X'0102030405060708','POINT(2 2)',99999999.99,9999999999.9999999999,X'01020304',X'01020304')",
      "INSERT INTO frag_plain VALUES ('2026-01-01 00:00:00.000',0,'plain') ('2026-01-01 00:00:01.000',1,'second') ('2026-01-01 00:00:02.000',-1,'third') ('2026-01-01 00:00:03.000',2147483647,'max')",
  };
  static const char *const count_sql[] = {
      "SELECT COUNT(*) FROM db_frag.fq_local_series",
      "SELECT COUNT(*) FROM db_frag.fq_local_device",
      "SELECT COUNT(*) FROM db_frag.fq_local_series_empty",
      "SELECT COUNT(*) FROM db_frag.fq_local_precision",
      "SELECT COUNT(*) FROM db_frag.frag_all_types",
      "SELECT COUNT(*) FROM db_frag.frag_plain",
  };
  static const int64_t expected_count[] = {4, 4, 0, 2, 4, 4};
  EQstCode code = QST_OK;

  if (NULL == client || NULL == out_native_code) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstDatasetExecuteAll(NULL, 0, client, setup_sql,
                              sizeof(setup_sql) / sizeof(setup_sql[0]), 0,
                              out_native_code);
  for (uint32_t index = 0;
       code == QST_OK && index < sizeof(count_sql) / sizeof(count_sql[0]);
       ++index) {
    code = qstDatasetVerifyCount(NULL, 0, client, count_sql[index],
                                 expected_count[index]);
  }
  return code;
}

static void qstDatasetReleaseRefs(SQstShmMapping *mapping, SQstRef *refs,
                                  uint32_t ref_count) {
  for (uint32_t index = 0; index < ref_count; ++index) {
    qstReleaseRunRef(mapping, &refs[index]);
  }
}

static EQstCode qstDatasetReleaseObjectRefs(SQstShmMapping *mapping,
                                            SQstRef directory_ref) {
  SQstManifestHeader header = {0};

  if (qstGetManifest(mapping, directory_ref, sizeof(SQstObjectDescriptor),
                     kQstObjectMagic, &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    SQstObjectDescriptor entry = {0};
    EQstCode code = qstReadObjectEntry(mapping, directory_ref, index, &entry);

    if (code != QST_OK) {
      return code;
    }
    qstReleaseRunRef(mapping, &entry.qualified_name_ref);
    qstReleaseRunRef(mapping, &entry.schema_ref);
  }
  return QST_OK;
}

static EQstCode qstDatasetReleaseFixtureRefs(SQstShmMapping *mapping,
                                             SQstRef fixture_ref) {
  SQstManifestHeader header = {0};

  if (qstGetManifest(mapping, fixture_ref, sizeof(SQstFixtureDescriptor),
                     kQstFixtureMagic, &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    SQstFixtureDescriptor entry = {0};

    if (qst_block_copy(mapping, fixture_ref, 0,
                       sizeof(SQstManifestHeader) +
                           (uint64_t)index * sizeof(entry),
                       &entry, sizeof(entry)) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    qstReleaseRunRef(mapping, &entry.verification_ref);
  }
  return QST_OK;
}

EQstCode qst_dataset_recover_stale_generation(SQstShmMapping *mapping,
                                              uint32_t generation) {
  SQstGenerationState *state = NULL;
  uint32_t reclaimed_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_block_reclaim_run_owned(mapping, generation, &reclaimed_count);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: stale dataset block recovery failed "
                  "generation=%u code=%d\n",
                  generation, code);
    return code;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK) {
    return code;
  }
  /* The state may contain a partially published snapshot when the owner was
   * terminated between block sealing and the release-store of dataset_state.
   * The block scan above is authoritative once no older generation remains. */
  memset(&state->dataset_digest, 0, sizeof(state->dataset_digest));
  memset(&state->fixture_manifest_digest, 0,
         sizeof(state->fixture_manifest_digest));
  memset(&state->object_directory_ref, 0, sizeof(state->object_directory_ref));
  memset(&state->fixture_manifest_ref, 0, sizeof(state->fixture_manifest_ref));
  memset(&state->dataset_snapshot_sha256, 0,
         sizeof(state->dataset_snapshot_sha256));
  atomic_store_explicit(&state->dataset_state, 0, memory_order_release);
  (void)reclaimed_count;
  return QST_OK;
}

EQstCode qst_dataset_prepare_for_generation(SQstShmMapping *mapping,
                                            const SQstRunConfig *config,
                                            uint32_t generation,
                                            SQstDatasetInfo *out_dataset) {
  static const char *const setup_sql[] = {
      "DROP DATABASE IF EXISTS db_frag",
      "CREATE DATABASE db_frag PRECISION 'ms'",
      "USE db_frag",
      "CREATE TABLE fq_local_series (ts TIMESTAMP, device_id INT, reading DOUBLE, active BOOL, region VARCHAR(16), note NCHAR(64), jdoc NCHAR(256), payload VARBINARY(8))",
      "CREATE TABLE fq_local_device (ts TIMESTAMP, device_id INT, device_name VARCHAR(32), tier VARCHAR(16))",
      "CREATE TABLE fq_local_series_empty (ts TIMESTAMP, device_id INT, reading DOUBLE)",
      "CREATE TABLE fq_local_precision (ts TIMESTAMP, local_note NCHAR(16))",
      "CREATE STABLE frag_stb_all_types (ts TIMESTAMP,c_bool BOOL,c_tiny TINYINT,c_small SMALLINT,c_int INT,c_big BIGINT,c_utiny TINYINT UNSIGNED,c_usmall SMALLINT UNSIGNED,c_uint INT UNSIGNED,c_ubig BIGINT UNSIGNED,c_float FLOAT,c_double DOUBLE,c_binary BINARY(8),c_nchar NCHAR(8),c_varchar VARCHAR(8),c_varbinary VARBINARY(64),c_geometry GEOMETRY(64),c_decimal_10_2 DECIMAL(10,2),c_decimal_20_10 DECIMAL(20,10),c_mediumblob VARBINARY(64),c_blob VARBINARY(64),tag_nchar NCHAR(8),tag_binary BINARY(8)) TAGS (jdoc JSON)",
      "CREATE TABLE frag_all_types USING frag_stb_all_types TAGS ('{\"k\":1}')",
      "CREATE TABLE frag_plain (ts TIMESTAMP,c_int INT,c_varchar VARCHAR(8))",
      "INSERT INTO fq_local_series VALUES ('2026-01-01 00:00:00.123',1001,10.5,true,'north','alpha','{\"k\":\"v\",\"n\":1}','0102') ('2026-01-01 00:00:01.000',1001,11.0,false,'north',NULL,'{\"k\":\"w\",\"n\":2}','0304') ('2026-01-01 00:00:02.000',1002,20.25,true,'south','\xE4\xB8\xAD\xE6\x96\x87','{\"k\":\"\xE4\xB8\xAD\",\"n\":3}','0506') ('2026-01-01 00:00:03.000',1003,-1.5,true,'east','%_\\\\\\\'','{\"k\":\"x\",\"n\":4}','0708')",
      "INSERT INTO fq_local_device VALUES ('2026-01-01 00:00:00.123',1001,'meter-a','gold') ('2026-01-01 00:00:01.000',1001,'meter-a','gold') ('2026-01-01 00:00:02.000',1002,'meter-b','silver') ('2026-01-01 00:00:03.000',1003,'meter-c','bronze')",
      "INSERT INTO fq_local_precision VALUES ('2026-01-01 00:00:10.123','ms') ('2026-01-01 00:00:10.000','zero')",
      "INSERT INTO frag_all_types VALUES ('1970-01-01 00:00:00.000',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) ('2026-01-01 00:00:00.000',true,0,0,0,0,0,0,0,0,0.0,0.0,'a','\xE4\xB8\xAD','text',X'01','POINT(0 0)',0.00,0.0000000000,X'01',X'01') ('2026-01-01 00:00:01.000',true,1,100,-1,100,1,100,1,100,1.5,1.5,'bin','\xE4\xB8\xAD\xE6\x96\x87','value',X'0102','POINT(1 1)',10.99,10.9900000000,X'0102',X'0102') ('2026-01-01 00:00:02.000',false,127,32767,2147483647,9223372036854775807,255,65535,4294967295,18446744073709551615,3.4e38,1.7e308,'12345678','\xE4\xB8\xAD\xE6\x96\x87','12345678',X'0102030405060708','POINT(2 2)',99999999.99,9999999999.9999999999,X'01020304',X'01020304')",
      "INSERT INTO frag_plain VALUES ('2026-01-01 00:00:00.000',0,'plain') ('2026-01-01 00:00:01.000',1,'second') ('2026-01-01 00:00:02.000',-1,'third') ('2026-01-01 00:00:03.000',2147483647,'max')",
  };
  static const char *const count_sql[] = {
      "SELECT COUNT(*) FROM db_frag.fq_local_series",
      "SELECT COUNT(*) FROM db_frag.fq_local_device",
      "SELECT COUNT(*) FROM db_frag.fq_local_series_empty",
      "SELECT COUNT(*) FROM db_frag.fq_local_precision",
      "SELECT COUNT(*) FROM db_frag.frag_all_types",
      "SELECT COUNT(*) FROM db_frag.frag_plain",
  };
  static const int64_t expected_count[] = {4, 4, 0, 2, 4, 4};
  static const char kSourceName[] = "local";
  static const char kSourceSchema[] = "provider=tdengine-native";
  static const char kDatabaseName[] = "db_frag";
  static const char kDatabaseSchema[] = "precision=ms";
  static const char kSeriesName[] = "db_frag.fq_local_series";
  static const char kSeriesSchema[] =
      "ts:timestamp,device_id:int,reading:double,active:bool,region:varchar,note:nchar,jdoc:nchar,payload:varbinary";
  static const char kDeviceName[] = "db_frag.fq_local_device";
  static const char kDeviceSchema[] =
      "ts:timestamp,device_id:int,device_name:varchar,tier:varchar";
  static const char kEmptyName[] = "db_frag.fq_local_series_empty";
  static const char kEmptySchema[] = "ts:timestamp,device_id:int,reading:double";
  static const char kPrecisionName[] = "db_frag.fq_local_precision";
  static const char kPrecisionSchema[] = "ts:timestamp,local_note:nchar";
  static const char kStableName[] = "db_frag.frag_stb_all_types";
  static const char kStableSchema[] =
      "ts:timestamp,c_bool:bool,c_tiny:tinyint,c_small:smallint,c_int:int,c_big:bigint,c_utiny:utinyint,c_usmall:usmallint,c_uint:uint,c_ubig:ubigint,c_float:float,c_double:double,c_binary:binary,c_nchar:nchar,c_varchar:varchar,c_varbinary:varbinary,c_geometry:geometry,c_decimal_10_2:decimal64,c_decimal_20_10:decimal,c_mediumblob:varbinary,c_blob:varbinary,tag_nchar:nchar,tag_binary:binary;tags=jdoc:json";
  static const char kAllTypesName[] = "db_frag.frag_all_types";
  static const char kAllTypesSchema[] = "using=frag_stb_all_types;rows=4";
  static const char kPlainName[] = "db_frag.frag_plain";
  static const char kPlainSchema[] = "ts:timestamp,c_int:int,c_varchar:varchar;rows=4";
  static const char kVerification[] =
      "rows=series:4,device:4,empty:0,precision:2;precision=ms";
  static const SQstDatasetColumn kLocalColumns[] = {
      {"ts", "type=TIMESTAMP", 3, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"device_id", "type=INT", 3, 0},
      {"reading", "type=DOUBLE", 3, 0},
      {"active", "type=BOOL", 3, 0},
      {"region", "type=VARCHAR(16)", 3, 0},
      {"note", "type=NCHAR(64)", 3, 0},
      {"jdoc", "type=NCHAR(256)", 3, 0},
      {"payload", "type=VARBINARY(8)", 3, 0},
      {"ts", "type=TIMESTAMP", 4, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"device_id", "type=INT", 4, 0},
      {"device_name", "type=VARCHAR(32)", 4, 0},
      {"tier", "type=VARCHAR(16)", 4, 0},
      {"ts", "type=TIMESTAMP", 5, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"device_id", "type=INT", 5, 0},
      {"reading", "type=DOUBLE", 5, 0},
      {"ts", "type=TIMESTAMP", 6, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"local_note", "type=NCHAR(16)", 6, 0},
      {"ts", "type=TIMESTAMP", 7, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"c_bool", "type=BOOL", 7, 0},
      {"c_tiny", "type=TINYINT", 7, 0},
      {"c_small", "type=SMALLINT", 7, 0},
      {"c_int", "type=INT", 7, 0},
      {"c_big", "type=BIGINT", 7, 0},
      {"c_utiny", "type=TINYINT UNSIGNED", 7, 0},
      {"c_usmall", "type=SMALLINT UNSIGNED", 7, 0},
      {"c_uint", "type=INT UNSIGNED", 7, 0},
      {"c_ubig", "type=BIGINT UNSIGNED", 7, 0},
      {"c_float", "type=FLOAT", 7, 0},
      {"c_double", "type=DOUBLE", 7, 0},
      {"c_binary", "type=BINARY(8)", 7, 0},
      {"c_nchar", "type=NCHAR(8)", 7, 0},
      {"c_varchar", "type=VARCHAR(8)", 7, 0},
      {"c_varbinary", "type=VARBINARY(8)", 7, 0},
      {"c_geometry", "type=GEOMETRY(64)", 7, 0},
      {"c_decimal_10_2", "type=DECIMAL(10,2)", 7, 0},
      {"c_decimal_20_10", "type=DECIMAL(20,10)", 7, 0},
      {"c_mediumblob", "type=VARBINARY(8)", 7, 0},
      {"c_blob", "type=VARBINARY(8)", 7, 0},
      {"tag_nchar", "type=NCHAR(8)", 7, 0},
      {"tag_binary", "type=BINARY(8)", 7, 0},
      {"jdoc", "type=JSON", 7, QST_OBJECT_FLAG_TAG},
      {"ts", "type=TIMESTAMP", 9, QST_OBJECT_FLAG_PRIMARY_TIMESTAMP},
      {"c_int", "type=INT", 9, 0},
      {"c_varchar", "type=VARCHAR(8)", 9, 0},
  };
  SQstNativeClient client = {0};
  SQstObjectDescriptor objects[QST_LOCAL_OBJECT_CAPACITY] = {{0}};
  SQstFixtureDescriptor fixture = {0};
  SQstDatasetInfo dataset = {0};
  SQstDigest256 directory_digest = {0};
  SQstRef refs[QST_LOCAL_REF_CAPACITY] = {{0}};
  uint32_t object_count = 0;
  uint32_t started_external_target_bits = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == config ||
      NULL == out_dataset || generation == 0 || config->run_id == 0 ||
      config->run_id != mapping->header->run_id) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_dataset_recover_stale_generation(mapping, generation);
  if (code != QST_OK) {
    return code;
  }
  code = qstDatasetCopyAssets(config);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset asset copy failed code=%d\n", code);
    return code;
  }
  code = qstDatasetValidateExternalMappings(config);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset mapping validation failed code=%d\n", code);
    return code;
  }
  if ((config->target_selection_bits &
       (QST_TARGET_MYSQL_BIT | QST_TARGET_PG_BIT | QST_TARGET_INFLUX_BIT)) != 0) {
    code = qst_provider_preflight(config);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: provider preflight failed code=%d\n", code);
      return code;
    }
  }
  code = qst_native_open(&client);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset native connector open failed code=%d\n", code);
    return code;
  }
  code = qstDatasetWriteReadiness(config, &client);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: TDengine readiness collection failed code=%d\n",
                  code);
    (void)qst_native_close(&client);
    return code;
  }
  code = qstDatasetExecuteAll(mapping, generation, &client, setup_sql,
                              sizeof(setup_sql) / sizeof(setup_sql[0]), 1,
                              NULL);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset external target preparation failed code=%d\n", code);
    (void)qst_native_execute(&client, "DROP DATABASE IF EXISTS db_frag");
    (void)qst_native_close(&client);
    return code;
  }
  for (uint32_t index = 0; index < sizeof(count_sql) / sizeof(count_sql[0]); ++index) {
    code = qstDatasetVerifyCount(mapping, generation, &client, count_sql[index],
                                 expected_count[index]);
    if (code != QST_OK) {
      (void)qst_native_execute(&client, "DROP DATABASE IF EXISTS db_frag");
      (void)qst_native_close(&client);
      return code;
    }
  }
  code = qstDatasetHeartbeat(mapping, generation);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: dataset heartbeat before count code=%d\n",
                  code);
  }
  if (code == QST_OK) {
    code = qstDatasetPrepareExternalTargets(
        config, &client, &started_external_target_bits);
  }
  if (code == QST_OK) {
    code = qstDatasetHeartbeat(mapping, generation);
  }
  if (code != QST_OK) {
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  code = qstSealRunBytes(mapping, generation, kSourceName,
                         sizeof(kSourceName) - 1, &refs[0]);
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kSourceSchema,
                           sizeof(kSourceSchema) - 1, &refs[1]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kDatabaseName,
                           sizeof(kDatabaseName) - 1, &refs[2]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kDatabaseSchema,
                           sizeof(kDatabaseSchema) - 1, &refs[3]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kSeriesName,
                           sizeof(kSeriesName) - 1, &refs[4]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kSeriesSchema,
                           sizeof(kSeriesSchema) - 1, &refs[5]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kDeviceName,
                           sizeof(kDeviceName) - 1, &refs[6]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kDeviceSchema,
                           sizeof(kDeviceSchema) - 1, &refs[7]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kEmptyName,
                           sizeof(kEmptyName) - 1, &refs[8]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kEmptySchema,
                           sizeof(kEmptySchema) - 1, &refs[9]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kPrecisionName,
                           sizeof(kPrecisionName) - 1, &refs[10]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kPrecisionSchema,
                           sizeof(kPrecisionSchema) - 1, &refs[11]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kStableName,
                           sizeof(kStableName) - 1,
                           &refs[12]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kStableSchema,
                           sizeof(kStableSchema) - 1,
                           &refs[13]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kAllTypesName,
                           sizeof(kAllTypesName) - 1,
                           &refs[14]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kAllTypesSchema,
                           sizeof(kAllTypesSchema) - 1, &refs[15]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kPlainName,
                           sizeof(kPlainName) - 1,
                           &refs[16]);
  }
  if (code == QST_OK) {
    code = qstSealRunBytes(mapping, generation, kPlainSchema,
                           sizeof(kPlainSchema) - 1,
                           &refs[17]);
  }
  if (code != QST_OK) {
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  objects[0].object_id = 1;
  objects[0].target = QST_TARGET_TD;
  objects[0].object_kind = QST_OBJECT_SOURCE;
  objects[0].qualified_name_ref = refs[0];
  objects[0].schema_ref = refs[1];
  objects[1].object_id = 2;
  objects[1].parent_object_id = 1;
  objects[1].target = QST_TARGET_TD;
  objects[1].object_kind = QST_OBJECT_DATABASE_OR_SCHEMA;
  objects[1].qualified_name_ref = refs[2];
  objects[1].schema_ref = refs[3];
  for (uint32_t index = 0; index < 7; ++index) {
    objects[index + 2].object_id = index + 3;
    objects[index + 2].parent_object_id = 2;
    objects[index + 2].target = QST_TARGET_TD;
    objects[index + 2].object_kind = QST_OBJECT_TABLE_OR_MEASUREMENT;
    objects[index + 2].ordinal = index;
    objects[index + 2].qualified_name_ref = refs[4 + index * 2];
    objects[index + 2].schema_ref = refs[5 + index * 2];
  }
  object_count = 9;
  for (uint32_t index = 0; index <
                           sizeof(kLocalColumns) / sizeof(kLocalColumns[0]);
       ++index) {
    const uint32_t ref_index = 18 + index * 2;
    uint32_t ordinal = 0;

    if (ref_index + 1 >= QST_LOCAL_REF_CAPACITY ||
        object_count >= QST_LOCAL_OBJECT_CAPACITY) {
      code = QST_E_RESOURCE;
      break;
    }
    code = qstSealRunBytes(mapping, generation, kLocalColumns[index].name,
                           strlen(kLocalColumns[index].name),
                           &refs[ref_index]);
    if (code != QST_OK) {
      break;
    }
    code = qstSealRunBytes(mapping, generation, kLocalColumns[index].schema,
                           strlen(kLocalColumns[index].schema),
                           &refs[ref_index + 1]);
    if (code != QST_OK) {
      break;
    }
    objects[object_count].object_id = object_count + 1;
    objects[object_count].parent_object_id = kLocalColumns[index].parent_object_id;
    objects[object_count].target = QST_TARGET_TD;
    objects[object_count].object_kind = QST_OBJECT_COLUMN_OR_TAG;
    for (uint32_t previous = 0; previous < index; ++previous) {
      if (kLocalColumns[previous].parent_object_id ==
          kLocalColumns[index].parent_object_id) {
        ++ordinal;
      }
    }
    objects[object_count].ordinal = ordinal;
    objects[object_count].flags = kLocalColumns[index].flags;
    objects[object_count].qualified_name_ref = refs[ref_index];
    objects[object_count].schema_ref = refs[ref_index + 1];
    ++object_count;
  }
  if (code != QST_OK) {
    qstDatasetReleaseRefs(mapping, refs,
                          sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  /* External descriptors are appended only after authenticated DDL and the
   * bounded projection checks above have succeeded. This prevents an object
   * directory from claiming readiness for a provider that was not reachable. */
  {
    static const struct {
      uint32_t bit;
      uint32_t target;
      const char *default_source;
      const char *explicit_source;
      const char *namespace_name;
      const char *namespace_schema;
      const char *table_name;
      const char *table_schema;
      const char *first_column;
    } external_specs[] = {
        {QST_TARGET_MYSQL_BIT, QST_TARGET_MYSQL, "src_fq_mysql_default",
         "src_fq_mysql_explicit", "fq_default", "type=DATABASE",
         "fq_series", "kind=table", "ts"},
        {QST_TARGET_PG_BIT, QST_TARGET_PG, "src_fq_pg_default",
         "src_fq_pg_explicit", "fq_default", "type=SCHEMA", "fq_series",
         "kind=table", "ts"},
        {QST_TARGET_INFLUX_BIT, QST_TARGET_INFLUX, "src_fq_influx_default",
         "src_fq_influx_explicit", "fq_default", "type=DATABASE",
         "fq_series", "kind=measurement", "time"},
    };
    uint32_t ref_index = 18U +
                         2U * (uint32_t)(sizeof(kLocalColumns) /
                                         sizeof(kLocalColumns[0]));
    for (uint32_t spec_index = 0;
         spec_index < sizeof(external_specs) / sizeof(external_specs[0]);
         ++spec_index) {
      const uint32_t bit = external_specs[spec_index].bit;
      const char *sources[2] = {external_specs[spec_index].default_source,
                                external_specs[spec_index].explicit_source};
      if ((config->target_selection_bits & bit) == 0) {
        continue;
      }
      for (uint32_t source_index = 0; source_index < 2U; ++source_index) {
        uint64_t source_id = 0;
        uint64_t namespace_id = 0;
        uint64_t table_id = 0;
        const char *second_column = "device_id";
        code = qstDatasetAppendObject(
            mapping, objects, &object_count, QST_LOCAL_OBJECT_CAPACITY, refs,
            &ref_index, QST_LOCAL_REF_CAPACITY, generation,
            external_specs[spec_index].target,
            QST_OBJECT_SOURCE, 0, source_index, 0, sources[source_index],
            external_specs[spec_index].target == QST_TARGET_MYSQL ?
                "provider=mysql" :
            (external_specs[spec_index].target == QST_TARGET_PG ?
                 "provider=postgresql" : "provider=influxdb"));
        if (code != QST_OK) break;
        source_id = objects[object_count - 1U].object_id;
        code = qstDatasetAppendObject(
            mapping, objects, &object_count, QST_LOCAL_OBJECT_CAPACITY, refs,
            &ref_index, QST_LOCAL_REF_CAPACITY, generation,
            external_specs[spec_index].target,
            QST_OBJECT_DATABASE_OR_SCHEMA, source_id, 0, 0,
            external_specs[spec_index].namespace_name,
            external_specs[spec_index].namespace_schema);
        if (code != QST_OK) break;
        namespace_id = objects[object_count - 1U].object_id;
        code = qstDatasetAppendObject(
            mapping, objects, &object_count, QST_LOCAL_OBJECT_CAPACITY, refs,
            &ref_index, QST_LOCAL_REF_CAPACITY, generation,
            external_specs[spec_index].target,
            QST_OBJECT_TABLE_OR_MEASUREMENT, namespace_id, 0, 0,
            external_specs[spec_index].table_name,
            external_specs[spec_index].table_schema);
        if (code != QST_OK) break;
        table_id = objects[object_count - 1U].object_id;
        code = qstDatasetAppendObject(
            mapping, objects, &object_count, QST_LOCAL_OBJECT_CAPACITY, refs,
            &ref_index, QST_LOCAL_REF_CAPACITY, generation,
            external_specs[spec_index].target,
            QST_OBJECT_COLUMN_OR_TAG, table_id, 0,
            QST_OBJECT_FLAG_PRIMARY_TIMESTAMP,
            external_specs[spec_index].first_column, "type=TIMESTAMP");
        if (code == QST_OK) {
          code = qstDatasetAppendObject(
              mapping, objects, &object_count, QST_LOCAL_OBJECT_CAPACITY, refs,
              &ref_index, QST_LOCAL_REF_CAPACITY, generation,
              external_specs[spec_index].target,
              QST_OBJECT_COLUMN_OR_TAG, table_id, 1, 0, second_column,
              "type=INT");
        }
        if (code != QST_OK) break;
      }
      if (code != QST_OK) break;
    }
  }
  if (code != QST_OK) {
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  for (uint32_t index = 0; index < object_count; ++index) {
    qstObjectDescriptorDigest(&objects[index], &objects[index].descriptor_sha256);
  }
  code = qstSealManifest(mapping, generation, kQstObjectMagic, objects,
                         sizeof(objects[0]),
                         object_count,
                         &dataset.object_directory_ref, &directory_digest);
  if (code != QST_OK) {
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  fixture.registry_id = 1;
  fixture.target = QST_TARGET_TD;
  fixture.state = QST_FIXTURE_READY;
  fixture.object_ref = dataset.object_directory_ref;
  fixture.expected_schema_ref = refs[5];
  code = qstSealRunBytes(mapping, generation, kVerification,
                         sizeof(kVerification) - 1,
                         &fixture.verification_ref);
  if (code == QST_OK) {
    qstFixtureDescriptorDigest(&fixture, &fixture.fixture_sha256);
  }
  if (code != QST_OK) {
    qstReleaseRunRef(mapping, &dataset.object_directory_ref);
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  code = qstSealManifest(mapping, generation, kQstFixtureMagic, &fixture,
                         sizeof(fixture), 1,
                         &dataset.fixture_manifest_ref,
                         &dataset.fixture_manifest_digest);
  if (code != QST_OK) {
    qstReleaseRunRef(mapping, &dataset.object_directory_ref);
    qstReleaseRunRef(mapping, &fixture.verification_ref);
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    qstDatasetRollbackPreparedTargets(&client, config,
                                      started_external_target_bits);
    (void)qst_native_close(&client);
    return code;
  }
  code = qst_native_close(&client);
  if (code != QST_OK) {
    qstDatasetRollbackAfterCloseFailure(config, started_external_target_bits);
    qstReleaseRunRef(mapping, &dataset.fixture_manifest_ref);
    qstReleaseRunRef(mapping, &fixture.verification_ref);
    qstReleaseRunRef(mapping, &dataset.object_directory_ref);
    qstDatasetReleaseRefs(mapping, refs, sizeof(refs) / sizeof(refs[0]));
    return code;
  }
  qstDatasetFingerprint(config, &directory_digest, &dataset.fixture_manifest_digest,
                        &dataset.ds_fingerprint);
  dataset.run_id = config->run_id;
  *out_dataset = dataset;
  return QST_OK;
}

EQstCode qst_dataset_prepare(SQstShmMapping *mapping,
                             const SQstRunConfig *config,
                             SQstDatasetInfo *out_dataset) {
  uint32_t generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == config ||
      NULL == out_dataset || config->run_id == 0 ||
      config->run_id != mapping->header->run_id) {
    return QST_E_INVALID_ARGUMENT;
  }
  /* Preserve the public API's asset-preparation contract even when the caller
   * is not a registered main owner; the owner check applies to native setup. */
  code = qstDatasetCopyAssets(config);
  if (code == QST_OK) {
    code = qstDatasetValidateExternalMappings(config);
  }
  if (code != QST_OK) {
    return code;
  }
  code = qst_process_find_generation(mapping, QST_ROLE_MAIN,
                                     (int32_t)getpid(), &generation);
  if (code != QST_OK || generation == 0) {
    return code == QST_E_STALE_OWNER ? QST_E_INVALID_ARGUMENT :
                                       (code != QST_OK ? code : QST_E_STALE_OWNER);
  }
  return qst_dataset_prepare_for_generation(mapping, config, generation,
                                            out_dataset);
}

EQstCode qst_dataset_cleanup(SQstShmMapping *mapping,
                             const SQstRunConfig *config,
                             SQstDatasetInfo *inout_dataset) {
  SQstNativeClient client = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == config || NULL == inout_dataset ||
      inout_dataset->run_id == 0 || inout_dataset->run_id != config->run_id ||
      qstGetManifest(mapping, inout_dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &(SQstManifestHeader){0}) != QST_OK ||
      qstGetManifest(mapping, inout_dataset->fixture_manifest_ref,
                     sizeof(SQstFixtureDescriptor), kQstFixtureMagic,
                     &(SQstManifestHeader){0}) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  {
    SQstManifestHeader object_header = {0};
    SQstManifestHeader fixture_header = {0};
    code = qstGetManifest(mapping, inout_dataset->object_directory_ref,
                          sizeof(SQstObjectDescriptor), kQstObjectMagic,
                          &object_header);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset cleanup object manifest invalid "
                    "run_id=%llu code=%d ref_offset=%llu ref_length=%u\n",
                    (unsigned long long)inout_dataset->run_id, code,
                    (unsigned long long)inout_dataset->object_directory_ref.offset,
                    inout_dataset->object_directory_ref.length);
      return QST_E_SHM_CORRUPT;
    }
    code = qstGetManifest(mapping, inout_dataset->fixture_manifest_ref,
                          sizeof(SQstFixtureDescriptor), kQstFixtureMagic,
                          &fixture_header);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset cleanup fixture manifest invalid "
                    "run_id=%llu code=%d ref_offset=%llu ref_length=%u\n",
                    (unsigned long long)inout_dataset->run_id, code,
                    (unsigned long long)inout_dataset->fixture_manifest_ref.offset,
                    inout_dataset->fixture_manifest_ref.length);
      return QST_E_SHM_CORRUPT;
    }
    code = qstValidateObjectDirectory(mapping, inout_dataset->object_directory_ref,
                                      &object_header);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset cleanup object directory invalid "
                    "run_id=%llu code=%d entries=%u\n",
                    (unsigned long long)inout_dataset->run_id, code,
                    object_header.entry_count);
      return QST_E_SHM_CORRUPT;
    }
    code = qstValidateReadyFixture(mapping, inout_dataset->object_directory_ref,
                                   inout_dataset->fixture_manifest_ref,
                                   &fixture_header, QST_TARGET_TD);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: dataset cleanup fixture invalid "
                    "run_id=%llu code=%d entries=%u\n",
                    (unsigned long long)inout_dataset->run_id, code,
                    fixture_header.entry_count);
      return QST_E_SHM_CORRUPT;
    }
  }
  code = qst_native_open(&client);
  if (code != QST_OK) {
    return code;
  }
  code = qstDatasetCleanupOwnedExternalTargets(
      &client, config, config->target_selection_bits &
                           (QST_TARGET_MYSQL_BIT | QST_TARGET_PG_BIT |
                            QST_TARGET_INFLUX_BIT));
  {
    EQstCode database_code = qst_native_execute(&client,
                                                 "DROP DATABASE IF EXISTS db_frag");
    if (code == QST_OK) {
      code = database_code;
    }
  }
  if (qst_native_close(&client) != QST_OK && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (code != QST_OK) {
    return code;
  }
  if (qstDatasetReleaseFixtureRefs(mapping, inout_dataset->fixture_manifest_ref) !=
          QST_OK ||
      qstDatasetReleaseObjectRefs(mapping, inout_dataset->object_directory_ref) !=
          QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  qstReleaseRunRef(mapping, &inout_dataset->fixture_manifest_ref);
  qstReleaseRunRef(mapping, &inout_dataset->object_directory_ref);
  memset(inout_dataset, 0, sizeof(*inout_dataset));
  return QST_OK;
}

EQstCode qst_dataset_get_object(const SQstShmMapping *mapping,
                                const SQstDatasetInfo *dataset,
                                uint32_t target, uint32_t object_kind,
                                uint32_t object_index, SQstRef *out_object_ref) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_object_ref || target < QST_TARGET_TD ||
      target > QST_TARGET_INFLUX || object_kind < QST_OBJECT_SOURCE ||
      object_kind > QST_OBJECT_COLUMN_OR_TAG) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    SQstObjectDescriptor entry = {0};
    if (qstReadObjectEntry(mapping, dataset->object_directory_ref, index,
                           &entry) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    if (entry.target == target && entry.object_kind == object_kind &&
        entry.ordinal == object_index) {
      *out_object_ref = dataset->object_directory_ref;
      return QST_OK;
    }
  }
  return QST_E_INVALID_ARGUMENT;
}

EQstCode qst_dataset_get_object_by_id(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint64_t object_id,
                                      SQstObjectDescriptor *out_object) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_object || object_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    SQstObjectDescriptor entry = {0};

    if (qstReadObjectEntry(mapping, dataset->object_directory_ref, index,
                           &entry) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    if (entry.object_id == object_id) {
      *out_object = entry;
      return QST_OK;
    }
  }
  return QST_E_INVALID_ARGUMENT;
}

EQstCode qst_dataset_get_object_at(const SQstShmMapping *mapping,
                                   const SQstDatasetInfo *dataset,
                                   uint32_t directory_index,
                                   SQstObjectDescriptor *out_object) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_object) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  if (directory_index >= header.entry_count ||
      qstReadObjectEntry(mapping, dataset->object_directory_ref,
                         directory_index, out_object) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  return QST_OK;
}

EQstCode qst_dataset_get_object_count(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint32_t *out_count) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  *out_count = header.entry_count;
  return QST_OK;
}

EQstCode qst_dataset_copy_object_directory(
    const SQstShmMapping *mapping, const SQstDatasetInfo *dataset,
    SQstObjectDescriptor *out_objects, uint32_t object_capacity,
    uint32_t *out_count) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_objects || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  if (object_capacity < header.entry_count) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    if (qstReadObjectEntry(mapping, dataset->object_directory_ref, index,
                           &out_objects[index]) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
  }
  *out_count = header.entry_count;
  return QST_OK;
}

EQstCode qst_dataset_get_child_object(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint32_t target,
                                      uint64_t parent_object_id,
                                      uint32_t object_kind,
                                      uint32_t object_index,
                                      SQstObjectDescriptor *out_object) {
  SQstManifestHeader header = {0};

  if (NULL == dataset || NULL == out_object || parent_object_id == 0 ||
      target < QST_TARGET_TD || target > QST_TARGET_INFLUX ||
      object_kind < QST_OBJECT_DATABASE_OR_SCHEMA ||
      object_kind > QST_OBJECT_COLUMN_OR_TAG) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &header) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &header) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < header.entry_count; ++index) {
    SQstObjectDescriptor entry = {0};

    if (qstReadObjectEntry(mapping, dataset->object_directory_ref, index,
                           &entry) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    if (entry.target == target && entry.parent_object_id == parent_object_id &&
        entry.object_kind == object_kind && entry.ordinal == object_index) {
      *out_object = entry;
      return QST_OK;
    }
  }
  return QST_E_INVALID_ARGUMENT;
}

EQstCode qst_dataset_copy_object_name(const SQstShmMapping *mapping,
                                      const SQstObjectDescriptor *object,
                                      char *out_name, uint64_t out_bytes) {
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == object || NULL == out_name || out_bytes == 0 ||
      qstRefIsEmpty(object->qualified_name_ref) ||
      object->qualified_name_ref.length >= out_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_block_copy(mapping, object->qualified_name_ref, 0, 0, out_name,
                        object->qualified_name_ref.length);
  if (code != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  out_name[object->qualified_name_ref.length] = '\0';
  return QST_OK;
}

EQstCode qst_dataset_validate_binding(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint64_t gid, uint32_t target,
                                      SQstRef binding_ref) {
  SQstManifestHeader objects = {0};
  SQstManifestHeader fixtures = {0};

  if (NULL == dataset || gid == 0 || target < QST_TARGET_TD ||
      target > QST_TARGET_INFLUX || qstRefIsEmpty(binding_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstGetManifest(mapping, dataset->object_directory_ref,
                     sizeof(SQstObjectDescriptor), kQstObjectMagic,
                     &objects) != QST_OK ||
      qstGetManifest(mapping, dataset->fixture_manifest_ref,
                     sizeof(SQstFixtureDescriptor), kQstFixtureMagic,
                     &fixtures) != QST_OK ||
      qstValidateObjectDirectory(mapping, dataset->object_directory_ref,
                                 &objects) != QST_OK ||
      qstValidateReadyFixture(mapping, dataset->object_directory_ref,
                              dataset->fixture_manifest_ref,
                              &fixtures, target) != QST_OK ||
      qst_block_validate(mapping, binding_ref, gid) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}
