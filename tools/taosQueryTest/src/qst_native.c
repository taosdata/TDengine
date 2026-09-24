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

/* Native TDengine C connector loader; no non-native protocol is permitted. */

#define _POSIX_C_SOURCE 200809L

#include "qst_native.h"

#include <errno.h>
#include <dlfcn.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "taos.h"
#include "toolscJson.h"

static EQstCode qstNativeResolve(void *library, const char *name, void *out_fn,
                                 uint64_t out_bytes) {
  void *symbol = NULL;

  if (NULL == library || NULL == name || NULL == out_fn ||
      out_bytes != sizeof(symbol)) {
    return QST_E_INVALID_ARGUMENT;
  }
  dlerror();
  symbol = dlsym(library, name);
  {
    const char *error = dlerror();

    if (NULL == symbol || NULL != error) {
      (void)fprintf(stderr,
                    "taosQueryTest: native client symbol resolve failed "
                    "symbol=%s error=%s\n",
                    name, NULL != error ? error : "not found");
      return QST_E_RESOURCE;
    }
  }
  memcpy(out_fn, &symbol, sizeof(symbol));
  return QST_OK;
}

/* Statement APIs are an optional acceleration. Older test doubles only
 * expose the query symbols, in which case callers retain their SQL fallback. */
static void qstNativeResolveOptional(void *library, const char *name,
                                     void *out_fn, uint64_t out_bytes) {
  void *symbol = NULL;

  if (NULL == library || NULL == name || NULL == out_fn ||
      out_bytes != sizeof(symbol)) {
    return;
  }
  dlerror();
  symbol = dlsym(library, name);
  if (NULL == symbol || NULL != dlerror()) {
    return;
  }
  memcpy(out_fn, &symbol, sizeof(symbol));
}

static EQstCode qstNativeEndpoint(uint16_t *out_port, const char **out_host) {
  const char *host = getenv("TAOS_HOST");
  const char *port_text = getenv("TAOS_PORT");
  char *end = NULL;
  unsigned long port = 0;

  if (NULL == out_port || NULL == out_host) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_port = 0;
  *out_host = NULL;
  if (NULL == host || host[0] == '\0') {
    return QST_OK;
  }
  if (NULL != port_text && port_text[0] != '\0') {
    errno = 0;
    port = strtoul(port_text, &end, 10);
    if (errno != 0 || end == port_text || *end != '\0' || port == 0 ||
        port > UINT16_MAX) {
      return QST_E_INVALID_ARGUMENT;
    }
    *out_port = (uint16_t)port;
  }
  *out_host = host;
  return QST_OK;
}

static EQstCode qstNativeConfigureClient(const SQstNativeClient *client,
                                         const char *config_dir) {
  if (NULL == config_dir || '\0' == config_dir[0]) {
    return QST_OK;
  }
  if (client == NULL || client->options == NULL) {
    return QST_E_RESOURCE;
  }
  /* External SQL is translated by the native client before it reaches taosd.
   * Load the target client config so federatedQueryEnable applies on both sides. */
  if (client->options((int32_t)TSDB_OPTION_CONFIGDIR, config_dir) != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: native client config directory rejected "
                  "path=%s\n",
                  config_dir);
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

EQstCode qst_native_open_with_endpoint_credentials(
    SQstNativeClient *out_client, const char *host, uint16_t port,
    const char *config_dir, const char *user, const char *password) {
  SQstNativeClient client = {0};
  EQstCode code = QST_OK;

  if (NULL == out_client) {
    return QST_E_INVALID_ARGUMENT;
  }
  client.library = dlopen("libtaos.so", RTLD_NOW | RTLD_LOCAL);
  if (NULL == client.library) {
    const char *error = dlerror();

    (void)fprintf(stderr,
                  "taosQueryTest: native client load failed library=libtaos.so "
                  "error=%s\n",
                  NULL != error ? error : "unknown");
    return QST_E_RESOURCE;
  }
  code = qstNativeResolve(client.library, "taos_connect", &client.connect,
                          sizeof(client.connect));
  if (code == QST_OK && NULL != config_dir && '\0' != config_dir[0]) {
    code = qstNativeResolve(client.library, "taos_options", &client.options,
                            sizeof(client.options));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_close", &client.close,
                            sizeof(client.close));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_query", &client.query,
                            sizeof(client.query));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_query_a", &client.query_a,
                            sizeof(client.query_a));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_fetch_rows_a",
                            &client.fetch_rows_a, sizeof(client.fetch_rows_a));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_kill_query",
                            &client.kill_query, sizeof(client.kill_query));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_errno", &client.errno_fn,
                            sizeof(client.errno_fn));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_errstr", &client.errstr,
                            sizeof(client.errstr));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_free_result", &client.free_result,
                            sizeof(client.free_result));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_fetch_row", &client.fetch_row,
                            sizeof(client.fetch_row));
  }
  if (code == QST_OK) {
    qstNativeResolveOptional(client.library, "taos_fetch_raw_block",
                             &client.fetch_raw_block,
                             sizeof(client.fetch_raw_block));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_num_fields", &client.num_fields,
                            sizeof(client.num_fields));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_fetch_fields",
                            &client.fetch_fields, sizeof(client.fetch_fields));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_fetch_lengths",
                            &client.fetch_lengths, sizeof(client.fetch_lengths));
  }
  if (code == QST_OK) {
    code = qstNativeResolve(client.library, "taos_result_precision",
                            &client.result_precision,
                            sizeof(client.result_precision));
  }
  if (code == QST_OK) {
    qstNativeResolveOptional(client.library, "taos_stmt_init", &client.stmt_init,
                             sizeof(client.stmt_init));
    qstNativeResolveOptional(client.library, "taos_stmt_prepare",
                             &client.stmt_prepare, sizeof(client.stmt_prepare));
    qstNativeResolveOptional(client.library, "taos_stmt_bind_param",
                             &client.stmt_bind_param,
                             sizeof(client.stmt_bind_param));
    qstNativeResolveOptional(client.library, "taos_stmt_add_batch",
                             &client.stmt_add_batch, sizeof(client.stmt_add_batch));
    qstNativeResolveOptional(client.library, "taos_stmt_execute",
                             &client.stmt_execute, sizeof(client.stmt_execute));
    qstNativeResolveOptional(client.library, "taos_stmt_close", &client.stmt_close,
                             sizeof(client.stmt_close));
    qstNativeResolveOptional(client.library, "taos_stmt_errstr", &client.stmt_errstr,
                             sizeof(client.stmt_errstr));
  }
  if (code != QST_OK) {
    (void)dlclose(client.library);
    return code;
  }
  code = qstNativeConfigureClient(&client, config_dir);
  if (code != QST_OK) {
    (void)dlclose(client.library);
    return code;
  }
  client.connection = client.connect(host, user, password, NULL, port);
  if (NULL == client.connection) {
    const char *message = client.errstr(NULL);
    int32_t native_code = client.errno_fn(NULL);

    (void)fprintf(stderr,
                  "taosQueryTest: native client connect failed host=%s port=%u "
                  "native_code=%d message=%s\n",
                  NULL != host ? host : "default", port, native_code,
                  NULL != message ? message : "unknown");
    (void)dlclose(client.library);
    return QST_E_RESOURCE;
  }
  *out_client = client;
  return QST_OK;
}

EQstCode qst_native_open_with_endpoint(SQstNativeClient *out_client,
                                       const char *host, uint16_t port,
                                       const char *config_dir) {
  return qst_native_open_with_endpoint_credentials(out_client, host, port,
                                                   config_dir, NULL, NULL);
}

EQstCode qst_native_open(SQstNativeClient *out_client) {
  const char *config_dir = getenv("TAOS_CONFIG_DIR");
  const char *host = NULL;
  uint16_t port = 0;
  EQstCode code = qstNativeEndpoint(&port, &host);

  if (code != QST_OK) {
    return code;
  }
  return qst_native_open_with_endpoint(out_client, host, port, config_dir);
}

static EQstCode qstNativeExecute(SQstNativeClient *client, const char *sql,
                                 int32_t *out_native_code,
                                 int32_t report_native_error) {
  void *result = NULL;
  int32_t code = 0;

  if (NULL == client || NULL == client->connection || NULL == client->query ||
      NULL == client->errno_fn || NULL == client->free_result || NULL == sql) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (out_native_code != NULL) {
    *out_native_code = 0;
  }
  result = client->query(client->connection, sql);
  if (NULL == result) {
    return QST_E_RESULT_STORE;
  }
  code = client->errno_fn(result);
  if (out_native_code != NULL) {
    *out_native_code = code;
  }
  if (code != 0 && report_native_error != 0 && NULL != client->errstr) {
    (void)fprintf(stderr, "taosQueryTest: native query error code=%d message=%s\n",
                  code, client->errstr(result));
  }
  client->free_result(result);
  return code == 0 ? QST_OK : QST_E_RESULT_STORE;
}

EQstCode qst_native_execute(SQstNativeClient *client, const char *sql) {
  return qstNativeExecute(client, sql, NULL, 1);
}

EQstCode qst_native_execute_silent(SQstNativeClient *client, const char *sql,
                                   int32_t *out_native_code) {
  return qstNativeExecute(client, sql, out_native_code, 0);
}

EQstCode qst_native_execute_stmt_batch(SQstNativeClient *client,
                                       const char *sql,
                                       void *row_binds,
                                       uint32_t row_count,
                                       uint32_t field_count,
                                       int32_t *out_execute_attempted) {
  void *statement = NULL;
  int32_t native_code = 0;
  EQstCode code = QST_E_RESOURCE;

  if (NULL == out_execute_attempted) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_execute_attempted = 0;
  if (NULL == client || NULL == client->connection || NULL == sql ||
      NULL == row_binds || row_count == 0 || field_count == 0 ||
      client->stmt_init == NULL || client->stmt_prepare == NULL ||
      client->stmt_bind_param == NULL || client->stmt_add_batch == NULL ||
      client->stmt_execute == NULL || client->stmt_close == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  statement = client->stmt_init(client->connection);
  if (NULL == statement) {
    return QST_E_RESOURCE;
  }
  native_code = client->stmt_prepare(statement, sql, (unsigned long)strlen(sql));
  if (native_code != 0) {
    goto _return;
  }
  for (uint32_t row = 0; row < row_count; ++row) {
    native_code = client->stmt_bind_param(
        statement, (TAOS_MULTI_BIND *)row_binds + (uint64_t)row * field_count);
    if (native_code != 0) {
      goto _return;
    }
    native_code = client->stmt_add_batch(statement);
    if (native_code != 0) {
      goto _return;
    }
  }
  *out_execute_attempted = 1;
  native_code = client->stmt_execute(statement);

_return:
  if (native_code != 0 && client->stmt_errstr != NULL) {
    const char *message = client->stmt_errstr(statement);

    (void)fprintf(stderr,
                  "taosQueryTest: native statement batch failed code=%d message=%s\n",
                  native_code, message != NULL ? message : "unknown");
  }
  if (client->stmt_close(statement) != 0 && native_code == 0) {
    native_code = -1;
  }
  code = native_code == 0 ? QST_OK : QST_E_RESULT_STORE;
  return code;
}

EQstCode qst_native_execute_async(SQstNativeClient *client, const char *sql,
                                  QstNativeAsyncCallback callback,
                                  void *callback_param) {
  if (NULL == client || NULL == client->connection || NULL == client->query_a ||
      NULL == sql || NULL == callback) {
    return QST_E_INVALID_ARGUMENT;
  }
  client->query_a(client->connection, sql, callback, callback_param);
  return QST_OK;
}

EQstCode qst_native_kill_query(SQstNativeClient *client) {
  if (NULL == client || NULL == client->connection || NULL == client->kill_query) {
    return QST_E_INVALID_ARGUMENT;
  }
  client->kill_query(client->connection);
  return QST_OK;
}

EQstCode qst_native_count(SQstNativeClient *client, const char *sql,
                          int64_t *out_count) {
  void *result = NULL;
  void **row = NULL;
  int32_t code = 0;

  if (NULL == client || NULL == client->connection || NULL == client->query ||
      NULL == client->errno_fn || NULL == client->free_result ||
      NULL == client->fetch_row || NULL == sql || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  result = client->query(client->connection, sql);
  if (NULL == result) {
    return QST_E_RESULT_STORE;
  }
  code = client->errno_fn(result);
  row = code == 0 ? client->fetch_row(result) : NULL;
  if (code != 0 || NULL == row || NULL == row[0]) {
    client->free_result(result);
    return QST_E_RESULT_STORE;
  }
  memcpy(out_count, row[0], sizeof(*out_count));
  client->free_result(result);
  return QST_OK;
}

EQstCode qst_native_query_result(SQstNativeClient *client, const char *sql,
                                 SQstQueryResult *out_result) {
  void *result = NULL;
  EQstCode code = QST_OK;

  if (NULL == client || NULL == client->connection || NULL == client->query ||
      NULL == client->free_result || NULL == sql || NULL == out_result) {
    return QST_E_INVALID_ARGUMENT;
  }
  result = client->query(client->connection, sql);
  if (NULL == result) {
    return QST_E_RESULT_STORE;
  }
  code = qst_native_capture_result(client, result, out_result);
  client->free_result(result);
  return code;
}

static EQstCode qstNativeReadCell(const SQstQueryResult *result,
                                  uint32_t row_index, uint32_t column_index,
                                  char *out_text, uint64_t out_bytes) {
  tools_cJSON *root = NULL;
  tools_cJSON *rows = NULL;
  tools_cJSON *row = NULL;
  tools_cJSON *cell = NULL;
  tools_cJSON *value = NULL;
  const char *text = NULL;
  char *text_copy = NULL;
  int32_t row_count = 0;
  EQstCode code = QST_OK;

  if (result == NULL || out_text == NULL || out_bytes == 0 ||
      result->data == NULL || result->bytes == 0 || result->bytes >= SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  text_copy = (char *)calloc((size_t)result->bytes + 1U, 1);
  if (text_copy == NULL) {
    return QST_E_RESOURCE;
  }
  memcpy(text_copy, result->data, (size_t)result->bytes);
  root = tools_cJSON_ParseWithOpts(text_copy, NULL, 1);
  free(text_copy);
  if (root == NULL || !tools_cJSON_IsObject(root)) {
    tools_cJSON_Delete(root);
    return QST_E_RESULT_STORE;
  }
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  row_count = tools_cJSON_GetArraySize(rows);
  row = row_count > (int32_t)row_index ? tools_cJSON_GetArrayItem(rows,
                                                                   (int32_t)row_index)
                                       : NULL;
  cell = row == NULL ? NULL : tools_cJSON_GetArrayItem(row, (int32_t)column_index);
  value = cell != NULL && tools_cJSON_IsObject(cell)
              ? tools_cJSON_GetObjectItemCaseSensitive(cell, "value")
              : cell;
  if (value == NULL || !tools_cJSON_IsString(value) || value->valuestring == NULL) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  text = value->valuestring;
  if (snprintf(out_text, (size_t)out_bytes, "%s", text) < 0 ||
      strlen(text) >= out_bytes) {
    code = QST_E_RESOURCE;
  }

_return:
  tools_cJSON_Delete(root);
  return code;
}

EQstCode qst_native_readiness_json(SQstNativeClient *client, char *out_json,
                                   uint64_t out_bytes) {
  SQstQueryResult version = {0};
  SQstQueryResult variable = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *capabilities = NULL;
  char server_version[256] = {0};
  char variable_name[128] = {0};
  char variable_value[128] = {0};
  char *printed = NULL;
  EQstCode version_code = QST_OK;
  EQstCode variable_code = QST_OK;
  EQstCode code = QST_OK;

  if (client == NULL || out_json == NULL || out_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  version_code = qst_native_query_result(client, "SELECT server_version()",
                                         &version);
  if (version_code == QST_OK) {
    version_code = qstNativeReadCell(&version, 0, 0, server_version,
                                     sizeof(server_version));
  }
  variable_code = qst_native_query_result(
      client, "SHOW VARIABLES LIKE 'federatedQueryEnable'", &variable);
  if (variable_code == QST_OK) {
    variable_code = qstNativeReadCell(&variable, 0, 0, variable_name,
                                      sizeof(variable_name));
    if (variable_code == QST_OK) {
      variable_code = qstNativeReadCell(&variable, 0, 1, variable_value,
                                        sizeof(variable_value));
    }
  }
  root = tools_cJSON_CreateObject();
  capabilities = tools_cJSON_CreateObject();
  if (root == NULL || capabilities == NULL) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  tools_cJSON_AddBoolToObject(root, "available",
                              version_code == QST_OK && variable_code == QST_OK);
  tools_cJSON_AddStringToObject(root, "server_version",
                                version_code == QST_OK ? server_version : "");
  tools_cJSON_AddStringToObject(root, "federated_query_enable",
                                variable_code == QST_OK ? variable_value : "");
  tools_cJSON_AddStringToObject(root, "external_source_ddl", "not_probed");
  tools_cJSON_AddStringToObject(root, "error",
                                version_code != QST_OK ? "server_version query failed" :
                                (variable_code != QST_OK ?
                                     "federatedQueryEnable query failed" : ""));
  tools_cJSON_AddBoolToObject(capabilities, "native_query", 1);
  tools_cJSON_AddBoolToObject(capabilities, "result_oracle_v1", 1);
  tools_cJSON_AddBoolToObject(capabilities, "external_source_ddl", 0);
  tools_cJSON_AddItemToObject(root, "capabilities", capabilities);
  capabilities = NULL;
  printed = tools_cJSON_PrintUnformatted(root);
  if (printed == NULL || strlen(printed) >= out_bytes) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  memcpy(out_json, printed, strlen(printed) + 1U);

_return:
  tools_cJSON_Delete(capabilities);
  tools_cJSON_Delete(root);
  tools_cJSON_free(printed);
  qst_result_reset(&version);
  qst_result_reset(&variable);
  return code;
}

EQstCode qst_native_close(SQstNativeClient *client) {
  EQstCode code = QST_OK;

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != client->connection && NULL != client->close) {
    client->close(client->connection);
  }
  if (NULL != client->library && dlclose(client->library) != 0) {
    code = QST_E_RESOURCE;
  }
  memset(client, 0, sizeof(*client));
  return code;
}
