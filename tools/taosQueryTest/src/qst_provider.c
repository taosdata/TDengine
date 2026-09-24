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

/* External-provider preflight with redacted, durable evidence. */

#define _POSIX_C_SOURCE 200809L

#include "qst_provider.h"

#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

enum {
  QST_PROVIDER_TD = 1U << 0,
  QST_PROVIDER_MYSQL = 1U << 1,
  QST_PROVIDER_PG = 1U << 2,
  QST_PROVIDER_INFLUX = 1U << 3,
  QST_PROVIDER_CAPTURE_BYTES = 4096,
  QST_PROVIDER_PATH_BYTES = 4096,
};

typedef struct {
  const char *name;
  uint32_t bit;
  const char *command;
  const char *client_override;
  const char *standard_client_pattern;
  const char *const *environment;
  uint32_t environment_count;
  const char *version_arg;
} SQstProviderSpec;

static const char *const kMysqlEnvironment[] = {
    "FQ_MYSQL_HOST", "FQ_MYSQL_PORT", "FQ_MYSQL_USER", "FQ_MYSQL_PASSWORD"};
static const char *const kPgEnvironment[] = {
    "FQ_PG_HOST", "FQ_PG_PORT", "FQ_PG_USER", "FQ_PG_PASSWORD"};
static const char *const kInfluxEnvironment[] = {
    "FQ_INFLUX_HOST", "FQ_INFLUX_PORT", "FQ_INFLUX_TOKEN"};

static const SQstProviderSpec kProviderSpecs[] = {
    {"mysql", QST_PROVIDER_MYSQL, "mysql", "QST_MYSQL_CLIENT",
     "/opt/taostest/fq/mysql/*/bin/mysql", kMysqlEnvironment,
     sizeof(kMysqlEnvironment) / sizeof(kMysqlEnvironment[0]), "--version"},
    {"pg", QST_PROVIDER_PG, "psql", "QST_PG_CLIENT", NULL, kPgEnvironment,
     sizeof(kPgEnvironment) / sizeof(kPgEnvironment[0]), "--version"},
    {"influx", QST_PROVIDER_INFLUX, "curl", "QST_INFLUX_CLIENT", NULL,
     kInfluxEnvironment,
     sizeof(kInfluxEnvironment) / sizeof(kInfluxEnvironment[0]), "--version"},
};

static int32_t qstProviderPathHasCommand(const SQstProviderSpec *spec) {
  const char *path = NULL;
  const char *cursor = NULL;

  if (spec == NULL || spec->command == NULL || strchr(spec->command, '/') != NULL) {
    return 0;
  }
  path = getenv("PATH");
  if (path == NULL || path[0] == '\0') return 0;
  cursor = path;
  while (*cursor != '\0') {
    const char *separator = strchr(cursor, ':');
    const size_t directory_bytes = separator == NULL ? strlen(cursor) :
        (size_t)(separator - cursor);
    char candidate[QST_PROVIDER_PATH_BYTES] = {0};
    int32_t written = snprintf(candidate, sizeof(candidate), "%.*s/%s",
                               (int)directory_bytes, cursor, spec->command);
    if (written > 0 && (uint64_t)written < sizeof(candidate) &&
        access(candidate, X_OK) == 0) {
      return 1;
    }
    if (separator == NULL) break;
    cursor = separator + 1;
  }
  return 0;
}

static const char *qstProviderCommandPath(const SQstProviderSpec *spec) {
  static char discovered[QST_PROVIDER_PATH_BYTES] = {0};
  const char *override = NULL;
  glob_t matches = {0};

  if (spec == NULL) return NULL;
  override = spec->client_override == NULL ? NULL : getenv(spec->client_override);
  if (override != NULL && override[0] != '\0') {
    return access(override, X_OK) == 0 ? override : NULL;
  }
  if (qstProviderPathHasCommand(spec)) return spec->command;
  if (spec->standard_client_pattern == NULL ||
      glob(spec->standard_client_pattern, 0, NULL, &matches) != 0) {
    return NULL;
  }
  for (size_t index = 0; index < matches.gl_pathc; ++index) {
    if (access(matches.gl_pathv[index], X_OK) == 0 &&
        snprintf(discovered, sizeof(discovered), "%s", matches.gl_pathv[index]) > 0 &&
        strlen(discovered) < sizeof(discovered)) {
      globfree(&matches);
      return discovered;
    }
  }
  globfree(&matches);
  return NULL;
}

static int32_t qstProviderWriteAll(int32_t fd, const char *bytes,
                                   uint64_t length) {
  uint64_t offset = 0;

  while (offset < length) {
    ssize_t written = write(fd, bytes + offset, (size_t)(length - offset));
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) return 0;
    offset += (uint64_t)written;
  }
  return 1;
}

static int32_t qstProviderReadText(const char *path, char *out, size_t out_bytes) {
  int32_t fd = -1;
  ssize_t bytes = 0;

  if (path == NULL || out == NULL || out_bytes < 2) return 0;
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) return 0;
  bytes = read(fd, out, out_bytes - 1);
  (void)close(fd);
  if (bytes < 0) return 0;
  out[bytes] = '\0';
  for (ssize_t index = bytes - 1; index >= 0; --index) {
    if (out[index] == '\n' || out[index] == '\r' || out[index] == ' ' ||
        out[index] == '\t') {
      out[index] = '\0';
    } else {
      break;
    }
  }
  return 1;
}

static int32_t qstProviderRun(const SQstProviderSpec *spec, const char *const *args,
                              const char *capture_path) {
  pid_t child = -1;
  int32_t status = 0;

  child = fork();
  if (child < 0) return 0;
  if (child == 0) {
    int32_t fd = -1;
    if (capture_path != NULL) {
      fd = open(capture_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
      if (fd < 0 || dup2(fd, STDOUT_FILENO) < 0 || dup2(fd, STDERR_FILENO) < 0) {
        _exit(126);
      }
      (void)close(fd);
    } else {
      fd = open("/dev/null", O_WRONLY | O_CLOEXEC);
      if (fd >= 0) {
        (void)dup2(fd, STDOUT_FILENO);
        (void)dup2(fd, STDERR_FILENO);
        (void)close(fd);
      }
    }
    const char *command_path = qstProviderCommandPath(spec);
    if (command_path == NULL) _exit(127);
    if (strcmp(spec->name, "mysql") == 0) {
      const char *password = getenv("FQ_MYSQL_PASSWORD");
      if (password != NULL) (void)setenv("MYSQL_PWD", password, 1);
      if (strchr(command_path, '/') != NULL) {
        execl(command_path, command_path, args[0], (char *)NULL);
      } else {
        execlp(command_path, command_path, args[0], (char *)NULL);
      }
    } else if (strcmp(spec->name, "pg") == 0) {
      const char *password = getenv("FQ_PG_PASSWORD");
      if (password != NULL) (void)setenv("PGPASSWORD", password, 1);
      if (strchr(command_path, '/') != NULL) {
        execl(command_path, command_path, args[0], (char *)NULL);
      } else {
        execlp(command_path, command_path, args[0], (char *)NULL);
      }
    } else {
      if (strchr(command_path, '/') != NULL) {
        execl(command_path, command_path, args[0], (char *)NULL);
      } else {
        execlp(command_path, command_path, args[0], (char *)NULL);
      }
    }
    _exit(127);
  }
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) return 0;
  }
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static int32_t qstProviderProbeConnection(const SQstProviderSpec *spec,
                                           const char *capture_path) {
  pid_t child = -1;
  int32_t status = 0;

  child = fork();
  if (child < 0) return 0;
  if (child == 0) {
    int32_t fd = open(capture_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0 || dup2(fd, STDOUT_FILENO) < 0 || dup2(fd, STDERR_FILENO) < 0) _exit(126);
    (void)close(fd);
    const char *command_path = qstProviderCommandPath(spec);
    if (command_path == NULL) _exit(127);
    if (strcmp(spec->name, "mysql") == 0) {
      const char *password = getenv("FQ_MYSQL_PASSWORD");
      if (password != NULL) (void)setenv("MYSQL_PWD", password, 1);
      if (strchr(command_path, '/') != NULL) {
        execl(command_path, command_path, "--protocol=TCP", "--connect-timeout=5",
              "--host", getenv("FQ_MYSQL_HOST"), "--port", getenv("FQ_MYSQL_PORT"),
              "--user", getenv("FQ_MYSQL_USER"), "-e", "SELECT 1", (char *)NULL);
      } else {
        execlp(command_path, command_path, "--protocol=TCP", "--connect-timeout=5",
               "--host", getenv("FQ_MYSQL_HOST"), "--port", getenv("FQ_MYSQL_PORT"),
               "--user", getenv("FQ_MYSQL_USER"), "-e", "SELECT 1", (char *)NULL);
      }
    } else if (strcmp(spec->name, "pg") == 0) {
      const char *password = getenv("FQ_PG_PASSWORD");
      if (password != NULL) (void)setenv("PGPASSWORD", password, 1);
      if (strchr(command_path, '/') != NULL) {
        execl(command_path, command_path, "--host", getenv("FQ_PG_HOST"), "--port",
              getenv("FQ_PG_PORT"), "--username", getenv("FQ_PG_USER"),
              "--dbname", "postgres", "--command", "SELECT 1", (char *)NULL);
      } else {
        execlp(command_path, command_path, "--host", getenv("FQ_PG_HOST"), "--port",
               getenv("FQ_PG_PORT"), "--username", getenv("FQ_PG_USER"),
               "--dbname", "postgres", "--command", "SELECT 1", (char *)NULL);
      }
    } else {
      char endpoint[1024] = {0};
      (void)snprintf(endpoint, sizeof(endpoint), "http://%s:%s/health",
                     getenv("FQ_INFLUX_HOST"), getenv("FQ_INFLUX_PORT"));
      {
        char auth[2048] = {0};
        (void)snprintf(auth, sizeof(auth), "Authorization: Bearer %s",
                       getenv("FQ_INFLUX_TOKEN"));
        if (strchr(command_path, '/') != NULL) {
          execl(command_path, command_path, "--fail", "--silent", "--show-error",
                "--max-time", "5", "--header", auth, endpoint, (char *)NULL);
        } else {
          execlp(command_path, command_path, "--fail", "--silent", "--show-error",
                 "--max-time", "5", "--header", auth, endpoint, (char *)NULL);
        }
      }
    }
    _exit(127);
  }
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) return 0;
  }
  return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static int32_t qstProviderAppendJson(char *out, size_t out_bytes, size_t *used,
                                     const char *text) {
  int32_t written = snprintf(out + *used, out_bytes - *used, "%s", text);
  if (written < 0 || (size_t)written >= out_bytes - *used) return 0;
  *used += (size_t)written;
  return 1;
}

static int32_t qstProviderEscape(const char *input, char *out, size_t out_bytes) {
  size_t used = 0;

  if (input == NULL || out == NULL || out_bytes == 0) return 0;
  for (const unsigned char *cursor = (const unsigned char *)input;
       *cursor != '\0'; ++cursor) {
    const char *replacement = NULL;
    char escaped[7] = {0};
    if (*cursor == '"') replacement = "\\\"";
    else if (*cursor == '\\') replacement = "\\\\";
    else if (*cursor == '\n') replacement = "\\n";
    else if (*cursor == '\r') replacement = "\\r";
    else if (*cursor == '\t') replacement = "\\t";
    else if (*cursor < 0x20U) {
      (void)snprintf(escaped, sizeof(escaped), "\\u%04x", *cursor);
      replacement = escaped;
    }
    if (replacement != NULL) {
      const size_t length = strlen(replacement);
      if (length > out_bytes - used - 1U) return 0;
      memcpy(out + used, replacement, length);
      used += length;
    } else {
      if (used + 1U >= out_bytes) return 0;
      out[used++] = (char)*cursor;
    }
  }
  out[used] = '\0';
  return 1;
}

static int32_t qstProviderSelected(uint32_t bits, uint32_t bit) {
  return (bits & bit) != 0;
}

EQstCode qst_provider_preflight(const SQstRunConfig *config) {
  char dataset_dir[QST_PROVIDER_PATH_BYTES] = {0};
  char manifest_path[QST_PROVIDER_PATH_BYTES] = {0};
  char temporary_path[QST_PROVIDER_PATH_BYTES] = {0};
  char capture_path[QST_PROVIDER_PATH_BYTES] = {0};
  char version[QST_PROVIDER_CAPTURE_BYTES] = {0};
  char probe_error[QST_PROVIDER_CAPTURE_BYTES] = {0};
  char manifest[32768] = {0};
  size_t used = 0;
  uint32_t selected_count = 0;
  EQstCode code = QST_OK;

  if (config == NULL || config->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if ((config->target_selection_bits &
       (QST_PROVIDER_MYSQL | QST_PROVIDER_PG | QST_PROVIDER_INFLUX)) == 0) {
    return QST_OK;
  }
  if (config->result_dir == NULL) return QST_E_INVALID_ARGUMENT;
  if (snprintf(dataset_dir, sizeof(dataset_dir), "%s/dataset", config->result_dir) < 0 ||
      snprintf(manifest_path, sizeof(manifest_path), "%s/provider-readiness.json", dataset_dir) < 0 ||
      snprintf(temporary_path, sizeof(temporary_path), "%s.tmp", manifest_path) < 0 ||
      snprintf(capture_path, sizeof(capture_path), "%s/.provider-probe.XXXXXX", dataset_dir) < 0 ||
      strlen(dataset_dir) >= sizeof(dataset_dir) || strlen(manifest_path) >= sizeof(manifest_path) ||
      strlen(temporary_path) >= sizeof(temporary_path) || strlen(capture_path) >= sizeof(capture_path)) {
    return QST_E_RESOURCE;
  }
  if (mkdir(dataset_dir, 0750) != 0 && errno != EEXIST) return QST_E_RESOURCE;
  if (!qstProviderAppendJson(manifest, sizeof(manifest), &used,
                             "{\"format_version\":1,\"run_id\":")) return QST_E_RESOURCE;
  {
    char run_text[64] = {0};
    (void)snprintf(run_text, sizeof(run_text), "%llu,\"providers\":[",
                   (unsigned long long)config->run_id);
    if (!qstProviderAppendJson(manifest, sizeof(manifest), &used, run_text)) return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < sizeof(kProviderSpecs) / sizeof(kProviderSpecs[0]); ++index) {
    const SQstProviderSpec *spec = &kProviderSpecs[index];
    int32_t selected = qstProviderSelected(config->target_selection_bits, spec->bit);
    int32_t environment_ready = 1;
    int32_t command_ready = 0;
    int32_t version_ready = 0;
    int32_t connection_ready = 0;
    char escaped_version[QST_PROVIDER_CAPTURE_BYTES * 2U] = {0};
    char escaped_error[QST_PROVIDER_CAPTURE_BYTES * 2U] = {0};
    if (!selected) continue;
    ++selected_count;
    for (uint32_t env_index = 0; env_index < spec->environment_count; ++env_index) {
      const char *value = getenv(spec->environment[env_index]);
      if (value == NULL || value[0] == '\0') environment_ready = 0;
    }
    if (!environment_ready) {
      code = QST_E_RESOURCE;
      (void)snprintf(probe_error, sizeof(probe_error), "missing required environment");
    } else {
      char *command_path = NULL;
      command_path = getenv("PATH");
      (void)command_path;
      {
        char *const version_args[] = {(char *)spec->version_arg, NULL};
        command_ready = qstProviderRun(spec, (const char *const *)version_args, capture_path);
      }
      if (command_ready && qstProviderReadText(capture_path, version, sizeof(version))) {
        version_ready = version[0] != '\0';
      }
      if (!version_ready) {
        code = QST_E_RESOURCE;
        if (!qstProviderReadText(capture_path, probe_error, sizeof(probe_error)) ||
            probe_error[0] == '\0') {
          (void)snprintf(probe_error, sizeof(probe_error),
                         "client version probe failed");
        }
      }
      if (version_ready) {
        connection_ready = qstProviderProbeConnection(spec, capture_path);
        if (!connection_ready) {
          (void)qstProviderReadText(capture_path, probe_error, sizeof(probe_error));
          if (probe_error[0] == '\0') (void)snprintf(probe_error, sizeof(probe_error), "health probe failed");
          code = QST_E_RESOURCE;
        }
      }
    }
    (void)unlink(capture_path);
    if (!qstProviderEscape(version, escaped_version, sizeof(escaped_version)) ||
        !qstProviderEscape(probe_error, escaped_error, sizeof(escaped_error))) {
      return QST_E_RESOURCE;
    }
    if (selected_count > 1 && !qstProviderAppendJson(manifest, sizeof(manifest), &used, ",")) return QST_E_RESOURCE;
    {
      char item[8192] = {0};
      (void)snprintf(item, sizeof(item),
                     "{\"target\":\"%s\",\"selected\":true,\"environment_ready\":%s,"
                     "\"client\":\"%s\",\"client_ready\":%s,\"version\":\"%s\","
                     "\"connection_ready\":%s,\"error\":\"%s\"}", spec->name,
                     environment_ready ? "true" : "false", spec->command,
                     (command_ready && version_ready) ? "true" : "false", escaped_version,
                     connection_ready ? "true" : "false", escaped_error);
      if (!qstProviderAppendJson(manifest, sizeof(manifest), &used, item)) return QST_E_RESOURCE;
    }
    memset(version, 0, sizeof(version));
    memset(probe_error, 0, sizeof(probe_error));
  }
  if (!qstProviderAppendJson(manifest, sizeof(manifest), &used,
                             "]}\n")) return QST_E_RESOURCE;
  {
    int32_t fd = open(temporary_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (fd < 0 || !qstProviderWriteAll(fd, manifest, used) || fsync(fd) != 0 || close(fd) != 0) {
      if (fd >= 0) (void)close(fd);
      (void)unlink(temporary_path);
      return QST_E_RESOURCE;
    }
  }
  if (rename(temporary_path, manifest_path) != 0) {
    (void)unlink(temporary_path);
    return QST_E_RESOURCE;
  }
  return selected_count == 0 ? QST_OK : code;
}

EQstCode qst_provider_readiness_json(const SQstRunConfig *config, char *out_json,
                                     uint64_t out_bytes) {
  char path[QST_PROVIDER_PATH_BYTES] = {0};
  int32_t fd = -1;
  uint64_t used = 0;

  if (config == NULL || out_json == NULL || out_bytes < 2U ||
      config->result_dir == NULL || config->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if ((config->target_selection_bits &
       (QST_PROVIDER_MYSQL | QST_PROVIDER_PG | QST_PROVIDER_INFLUX)) == 0) {
    const int32_t written = snprintf(out_json, (size_t)out_bytes,
                                     "{\"format_version\":1,\"run_id\":%llu,\"providers\":[]}",
                                     (unsigned long long)config->run_id);
    return written > 0 && (uint64_t)written < out_bytes ? QST_OK : QST_E_RESOURCE;
  }
  if (snprintf(path, sizeof(path), "%s/dataset/provider-readiness.json",
               config->result_dir) < 0 || strlen(path) >= sizeof(path)) {
    return QST_E_RESOURCE;
  }
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) return QST_E_PENDING_IO;
  while (used + 1U < out_bytes) {
    const ssize_t bytes = read(fd, out_json + used, (size_t)(out_bytes - used - 1U));
    if (bytes < 0 && errno == EINTR) continue;
    if (bytes < 0 || bytes == 0) break;
    used += (uint64_t)bytes;
  }
  if (close(fd) != 0 || used == 0 || used + 1U >= out_bytes ||
      memchr(out_json, '\0', (size_t)used) != NULL) {
    return QST_E_PENDING_IO;
  }
  out_json[used] = '\0';
  while (used > 0 && (out_json[used - 1U] == ' ' || out_json[used - 1U] == '\n' ||
                      out_json[used - 1U] == '\r' || out_json[used - 1U] == '\t')) {
    out_json[--used] = '\0';
  }
  if (used < 2U || out_json[0] != '{' || out_json[used - 1U] != '}') {
    return QST_E_PENDING_IO;
  }
  return QST_OK;
}
