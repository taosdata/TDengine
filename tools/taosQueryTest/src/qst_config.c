/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _POSIX_C_SOURCE 200809L

#include "qst_config.h"

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "qst_generator.h"
#include "qst_task.h"

enum {
  QST_TARGET_TD_BIT = 1U << 0,
  QST_TARGET_MYSQL_BIT = 1U << 1,
  QST_TARGET_PG_BIT = 1U << 2,
  QST_TARGET_INFLUX_BIT = 1U << 3,
  QST_TARGET_ALL_BITS = QST_TARGET_TD_BIT | QST_TARGET_MYSQL_BIT |
                        QST_TARGET_PG_BIT | QST_TARGET_INFLUX_BIT,
};

static void qstSetError(char *error, uint64_t error_bytes, const char *format,
                        ...) {
  va_list args;

  if (NULL == error || error_bytes == 0) {
    return;
  }
  va_start(args, format);
  (void)vsnprintf(error, (size_t)error_bytes, format, args);
  va_end(args);
}

static int32_t qstCopyString(char *destination, uint64_t destination_bytes,
                             const char *source) {
  const uint64_t source_bytes = source == NULL ? 0 : strlen(source);

  if (NULL == destination || NULL == source || source_bytes >= destination_bytes) {
    return 0;
  }
  memcpy(destination, source, (size_t)source_bytes + 1);
  return 1;
}

static char *qstTrim(char *text) {
  char *end = NULL;

  while (*text != '\0' && isspace((unsigned char)*text)) {
    ++text;
  }
  end = text + strlen(text);
  while (end > text && isspace((unsigned char)end[-1])) {
    --end;
  }
  *end = '\0';
  return text;
}

static int32_t qstParseUint64(const char *text, uint64_t *out_value,
                              int32_t allow_zero) {
  char *end = NULL;
  unsigned long long value = 0;

  if (NULL == text || *text == '\0' || NULL == out_value || text[0] == '-') {
    return 0;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' ||
      (!allow_zero && value == 0)) {
    return 0;
  }
  *out_value = (uint64_t)value;
  return 1;
}

static int32_t qstParseMode(const char *text, uint32_t *out_mode) {
  if (strcmp(text, "full") == 0) {
    *out_mode = QST_MODE_FULL;
    return 1;
  }
  if (strcmp(text, "random") == 0) {
    *out_mode = QST_MODE_RANDOM;
    return 1;
  }
  return 0;
}

static int32_t qstParseFocus(const char *text, uint32_t *out_focus) {
  if (strcmp(text, "coverage") == 0) {
    *out_focus = QST_FOCUS_COVERAGE;
    return 1;
  }
  if (strcmp(text, "crash") == 0) {
    *out_focus = QST_FOCUS_CRASH;
    return 1;
  }
  if (strcmp(text, "correctness") == 0) {
    *out_focus = QST_FOCUS_CORRECTNESS;
    return 1;
  }
  return 0;
}

static int32_t qstParseTargets(const char *text, uint32_t *out_bits) {
  char copy[128] = {0};
  char *save = NULL;
  char *token = NULL;
  uint32_t bits = 0;

  if (NULL == text || text[0] == '\0' || text[0] == ',' ||
      text[strlen(text) - 1] == ',' || strstr(text, ",,") != NULL ||
      !qstCopyString(copy, sizeof(copy), text)) {
    return 0;
  }
  for (token = strtok_r(copy, ",", &save); token != NULL;
       token = strtok_r(NULL, ",", &save)) {
    uint32_t bit = 0;
    if (*token == '\0') {
      return 0;
    }
    if (strcmp(token, "all") == 0) {
      if (bits != 0 || (save != NULL && *save != '\0')) {
        return 0;
      }
      *out_bits = QST_TARGET_ALL_BITS;
      return 1;
    }
    if (strcmp(token, "td") == 0) {
      bit = QST_TARGET_TD_BIT;
    } else if (strcmp(token, "mysql") == 0) {
      bit = QST_TARGET_MYSQL_BIT;
    } else if (strcmp(token, "pg") == 0) {
      bit = QST_TARGET_PG_BIT;
    } else if (strcmp(token, "influx") == 0) {
      bit = QST_TARGET_INFLUX_BIT;
    } else if (strcmp(token, "all") == 0) {
      return 0;
    } else {
      return 0;
    }
    bits |= bit;
  }
  if (bits == 0) {
    return 0;
  }
  *out_bits = bits;
  return 1;
}

static int32_t qstParseYamlString(const char *text, char *out_value,
                                  uint64_t value_bytes) {
  char copy[QST_PATH_BYTES] = {0};
  char *value = NULL;
  uint64_t length = 0;

  if (!qstCopyString(copy, sizeof(copy), text)) {
    return 0;
  }
  value = qstTrim(copy);
  length = strlen(value);
  if (length >= 2 && ((value[0] == '\'' && value[length - 1] == '\'') ||
                      (value[0] == '"' && value[length - 1] == '"'))) {
    value[length - 1] = '\0';
    ++value;
  }
  return qstCopyString(out_value, value_bytes, value);
}

static EQstCode qstReadYaml(const char *tool_root, SQstCliConfig *config,
                            char *error, uint64_t error_bytes) {
  char path[QST_PATH_BYTES] = {0};
  char line[QST_PATH_BYTES] = {0};
  FILE *file = NULL;
  int32_t in_execution = 0;
  int32_t in_ai = 0;
  int32_t seen_version = 0;
  int32_t seen_timeout = 0;
  int32_t seen_retry = 0;
  int32_t seen_provider = 0;
  int32_t seen_command = 0;
  char ai_provider[32] = {0};

  if (snprintf(path, sizeof(path), "%s/taosQueryTest.yaml", tool_root) < 0 ||
      strlen(path) >= sizeof(path)) {
    qstSetError(error, error_bytes, "configuration path is too long");
    return QST_E_INVALID_ARGUMENT;
  }
  file = fopen(path, "r");
  if (NULL == file) {
    qstSetError(error, error_bytes, "cannot open %s: %s", path, strerror(errno));
    return QST_E_RESOURCE;
  }
  while (fgets(line, sizeof(line), file) != NULL) {
    char *raw = line;
    uint32_t indentation = 0;
    char *text = NULL;
    char *colon = NULL;
    char *key = NULL;
    char *value = NULL;
    uint64_t number = 0;

    while (*raw == ' ') {
      ++raw;
      ++indentation;
    }
    text = qstTrim(raw);
    if (*text == '\0' || *text == '#') {
      continue;
    }
    if (strcmp(text, "execution:") == 0) {
      in_execution = 1;
      in_ai = 0;
      continue;
    }
    if (strcmp(text, "ai:") == 0) {
      in_execution = 0;
      in_ai = 1;
      continue;
    }
    colon = strchr(text, ':');
    if (NULL == colon) {
      fclose(file);
      qstSetError(error, error_bytes, "invalid YAML line: %s", text);
      return QST_E_INVALID_ARGUMENT;
    }
    *colon = '\0';
    key = qstTrim(text);
    value = qstTrim(colon + 1);
    if (strcmp(key, "version") == 0 && !in_execution && !in_ai) {
      if (!qstParseUint64(value, &number, 0) || number != 1) {
        fclose(file);
        qstSetError(error, error_bytes, "version must be 1");
        return QST_E_INVALID_ARGUMENT;
      }
      seen_version = 1;
    } else if (in_execution && strcmp(key, "statement_timeout_seconds") == 0) {
      if (!qstParseUint64(value, &number, 0) || number > UINT32_MAX) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.statement_timeout_seconds must be positive");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.statement_timeout_seconds = (uint32_t)number;
      seen_timeout = 1;
    } else if (in_execution && strcmp(key, "statement_retry_count") == 0) {
      if (!qstParseUint64(value, &number, 1) || number > UINT32_MAX) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.statement_retry_count must be non-negative");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.statement_retry_count = (uint32_t)number;
      seen_retry = 1;
    } else if (in_execution && strcmp(key, "pair_count") == 0) {
      if (!qstParseUint64(value, &number, 0) || number > UINT32_MAX) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.pair_count must be positive");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.execution_pair_count_override = (uint32_t)number;
    } else if (in_execution && strcmp(key, "judge_worker_count") == 0) {
      if (!qstParseUint64(value, &number, 0) || number > UINT32_MAX) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.judge_worker_count must be positive");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.judge_worker_count_override = (uint32_t)number;
    } else if (in_execution && strcmp(key, "queue_capacity") == 0) {
      if (!qstParseUint64(value, &number, 0) || number > UINT32_MAX) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.queue_capacity must be positive");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.queue_capacity_override = (uint32_t)number;
    } else if (in_execution && strcmp(key, "pair_async_window") == 0) {
      if (!qstParseUint64(value, &number, 0) ||
          number > QST_PAIR_MAX_INFLIGHT) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.pair_async_window must be between 1 and %u",
                    QST_PAIR_MAX_INFLIGHT);
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.pair_async_window_cap = (uint32_t)number;
    } else if (in_execution && strcmp(key, "random_builder_count") == 0) {
      if (!qstParseUint64(value, &number, 0) ||
          number > QST_GENERATOR_MAX_RANDOM_BUILDERS ||
          (number & (number - 1U)) != 0) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.random_builder_count must be a power of two "
                    "between 1 and %u",
                    QST_GENERATOR_MAX_RANDOM_BUILDERS);
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.random_builder_count_override = (uint32_t)number;
    } else if (in_execution && strcmp(key, "random_seed") == 0) {
      if (!qstParseUint64(value, &number, 0)) {
        fclose(file);
        qstSetError(error, error_bytes,
                    "execution.random_seed must be positive");
        return QST_E_INVALID_ARGUMENT;
      }
      config->run_config.random_seed = number;
    } else if (in_ai && strcmp(key, "provider") == 0) {
      if (!qstParseYamlString(value, ai_provider, sizeof(ai_provider)) ||
          strcmp(ai_provider, "codex") != 0) {
        fclose(file);
        qstSetError(error, error_bytes, "ai.provider must be codex");
        return QST_E_INVALID_ARGUMENT;
      }
      seen_provider = 1;
    } else if (in_ai && strcmp(key, "command") == 0) {
      if (!qstParseYamlString(value, config->ai_command,
                              sizeof(config->ai_command)) ||
          config->ai_command[0] == '\0') {
        fclose(file);
        qstSetError(error, error_bytes, "ai.command must be non-empty");
        return QST_E_INVALID_ARGUMENT;
      }
      seen_command = 1;
    } else if (in_ai && strcmp(key, "working_directory") == 0) {
      if (!qstParseYamlString(value, config->ai_working_directory,
                              sizeof(config->ai_working_directory))) {
        fclose(file);
        qstSetError(error, error_bytes, "invalid ai.working_directory");
        return QST_E_INVALID_ARGUMENT;
      }
    } else {
      fclose(file);
      qstSetError(error, error_bytes, "unknown or unsupported YAML field: %s", key);
      return QST_E_INVALID_ARGUMENT;
    }
  }
  if (ferror(file)) {
    fclose(file);
    qstSetError(error, error_bytes, "cannot read %s", path);
    return QST_E_RESOURCE;
  }
  fclose(file);
  if (!seen_version || !seen_timeout || !seen_retry || !seen_provider ||
      !seen_command) {
    qstSetError(error, error_bytes, "configuration is missing required fields");
    return QST_E_INVALID_ARGUMENT;
  }
  if (config->ai_working_directory[0] != '\0') {
    struct stat st = {0};
    if (stat(config->ai_working_directory, &st) != 0 || !S_ISDIR(st.st_mode)) {
      qstSetError(error, error_bytes, "ai.working_directory is not a directory");
      return QST_E_INVALID_ARGUMENT;
    }
  }
  return QST_OK;
}

void qst_config_print_usage(const char *program_name) {
  (void)fprintf(stdout,
                "Usage: %s [-t full|random] [-f coverage|crash|correctness] "
                "[-n statements|-m minutes] [-q targets] [-g output]\n"
                "       %s -v|-av run_id [prompt]\n",
                program_name, program_name);
}

EQstCode qst_config_parse(int32_t argc, char *const argv[],
                          const char *tool_root, SQstCliConfig *out_config,
                          char *error, uint64_t error_bytes) {
  static const struct option options[] = {
      {"help", no_argument, NULL, 'h'},
      {NULL, 0, NULL, 0},
  };
  int32_t option = 0;
  int32_t ai_option_count = 0;
  int32_t seen_mode = 0;
  int32_t seen_focus = 0;
  int32_t seen_count = 0;
  int32_t seen_duration = 0;
  int32_t seen_targets = 0;
  int32_t seen_generate_output = 0;
  EQstCode code = QST_OK;
  char *arguments[argc + 1];

  if (NULL == tool_root || NULL == out_config) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_config, 0, sizeof(*out_config));
  out_config->run_config.mode = QST_MODE_RANDOM;
  out_config->run_config.focus = QST_FOCUS_COVERAGE;
  out_config->run_config.target_selection_bits = QST_TARGET_TD_BIT;
  if (!qstCopyString(out_config->tool_root, sizeof(out_config->tool_root),
                     tool_root)) {
    qstSetError(error, error_bytes, "tool root path is too long");
    return QST_E_INVALID_ARGUMENT;
  }

  for (int32_t index = 0; index < argc; ++index) {
    arguments[index] = argv[index];
    if (strcmp(arguments[index], "-av") == 0) {
      arguments[index] = "-a";
    }
  }
  arguments[argc] = NULL;

  optind = 1;
  while ((option = getopt_long(argc, arguments, "ht:f:n:m:q:g:v:a:", options,
                               NULL)) != -1) {
    uint64_t number = 0;
    switch (option) {
      case 'h':
        return QST_E_STOPPING;
      case 't':
        if (seen_mode || !qstParseMode(optarg, &out_config->run_config.mode)) {
          qstSetError(error, error_bytes, "invalid or repeated -t value");
          return QST_E_INVALID_ARGUMENT;
        }
        seen_mode = 1;
        break;
      case 'f':
        if (seen_focus || !qstParseFocus(optarg, &out_config->run_config.focus)) {
          qstSetError(error, error_bytes, "invalid or repeated -f value");
          return QST_E_INVALID_ARGUMENT;
        }
        seen_focus = 1;
        break;
      case 'n':
        if (seen_count || !qstParseUint64(optarg, &number, 0)) {
          qstSetError(error, error_bytes, "-n must be one positive integer");
          return QST_E_INVALID_ARGUMENT;
        }
        out_config->run_config.statement_limit = number;
        seen_count = 1;
        break;
      case 'm':
        if (seen_duration || !qstParseUint64(optarg, &number, 0)) {
          qstSetError(error, error_bytes, "-m must be one positive integer");
          return QST_E_INVALID_ARGUMENT;
        }
        out_config->run_config.duration_limit_minutes = number;
        seen_duration = 1;
        break;
      case 'q':
        if (seen_targets || !qstParseTargets(optarg,
                                             &out_config->run_config.target_selection_bits)) {
          qstSetError(error, error_bytes, "invalid -q target set");
          return QST_E_INVALID_ARGUMENT;
        }
        seen_targets = 1;
        break;
      case 'g':
        if (seen_generate_output || !qstCopyString(
                out_config->generate_output_path,
                sizeof(out_config->generate_output_path), optarg)) {
          qstSetError(error, error_bytes, "invalid or repeated -g output path");
          return QST_E_INVALID_ARGUMENT;
        }
        out_config->run_config.generate_output_path =
            out_config->generate_output_path;
        seen_generate_output = 1;
        break;
      case 'v':
      case 'a':
        if (++ai_option_count != 1 || !qstParseUint64(optarg, &number, 0)) {
          qstSetError(error, error_bytes, "AI options require one positive run_id");
          return QST_E_INVALID_ARGUMENT;
        }
        out_config->existing_run_id = number;
        out_config->ai_entry = option == 'v' ? QST_AI_ENTRY_MANUAL_UNVERIFIED :
                                               QST_AI_ENTRY_MANUAL_ALL;
        break;
      default:
        qstSetError(error, error_bytes, "unknown command-line option");
        return QST_E_INVALID_ARGUMENT;
    }
  }
  if (ai_option_count != 0) {
    if (seen_mode || seen_focus || seen_count || seen_duration || seen_targets ||
        seen_generate_output ||
        argc - optind > 1 ||
        (argc - optind == 1 &&
         !qstCopyString(out_config->prompt, sizeof(out_config->prompt),
                        argv[optind]))) {
      qstSetError(error, error_bytes, "AI entry cannot combine test options");
      return QST_E_INVALID_ARGUMENT;
    }
  } else if (argc != optind ||
             (out_config->run_config.mode == QST_MODE_FULL && seen_count) ||
             (seen_count && seen_duration) ||
             (seen_generate_output && out_config->run_config.mode == QST_MODE_RANDOM &&
              !seen_count && !seen_duration)) {
    qstSetError(error, error_bytes, "invalid test option combination");
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstReadYaml(tool_root, out_config, error, error_bytes);
  if (code != QST_OK) {
    return code;
  }
  return QST_OK;
}
