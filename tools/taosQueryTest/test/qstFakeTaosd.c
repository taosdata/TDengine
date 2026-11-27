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

/* Minimal taosd process fixture for pair-supervisor lifecycle tests. */

#define _POSIX_C_SOURCE 200809L

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t gQstFakeTaosdStop = 0;

static void qstFakeTaosdStop(int32_t signal_number) {
  (void)signal_number;
  gQstFakeTaosdStop = 1;
}

/* Test-only startup fault injection. The marker is created exactly once, so
 * later replacement servers exercise the normal successful startup path. */
static int32_t qstFakeTaosdFailFirstStart(void) {
  const char *marker_path = getenv("QST_FAKE_TAOSD_FAIL_FIRST_START_FILE");
  int32_t marker_fd = -1;

  if (marker_path == NULL || marker_path[0] == '\0') {
    return 0;
  }
  marker_fd = open(marker_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (marker_fd < 0) {
    return 0;
  }
  if (close(marker_fd) != 0) {
    return 0;
  }
  return 1;
}

int main(int argc, char **argv) {
  int32_t has_config = 0;
  int32_t has_log_dir = 0;
  const char *config_dir = NULL;

  for (int32_t index = 1; index < argc; ++index) {
    if (strcmp(argv[index], "-c") == 0 && index + 1 < argc) {
      has_config = 1;
      config_dir = argv[++index];
    } else if (strcmp(argv[index], "-o") == 0 && index + 1 < argc) {
      has_log_dir = 1;
      ++index;
    }
  }
  if (has_config == 0 || has_log_dir == 0) {
    return 2;
  }
  if (qstFakeTaosdFailFirstStart() != 0) {
    return 6;
  }
  if (getenv("QST_FAKE_TAOSD_REQUIRE_CLEAN_CONFIG") != NULL &&
      getenv("TAOS_CONFIG_DIR") != NULL) {
    return 4;
  }
  if (getenv("QST_FAKE_TAOSD_REQUIRE_DATA_DIR_LAYOUT") != NULL ||
      getenv("QST_FAKE_TAOSD_REQUIRE_SHORT_DATA_DIR") != NULL) {
    char config_path[4096] = {0};
    char line[4096] = {0};
    FILE *config_file = NULL;
    int32_t has_data_dir_layout = 0;
    int32_t has_short_data_dir = 0;
    int32_t written = 0;

    written = snprintf(config_path, sizeof(config_path), "%s/taos.cfg",
                       config_dir);
    if (written < 0 || (uint64_t)written >= sizeof(config_path)) {
      return 5;
    }
    config_file = fopen(config_path, "r");
    if (config_file == NULL) {
      return 5;
    }
    while (fgets(line, sizeof(line), config_file) != NULL) {
      const size_t line_length = strlen(line);
      static const char kDataDirPrefix[] = "dataDir ";
      static const char kDataDirSuffix[] = " 0 1\n";

      if (strncmp(line, kDataDirPrefix, sizeof(kDataDirPrefix) - 1U) == 0 &&
          line_length >= sizeof(kDataDirSuffix) - 1U &&
          strcmp(line + line_length - (sizeof(kDataDirSuffix) - 1U),
                 kDataDirSuffix) == 0) {
        has_data_dir_layout = 1;
      }
      if (strstr(line, "/qst-pair-") != NULL) {
        has_short_data_dir = 1;
      }
    }
    if (fclose(config_file) != 0 ||
        (getenv("QST_FAKE_TAOSD_REQUIRE_DATA_DIR_LAYOUT") != NULL &&
         has_data_dir_layout == 0) ||
        (getenv("QST_FAKE_TAOSD_REQUIRE_SHORT_DATA_DIR") != NULL &&
         has_short_data_dir == 0)) {
      return 5;
    }
  }
  if (signal(SIGTERM, qstFakeTaosdStop) == SIG_ERR ||
      signal(SIGINT, qstFakeTaosdStop) == SIG_ERR) {
    return 3;
  }
  while (gQstFakeTaosdStop == 0) {
    (void)pause();
  }
  return 0;
}
