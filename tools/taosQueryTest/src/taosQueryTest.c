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

/* taosQueryTest command-line entry point. */

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "qst_config.h"
#include "qst_ai_runner.h"
#include "qst_audit.h"

int main(int argc, char *argv[]) {
  SQstCliConfig config = {0};
  char tool_root[QST_PATH_BYTES] = {0};
  char error[1024] = {0};
  EQstCode code = QST_OK;

  if (argc > 1 && strcmp(argv[1], "--audit-run") == 0) {
    code = qst_audit_run_cli(argc, argv);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: audit failed (%d)\n", code);
      return 1;
    }
    return 0;
  }
  if (argc > 1 && strcmp(argv[1], "--reset-result-store") == 0) {
    code = qst_audit_reset_result_store_cli(argc, argv);
    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: result-store reset failed (%d)\n", code);
      return 1;
    }
    return 0;
  }

  {
    const ssize_t length = readlink("/proc/self/exe", tool_root,
                                   sizeof(tool_root) - 1U);
    char *separator = NULL;

    if (length <= 0 || (uint64_t)length >= sizeof(tool_root)) {
      (void)fprintf(stderr, "taosQueryTest: cannot determine executable path\n");
      return 1;
    }
    tool_root[length] = '\0';
    separator = strrchr(tool_root, '/');
    if (separator == NULL || separator == tool_root) {
      (void)fprintf(stderr, "taosQueryTest: executable path has no tool root\n");
      return 1;
    }
    *separator = '\0';
  }
  if (tool_root[0] == '\0') {
    (void)fprintf(stderr, "taosQueryTest: cannot determine tool root\n");
    return 1;
  }
  code = qst_config_parse(argc, argv, tool_root, &config, error, sizeof(error));
  if (code == QST_E_STOPPING) {
    qst_config_print_usage(argv[0]);
    return 0;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: %s\n", error[0] == '\0' ?
                  "invalid configuration" : error);
    return 2;
  }
  if (config.ai_entry != 0) {
    code = qst_ai_run_cli(&config, 0);
  } else {
    config.run_config.tool_root = config.tool_root;
    code = qst_launcher_run(&config.run_config);
  }
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: run failed (%d)\n", code);
    return 1;
  }
  return 0;
}
