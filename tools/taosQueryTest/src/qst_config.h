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

/* Internal command-line and YAML configuration support. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CONFIG_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CONFIG_H_

#include <stdint.h>

#include "qst_runner.h"

enum {
  QST_PATH_BYTES = 4096,
  QST_COMMAND_BYTES = 1024,
  QST_PROMPT_BYTES = 8192,
};

typedef struct {
  SQstRunConfig run_config;
  uint32_t ai_entry;
  uint64_t existing_run_id;
  char tool_root[QST_PATH_BYTES];
  char result_dir[QST_PATH_BYTES];
  char generate_output_path[QST_PATH_BYTES];
  char ai_command[QST_COMMAND_BYTES];
  char ai_working_directory[QST_PATH_BYTES];
  char prompt[QST_PROMPT_BYTES];
} SQstCliConfig;

EQstCode qst_config_parse(int32_t argc, char *const argv[],
                          const char *tool_root, SQstCliConfig *out_config,
                          char *error, uint64_t error_bytes);
void qst_config_print_usage(const char *program_name);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CONFIG_H_
