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

/* Standalone AI command entry support. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_

#include "qst_config.h"

/*
 * Performs the standalone AI entry selected by the parsed CLI configuration.
 * This API never starts SQL generation or execution. A zero timeout uses the
 * DS-required 900-second deadline; a non-zero timeout is reserved for tests.
 */
EQstCode qst_ai_run_cli(const SQstCliConfig *config,
                        uint32_t timeout_milliseconds);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AI_RUNNER_H_
