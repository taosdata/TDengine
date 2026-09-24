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

/* Read-only durable result-store audit command. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_

#include <stdint.h>

#include "qst_types.h"

/* Parses --audit-run and --audit-targets, queries only the configured result
 * store, and writes one bounded JSON document to stdout. */
EQstCode qst_audit_run_cli(int32_t argc, char *const argv[]);

/* Parses --reset-result-store RUN_ID and drops the test-only res_db after a
 * caller has durably retained the completed run audit. The destructive path
 * requires QST_RESULT_STORE_ALLOW_RESET=1 and verifies that RUN_ID exists. */
EQstCode qst_audit_reset_result_store_cli(int32_t argc, char *const argv[]);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_AUDIT_H_
