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

/* External-provider readiness and version manifest. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_

#include "qst_runner.h"

/*
 * Probe only providers selected by target_selection_bits.  The probe does not
 * persist credentials; it writes a redacted readiness manifest below the run
 * dataset directory and returns an error before any provider setup script is
 * started when a selected provider is not usable.
 */
EQstCode qst_provider_preflight(const SQstRunConfig *config);

/* Reads the already atomically published, redacted provider manifest. */
EQstCode qst_provider_readiness_json(const SQstRunConfig *config, char *out_json,
                                     uint64_t out_bytes);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROVIDER_H_
