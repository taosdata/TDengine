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

/* Pair-local fixture materialization for an already connected native endpoint. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_

#include "qst_native.h"

EQstCode qst_dataset_materialize_local_fixture(SQstNativeClient *client);
/* Materializes the pair-local fixture for a bounded server readiness retry.
 * Caller receives the native error code without per-attempt error logging. */
EQstCode qst_dataset_materialize_local_fixture_silent(
    SQstNativeClient *client, int32_t *out_native_code);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_DATASET_FIXTURE_H_
