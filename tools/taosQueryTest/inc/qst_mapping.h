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

/* Strict external-fixture mapping-rules validation. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_

#include "qst_types.h"

/* Validates one copied rule file against its copied closed JSON Schema. */
EQstCode qst_mapping_validate_files(const char *rules_path,
                                    const char *schema_path);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_MAPPING_H_
