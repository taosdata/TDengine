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

/* Versioned compiled SQL-dimension catalog entry point. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SQL_DIMENSION_CONFIG_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SQL_DIMENSION_CONFIG_H_

#include "qst_generator.h"

typedef struct {
  uint32_t fragment_id;
  const char *name;
  const char *relation;
} SQstSqlDimensionSourceFragment;

typedef struct {
  uint32_t value_id;
  const char *name;
  const char *text;
} SQstSqlDimensionSourceValue;

/* Returns the immutable DS chapter-8 source directory compiled into the tool. */
EQstCode qst_sql_dimension_source(
    const SQstSqlDimensionSourceFragment **out_fragments,
    uint32_t *out_fragment_count, const SQstSqlDimensionSourceValue **out_values,
    uint32_t *out_value_count);

/* Returns the immutable compiled catalog and verifies its stored digest. */
EQstCode qst_sql_dimension_catalog(const SQstGeneratorCatalog **out_catalog);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SQL_DIMENSION_CONFIG_H_
