/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TAOS_METRIC_FORMATTER_T_H
#define TAOS_METRIC_FORMATTER_T_H

#include "taos_string_builder_t.h"

typedef struct taos_metric_formatter {
  taos_string_builder_t *string_builder;
  taos_string_builder_t *err_builder;
} taos_metric_formatter_t;

#endif  // TAOS_METRIC_FORMATTER_T_H
