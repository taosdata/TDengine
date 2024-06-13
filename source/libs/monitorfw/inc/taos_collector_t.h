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

#ifndef TAOS_COLLECTOR_T_H
#define TAOS_COLLECTOR_T_H

#include "taos_collector.h"
#include "taos_map_t.h"
#include "taos_string_builder_t.h"

struct taos_collector {
  const char *name;
  taos_map_t *metrics;
  taos_collect_fn *collect_fn;
  taos_string_builder_t *string_builder;
  const char *proc_limits_file_path;
  const char *proc_stat_file_path;
};

#endif  // TAOS_COLLECTOR_T_H
