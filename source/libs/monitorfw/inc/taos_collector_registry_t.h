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

#ifndef TAOS_REGISTRY_T_H
#define TAOS_REGISTRY_T_H

#include <pthread.h>
#include <stdbool.h>

// Public
#include "taos_collector_registry.h"

// Private
#include "taos_map_t.h"
#include "taos_metric_formatter_t.h"
#include "taos_string_builder_t.h"

struct taos_collector_registry {
  const char *name;
  bool disable_process_metrics;              /**< Disables the collection of process metrics */
  taos_map_t *collectors;                    /**< Map of collectors keyed by name */
  taos_string_builder_t *string_builder;     /**< Enables string building */
  taos_metric_formatter_t *metric_formatter; /**< metric formatter for metric exposition on bridge call */
  pthread_rwlock_t *lock;                    /**< mutex for safety against concurrent registration */
  taos_string_builder_t *string_builder_batch;
};

#endif  // TAOS_REGISTRY_T_H
