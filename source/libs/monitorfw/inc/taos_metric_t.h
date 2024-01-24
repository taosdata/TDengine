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

#ifndef TAOS_METRIC_T_H
#define TAOS_METRIC_T_H

#include <pthread.h>

// Public
#include "taos_metric.h"

// Private
#include "taos_map_i.h"
#include "taos_map_t.h"
#include "taos_metric_formatter_t.h"

/**
 * @brief API PRIVATE Contains metric type constants
 */
typedef enum taos_metric_type { TAOS_COUNTER, TAOS_GAUGE, TAOS_HISTOGRAM, TAOS_SUMMARY } taos_metric_type_t;

/**
 * @brief API PRIVATE Maps metric type constants to human readable string values
 */
extern char *taos_metric_type_map[4];

/**
 * @brief API PRIVATE An opaque struct to users containing metric metadata; one or more metric samples; and a metric
 * formatter for locating metric samples and exporting metric data
 */
struct taos_metric {
  taos_metric_type_t type;            /**< metric_type      The type of metric */
  char *name;                   /**< name             The name of the metric */
  const char *help;                   /**< help             The help output for the metric */
  taos_map_t *samples;                /**< samples          Map comprised of samples for the given metric */
  size_t label_key_count;             /**< label_keys_count The count of labe_keys*/
  taos_metric_formatter_t *formatter; /**< formatter        The metric formatter  */
  pthread_rwlock_t *rwlock;           /**< rwlock           Required for locking on certain non-atomic operations */
  const char **label_keys;            /**< labels           Array comprised of const char **/
};

#endif  // TAOS_METRIC_T_H
