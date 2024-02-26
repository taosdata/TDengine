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

#ifndef TAOS_COLLECTOR_H
#define TAOS_COLLECTOR_H

#include "taos_map.h"
#include "taos_metric.h"

/**
 * @file taos_collector.h
 * @brief A Prometheus collector returns a collection of metrics
 */

/**
 * @brief A prometheus collector calls collect to prepare metrics and return them to the registry to which it is
 * registered.
 */
typedef struct taos_collector taos_collector_t;

/**
 * @brief The function responsible for preparing metric data and returning metrics for a given collector.
 *
 * If you use the default collector registry, this should not concern you. If you are using a custom collector, you may
 * set this function on your collector to do additional work before returning the contained metrics.
 *
 * @param self The target taos_collector_t*
 * @return The taos_map_t* containing the collected metrics
 */
typedef taos_map_t *taos_collect_fn(taos_collector_t *self);

/**
 * @brief Create a collector
 * @param name The name of the collector. The name MUST NOT be default or process.
 * @return The constructed taos_collector_t*
 */
taos_collector_t *taos_collector_new(const char *name);

/**
 * @brief Destroy a collector. You MUST set self to NULL after destruction.
 * @param self The target taos_collector_t*
 * @return A non-zero integer value upon failure.
 */
int taos_collector_destroy(taos_collector_t *self);

/**
 * @brief Frees a collector passed as a void pointer. You MUST set self to NULL after destruction.
 * @param gen The target taos_collector_t* represented as a void*
 */
void taos_collector_free_generic(void *gen);

/**
 * @brief Destroys a collector passed as a void pointer. You MUST set self to NULL after destruction.
 * @param gen The target taos_collector_t* represented as a void*
 * @return A non-zero integer value upon failure.
 */
int taos_collector_destroy_generic(void *gen);

/**
 * @brief Add a metric to a collector
 * @param self The target taos_collector_t*
 * @param metric the taos_metric_t* to add to the taos_collector_t* passed as self.
 * @return A non-zero integer value upon failure.
 */
int taos_collector_add_metric(taos_collector_t *self, taos_metric_t *metric);

int taos_collector_remove_metric(taos_collector_t *self, const char* key);

taos_metric_t* taos_collector_get_metric(taos_collector_t *self, char *metric_name);

/**
 * @brief The collect function is responsible for doing any work involving a set of metrics and then returning them
 *        for metric exposition.
 * @param self The target taos_collector_t*
 * @param fn The taos_collect_fn* which will be responsible for handling any metric collection operations before
 *           returning the collected metrics for exposition.
 * @return A non-zero integer value upon failure.
 */
int taos_collector_set_collect_fn(taos_collector_t *self, taos_collect_fn *fn);

#endif  // TAOS_COLLECTOR_H
