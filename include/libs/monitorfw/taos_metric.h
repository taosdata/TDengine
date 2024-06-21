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

/**
 * @file taos_metric.h
 * @brief Functions for retrieving metric samples from metrics given an ordered set of labels
 */

#ifndef TAOS_METRIC_H
#define TAOS_METRIC_H

#include "taos_metric_sample.h"

struct taos_metric;
/**
 * @brief A prometheus metric.
 *
 * Reference: https://prometheus.io/docs/concepts/data_model
 */
typedef struct taos_metric taos_metric_t;

/**
 * @brief Returns a taos_metric_sample_t*. The order of label_values is significant.
 *
 * You may use this function to cache metric samples to avoid sample lookup. Metric samples are stored in a hash map
 * with O(1) lookups in average case; nonethless, caching metric samples and updating them directly might be
 * preferrable in performance-sensitive situations.
 *
 * @param self The target taos_metric_t*
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the counter's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A taos_metric_sample_t*
 */
taos_metric_sample_t *taos_metric_sample_from_labels(taos_metric_t *self, const char **label_values);

#endif  // TAOS_METRIC_H
