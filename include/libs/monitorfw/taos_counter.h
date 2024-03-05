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

#ifndef TAOS_COUNTER_H
#define TAOS_COUNTER_H

#include <stdlib.h>

#include "taos_metric.h"

/**
 * @file taos_counter.h
 * @brief https://prometheus.io/docs/concepts/metric_types/#counter
 */

/**
 * @brief A prometheus counter.
 *
 * References
 * * See https://prometheus.io/docs/concepts/metric_types/#counter
 */
typedef taos_metric_t taos_counter_t;

/**
 * @brief Construct a taos_counter_t*
 * @param name The name of the metric
 * @param help The metric description
 * @param label_key_count The number of labels associated with the given metric. Pass 0 if the metric does not
 *                        require labels.
 * @param label_keys A collection of label keys. The number of keys MUST match the value passed as label_key_count. If
 *                   no labels are required, pass NULL. Otherwise, it may be convenient to pass this value as a
 *                   literal.
 * @return The constructed taos_counter_t*
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_counter_new("foo", "foo is a counter with labels", 2, (const char**) { "one", "two" });
 *
 *     // An example without labels
 *     taos_counter_new("foo", "foo is a counter without labels", 0, NULL);
 */
taos_counter_t *taos_counter_new(const char *name, const char *help, size_t label_key_count, const char **label_keys);

/**
 * @brief Destroys a taos_counter_t*. You must set self to NULL after destruction. A non-zero integer value will be
 *        returned on failure.
 * @param self A taos_counter_t*
 * @return A non-zero integer value upon failure.
 */
int taos_counter_destroy(taos_counter_t *self);

/**
 * @brief Increment the taos_counter_t by 1. A non-zero integer value will be returned on failure.
 * @param self The target  taos_counter_t*
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the counter's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_counter_inc(foo_counter, (const char**) { "bar", "bang" });
 **
 *     // An example without labels
 *     taos_counter_inc(foo_counter, NULL);
 */
int taos_counter_inc(taos_counter_t *self, const char **label_values);

/**
 * @brief Add the value to the taos_counter_t*. A non-zero integer value will be returned on failure.
 * @param self The target  taos_counter_t*
 * @param r_value The double to add to the taos_counter_t passed as self. The value MUST be greater than or equal to 0.
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the counter's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_counter_add(foo_counter, 22, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_counter_add(foo_counter, 22, NULL);
 */
int taos_counter_add(taos_counter_t *self, double r_value, const char **label_values);

#endif  // TAOS_COUNTER_H
