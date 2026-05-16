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
 * @file taos_gauge.h
 * @brief https://prometheus.io/docs/concepts/metric_types/#gauge
 */

#ifndef TAOS_GAUGE_H
#define TAOS_GAUGE_H

#include <stdlib.h>

#include "taos_metric.h"

/**
 * @brief A prometheus gauge.
 *
 * References
 * * See https://prometheus.io/docs/concepts/metric_types/#gauge
 */
typedef taos_metric_t taos_gauge_t;

/**
 * @brief Constructs a taos_gauge_t*
 * @param name The name of the metric
 * @param help The metric description
 * @param label_key_count The number of labels associated with the given metric. Pass 0 if the metric does not
 *                        require labels.
 * @param label_keys A collection of label keys. The number of keys MUST match the value passed as label_key_count. If
 *                   no labels are required, pass NULL. Otherwise, it may be convenient to pass this value as a
 *                   literal.
 * @return The constructed taos_guage_t*
 *
 *     // An example with labels
 *     taos_gauge_new("foo", "foo is a gauge with labels", 2, (const char**) { "one", "two" });
 *
 *     // An example without labels
 *     taos_gauge_new("foo", "foo is a gauge without labels", 0, NULL);
 */
taos_gauge_t *taos_gauge_new(const char *name, const char *help, size_t label_key_count, const char **label_keys);

/**
 * @brief Destroys a taos_gauge_t*. You must set self to NULL after destruction. A non-zero integer value will be
 *        returned on failure.
 * @param self The target taos_gauge_t*
 * @return A non-zero integer value upon failure
 */
int taos_gauge_destroy(taos_gauge_t *self);

/**
 * @brief Increment the taos_gauge_t* by 1.
 * @param self The target  taos_gauger_t*
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the gauge's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure
 * *Example*
 *
 *     // An example with labels
 *     taos_gauge_inc(foo_gauge, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_gauge_inc(foo_gauge, NULL);
 */
int taos_gauge_inc(taos_gauge_t *self, const char **label_values);

/**
 * @brief Decrement the taos_gauge_t* by 1.
 * @param self The target  taos_gauger_t*
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the gauge's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 * *Example*
 *
 *     // An example with labels
 *     taos_gauge_dec(foo_gauge, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_gauge_dec(foo_gauge, NULL);
 */
int taos_gauge_dec(taos_gauge_t *self, const char **label_values);

/**
 * @brief Add the value to the taos_gauge_t*.
 * @param self The target taos_gauge_t*
 * @param r_value The double to add to the taos_gauge_t passed as self.
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the gauge's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_gauge_add(foo_gauge 22, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_gauge_add(foo_gauge, 22, NULL);
 */
int taos_gauge_add(taos_gauge_t *self, double r_value, const char **label_values);

/**
 * @brief Subtract the value to the taos_gauge. A non-zero integer value will be returned on failure.
 * @param self The target taos_gauge_t*
 * @param r_value The double to add to the taos_gauge_t passed as self.
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the gauge's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_gauge_sub(foo_gauge 22, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_gauge_sub(foo_gauge, 22, NULL);
 */
int taos_gauge_sub(taos_gauge_t *self, double r_value, const char **label_values);

/**
 * @brief Set the value for the taos_gauge_t*
 * @param self The target taos_gauge_t*
 * @param r_value The double to which the taos_gauge_t* passed as self will be set
 * @param label_values The label values associated with the metric sample being updated. The number of labels must
 *                     match the value passed to label_key_count in the gauge's constructor. If no label values are
 *                     necessary, pass NULL. Otherwise, It may be convenient to pass this value as a literal.
 * @return A non-zero integer value upon failure.
 *
 * *Example*
 *
 *     // An example with labels
 *     taos_gauge_set(foo_gauge 22, (const char**) { "bar", "bang" });
 *
 *     // An example without labels
 *     taos_gauge_set(foo_gauge, 22, NULL);
 */
int taos_gauge_set(taos_gauge_t *self, double r_value, const char **label_values);

#endif  // TAOS_GAUGE_H
