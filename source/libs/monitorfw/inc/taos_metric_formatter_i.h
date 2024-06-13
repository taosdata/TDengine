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

#ifndef TAOS_METRIC_FORMATTER_I_H
#define TAOS_METRIC_FORMATTER_I_H

// Private
#include "taos_metric_formatter_t.h"
#include "taos_metric_t.h"

/**
 * @brief API PRIVATE taos_metric_formatter constructor
 */
taos_metric_formatter_t *taos_metric_formatter_new();

/**
 * @brief API PRIVATE taos_metric_formatter destructor
 */
int taos_metric_formatter_destroy(taos_metric_formatter_t *self);

/**
 * @brief API PRIVATE Loads the help text
 */
int taos_metric_formatter_load_help(taos_metric_formatter_t *self, const char *name, const char *help);

/**
 * @brief API PRIVATE Loads the type text
 */
int taos_metric_formatter_load_type(taos_metric_formatter_t *self, const char *name, taos_metric_type_t metric_type);

/**
 * @brief API PRIVATE Loads the formatter with a metric sample L-value
 * @param name The metric name
 * @param suffix The metric suffix. This is applicable to Summary and Histogram metric types.
 * @param label_count The number of labels for the given metric.
 * @param label_keys An array of constant strings.
 * @param label_values An array of constant strings.
 *
 * The number of const char **and taos_label_value must be the same.
 */
int taos_metric_formatter_load_l_value(taos_metric_formatter_t *metric_formatter, const char *name, const char *suffix,
                                       size_t label_count, const char **label_keys, const char **label_values);

/**
 * @brief API PRIVATE Loads the formatter with a metric sample
 */
int taos_metric_formatter_load_sample(taos_metric_formatter_t *metric_formatter, taos_metric_sample_t *sample, 
                                        char *ts, char *format);

/**
 * @brief API PRIVATE Loads a metric in the string exposition format
 */
int taos_metric_formatter_load_metric(taos_metric_formatter_t *self, taos_metric_t *metric, char *ts, char *format);

/**
 * @brief API PRIVATE Loads the given metrics
 */
int taos_metric_formatter_load_metrics(taos_metric_formatter_t *self, taos_map_t *collectors, char *ts, char *format);

/**
 * @brief API PRIVATE Clear the underlying string_builder
 */
int taos_metric_formatter_clear(taos_metric_formatter_t *self);

/**
 * @brief API PRIVATE Returns the string built by taos_metric_formatter
 */
char *taos_metric_formatter_dump(taos_metric_formatter_t *metric_formatter);

#endif  // TAOS_METRIC_FORMATTER_I_H
