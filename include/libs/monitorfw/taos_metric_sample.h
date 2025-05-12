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
 * @file taos_metric_sample.h
 * @brief Functions for interfacting with metric samples directly
 */

#ifndef TAOS_METRIC_SAMPLE_H
#define TAOS_METRIC_SAMPLE_H

struct taos_metric_sample;
/**
 * @brief Contains the specific metric and value given the name and label set
 * Reference: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
 */
typedef struct taos_metric_sample taos_metric_sample_t;

/**
 * @brief Add the r_value to the sample. The value must be greater than or equal to zero.
 * @param self The target taos_metric_sample_t*
 * @param r_value The double to add to taos_metric_sample_t* provided by self
 * @return Non-zero integer value upon failure
 */
int taos_metric_sample_add(taos_metric_sample_t *self, double r_value);

/**
 * @brief Subtract the r_value from the sample.
 *
 * This operation MUST be called a sample derived from a gauge metric.
 * @param self The target taos_metric_sample_t*
 * @param r_value The double to subtract from the taos_metric_sample_t* provided by self
 * @return Non-zero integer value upon failure
 */
int taos_metric_sample_sub(taos_metric_sample_t *self, double r_value);

/**
 * @brief Set the r_value of the sample.
 *
 * This operation MUST be called on a sample derived from a gauge metric.
 * @param self The target taos_metric_sample_t*
 * @param r_value The double which will be set to the taos_metric_sample_t* provided by self
 * @return Non-zero integer value upon failure
 */
int taos_metric_sample_set(taos_metric_sample_t *self, double r_value);

int taos_metric_sample_exchange(taos_metric_sample_t *self, double r_value, double* old_value);

#endif  // TAOS_METRIC_SAMPLE_H
