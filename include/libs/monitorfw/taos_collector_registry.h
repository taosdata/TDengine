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
 * @file taos_collector_registry.h
 * @brief The collector registry registers collectors for metric exposition.
 */

#ifndef TAOS_REGISTRY_H
#define TAOS_REGISTRY_H

#include "taos_collector.h"
#include "taos_metric.h"

/**
 * @brief A taos_registry_t is responsible for registering metrics and briding them to the string exposition format
 */
typedef struct taos_collector_registry taos_collector_registry_t;

/**
 * @brief Initialize the default registry by calling taos_collector_registry_init within your program. You MUST NOT
 * modify this value.
 */
extern taos_collector_registry_t *TAOS_COLLECTOR_REGISTRY_DEFAULT;

/**
 * @brief Initializes the default collector registry and enables metric collection on the executing process
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_default_init(void);

/**
 * @brief Constructs a taos_collector_registry_t*
 * @param name The name of the collector registry. It MUST NOT be default.
 * @return The constructed taos_collector_registry_t*
 */
taos_collector_registry_t *taos_collector_registry_new(const char *name);

/**
 * @brief Destroy a collector registry. You MUST set self to NULL after destruction.
 * @param self The target taos_collector_registry_t*
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_destroy(taos_collector_registry_t *self);

/**
 * @brief Enable process metrics on the given collector registry
 * @param self The target taos_collector_registry_t*
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_enable_process_metrics(taos_collector_registry_t *self);

/**
 * @brief Registers a metric with the default collector on TAOS_DEFAULT_COLLECTOR_REGISTRY
 *
 * The metric to be registered MUST NOT already be registered with the given . If so, the program will
 * halt. It returns a taos_metric_t* to simplify metric creation and registration. Furthermore,
 * TAOS_DEFAULT_COLLECTOR_REGISTRY must be registered via taos_collector_registry_default_init() prior to calling this
 * function. The metric will be added to the default registry's default collector.
 *
 * @param metric The metric to register on TAOS_DEFAULT_COLLECTOR_REGISTRY*
 * @return The registered taos_metric_t*
 */
taos_metric_t *taos_collector_registry_must_register_metric(taos_metric_t *metric);

/**
 * @brief Registers a metric with the default collector on TAOS_DEFAULT_COLLECTOR_REGISTRY. Returns an non-zero integer
 * value on failure.
 *
 * See taos_collector_registry_must_register_metric.
 *
 * @param metric The metric to register on TAOS_DEFAULT_COLLECTOR_REGISTRY*
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_register_metric(taos_metric_t *metric);

int taos_collector_registry_deregister_metric(const char *key);

taos_metric_t *taos_collector_registry_get_metric(char* metric_name);

/**
 * @brief Register a collector with the given registry. Returns a non-zero integer value on failure.
 * @param self The target taos_collector_registry_t*
 * @param collector The taos_collector_t* to register onto the taos_collector_registry_t* as self
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_register_collector(taos_collector_registry_t *self, taos_collector_t *collector);

/**
 * @brief Returns a string in the default metric exposition format. The string MUST be freed to avoid unnecessary heap
 * memory growth.
 *
 * Reference: https://prometheus.io/docs/instrumenting/exposition_formats/
 *
 * @param self The target taos_collector_registry_t*
 * @return The string int he default metric exposition format.
 */
const char *taos_collector_registry_bridge(taos_collector_registry_t *self, char *ts, char *format);

int taos_collector_registry_clear_batch(taos_collector_registry_t *self);

const char *taos_collector_registry_bridge_new(taos_collector_registry_t *self, char *ts, char *format, char** prom_str);

/**
 *@brief Validates that the given metric name complies with the specification:
 *
 * Reference: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
 *
 * Returns a non-zero integer value on failure.
 *
 * @param self The target taos_collector_registry_t*
 * @param metric_name The metric name to validate
 * @return A non-zero integer value upon failure
 */
int taos_collector_registry_validate_metric_name(taos_collector_registry_t *self, const char *metric_name);

#endif  // TAOS_H
