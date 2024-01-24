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

#ifndef TAOS_METRIC_FORMATTER_CUSTOMV2_I_H
#define TAOS_METRIC_FORMATTER_CUSTOMV2_I_H

#define ALLOW_FORBID_FUNC

#include <stdint.h>
#include "tjson.h"

int taos_metric_formatter_load_sample_new(taos_metric_formatter_t *self, taos_metric_sample_t *sample, 
                                      char *ts, char *format, char *metricName, int32_t metric_type, 
                                      SJson *arrayMetricGroups);
int taos_metric_formatter_load_metric_new(taos_metric_formatter_t *self, taos_metric_t *metric, char *ts, char *format, 
                                          SJson* tableArray);                                      
int taos_metric_formatter_load_metrics_new(taos_metric_formatter_t *self, taos_map_t *collectors, char *ts, 
                                            char *format, SJson* tableArray);
#endif  // TAOS_METRIC_FORMATTER_CUSTOMV2_I_H