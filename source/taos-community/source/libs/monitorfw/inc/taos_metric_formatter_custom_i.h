/**
 * Copyright 2019-2020 DigitalOcean Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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