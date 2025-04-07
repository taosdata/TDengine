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

#ifndef _TD_METRICS_INT_H_
#define _TD_METRICS_INT_H_

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "metrics.h"
#include "tarray.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

// Metric types

// Initialization and cleanup
int32_t tmetrics_init();
void    tmetrics_cleanup();

// Configuration
bool    tmetrics_is_high_level_enabled();
void    tmetrics_enable_high_level(bool enable);
int32_t tmetrics_set_collection_interval(int64_t interval_ms);
int32_t tmetrics_set_log_path(const char* path);

// Write metrics management
SWriteMetricsEx* tmetrics_get_write_metrics();
SWriteMetricsEx* tmetrics_get_vnode_write_metrics(int32_t vgId);
void             tmetrics_init_write_metrics(SWriteMetricsEx* metrics, int32_t vgId);
void             tmetrics_collect_write_metrics(SWriteMetricsEx* metrics);
void             tmetrics_log_write_metrics(SWriteMetricsEx* metrics, const char* log_file);
void             tmetrics_collect_all_vnodes();
void             tmetrics_log_all_vnodes(const char* log_file);

// Write metrics recording
void tmetrics_record_write_begin(SWriteMetricsEx* metrics, int32_t vgId, int32_t rows, int32_t bytes);
void tmetrics_record_write_end(SWriteMetricsEx* metrics, int32_t vgId, int64_t start_time, bool is_commit);
void tmetrics_record_merge(SWriteMetricsEx* metrics, int32_t vgId, int64_t merge_time);
void tmetrics_record_blocked_commit(SWriteMetricsEx* metrics, int32_t vgId);
void tmetrics_update_stt_trigger(SWriteMetricsEx* metrics, int32_t vgId, int32_t value);

// Query metrics management
SQueryMetricsEx* tmetrics_get_query_metrics();
void             tmetrics_init_query_metrics(SQueryMetricsEx* metrics);
void             tmetrics_collect_query_metrics(SQueryMetricsEx* metrics);
void             tmetrics_log_query_metrics(SQueryMetricsEx* metrics, const char* log_file);

// Stream metrics management
SStreamMetricsEx* tmetrics_get_stream_metrics();
void              tmetrics_init_stream_metrics(SStreamMetricsEx* metrics);
void              tmetrics_collect_stream_metrics(SStreamMetricsEx* metrics);
void              tmetrics_log_stream_metrics(SStreamMetricsEx* metrics, const char* log_file);

// Custom metrics management
int32_t tmetrics_register_custom_metric(SMetricDef* def, void* collector);
int32_t tmetrics_unregister_custom_metric(const char* name);

#ifdef __cplusplus
}
#endif

#endif  // _TD_METRICS_INT_H_