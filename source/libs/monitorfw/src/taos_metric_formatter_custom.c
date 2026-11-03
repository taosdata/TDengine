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

#define ALLOW_FORBID_FUNC

#include <stdio.h>
#include "taos_metric_formatter_i.h"
#include "taos_metric_sample_t.h"
#include "tjson.h"
#include "taos_monitor_util_i.h"
#include "taos_test.h"
#include "tdef.h"
#include "taos_collector_t.h"
#include "taos_log.h"

int taos_metric_formatter_load_sample_new(taos_metric_formatter_t *self, const char *l_value, double value,
                                      char *ts, char *format, char *metricName, int32_t metric_type,
                                      SJson *arrayMetricGroups) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  char* start = strstr(l_value, "{");
  char* end = strstr(l_value, "}");

  int32_t len = end -start;

  char* keyvalues = taosMemoryMalloc(len);
  if (keyvalues == NULL) return 1;
  memset(keyvalues, 0, len);
  memcpy(keyvalues, start + 1, len - 1);

  int32_t count = taos_monitor_count_occurrences(keyvalues, ",");

  char** keyvalue = taosMemoryMalloc(sizeof(char*) * (count + 1));
  if (keyvalue == NULL) {
    taosMemoryFreeClear(keyvalues);
    return 1;
  }
  memset(keyvalue, 0, sizeof(char*) * (count + 1));
  taos_monitor_split_str(keyvalue, keyvalues, ",");

  char** arr = taosMemoryMalloc(sizeof(char*) * (count + 1) * 2);
  if (arr == NULL) {
    taosMemoryFreeClear(keyvalue);
    taosMemoryFreeClear(keyvalues);
    return 1;
  }
  memset(arr, 0, sizeof(char*) * (count + 1) * 2);

  bool isfound = true;
  for(int32_t i = 0; i < count + 1; i++){
    char* str = *(keyvalue + i);

    char** pair = arr + i * 2;
    // Split the label token on '=' into at most two slots (pair[0] = key,
    // pair[1] = value). The original code called taos_monitor_split_str(pair, str, "=")
    // which wrote one pointer per strsep token into the fixed 2-slot 'pair', so a
    // value containing multiple '=' characters (e.g. an untrusted RPC username such
    // as "a=b=c=d=e") overflowed 'arr' with out-of-bounds heap pointer writes and
    // corrupted the root taosd process. Bound the split to two slots and ignore any
    // extra '=' so the write can never escape 'arr' (malloc'd as (count + 1) * 2).
    char* lasts = NULL;
    char* token = strsep(&str, "=");
    pair[0] = token;
    token = strsep(&str, "=");
    pair[1] = token;  // NULL when the token has no '=' (a malformed label)

    // A label value that contains the ',' delimiter (e.g. RPC username "a,b") makes
    // the outer ',' split emit a token without '=', leaving pair[1] == NULL. The
    // original code passed that NULL straight into taos_monitor_strip -> strlen(NULL)
    // and crashed taosd. Guard against NULL and fall back to an empty value so the
    // monitor report is built safely instead of segfaulting.
    if (pair[1] != NULL) {
      taos_monitor_strip(pair[1]);
    } else {
      pair[1] = (char*)"";
    }
  }

  int32_t table_size = tjsonGetArraySize(arrayMetricGroups);

  SJson* item = NULL;
  for(int32_t i = 0; i < table_size; i++){
    SJson *cur = tjsonGetArrayItem(arrayMetricGroups, i);

    SJson* tag = tjsonGetObjectItem(cur, "tags");

    if(taos_monitor_is_match(tag, arr, count + 1)) {
      item = cur;
      break;
    }
  }

  SJson* metrics = NULL;
  if(item == NULL) {
    item = tjsonCreateObject();

    SJson* arrayTag = tjsonCreateArray();
    for(int32_t i = 0; i < count + 1; i++){
      char** pair = arr + i * 2;

      char* key = *pair;
      char* value = *(pair + 1);

      SJson* tag = tjsonCreateObject();
      if ((r = tjsonAddStringToObject(tag, "name", key)) != 0) {
        taosMemoryFreeClear(arr);
        taosMemoryFreeClear(keyvalue);
        taosMemoryFreeClear(keyvalues);
        return r;
      }
      if ((r = tjsonAddStringToObject(tag, "value", value)) != 0) {
        taosMemoryFreeClear(arr);
        taosMemoryFreeClear(keyvalue);
        taosMemoryFreeClear(keyvalues);
        return r;
      }
      if ((r = tjsonAddItemToArray(arrayTag, tag)) != 0) {
        taosMemoryFreeClear(arr);
        taosMemoryFreeClear(keyvalue);
        taosMemoryFreeClear(keyvalues);
        return r;
      }
    }
    if ((r = tjsonAddItemToObject(item, "tags", arrayTag)) != 0) {
      taosMemoryFreeClear(arr);
      taosMemoryFreeClear(keyvalue);
      taosMemoryFreeClear(keyvalues);
      return r;
    }

    metrics = tjsonCreateArray();
    if ((r = tjsonAddItemToObject(item, "metrics", metrics)) != 0) {
      taosMemoryFreeClear(arr);
      taosMemoryFreeClear(keyvalue);
      taosMemoryFreeClear(keyvalues);
      return r;
    }

    if ((r = tjsonAddItemToArray(arrayMetricGroups, item)) != 0) {
      taosMemoryFreeClear(arr);
      taosMemoryFreeClear(keyvalue);
      taosMemoryFreeClear(keyvalues);
      return r;
    }
  }
  else{
    metrics = tjsonGetObjectItem(item, "metrics");
  }

  taosMemoryFreeClear(arr);
  taosMemoryFreeClear(keyvalue);
  taosMemoryFreeClear(keyvalues);

  SJson* metric = tjsonCreateObject();
  if ((r = tjsonAddStringToObject(metric, "name", metricName)) != 0) return r;

  // [TD-SEC-2026-ITEM84] The value was already atomically exchanged (and the counter reset) while the
  // sample was still protected by metric->samples->rwlock, during the snapshot phase in
  // taos_metric_formatter_load_metric_new. We must NOT touch the live sample here, so just use it.
  double old_value = value;

  if ((r = tjsonAddDoubleToObject(metric, "value", old_value)) != 0) return r;
  if ((r = tjsonAddDoubleToObject(metric, "type", metric_type)) != 0) return r;
  if ((r = tjsonAddItemToArray(metrics, metric)) != 0) return r;

  return 0;
}

// [TD-SEC-2026-ITEM84] Per-sample snapshot captured under metric->samples->rwlock so the report can be
// built fully lock-free afterwards. Holds an independent copy of the label string plus the atomically
// exchanged counter value, so it does not depend on the live sample object's lifetime (a concurrent
// taos_map_delete with free_value_fn = taos_metric_sample_free_generic would otherwise free it -> UAF).
typedef struct {
  char  *l_value;  // copy of taos_metric_sample_t::l_value (freed after the report)
  double value;    // value taken via taos_metric_sample_exchange under the lock
} taos_monitor_sample_snapshot_t;

int taos_metric_formatter_load_metric_new(taos_metric_formatter_t *self, taos_metric_t *metric, char *ts, char *format, 
                                          SJson* tableArray) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  int32_t size = strlen(metric->name);
  char* name = taosMemoryMalloc(size + 1);
  if (name == NULL) return 1;
  memset(name, 0, size + 1);
  memcpy(name, metric->name, size);
  char* arr[2] = {0}; //arr[0] is table name, arr[1] is metric name
  taos_monitor_split_str((char**)&arr, name, ":");

  bool isFound = false;
  SJson* table = NULL;
  SJson* arrayMetricGroups = NULL;

  int32_t table_count = tjsonGetArraySize(tableArray);
  for(int32_t i = 0; i < table_count; i++){
    SJson* table = tjsonGetArrayItem(tableArray, i);

    char tableName[MONITOR_TABLENAME_LEN] = {0};
    if ((r = tjsonGetStringValue1(table, "name", tableName, sizeof(tableName))) != 0) {
      if (!isFound && table != NULL) tjsonDelete(table);
      taosMemoryFreeClear(name);
      return r;
    }
    if(strcmp(tableName, arr[0]) == 0){
      isFound = true;
      arrayMetricGroups = tjsonGetObjectItem(table, "metric_groups");
      break;
    }
  }

  if(!isFound){
    table = tjsonCreateObject();

    if ((r = tjsonAddStringToObject(table, "name", arr[0])) != 0) {
      taosMemoryFreeClear(name);
      return r;
    }

    arrayMetricGroups = tjsonCreateArray();
    if ((r = tjsonAddItemToObject(table, "metric_groups", arrayMetricGroups)) != 0) {
      if (!isFound && table != NULL) tjsonDelete(table);
      taosMemoryFreeClear(name);
      return r;
    }
  }
  
  int32_t sample_count = 0;

  // [TD-SEC-2026-ITEM84] Fix use-after-free (CWE-416) from security finding "item84".
  //
  // Root cause: the reporter walked metric->samples->keys while holding only the collector's
  // metrics->rwlock, never the sample map's own rwlock. Writers
  // (taos_counter_add -> taos_metric_sample_from_labels -> taos_map_set) take metric->samples->rwlock
  // (as a WRITE lock) and, past the rehash threshold, taos_map_ensure_space frees and replaces
  // metric->samples->keys -> UAF when the reporter reads freed nodes / key strings.
  //
  // The sample OBJECTS must also be protected: a concurrent delete
  // (dmMetricsCleanExpiredSamples -> ... -> taos_counter_delete -> taos_map_delete, whose free_value_fn
  // is taos_metric_sample_free_generic) frees a sample on another thread. Touching the sample after
  // dropping the lock would be a UAF on the value, not just the key.
  //
  // Fix: take metric->samples->rwlock (READ) FIRST, then read keys->size and allocate the snapshot
  // (no TOCTOU: size and the list walked are both observed under the same lock, and writers need the
  // WRITE lock so the list cannot change mid-snapshot). While holding the lock we copy, per sample,
  // BOTH its label string (strdup) AND its value (via taos_metric_sample_exchange, which also resets
  // the counter). The critical section is therefore only "O(n) string/value copies". After releasing
  // the lock we build the JSON purely from the snapshot, never touching the live map or sample
  // objects -> no UAF, and writers are only briefly contended during the snapshot copy, not for the
  // whole formatting cycle.
  r = pthread_rwlock_rdlock(metric->samples->rwlock);
  if (r) {
    TAOS_LOG("failed to rdlock metric samples in monitor report");
    if (!isFound && table != NULL) tjsonDelete(table);
    taosMemoryFreeClear(name);
    return r;
  }

  int32_t key_count = (int32_t)metric->samples->keys->size;  // read UNDER the lock -> no TOCTOU
  taos_monitor_sample_snapshot_t *snap =
      (taos_monitor_sample_snapshot_t *)taosMemoryMalloc((key_count + 1) * sizeof(taos_monitor_sample_snapshot_t));
  if (snap == NULL) {
    TAOS_LOG("failed to alloc sample snapshot in monitor report");
    pthread_rwlock_unlock(metric->samples->rwlock);
    if (!isFound && table != NULL) tjsonDelete(table);
    taosMemoryFreeClear(name);
    return 1;
  }
  for (int32_t j = 0; j < key_count + 1; j++) snap[j].l_value = NULL;

  int32_t snap_idx = 0;
  for (taos_linked_list_node_t *current_node = metric->samples->keys->head; current_node != NULL;
       current_node = current_node->next) {
    const char *key = (const char *)current_node->item;
    taos_metric_sample_t *sample = (taos_metric_sample_t *)taos_map_get_withoutlock(metric->samples, key);
    if (sample == NULL) continue;  // cannot happen while holding the lock, but be safe
    double val = 0;
    if (taos_metric_sample_exchange(sample, 0, &val) != 0) continue;  // read + reset counter under lock
    char *lval_copy = (char *)taosMemoryMalloc(strlen(sample->l_value) + 1);
    if (lval_copy == NULL) {
      for (int32_t j = 0; j < snap_idx; j++) taosMemoryFree(snap[j].l_value);
      taosMemoryFree(snap);
      pthread_rwlock_unlock(metric->samples->rwlock);
      if (!isFound && table != NULL) tjsonDelete(table);
      taosMemoryFreeClear(name);
      return 1;
    }
    memcpy(lval_copy, sample->l_value, strlen(sample->l_value) + 1);
    snap[snap_idx].l_value = lval_copy;
    snap[snap_idx].value = val;
    snap_idx++;
  }

  r = pthread_rwlock_unlock(metric->samples->rwlock);
  if (r) {
    TAOS_LOG("failed to unlock metric samples in monitor report");
    for (int32_t j = 0; j < snap_idx; j++) taosMemoryFree(snap[j].l_value);
    taosMemoryFree(snap);
    if (!isFound && table != NULL) tjsonDelete(table);
    taosMemoryFreeClear(name);
    return r;
  }

  // Build the report fully lock-free from the snapshot. No live map / sample is touched here, so there
  // is no UAF and writers are never stalled.
  for (int32_t i = 0; i < snap_idx; i++) {
    if (metric->type == TAOS_HISTOGRAM) {
    } else {
      r = taos_metric_formatter_load_sample_new(self, snap[i].l_value, snap[i].value, ts, format, arr[1],
                                                metric->type, arrayMetricGroups);
      if (r) {
        for (int32_t j = 0; j < snap_idx; j++) taosMemoryFree(snap[j].l_value);
        taosMemoryFree(snap);
        if (!isFound && table != NULL) tjsonDelete(table);
        taosMemoryFreeClear(name);
        return r;
      }
    }
    sample_count++;
  }

  for (int32_t j = 0; j < snap_idx; j++) taosMemoryFree(snap[j].l_value);
  taosMemoryFree(snap);

  if(!isFound && sample_count > 0){
    if ((r = tjsonAddItemToArray(tableArray, table)) != 0) {
      if (!isFound && table != NULL) tjsonDelete(table);  // created but not added -> free it
      taosMemoryFreeClear(name);
      return r;
    }
  }
  else{
    // Only delete the table we created (when !isFound). When isFound it is an existing element of
    // tableArray and must NOT be deleted (would corrupt / drop the array entry).
    if(!isFound && table != NULL) tjsonDelete(table);
  }

  taosMemoryFreeClear(name);
  return r;
}

int taos_metric_formatter_load_metrics_new(taos_metric_formatter_t *self, taos_map_t *collectors, char *ts, 
                                            char *format, SJson* tableArray) {
  TAOS_TEST_PARA(self != NULL);
  int r = 0;

  for (taos_linked_list_node_t *current_node = collectors->keys->head; current_node != NULL;
       current_node = current_node->next) {
    const char *collector_name = (const char *)current_node->item;
    taos_collector_t *collector = (taos_collector_t *)taos_map_get(collectors, collector_name);
    if (collector == NULL) return 1;

    taos_map_t *metrics = collector->collect_fn(collector);
    if (metrics == NULL) return 1;

    //if(strcmp(collector->name, "custom") != 0 ){
      
      r = pthread_rwlock_wrlock(metrics->rwlock);
      if (r) {
        TAOS_LOG("failed to lock");
        return r;
      }

#ifdef TAOS_LOG_ENABLE
      int32_t count = 0;
#endif
      for (taos_linked_list_node_t *current_node = metrics->keys->head; current_node != NULL;
          current_node = current_node->next) {
#ifdef TAOS_LOG_ENABLE
        count++;
#endif
        const char *metric_name = (const char *)current_node->item;
        taos_metric_t *metric = (taos_metric_t *)taos_map_get_withoutlock(metrics, metric_name);
        if (metric == NULL) {
#ifdef TAOS_LOG_ENABLE
          char tmp[200] = {0};
          snprintf(tmp, sizeof(tmp), "fail to get metric(%d):%s", count, metric_name);
          TAOS_LOG(tmp);
#endif
          continue;;
        }
        r = taos_metric_formatter_load_metric_new(self, metric, ts, format, tableArray);
        if (r) {
          TAOS_LOG("failed to load metric");
          continue;
        }
      }

#ifdef TAOS_LOG_ENABLE
      char tmp[20] = {0};
      snprintf(tmp, sizeof(tmp), "list count:%d", count);
      TAOS_LOG(tmp);
#endif
      r = pthread_rwlock_unlock(metrics->rwlock);
      if (r) {
        TAOS_LOG("failed to unlock");
        return r;
      }
    //}
    //else{
     /*
      for (taos_linked_list_node_t *current_node = metrics->keys->head; current_node != NULL;
          current_node = current_node->next) {
        const char *metric_name = (const char *)current_node->item;
        taos_metric_t *metric = (taos_metric_t *)taos_map_get(metrics, metric_name);
        if (metric == NULL) return 1;
        r = taos_metric_formatter_load_metric(self, metric, ts, format);
        if (r) return r;
      }
      */ 
    //}
  }
  return r;
}