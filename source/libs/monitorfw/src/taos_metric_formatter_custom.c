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

int taos_metric_formatter_load_sample_new(taos_metric_formatter_t *self, taos_metric_sample_t *sample, 
                                      char *ts, char *format, char *metricName, int32_t metric_type,
                                      SJson *arrayMetricGroups) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  char* start = strstr(sample->l_value, "{");
  char* end = strstr(sample->l_value, "}");

  int32_t len = end -start;

  char* keyvalues = taosMemoryMalloc(len);
  memset(keyvalues, 0, len);
  memcpy(keyvalues, start + 1, len - 1);

  int32_t count = taos_monitor_count_occurrences(keyvalues, ",");

  char** keyvalue = taosMemoryMalloc(sizeof(char*) * (count + 1));
  memset(keyvalue, 0, sizeof(char*) * (count + 1));
  taos_monitor_split_str(keyvalue, keyvalues, ",");

  char** arr = taosMemoryMalloc(sizeof(char*) * (count + 1) * 2);
  memset(arr, 0, sizeof(char*) * (count + 1) * 2);

  bool isfound = true;
  for(int32_t i = 0; i < count + 1; i++){
    char* str = *(keyvalue + i);

    char** pair = arr + i * 2;
    taos_monitor_split_str(pair, str, "=");

    taos_monitor_strip(pair[1]);
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

  double old_value = 0;
#define USE_EXCHANGE
#ifdef USE_EXCHANGE
  if ((r = taos_metric_sample_exchange(sample, 0, &old_value)) != 0) return r;
#else
  old_value = sample->r_value;
  taos_metric_sample_set(sample, 0);
#endif

  if ((r = tjsonAddDoubleToObject(metric, "value", old_value)) != 0) return r;
  if ((r = tjsonAddDoubleToObject(metric, "type", metric_type)) != 0) return r;
  if ((r = tjsonAddItemToArray(metrics, metric)) != 0) return r;

  return 0;
}

int taos_metric_formatter_load_metric_new(taos_metric_formatter_t *self, taos_metric_t *metric, char *ts, char *format, 
                                          SJson* tableArray) {
  TAOS_TEST_PARA(self != NULL);
  if (self == NULL) return 1;

  int r = 0;

  int32_t size = strlen(metric->name);
  char* name = taosMemoryMalloc(size + 1);
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
    if ((r = tjsonGetStringValue(table, "name", tableName)) != 0) {
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
      taosMemoryFreeClear(name);
      return r;
    }
  }
  
  int32_t sample_count = 0;
  for (taos_linked_list_node_t *current_node = metric->samples->keys->head; current_node != NULL;
       current_node = current_node->next) {
    const char *key = (const char *)current_node->item;
    if (metric->type == TAOS_HISTOGRAM) {

    } else {
      taos_metric_sample_t *sample = (taos_metric_sample_t *)taos_map_get(metric->samples, key);
      if (sample == NULL) return 1;
      r = taos_metric_formatter_load_sample_new(self, sample, ts, format, arr[1], metric->type, arrayMetricGroups);
      if (r) return r;
    }
    sample_count++;
  }

  if(!isFound && sample_count > 0){
    if ((r = tjsonAddItemToArray(tableArray, table)) != 0) {
      taosMemoryFreeClear(name);
      return r;
    }
  }
  else{
    if(table != NULL) tjsonDelete(table);
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
          sprintf(tmp, "fail to get metric(%d):%s", count, metric_name);
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
      sprintf(tmp, "list count:%d", count);
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