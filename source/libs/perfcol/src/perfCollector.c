#include "../inc/perfCollector.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int64_t getCurrentTimeUs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

int32_t perfCollectorInit(SPerfCollector **ppCollector, const SPerfCollectionConfig *pConfig) {
  if (ppCollector == NULL) {
    return -1;
  }

  SPerfCollector *pCollector = (SPerfCollector *)calloc(1, sizeof(SPerfCollector));
  if (pCollector == NULL) {
    return -2;
  }

  if (pConfig != NULL) {
    pCollector->config = *pConfig;
  } else {
    pCollector->config = (SPerfCollectionConfig){.enable_query_metrics = 1,
                                                 .enable_system_metrics = 1,
                                                 .enable_cache_metrics = 1,
                                                 .enable_rpc_metrics = 1,
                                                 .collection_interval_ms = 1000,
                                                 .max_retained_records = 1000,
                                                 .sampling_rate = 100};
  }

  pCollector->queryMetricsCapacity = pCollector->config.max_retained_records;
  pCollector->queryMetrics = (SQueryPerfMetrics *)calloc(pCollector->queryMetricsCapacity, sizeof(SQueryPerfMetrics));
  if (pCollector->queryMetrics == NULL) {
    free(pCollector);
    return -3;
  }

  if (pthread_mutex_init(&pCollector->mutex, NULL) != 0) {
    free(pCollector->queryMetrics);
    free(pCollector);
    return -4;
  }

  pCollector->initialized = 1;
  *ppCollector = pCollector;
  return 0;
}

void perfCollectorCleanup(SPerfCollector *pCollector) {
  if (pCollector == NULL) {
    return;
  }

  pthread_mutex_destroy(&pCollector->mutex);
  if (pCollector->queryMetrics != NULL) {
    free(pCollector->queryMetrics);
  }
  free(pCollector);
}

int32_t perfCollectorRecordQueryStart(SPerfCollector *pCollector, const char *sql, const char *user, const char *db,
                                      int8_t queryType, int8_t priority, int64_t startTime) {
  if (pCollector == NULL || !pCollector->initialized || !pCollector->config.enable_query_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);

  if (pCollector->queryMetricsCount >= pCollector->queryMetricsCapacity) {
    pthread_mutex_unlock(&pCollector->mutex);
    return -2;
  }

  SQueryPerfMetrics *pMetric = &pCollector->queryMetrics[pCollector->queryMetricsCount];
  memset(pMetric, 0, sizeof(SQueryPerfMetrics));

  if (sql != NULL) {
    strncpy(pMetric->sql, sql, TSDB_QUERY_SQL_LEN - 1);
  }
  if (user != NULL) {
    strncpy(pMetric->user, user, TSDB_USER_LEN - 1);
  }
  if (db != NULL) {
    strncpy(pMetric->db, db, TSDB_DB_NAME_LEN - 1);
  }

  pMetric->start_time = startTime;
  pMetric->query_type = queryType;
  pMetric->priority = priority;

  pCollector->queryMetricsCount++;

  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorRecordQueryEnd(SPerfCollector *pCollector, const char *sql, int64_t endTime, int32_t affectedRows,
                                    int32_t errorCode) {
  if (pCollector == NULL || !pCollector->initialized || !pCollector->config.enable_query_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);

  for (int32_t i = pCollector->queryMetricsCount - 1; i >= 0; i--) {
    SQueryPerfMetrics *pMetric = &pCollector->queryMetrics[i];
    if (pMetric->end_time == 0 && (sql == NULL || strcmp(pMetric->sql, sql) == 0)) {
      pMetric->end_time = endTime;
      pMetric->affected_rows = affectedRows;
      pMetric->error_code = errorCode;
      pMetric->exec_time = endTime - pMetric->start_time;
      pthread_mutex_unlock(&pCollector->mutex);
      return 0;
    }
  }

  pthread_mutex_unlock(&pCollector->mutex);
  return -2;
}

int32_t perfCollectorUpdateSystemMetrics(SPerfCollector *pCollector, const SSystemPerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_system_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  pCollector->systemMetrics = *pMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorUpdateCacheMetrics(SPerfCollector *pCollector, const SCachePerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_cache_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  pCollector->cacheMetrics = *pMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorUpdateRpcMetrics(SPerfCollector *pCollector, const SRpcPerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_rpc_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  pCollector->rpcMetrics = *pMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorGetQueryMetrics(SPerfCollector *pCollector, SQueryPerfMetrics **ppMetrics, int32_t *pCount) {
  if (pCollector == NULL || ppMetrics == NULL || pCount == NULL || !pCollector->initialized) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);

  *pCount = pCollector->queryMetricsCount;
  if (*pCount > 0) {
    *ppMetrics = (SQueryPerfMetrics *)malloc(*pCount * sizeof(SQueryPerfMetrics));
    if (*ppMetrics == NULL) {
      pthread_mutex_unlock(&pCollector->mutex);
      return -2;
    }
    memcpy(*ppMetrics, pCollector->queryMetrics, *pCount * sizeof(SQueryPerfMetrics));
  } else {
    *ppMetrics = NULL;
  }

  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorGetSystemMetrics(SPerfCollector *pCollector, SSystemPerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_system_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  *pMetrics = pCollector->systemMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorGetCacheMetrics(SPerfCollector *pCollector, SCachePerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_cache_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  *pMetrics = pCollector->cacheMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorGetRpcMetrics(SPerfCollector *pCollector, SRpcPerfMetrics *pMetrics) {
  if (pCollector == NULL || pMetrics == NULL || !pCollector->initialized || !pCollector->config.enable_rpc_metrics) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);
  *pMetrics = pCollector->rpcMetrics;
  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int64_t perfCollectorGetCurrentTimestamp() { return getCurrentTimeUs() / 1000; }

const char *perfCollectorGetVersion() { return "1.0.0"; }

int32_t perfCollectorGetHealthStatus(SPerfCollector *pCollector) {
  if (pCollector == NULL || !pCollector->initialized) {
    return -1;
  }

  return 0;
}

int32_t perfCollectorExportToJson(SPerfCollector *pCollector, char **ppJsonStr) {
  if (pCollector == NULL || ppJsonStr == NULL || !pCollector->initialized) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);

  char *jsonStr = (char *)malloc(65536);
  if (jsonStr == NULL) {
    pthread_mutex_unlock(&pCollector->mutex);
    return -2;
  }

  int pos = snprintf(jsonStr, 65536, "{\n");

  pos += snprintf(jsonStr + pos, 65536 - pos, "  \"query_metrics\": {\n");
  pos += snprintf(jsonStr + pos, 65536 - pos, "    \"total_count\": %d,\n", pCollector->queryMetricsCount);

  pos += snprintf(jsonStr + pos, 65536 - pos, "  },\n");

  if (pCollector->config.enable_system_metrics) {
    pos += snprintf(jsonStr + pos, 65536 - pos, "  \"system_metrics\": {\n");
    pos += snprintf(jsonStr + pos, 65536 - pos, "    \"cpu_usage_percent\": %ld,\n",
                    pCollector->systemMetrics.cpu_usage_percent);
    pos += snprintf(jsonStr + pos, 65536 - pos, "    \"memory_usage_bytes\": %ld,\n",
                    pCollector->systemMetrics.memory_usage_bytes);
    pos += snprintf(jsonStr + pos, 65536 - pos, "    \"active_connections\": %ld\n",
                    pCollector->systemMetrics.active_connections);
    pos += snprintf(jsonStr + pos, 65536 - pos, "  },\n");
  }

  pos += snprintf(jsonStr + pos, 65536 - pos, "  \"config\": {\n");
  pos += snprintf(jsonStr + pos, 65536 - pos, "    \"enable_query_metrics\": %s,\n",
                  pCollector->config.enable_query_metrics ? "true" : "false");
  pos += snprintf(jsonStr + pos, 65536 - pos, "    \"collection_interval_ms\": %d\n",
                  pCollector->config.collection_interval_ms);
  pos += snprintf(jsonStr + pos, 65536 - pos, "  }\n");
  pos += snprintf(jsonStr + pos, 65536 - pos, "}\n");

  *ppJsonStr = jsonStr;

  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}

int32_t perfCollectorResetMetrics(SPerfCollector *pCollector) {
  if (pCollector == NULL || !pCollector->initialized) {
    return -1;
  }

  pthread_mutex_lock(&pCollector->mutex);

  pCollector->queryMetricsCount = 0;
  memset(&pCollector->systemMetrics, 0, sizeof(SSystemPerfMetrics));
  memset(&pCollector->cacheMetrics, 0, sizeof(SCachePerfMetrics));
  memset(&pCollector->rpcMetrics, 0, sizeof(SRpcPerfMetrics));

  pthread_mutex_unlock(&pCollector->mutex);
  return 0;
}