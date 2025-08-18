#include <dlfcn.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

typedef void* TAOS;
typedef void* TAOS_RES;
typedef TAOS* (*taos_connect_f)(const char*, const char*, const char*, const char*, int);
typedef int (*taos_query_a_f)(TAOS*, const char*, void (*)(void*, TAOS_RES*, int), void*);
typedef int (*taos_query_f)(TAOS*, const char*);
typedef void (*taos_free_result_f)(TAOS_RES*);
typedef void (*taos_close_f)(TAOS*);
typedef int (*taos_errno_f)(TAOS_RES*);
typedef const char* (*taos_errstr_f)(TAOS_RES*);
static taos_connect_f taos_connect_func = NULL;
static taos_query_a_f taos_query_a_func = NULL;
static taos_query_f       taos_query_func = NULL;
static taos_free_result_f taos_free_result_func = NULL;
static taos_close_f taos_close_func = NULL;
static taos_errno_f taos_errno_func = NULL;
static taos_errstr_f taos_errstr_func = NULL;

// Global array to store 16 SQL statements
static char* g_sql_statements[16] = {NULL};

// Function to generate table names for a given range
static char* generate_table_list(int start_idx, int count) {
  int   max_len = count * 10 + 1000;  // Estimate buffer size
  char* buffer = malloc(max_len);
  if (!buffer) return NULL;

  int pos = 0;
  pos += snprintf(buffer + pos, max_len - pos, "(");

  for (int i = 0; i < count; i++) {
    int table_num = start_idx + i + 1;
    if (i > 0) {
      pos += snprintf(buffer + pos, max_len - pos, ",'d%d'", table_num);
    } else {
      pos += snprintf(buffer + pos, max_len - pos, "'d%d'", table_num);
    }
  }

  pos += snprintf(buffer + pos, max_len - pos, ")");
  return buffer;
}

// Function to generate 16 SQL statements
static int generate_sql_statements() {
  const int tables_per_sql = 625;  // 10000 / 16 = 625

  for (int i = 0; i < 16; i++) {
    int   start_table = i * tables_per_sql;
    char* table_list = generate_table_list(start_table, tables_per_sql);
    if (!table_list) {
      printf("Failed to generate table list for SQL %d\n", i);
      return -1;
    }

    // Generate the complete SQL statement
    int sql_len = strlen("select tbname,last(*) from test.meters where tbname in ") + strlen(table_list) +
                  strlen(" partition by tbname;") + 100;

    g_sql_statements[i] = malloc(sql_len);
    if (!g_sql_statements[i]) {
      printf("Failed to allocate memory for SQL %d\n", i);
      free(table_list);
      return -1;
    }

    snprintf(g_sql_statements[i], sql_len,
             "select tbname,last(*) from test.meters where tbname in %s partition by tbname;", table_list);

    free(table_list);
  }

  return 0;
}

// Function to free SQL statements
static void free_sql_statements() {
  for (int i = 0; i < 16; i++) {
    if (g_sql_statements[i]) {
      free(g_sql_statements[i]);
      g_sql_statements[i] = NULL;
    }
  }
}

typedef struct {
    int thread_id;
    TAOS* taos;
    const char*      sql;
    int              queries_per_thread;
    pthread_mutex_t* mutex;
    struct timeval start_time;
    struct timeval end_time;
    double* thread_qps;
    int              max_query_nums;
    atomic_int       current_query_nums;
    atomic_int       completed_query_nums;
} ThreadParam;

static struct timeval global_start_time;
static struct timeval global_end_time;

static double calculate_qps(int count, double duration) {
    return duration > 0 ? count / duration : 0.0;
}

static int load_taos_functions(const char* lib_path) {
    void* handle = dlopen(lib_path, RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "Failed to load library %s: %s\n", lib_path, dlerror());
        return -1;
    }

    taos_connect_func = (taos_connect_f)dlsym(handle, "taos_connect");
    taos_query_func = (taos_query_f)dlsym(handle, "taos_query");
    taos_query_a_func = (taos_query_a_f)dlsym(handle, "taos_query_a");
    taos_free_result_func = (taos_free_result_f)dlsym(handle, "taos_free_result");
    taos_close_func = (taos_close_f)dlsym(handle, "taos_close");
    taos_errno_func = (taos_errno_f)dlsym(handle, "taos_errno");
    taos_errstr_func = (taos_errstr_f)dlsym(handle, "taos_errstr");

    if (!taos_connect_func || !taos_query_a_func || !taos_free_result_func || 
        !taos_close_func || !taos_errno_func || !taos_errstr_func) {
        fprintf(stderr, "Failed to load some functions: %s\n", dlerror());
        dlclose(handle);
        return -1;
    }

    return 0;
}

static void queryCallback(void* param, TAOS_RES* res, int code) {
  ThreadParam* thread_param = (ThreadParam*)param;
  if (code != 0) {
    printf("query failed case: code = %d\n", code);
  }
  atomic_fetch_sub(&thread_param->current_query_nums, 1);
  atomic_fetch_add(&thread_param->completed_query_nums, 1);

  taos_free_result_func(res);
}

static void* query_thread(void* arg) {
    ThreadParam* param = (ThreadParam*)arg;
    
    // 初始化线程参数
    atomic_init(&param->current_query_nums, 0);
    atomic_init(&param->completed_query_nums, 0);

    gettimeofday(&param->start_time, NULL);

    while (atomic_load(&param->completed_query_nums) < param->queries_per_thread) {
      if (atomic_load(&param->current_query_nums) < param->max_query_nums) {
        atomic_fetch_add(&param->current_query_nums, 1);
        taos_query_a_func(param->taos, param->sql, queryCallback, param);
      } else {
        usleep(50);
      }
    }

    gettimeofday(&param->end_time, NULL);

    double start_sec = param->start_time.tv_sec + param->start_time.tv_usec / 1000000.0;
    double end_sec = param->end_time.tv_sec + param->end_time.tv_usec / 1000000.0;
    double duration = end_sec - start_sec;
    *param->thread_qps = calculate_qps(param->queries_per_thread, duration);
    return NULL;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("Usage: %s <thread_count>\n", argv[0]);
    return 1;
  }

  int         query_mode = atoi(argv[1]);
  const char* lib_path = "../../debug/build/lib/libtaos.so";
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* pass = "taosdata";
  const char* db = "test";
  int         thread_count = 0;
  int         queries_per_thread = 0;
  int         max_query_nums = 0;
  int         qps_rate = 0;
  const char* sql = "";

  if (query_mode == 0) {
    sql = "SELECT last(ts,r32) FROM test.d1;";
    thread_count = 16;
    queries_per_thread = 10000;
    max_query_nums = 5;
    qps_rate = 1;
  } else if (query_mode == 1) {
    sql =
        "select tbname,last(*) from test.meters where tbname in "
        "('d1','d2','d3','d4','d5','d6','d7','d8','d9','d10','d11','d12','d13','d14','d15','d16','d17','d18','d19','"
        "d20',"
        "'d21','d22','d23','d24','d25','d26','d27','d28','d29','d30','d31','d32','d33','d34','d35','d36','d37','d38','"
        "d39','d40','d41','d42','d43','d44','d45','d46','d47','d48','d49','d50','d51','d52','d53','d54','d55','d56','"
        "d57','d58','d59','d60','d61','d62','d63','d64','d65','d66','d67','d68','d69','d70','d71','d72','d73','d74','"
        "d75','d76','d77','d78','d79','d80','d81','d82','d83','d84','d85','d86','d87','d88','d89','d90','d91','d92','"
        "d93','d94','d95','d96','d97','d98','d99','d100') "
        "partition by tbname;";
    thread_count = 16;
    queries_per_thread = 100;
    max_query_nums = 5;
    qps_rate = 100;
  } else if (query_mode == 2) {
    generate_sql_statements();
    thread_count = 16;
    queries_per_thread = 100;
    max_query_nums = 5;
    qps_rate = 625;
  } else if (query_mode == 3) {
    sql = "select tbname,last(*) from test.meters partition by tbname;";
    thread_count = 16;
    queries_per_thread = 2;
    max_query_nums = 1;
    qps_rate = 1000000;
  } else {
    printf("Usage: %s <query_mode>\n", argv[0]);
    return 1;
  }

  if (load_taos_functions(lib_path) != 0) {
    fprintf(stderr, "Failed to load TDengine functions\n");
    return 1;
  }

  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

  ThreadParam* params = malloc(thread_count * sizeof(ThreadParam));
  pthread_t*   threads = malloc(thread_count * sizeof(pthread_t));
  double*      thread_qps = malloc(thread_count * sizeof(double));

  if (query_mode == 2) {
    printf("Starting %d threads, each executing %d queries, max_query_nums: %d\n sql: %s\n", thread_count,
           queries_per_thread, max_query_nums, g_sql_statements[0]);
  } else {
    printf("Starting %d threads, each executing %d queries, max_query_nums: %d, sql: %s\n", thread_count,
           queries_per_thread, max_query_nums, sql);
  }

  gettimeofday(&global_start_time, NULL);

  for (int i = 0; i < thread_count; i++) {
    TAOS* taos = taos_connect_func(host, user, pass, db, 0);
    if (taos == NULL) {
      fprintf(stderr, "Failed to connect to TDengine\n");
      return 1;
    }
    params[i].thread_id = i;
    params[i].taos = taos;
    if (query_mode == 2) {
      params[i].sql = g_sql_statements[i];
    } else {
      params[i].sql = (const char*)sql;
    }
    params[i].queries_per_thread = queries_per_thread;
    params[i].mutex = &mutex;
    params[i].thread_qps = &thread_qps[i];
    thread_qps[i] = 0.0;
    params[i].max_query_nums = max_query_nums;
    if (pthread_create(&threads[i], NULL, query_thread, &params[i]) != 0) {
      fprintf(stderr, "Failed to create thread %d\n", i);
      return 1;
    }
  }

  for (int i = 0; i < thread_count; i++) {
    pthread_join(threads[i], NULL);
  }

  gettimeofday(&global_end_time, NULL);

  // 计算每个线程QPS的总和
  double total_thread_qps = 0.0;
  for (int i = 0; i < thread_count; i++) {
    total_thread_qps += thread_qps[i];
  }
  total_thread_qps = total_thread_qps * qps_rate;

  printf("\nTotal number of devices (sum of all threads): %.2f queries/second\n", total_thread_qps);

  free(params);
  free(threads);
  free(thread_qps);
  pthread_mutex_destroy(&mutex);
  return 0;
}