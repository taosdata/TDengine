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

int main(void) {
  const char* lib_path = "../../debug/build/lib/libtaos.so";
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* pass = "taosdata";
  const char* db = "test";
  const char* sql = "SELECT last(ts,r32) FROM test.d1;";
  int         thread_count = 32;
  int         queries_per_thread = 10000;
  int         max_query_nums = 10;

  if (load_taos_functions(lib_path) != 0) {
    fprintf(stderr, "Failed to load TDengine functions\n");
    return 1;
  }

  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

  ThreadParam* params = malloc(thread_count * sizeof(ThreadParam));
  pthread_t*   threads = malloc(thread_count * sizeof(pthread_t));
  double*      thread_qps = malloc(thread_count * sizeof(double));

  printf("Starting %d threads, each executing %d queries...\n", thread_count, queries_per_thread);

  gettimeofday(&global_start_time, NULL);

  for (int i = 0; i < thread_count; i++) {
    TAOS* taos = taos_connect_func(host, user, pass, db, 0);
    if (taos == NULL) {
      fprintf(stderr, "Failed to connect to TDengine\n");
      return 1;
    }
    params[i].thread_id = i;
    params[i].taos = taos;
    params[i].sql = (const char*)sql;
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

  printf("\nTotal QPS (sum of all threads): %.2f queries/second\n", total_thread_qps);

  free(params);
  free(threads);
  free(thread_qps);
  pthread_mutex_destroy(&mutex);
  return 0;
}